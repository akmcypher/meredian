"""Shared group ownership and fenced task dispatch without brokers or a control plane."""
import json
import os
from unittest.mock import patch
from pathlib import Path
import queue
import tempfile
import threading
import time
import unittest

from confluent_kafka import TopicPartition


class FakeConsumer:
    def __init__(self, partitions=(0,)):
        self.parts = [TopicPartition('topic', p) for p in partitions]
        self.events = queue.Queue()
        self.calls = []
        self.polls = 0
        self.polled = threading.Event()
        self.closed = False
        self.subscribed = False
    def subscribe(self, topics, **callbacks):
        self.callbacks = callbacks
        self.calls.append(('subscribe', topics))
        self.subscribed = True
        self.events.put(('assign', self.parts))
    def assign(self, parts): self.calls.append(('assign', [(p.topic, p.partition) for p in parts]))
    def pause(self, parts): self.calls.append(('pause', [(p.topic, p.partition) for p in parts]))
    def unassign(self): self.calls.append(('unassign',))
    def unsubscribe(self): self.calls.append(('unsubscribe',))
    def poll(self, timeout):
        self.polls += 1
        if self.polls >= 5: self.polled.set()
        try:
            kind, parts = self.events.get(timeout=min(timeout, .01))
            self.callbacks['on_'+kind](self, parts)
        except queue.Empty: pass
        return None
    def close(self): self.closed = True


class SharedGroupContracts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = {'bootstrap_servers': '127.0.0.1:1', 'consumer_group': True,
                       'group_id': 'group', 'private_worker_value': 'credential'}
        Path(self.tmp.name, 'broker.json').write_text(json.dumps(self.config))
        self.consumer = FakeConsumer()
        self.pending = True
        self.claims = []
        self.requests = []
        self.executed = []
        self.errors = []
    def claim(self, shard='0', token='lease'):
        return {'run_id': 'run', 'lease_token': token, 'task': True, 'operation': 'sync',
                'source_version': 'kafka-v1', 'credential_ref': 'broker',
                'streams': [{'id': 'stream', 'shard': shard, 'schema': {'x-kafka-topic': 'topic'}}]}
    def request(self, body):
        self.requests.append(body)
        if body['action'] == 'group_context':
            return {'context': {'id': 'connection', 'credential_ref': 'broker',
                                'source_version': 'kafka-v1', 'pending': self.pending,
                                'streams': [{'id': 'stream', 'topic': 'topic'}]}}
        if body['action'] == 'claim_group_task':
            return {'claim': self.claims.pop(0) if self.claims else None}
        if body['action'] == 'abandon_task':
            self.pending = False
            return {'ok': True}
        self.fail('unexpected request '+body['action'])
    def runtime(self, execute, **extra):
        from kafka_shared import SharedGroup
        return SharedGroup(self.tmp.name, self.request, execute, 'worker', once=True,
                           consumer_factory=lambda config: self.consumer,
                           poll_interval=.01, context_interval=.01, join_timeout=2,
                           **extra)
    def start(self, runtime):
        def run():
            try: runtime.run()
            except Exception as error: self.errors.append(error)
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        return thread
    def finish(self, thread):
        thread.join(5)
        self.assertFalse(thread.is_alive(), 'coordinator did not drain/stop')
        self.assertEqual(self.errors, [])
        self.assertTrue(self.consumer.closed)

    def test_partial_eager_assignment_is_paused_and_loss_clears_it(self):
        from kafka_shared import Membership
        changes = []
        member = Membership(changes.append)
        member.assign(self.consumer, [TopicPartition('topic', 1)])
        self.assertEqual(member.snapshot(), {('topic', 1)})
        self.assertIn(('pause', [('topic', 1)]), self.consumer.calls)
        member.lost(self.consumer, [TopicPartition('topic', 1)])
        self.assertEqual(member.snapshot(), set())
        self.assertEqual(changes[-1], set())

    def test_revocation_cancels_actor_and_fences_its_database_lease(self):
        self.claims = [self.claim()]
        started = threading.Event()
        observed = []
        def execute(claim, cancel=None, config_override=None):
            observed.append((cancel, config_override))
            child = json.loads(Path(config_override).read_text())
            self.assertFalse(child['consumer_group'])
            self.assertEqual(child['private_worker_value'], 'credential')
            started.set()
            self.assertTrue(cancel.wait(3))
        thread = self.start(self.runtime(execute))
        self.assertTrue(started.wait(3))
        self.consumer.events.put(('revoke', self.consumer.parts))
        self.finish(thread)
        self.assertTrue(observed[0][0].is_set())
        self.assertFalse(Path(observed[0][1]).exists())
        self.assertTrue(any(r['action'] == 'abandon_task' and r['token'] == 'lease' for r in self.requests))
        self.assertNotIn('credential', json.dumps(self.requests))

    def test_claim_returned_after_assignment_changed_is_released_without_reading(self):
        original = self.request
        def request(body):
            if body['action'] == 'claim_group_task':
                self.consumer.events.put(('revoke', self.consumer.parts))
                # Wait until callback processed the revoke, before returning stale work.
                deadline = time.monotonic()+2
                while not any(c[0] == 'unassign' for c in self.consumer.calls):
                    if time.monotonic() > deadline: self.fail('revoke callback never arrived')
                    time.sleep(.001)
                return {'claim': self.claim()}
            return original(body)
        runtime = self.runtime(lambda *a, **k: self.executed.append(a))
        runtime.request = request
        thread = self.start(runtime)
        self.finish(thread)
        self.assertEqual(self.executed, [])
        self.assertTrue(any(r['action'] == 'abandon_task' for r in self.requests))

    def test_slots_are_bounded_while_the_consumer_polls_during_blocked_work(self):
        self.consumer = FakeConsumer((0, 1))
        self.claims = [self.claim('0', 'first'), self.claim('1', 'second')]
        active = 0
        maximum = 0
        def execute(claim, **_):
            nonlocal active, maximum
            active += 1; maximum = max(maximum, active)
            self.assertTrue(self.consumer.polled.wait(3))
            self.executed.append(claim['lease_token'])
            active -= 1
            if len(self.executed) == 2: self.pending = False
        thread = self.start(self.runtime(execute, slots=1))
        self.finish(thread)
        self.assertEqual(self.executed, ['first', 'second'])
        self.assertEqual(maximum, 1)
        self.assertGreaterEqual(self.consumer.polls, 5)

    def test_pool_connection_selector_is_carried_on_metadata_and_claim_requests(self):
        self.claims = [self.claim()]
        def execute(*args, **kwargs): self.pending = False
        thread = self.start(self.runtime(execute, connection_id='connection', identity={'agent': 'node', 'slots': 1, 'version': 'shared'}))
        self.finish(thread)
        for request in self.requests:
            if request['action'] in ('group_context', 'claim_group_task'):
                self.assertEqual(request['connectionId'], 'connection')
        claim = next(r for r in self.requests if r['action'] == 'claim_group_task')
        self.assertEqual(claim['assignments'], {'stream': [0]})
        self.assertEqual(claim['agent'], 'node')

    def test_handoff_http_wait_does_not_block_the_membership_poll_thread(self):
        original = self.request
        def request(body):
            if body['action'] == 'claim_group_task':
                self.consumer.events.put(('revoke', self.consumer.parts))
                deadline = time.monotonic()+2
                while not any(c[0] == 'unassign' for c in self.consumer.calls):
                    if time.monotonic() > deadline: self.fail('revoke callback never arrived')
                    time.sleep(.001)
                return {'claim': self.claim()}
            if body['action'] == 'abandon_task':
                before = self.consumer.polls
                deadline = time.monotonic()+1
                while self.consumer.polls < before+3 and time.monotonic() < deadline:
                    time.sleep(.001)
                self.assertGreaterEqual(self.consumer.polls, before+3, 'control-plane handoff blocked group polling')
            return original(body)
        runtime = self.runtime(lambda *a, **k: None)
        runtime.request = request
        thread = self.start(runtime)
        self.finish(thread)

    def test_shared_runtime_requires_explicit_valid_group_config(self):
        self.config['consumer_group'] = False
        Path(self.tmp.name, 'broker.json').write_text(json.dumps(self.config))
        with self.assertRaisesRegex(ValueError, 'connector_failed'):
            self.runtime(lambda *a, **k: None).run()

    def test_partially_read_legacy_whole_topic_task_requires_a_new_run(self):
        runtime = self.runtime(lambda *a, **k: self.fail('legacy task executed'))
        runtime.membership.owned = {('topic', 0)}
        with self.assertRaisesRegex(ValueError, 'source_changed'):
            runtime._dispatch(self.claim(shard=''))
        self.assertTrue(any(r['action'] == 'abandon_task' for r in self.requests))

    def test_relative_secret_directory_preserves_absolute_tls_paths_for_children(self):
        from kafka_shared import SharedGroup
        self.config['ssl_ca_location'] = 'ca.pem'
        Path(self.tmp.name, 'broker.json').write_text(json.dumps(self.config))
        Path(self.tmp.name, 'ca.pem').write_text('test-ca')
        self.claims = [self.claim()]
        def execute(claim, cancel=None, config_override=None):
            child = json.loads(Path(config_override).read_text())
            self.assertEqual(child['ssl_ca_location'], str(Path(self.tmp.name, 'ca.pem')))
            self.pending = False
        previous = Path.cwd()
        try:
            os.chdir(Path(self.tmp.name).parent)
            runtime = SharedGroup(Path(self.tmp.name).name, self.request, execute, 'worker',
                once=True, consumer_factory=lambda config: self.consumer,
                poll_interval=.01, context_interval=.01)
            self.finish(self.start(runtime))
        finally:
            os.chdir(previous)

    def test_empty_topics_unsubscribe_without_invalid_subscribe(self):
        runtime = self.runtime(lambda *a, **k: None)
        runtime.consumer = self.consumer
        runtime.desired_topics = ('topic',)
        thread = threading.Thread(target=runtime._poll)
        thread.start()
        try:
            self.assertTrue(self.consumer.polled.wait(2))
            with runtime.membership.lock:
                runtime.desired_topics = ()
            deadline = time.monotonic()+2
            while ('unsubscribe',) not in self.consumer.calls and time.monotonic()<deadline:
                time.sleep(.001)
            self.assertIn(('unsubscribe',), self.consumer.calls)
            self.assertNotIn(('subscribe', []), self.consumer.calls)
        finally:
            runtime.halt.set()
            thread.join(2)


class AgentSharedHooks(unittest.TestCase):
    def test_rebalance_cancellation_does_not_report_a_source_failure(self):
        os.environ.setdefault('MERIDIAN_URL', 'http://127.0.0.1:9')
        os.environ.setdefault('INGEST_TOKEN_FILE', os.devnull)
        import agent
        cancelled = threading.Event()
        calls = []
        claim = {'run_id': 'run', 'lease_token': 'lease', 'operation': 'sync',
                 'credential_ref': 'broker', 'source_version': 'kafka-v1', 'task': True,
                 'streams': [{'id': 'stream', 'sequence': 0, 'state': {}, 'completed': False}]}
        def blocked_write(*args):
            cancelled.set()
            raise ValueError('timeout')
        with patch.object(agent, 'source', return_value=iter(())), patch.object(agent, 'write', side_effect=blocked_write), patch.object(agent, 'post', side_effect=lambda base, payload: calls.append(payload)):
            agent.execute(claim, cancel=cancelled, config_override='private-local-config.json')
        self.assertFalse(any(p['action'] == 'finish' for p in calls))

    def test_opt_in_selects_shared_runtime_without_changing_the_default_worker(self):
        os.environ.setdefault('MERIDIAN_URL', 'http://127.0.0.1:9')
        os.environ.setdefault('INGEST_TOKEN_FILE', os.devnull)
        import agent
        with patch.dict(os.environ, {'CONNECT_KAFKA_SHARED': '1', 'CONNECT_ONCE': '1', 'CONNECT_SLOTS': '2', 'CONNECT_GROUP_CONNECTION_ID': 'connection'}), patch('kafka_shared.SharedGroup') as runtime:
            agent.main()
            runtime.return_value.run.assert_called_once()
            self.assertEqual(runtime.call_args.kwargs['slots'], 2)
            self.assertEqual(runtime.call_args.kwargs['connection_id'], 'connection')


if __name__ == '__main__': unittest.main()
