"""Offline group safety: real actor/destination, controlled Kafka transport only."""
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import source_kafka as kafka
import destination_bronze as bronze


STREAM = {'name': 'events', 'namespace': 'kafka', 'generation': 0,
          'schema': {'x-kafka-topic': 'events'}}
CONFIG = {'bootstrap_servers': 'offline:9092', 'consumer_group': True,
          'group_id': 'meridian-test', 'max_batch_records': 1}


class Message:
    def __init__(self, partition, offset, value=b'{"id":1}'):
        self.p, self.o, self.v = partition, offset, value
    def partition(self): return self.p
    def offset(self): return self.o
    def value(self): return self.v
    def key(self): return None
    def timestamp(self): return (1, 1234)
    def error(self): return None


class Transport:
    """Broker callbacks are served by poll, as in librdkafka. Commit APIs forbidden."""
    def __init__(self, events, watermarks=None):
        self.events = list(events)
        self.watermarks = watermarks or {0: (0, 2), 1: (0, 1)}
        self.subscription = None
        self.assignments = []
        self.closed = False
    def list_topics(self, *args, **kwargs):
        return SimpleNamespace(topics={'events': SimpleNamespace(
            error=None, partitions={p: None for p in self.watermarks})})
    def get_watermark_offsets(self, tp, **kwargs): return self.watermarks[tp.partition]
    def subscribe(self, topics, **callbacks): self.subscription = (topics, callbacks)
    def assign(self, partitions):
        self.assignments.append([(p.topic, p.partition, p.offset) for p in partitions])
    def unassign(self): self.assignments.append([])
    def poll(self, timeout):
        if not self.subscription:
            raise AssertionError('group actor must subscribe before polling')
        if not self.events:
            raise AssertionError('actor did not finish or abort')
        event = self.events.pop(0)
        if isinstance(event, list):
            for callback, partitions in event:
                self.subscription[1][callback](self, [kafka.TopicPartition('events', p) for p in partitions])
            return None
        if isinstance(event, tuple):
            callback, partitions, *result = event
            self.subscription[1][callback](self, [kafka.TopicPartition('events', p) for p in partitions])
            return result[0] if result else None
        return event
    def close(self): self.closed = True
    def commit(self, *args, **kwargs): raise AssertionError('broker offsets must never be committed')
    def store_offsets(self, *args, **kwargs): raise AssertionError('broker offsets must never be stored')


class KafkaGroups(unittest.TestCase):
    def messages(self, transport, checkpoint=None, config=None, stream=None):
        with patch.object(kafka, 'consumer', return_value=transport), \
             patch.object(kafka, 'identity', return_value=('cluster', 'topic')):
            yield from kafka.read(config or CONFIG, stream or STREAM, checkpoint or {})

    def test_group_joins_and_uses_postgres_offsets_not_broker_positions(self):
        transport = Transport([('on_assign', [0, 1]), Message(0, 1), Message(1, 0)])
        messages = list(self.messages(transport, {'positions': {'0': '1'}}))
        records = [m['record']['primary_key'] for m in messages if m['type'] == 'RECORD']
        self.assertEqual(records, [[0, '1'], [1, '0']])
        self.assertEqual(transport.assignments, [[('events', 0, 1), ('events', 1, 0)]])
        self.assertEqual(messages[-2]['state']['positions'], {'0': '2', '1': '1'})
        self.assertTrue(transport.closed)

    def test_revoke_and_lost_abort_without_checkpointing_pending_records(self):
        for callback in ('on_revoke', 'on_lost'):
            with self.subTest(callback=callback):
                transport = Transport([('on_assign', [0, 1]), Message(0, 0),
                                       (callback, [0, 1]), Message(0, 1)])
                observed = []
                config = {**CONFIG, 'max_batch_records': 500}
                with self.assertRaises(kafka.Stop) as failure:
                    for message in self.messages(transport, config=config): observed.append(message)
                self.assertEqual(failure.exception.code, 'source_unavailable')
                self.assertEqual([m['type'] for m in observed], ['RECORD'])
                self.assertTrue(transport.closed)

    def test_revoke_callback_in_same_poll_as_record_discards_that_record(self):
        transport = Transport([('on_assign', [0, 1]),
                               ('on_revoke', [0, 1], Message(0, 0))])
        observed = []
        with self.assertRaises(kafka.Stop):
            for message in self.messages(transport): observed.append(message)
        self.assertEqual(observed, [])

    def test_revoke_and_reassignment_in_one_poll_preserves_original_failure(self):
        transport = Transport([('on_assign', [0, 1]),
                               [('on_revoke', [0, 1]), ('on_assign', [0])]])
        with self.assertRaises(kafka.Stop) as failure:
            list(self.messages(transport))
        self.assertEqual(failure.exception.detail, [{'reason': 'group_assignment_revoked'}])

    def test_empty_topic_still_joins_before_completing(self):
        transport = Transport([('on_assign', [0])], {0: (0, 0)})
        messages = list(self.messages(transport))
        self.assertEqual(transport.events, [])
        self.assertEqual(messages[-2]['state']['positions'], {'0': '0'})
        self.assertTrue(messages[-2]['state']['completed'])

    def test_group_join_timeout_does_not_emit_success_checkpoint(self):
        transport = Transport([], {0: (0, 0)})
        with patch.object(kafka.time, 'time', side_effect=[0, 1000]):
            with self.assertRaises(kafka.Stop) as failure:
                list(self.messages(transport))
        self.assertEqual(failure.exception.detail, [{'reason': 'group_assignment_timeout'}])

    def test_invalid_payload_dead_letter_and_offset_share_bronze_commit(self):
        transport = Transport([('on_assign', [0]), Message(0, 0, b'broken')], {0: (0, 1)})
        destination = {'sequence': 0, 'state': {}, 'run_id': 'run',
                       'lease_token': 'lease', 'stream_id': 'stream'}
        commits = []
        def post(config, payload):
            commits.append(payload)
            return {'sequence': len(commits)}
        with patch.object(bronze, 'post', side_effect=post):
            list(bronze.write(destination, self.messages(transport)))
        self.assertEqual(commits[0]['state']['state']['positions'], {'0': '1'})
        self.assertEqual(commits[0]['dead_letters'][0]['dead_letter']['position'], 'topic:0:0')

    def test_source_identity_change_prevents_group_join(self):
        transport = Transport([])
        with self.assertRaises(kafka.Stop) as failure:
            list(self.messages(transport, {'topic_id': 'previous-topic'}))
        self.assertEqual(failure.exception.code, 'source_changed')
        self.assertIsNone(transport.subscription)

    def test_partial_or_changed_assignment_never_completes_whole_topic(self):
        for events in ([('on_assign', [0])],
                       [('on_assign', [0, 1]), ('on_assign', [0, 1])]):
            with self.subTest(events=events):
                with self.assertRaises(kafka.Stop):
                    list(self.messages(Transport(events)))

    def test_failed_bronze_commit_never_moves_broker_offsets_and_resume_replays(self):
        transport = Transport([('on_assign', [0]), Message(0, 0)], {0: (0, 1)})
        destination = {'sequence': 0, 'state': {}, 'run_id': 'run',
                       'lease_token': 'lease', 'stream_id': 'stream'}
        with patch.object(bronze, 'post', side_effect=ValueError('destination_failed')):
            with self.assertRaises(ValueError):
                list(bronze.write(destination, self.messages(transport)))
        retry = Transport([('on_assign', [0]), Message(0, 0)], {0: (0, 1)})
        records = [m for m in self.messages(retry) if m['type'] == 'RECORD']
        self.assertEqual(records[0]['record']['primary_key'], [0, '0'])

    def test_invalid_group_config_and_partition_shards_fail_closed(self):
        for config, stream in (({**CONFIG, 'group_id': ''}, STREAM),
                               ({**CONFIG, 'consumer_group': 'yes'}, STREAM),
                               (CONFIG, {**STREAM, 'shard': '0'})):
            with self.subTest(config=config, stream=stream):
                with self.assertRaises(ValueError):
                    list(self.messages(Transport([]), config=config, stream=stream))

    def test_group_planning_and_spark_reject_explicit_partition_execution(self):
        with patch.object(kafka, 'consumer', return_value=Transport([])):
            with self.assertRaises(ValueError): kafka.plan(CONFIG, STREAM)
        with self.assertRaises(ValueError): kafka.portable(CONFIG)
        with self.assertRaises(ValueError): list(kafka.chunk(CONFIG, STREAM, 0, 0, 1, {}))

    def test_group_configuration_pins_eager_rebalance_and_disables_offset_writes(self):
        captured = []
        with patch.object(kafka, 'Consumer', side_effect=lambda config: captured.append(config)):
            kafka.consumer(CONFIG)
        self.assertEqual(captured[0].get('partition.assignment.strategy'), 'range')
        self.assertFalse(captured[0]['enable.auto.commit'])
        self.assertFalse(captured[0]['enable.auto.offset.store'])


if __name__ == '__main__': unittest.main()
