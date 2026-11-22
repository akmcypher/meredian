"""Opt-in real-broker group tests; never starts, stops or restarts infrastructure.

Run with KAFKA_CERTIFY_BROKERS set to the existing three-broker certification
cluster. Supplied actor checkpoints model committed Postgres state; these tests
do not claim to exercise the fenced Postgres/Bronze transaction itself.
"""
import json
import os
import threading
import time
import unittest
import uuid

from confluent_kafka import Consumer, TopicPartition, OFFSET_INVALID
import source_kafka as kafka
from test_kafka import KafkaCluster, records, run_actor, states


BROKERS = os.environ.get('KAFKA_CERTIFY_BROKERS')


@unittest.skipUnless(BROKERS, 'set KAFKA_CERTIFY_BROKERS to opt in to live group tests')
class KafkaGroupsLive(KafkaCluster):
    config = {'bootstrap_servers': BROKERS}

    def group_config(self, **overrides):
        return {**self.config, 'consumer_group': True,
                'group_id': 'meridian-groups-live-' + uuid.uuid4().hex,
                'max_run_seconds': 60, **overrides}

    def broker_consumer(self, config):
        return Consumer({'bootstrap.servers': BROKERS,
                         'group.id': config['group_id'],
                         'enable.auto.commit': False,
                         'enable.auto.offset.store': False,
                         'partition.assignment.strategy': 'range',
                         'auto.offset.reset': 'earliest'})

    def committed(self, config, topic, partitions):
        consumer = self.broker_consumer(config)
        try:
            return {p.partition: p.offset for p in consumer.committed(
                [TopicPartition(topic, p) for p in range(partitions)], timeout=20)}
        finally:
            consumer.close()

    def produce_partitioned(self, topic, partitions, count):
        self.produce(topic, [{'partition': p,
                             'value': json.dumps({'partition': p, 'n': n}).encode()}
                            for p in range(partitions) for n in range(count)])

    def read_group(self, config, stream, checkpoint=None):
        messages, code = run_actor('read', config, stream, checkpoint or {})
        self.assertEqual(code, 0, messages[-3:])
        self.assertTrue(states(messages)[-1]['completed'])
        return messages

    def test_advanced_broker_offsets_are_overridden_by_committed_checkpoint(self):
        topic = self.topic(2)
        self.produce_partitioned(topic, 2, 6)
        stream = self.stream(topic)
        config = self.group_config()
        seed = self.broker_consumer(config)
        try:
            seed.commit(offsets=[TopicPartition(topic, 0, 6),
                                 TopicPartition(topic, 1, 6)], asynchronous=False)
        finally:
            seed.close()
        self.assertEqual(self.committed(config, topic, 2), {0: 6, 1: 6})

        messages = self.read_group(config, stream,
                                   {'version': 1, 'positions': {'0': '2', '1': '4'}})
        keys = {tuple(r['primary_key']) for r in records(messages)}
        self.assertEqual(keys, {(0, '2'), (0, '3'), (0, '4'), (0, '5'),
                                (1, '4'), (1, '5')})
        self.assertEqual(states(messages)[-1]['positions'], {'0': '6', '1': '6'})
        self.assertEqual(self.committed(config, topic, 2), {0: 6, 1: 6},
                         'Meridian must not change pre-existing broker commits')

    def test_bounded_run_restart_uses_checkpoint_without_broker_commits(self):
        topic = self.topic(2)
        self.produce_partitioned(topic, 2, 5)
        stream = self.stream(topic)
        config = self.group_config(max_run_records=3, max_batch_records=2)
        first = self.read_group(config, stream)
        self.assertEqual(len(records(first)), 3)
        checkpoint = states(first)[-1]
        self.assertEqual(sum(int(v) for v in checkpoint['positions'].values()), 3)
        self.assertEqual(self.committed(config, topic, 2), {0: OFFSET_INVALID, 1: OFFSET_INVALID})

        second = self.read_group({**config, 'max_run_records': 100}, stream, checkpoint)
        keys = [tuple(r['primary_key']) for r in records(first) + records(second)]
        self.assertEqual(len(keys), 10)
        self.assertEqual(len(set(keys)), 10)
        self.assertEqual(set(keys), {(p, str(n)) for p in range(2) for n in range(5)})
        self.assertEqual(self.committed(config, topic, 2), {0: OFFSET_INVALID, 1: OFFSET_INVALID})

    def test_second_member_revokes_actor_and_restart_replays_uncheckpointed_rows(self):
        topic = self.topic(2)
        self.produce_partitioned(topic, 2, 200)
        stream = self.stream(topic)
        config = self.group_config(max_batch_records=1000)
        actor = kafka.read(config, stream, {})
        rival = self.broker_consumer(config)
        stop = threading.Event()
        started = threading.Event()
        errors = []
        observed = []

        def compete():
            try:
                rival.subscribe([topic])
                started.set()
                while not stop.is_set():
                    message = rival.poll(0.1)
                    if message is not None and message.error():
                        errors.append(str(message.error()))
                        return
            except Exception as error:
                errors.append(str(error))
            finally:
                rival.close()

        thread = None
        try:
            # A returned RECORD proves the actor has actually joined with a full assignment.
            first = next(actor)
            self.assertEqual(first['type'], 'RECORD')
            observed.append(first)
            thread = threading.Thread(target=compete, daemon=True)
            thread.start()
            self.assertTrue(started.wait(5), 'rival did not subscribe')
            with self.assertRaises(kafka.Stop) as failure:
                for message in actor:
                    observed.append(message)
                    # Keep unread offsets available while the real broker coordinates rebalance.
                    time.sleep(0.05)
            self.assertEqual(failure.exception.code, 'source_unavailable')
            self.assertIn(failure.exception.detail[0]['reason'],
                          ('group_assignment_revoked', 'group_assignment_lost'))
            self.assertFalse(any(m['type'] == 'STATE' for m in observed),
                             'uncheckpointed records must not gain a STATE after revocation')
        finally:
            actor.close()
            stop.set()
            if thread is not None:
                thread.join(20)
                self.assertFalse(thread.is_alive(), 'rival did not leave its group')
            else:
                rival.close()
        self.assertEqual(errors, [])
        self.assertEqual(self.committed(config, topic, 2), {0: OFFSET_INVALID, 1: OFFSET_INVALID})
        # No STATE was acknowledged: a fresh attempt must replay from the previous {} checkpoint.
        replay = self.read_group(config, stream, {})
        keys = {tuple(r['primary_key']) for r in records(replay)}
        self.assertEqual(len(keys), 400)
        self.assertIn(tuple(first['record']['primary_key']), keys)

    def test_existing_member_prevents_partial_topic_success(self):
        topic = self.topic(2)
        self.produce_partitioned(topic, 2, 2)
        stream = self.stream(topic)
        config = self.group_config()
        rival = self.broker_consumer(config)
        stop = threading.Event()
        assigned = threading.Event()
        errors = []

        def compete():
            try:
                rival.subscribe([topic], on_assign=lambda consumer, partitions: assigned.set())
                while not stop.is_set(): rival.poll(0.1)
            except Exception as error:
                errors.append(str(error))
            finally:
                rival.close()

        thread = threading.Thread(target=compete, daemon=True)
        thread.start()
        try:
            self.assertTrue(assigned.wait(30), 'existing member never received assignment')
            messages, code = run_actor('read', config, stream, {})
            self.assertNotEqual(code, 0)
            self.assertEqual(messages[-1]['trace']['code'], 'source_unavailable')
            self.assertEqual(messages[-1]['trace']['detail'],
                             [{'reason': 'group_assignment_incomplete'}])
            self.assertEqual(records(messages), [])
            self.assertEqual(states(messages), [])
        finally:
            stop.set()
            thread.join(20)
            self.assertFalse(thread.is_alive(), 'rival did not leave its group')
        self.assertEqual(errors, [])
        self.assertEqual(self.committed(config, topic, 2), {0: OFFSET_INVALID, 1: OFFSET_INVALID})


if __name__ == '__main__': unittest.main()
