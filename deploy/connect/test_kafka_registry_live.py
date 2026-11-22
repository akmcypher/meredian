"""Actual reference-aware registry and Kafka; no database or Docker lifecycle changes."""
import json
import os
import time
import unittest
import urllib.request
import uuid

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import source_kafka as kafka
from test_kafka_registry import ADDRESS, ORDER, wire


@unittest.skipUnless(os.environ.get('KAFKA_AVRO_REGISTRY'), 'Set KAFKA_AVRO_REGISTRY for the reference-aware local registry')
class LiveReferenceEvolution(unittest.TestCase):
    def test_actor_and_spark_chunks_resolve_real_reference_versions(self):
        topic = 'avro_refs_'+uuid.uuid4().hex[:12]
        registry = os.environ['KAFKA_AVRO_REGISTRY'].rstrip('/')
        brokers = os.environ.get('KAFKA_CERTIFY_BROKERS', '127.0.0.1:39191,127.0.0.1:39192,127.0.0.1:39193')
        admin = AdminClient({'bootstrap.servers': brokers})
        subjects = [topic+'-address', topic+'-value']
        def register(subject, schema, references=()):
            body = json.dumps({'schema': json.dumps(schema), 'references': list(references)}).encode()
            request = urllib.request.Request(registry+'/subjects/'+subject+'/versions', data=body,
                headers={'Content-Type': 'application/vnd.schemaregistry.v1+json'})
            with urllib.request.urlopen(request, timeout=15) as response:
                return json.load(response)['id']
        try:
            admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=3)])[topic].result(30)
            for _ in range(30):
                metadata = admin.list_topics(topic, timeout=10).topics.get(topic)
                if metadata and len(metadata.partitions)==3 and all(p.leader>=0 for p in metadata.partitions.values()): break
                time.sleep(.5)
            evolved = {**ADDRESS, 'fields': ADDRESS['fields']+[{'name': 'country', 'type': 'string', 'default': 'IN'}]}
            register(subjects[0], ADDRESS)
            register(subjects[0], evolved)
            first = register(subjects[1], ORDER, [{'name': 'example.Address', 'subject': subjects[0], 'version': 1}])
            second = register(subjects[1], ORDER, [{'name': 'example.Address', 'subject': subjects[0], 'version': 2}])
            self.assertNotEqual(first, second)
            producer = Producer({'bootstrap.servers': brokers, 'enable.idempotence': True, 'acks': 'all'})
            errors = []
            def delivered(error, message):
                if error: errors.append(error.code())
            for p in range(3):
                producer.produce(topic, wire(first, ORDER, {'id': p*2, 'address': {'city': 'Pune'}}, [ADDRESS]), partition=p, on_delivery=delivered)
                producer.produce(topic, wire(second, ORDER, {'id': p*2+1, 'address': {'city': 'Pune', 'country': 'IN'}}, [evolved]), partition=p, on_delivery=delivered)
            self.assertEqual(producer.flush(30), 0); self.assertEqual(errors, [])
            config = {'bootstrap_servers': brokers, 'schema_registry_url': registry, 'max_batch_records': 2}
            stream = {'name': topic, 'namespace': 'kafka', 'schema': {'x-kafka-topic': topic}}
            messages = list(kafka.read(config, stream, {}))
            records = [m['record']['data'] for m in messages if m['type']=='RECORD']
            self.assertEqual(len(records), 6)
            self.assertEqual(sum('country' in r['address'] for r in records), 3)
            chunks = []
            for p in range(3):
                reached = {}
                chunks.extend(m['record']['data'] for m, _ in kafka.chunk(config, stream, p, 0, 2, reached))
                self.assertEqual(reached['offset'], 2)
            self.assertEqual(sorted(records, key=lambda r:r['id']), sorted(chunks, key=lambda r:r['id']))
            reader = {**ORDER, 'fields': [ORDER['fields'][0], {'name': 'address', 'type': evolved}]}
            pinned = list(kafka.read(config, {**stream, 'schema': {**stream['schema'], 'x-avro-reader-schema': reader}}, {}))
            normalized = [m['record']['data'] for m in pinned if m['type']=='RECORD']
            self.assertEqual(len(normalized), 6)
            self.assertTrue(all(r['address']['country']=='IN' for r in normalized))
        finally:
            for subject in reversed(subjects):
                try:
                    with urllib.request.urlopen(urllib.request.Request(registry+'/subjects/'+subject, method='DELETE'), timeout=10): pass
                except Exception: pass
            try: admin.delete_topics([topic])[topic].result(30)
            except Exception: pass


if __name__ == '__main__': unittest.main()
