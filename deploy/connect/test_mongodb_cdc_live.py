"""MongoDB change-stream CDC actor against a real TLS-only replica set.

Opt in like test_mongodb_live.py (MONGODB_TEST_PORT, MONGODB_TEST_CA,
MONGODB_TEST_ROOT_PASSWORD). See deploy/connect/README.md, "MongoDB change streams".
"""
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import MagicMock

import source_mongodb_cdc as cdc

ACTOR = Path(__file__).parent/'source_mongodb_cdc.py'
PORT = os.environ.get('MONGODB_TEST_PORT')
CA = os.environ.get('MONGODB_TEST_CA')
ROOT = os.environ.get('MONGODB_TEST_ROOT_PASSWORD')
DATABASE = 'meridian_cdc_test'


def run_actor(command, config, catalog=None, state=None):
    with tempfile.TemporaryDirectory() as tmp:
        args = [sys.executable, str(ACTOR), command]
        for name, value in (('config', config), ('catalog', catalog), ('state', state)):
            if value is None: continue
            path = Path(tmp)/f'{name}.json'; path.write_text(json.dumps(value)); args += ['--'+name, str(path)]
        proc = subprocess.run(args, capture_output=True, text=True, timeout=300,
                              env={**os.environ, 'PYTHONPATH': str(ACTOR.parent)})
    return [json.loads(line) for line in proc.stdout.splitlines() if line.strip()], proc.returncode


def records(messages): return [m['record'] for m in messages if m['type'] == 'RECORD']
def states(messages): return [m['state'] for m in messages if m['type'] == 'STATE']
def trace(messages): return messages[-1]['trace']['code'] if messages and messages[-1]['type'] == 'TRACE' else None
def ops(messages): return [(r['operation'], r['primary_key']) for r in records(messages)]


class Offline(unittest.TestCase):
    def test_token_time_decodes_the_leading_timestamp(self):
        import bson
        t = cdc.token_time({'_data': '82' + format(1795383313, '08x') + format(7, '08x') + '2B0229296E04'})
        self.assertEqual(t, bson.Timestamp(1795383313, 7))
        for bad in (None, {}, {'_data': 'zz'}, {'_data': '81000000010000000101'}):
            self.assertIsNone(cdc.token_time(bad))

    def test_standalone_servers_are_refused(self):
        mongo = MagicMock()
        mongo.admin.command.return_value = {'isWritablePrimary': True}
        with self.assertRaises(cdc.Stop) as stop: cdc.prerequisites(mongo)
        self.assertEqual((str(stop.exception), stop.exception.reason), ('connector_failed', 'cdc_replica_set_required'))
        mongo.admin.command.return_value = {'setName': 'rs0'}
        cdc.prerequisites(mongo)


@unittest.skipUnless(PORT and CA and ROOT, 'set MONGODB_TEST_PORT, MONGODB_TEST_CA and MONGODB_TEST_ROOT_PASSWORD')
class MongoDBChangeStreams(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pymongo import MongoClient
        cls.password = secrets.token_hex(16)
        cls.mongo = MongoClient('localhost', int(PORT), username='root', password=ROOT, authSource='admin', tls=True,
                                tlsCAFile=CA, replicaSet='rs0', tz_aware=True, serverSelectionTimeoutMS=20000)
        cls.mongo.drop_database(DATABASE)
        db = cls.mongo[DATABASE]
        # Users survive dropDatabase; an interrupted earlier run may have left this one behind.
        if db.command('usersInfo', 'reader')['users']: db.command('dropUser', 'reader')
        db.command('createUser', 'reader', pwd=cls.password, roles=[{'role': 'read', 'db': DATABASE}])
        cls.config = {'hosts': 'localhost', 'port': int(PORT), 'database': DATABASE, 'user': 'reader',
                      'password': cls.password, 'auth_source': DATABASE, 'replica_set': 'rs0', 'ssl_ca': CA}

    @classmethod
    def tearDownClass(cls):
        cls.mongo.drop_database(DATABASE)
        cls.mongo.close()

    def setUp(self):
        self.db = self.mongo[DATABASE]
        self.name = 'c'+secrets.token_hex(4)
        self.db[self.name].insert_many([{'_id': i, 'v': i} for i in (1, 2, 3)])

    def stream(self):
        messages, code = run_actor('discover', self.config)
        self.assertEqual(code, 0, messages[-1:])
        found = next(s for s in messages[-1]['catalog']['streams'] if s['name'] == self.name)
        return {'namespace': DATABASE, 'name': self.name, 'primary_key': '_id', 'primary_keys': ['_id'],
                'cursor_field': '_id', 'sync_mode': 'cdc', 'schema': found['json_schema']}

    def read(self, stream, state=None):
        messages, code = run_actor('read', self.config, stream, state or {})
        self.assertEqual(code, 0, messages[-3:])
        return messages

    def test_check_needs_the_change_stream_privileges_and_a_replica_set(self):
        messages, code = run_actor('check', self.config)
        self.assertEqual(code, 0, messages)
        self.assertEqual(messages[-1]['connectionStatus']['status'], 'SUCCEEDED')

    def test_snapshot_then_every_change_kind_then_nothing(self):
        stream = self.stream()
        first = self.read(stream)
        self.assertEqual(ops(first), [('upsert', 1), ('upsert', 2), ('upsert', 3)])
        final = states(first)[-1]
        self.assertTrue(final['completed'])
        self.assertEqual(final['cdc']['phase'], 'stream')
        self.assertIsNotNone(cdc.token_time(final['cdc']['resume']))

        coll = self.db[self.name]
        coll.insert_one({'_id': 4, 'v': 4})
        coll.update_one({'_id': 2}, {'$set': {'v': 20}})
        coll.replace_one({'_id': 3}, {'v': 30, 'replaced': True})
        coll.delete_one({'_id': 1})
        second = self.read(stream, final)
        self.assertEqual(ops(second), [('upsert', 4), ('upsert', 2), ('upsert', 3), ('delete', 1)])
        got = records(second)
        self.assertEqual(got[1]['data'], {'_id': 2, 'v': 20})
        self.assertEqual(got[2]['data'], {'_id': 3, 'v': 30, 'replaced': True})
        self.assertEqual(got[3]['data'], {'_id': 1})

        after = states(second)[-1]
        self.assertEqual(records(self.read(stream, after)), [])
        # A retry from the earlier checkpoint replays exactly the same changes.
        self.assertEqual(ops(self.read(stream, final)), ops(second))

    def test_update_superseded_by_delete_is_reported_not_landed(self):
        stream = self.stream()
        final = states(self.read(stream))[-1]
        coll = self.db[self.name]
        coll.update_one({'_id': 1}, {'$set': {'v': 10}})
        coll.delete_one({'_id': 1})
        messages = self.read(stream, final)
        self.assertEqual(ops(messages), [('delete', 1)])
        log = [m['log'] for m in messages if m['type'] == 'LOG' and m['log']['code'] == 'cdc_updates_superseded_by_delete']
        self.assertEqual(log[0]['detail'], [{'count': '1'}])

    def test_dropped_collection_stops_the_stream(self):
        stream = self.stream()
        final = states(self.read(stream))[-1]
        self.db[self.name].drop()
        messages, code = run_actor('read', self.config, stream, final)
        self.assertNotEqual(code, 0)
        self.assertEqual(trace(messages), 'source_changed')

    def test_resume_of_a_partial_snapshot_continues_after_the_landed_key(self):
        stream = self.stream()
        first = self.read(stream)
        snapshot_state = next(s for s in states(first) if s['cdc']['phase'] == 'snapshot' and s['primary_key'] == 3)
        partial = {**snapshot_state, 'primary_key': 1}
        again = self.read(stream, partial)
        self.assertEqual(ops(again)[:2], [('upsert', 2), ('upsert', 3)])


if __name__ == '__main__':
    unittest.main()
