"""MongoDB actor against a real TLS-only, authenticated replica set.

Opt in with MONGODB_TEST_PORT, MONGODB_TEST_CA and MONGODB_TEST_ROOT_PASSWORD (root user
`root` in admin); MONGODB_TEST_WRONG_CA and MONGODB_TEST_IPV6=1 add negative TLS checks.
The replica set member must be advertised as localhost:<port>. See deploy/connect/README.md,
"MongoDB".
"""
import base64
import datetime
import decimal
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import unittest

ACTOR = Path(__file__).parent/'source_mongodb.py'
PORT = os.environ.get('MONGODB_TEST_PORT')
CA = os.environ.get('MONGODB_TEST_CA')
WRONG_CA = os.environ.get('MONGODB_TEST_WRONG_CA')
ROOT = os.environ.get('MONGODB_TEST_ROOT_PASSWORD')
DATABASE = 'meridian_connect_test'
T0 = datetime.datetime(2026, 11, 22, 10, 0, 0, 123000, tzinfo=datetime.timezone.utc)


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


@unittest.skipUnless(PORT and CA and ROOT, 'set MONGODB_TEST_PORT, MONGODB_TEST_CA and MONGODB_TEST_ROOT_PASSWORD')
class MongoDBLive(unittest.TestCase):
    @classmethod
    def admin(cls):
        from pymongo import MongoClient
        return MongoClient('localhost', int(PORT), username='root', password=ROOT, authSource='admin',
                           tls=True, tlsCAFile=CA, replicaSet='rs0', tz_aware=True, serverSelectionTimeoutMS=20000)

    @classmethod
    def setUpClass(cls):
        import bson
        cls.password = secrets.token_hex(16)
        cls.mongo = cls.admin()
        cls.mongo.drop_database(DATABASE)
        db = cls.mongo[DATABASE]
        if db.command('usersInfo', 'reader')['users']: db.command('dropUser', 'reader')
        db.command('createUser', 'reader', pwd=cls.password, roles=[{'role': 'read', 'db': DATABASE}])
        cls.ids = [bson.ObjectId() for _ in range(5)]
        same = T0
        db.orders.insert_many([
            {'_id': cls.ids[0], 'updated': same, 'amount': bson.Decimal128('12345678901234567890.0123456789'),
             'big': bson.Int64(2**53+1), 'name': 'café ✓', 'tags': ['a', 'b'], 'nested': {'n': 1}},
            {'_id': cls.ids[1], 'updated': same, 'name': 'tie'},
            {'_id': cls.ids[2], 'updated': same, 'name': 'tie too'},
            {'_id': cls.ids[3], 'updated': T0+datetime.timedelta(milliseconds=1), 'name': 'later'},
            {'_id': cls.ids[4], 'updated': T0+datetime.timedelta(seconds=1), 'name': 'last'},
        ])
        db.counters.insert_many([{'_id': i, 'seq': bson.Int64(i*10) if i % 2 else i*10} for i in range(1, 7)])
        db.create_collection('bad-name')
        db['bad-name'].insert_one({'x': 1})
        db.blobs.insert_many([{'_id': 1, 'at': T0, 'body': b'x'}, {'_id': 2, 'at': T0+datetime.timedelta(seconds=1), 'body': b'A'*600000}])
        db.strays.insert_many([{'_id': 1, 'at': T0}, {'_id': 2, 'at': T0}])
        cls.config = {'hosts': 'localhost', 'port': int(PORT), 'database': DATABASE, 'user': 'reader',
                      'password': cls.password, 'auth_source': DATABASE, 'replica_set': 'rs0', 'ssl_ca': CA}

    @classmethod
    def tearDownClass(cls):
        cls.mongo.drop_database(DATABASE)
        cls.mongo.close()

    def stream(self, name, mode='incremental', cursor='updated'):
        messages, code = run_actor('discover', self.config)
        self.assertEqual(code, 0, messages[-1:])
        found = next(s for s in messages[-1]['catalog']['streams'] if s['name'] == name)
        return {'namespace': DATABASE, 'name': name, 'primary_key': '_id', 'primary_keys': ['_id'],
                'cursor_field': cursor if mode == 'incremental' else '_id', 'sync_mode': mode, 'schema': found['json_schema']}

    def read(self, stream, state=None):
        messages, code = run_actor('read', self.config, stream, state or {})
        self.assertEqual(code, 0, messages[-1:])
        return messages

    def test_check_and_discovery(self):
        messages, code = run_actor('check', self.config)
        self.assertEqual(code, 0, messages)
        messages, code = run_actor('discover', self.config)
        self.assertEqual(code, 0)
        names = {s['name'] for s in messages[-1]['catalog']['streams']}
        self.assertEqual(names, {'orders', 'counters', 'blobs', 'strays'})
        skipped = [m for m in messages if m['type'] == 'LOG' and m['log']['code'] == 'collections_skipped']
        self.assertEqual(skipped[0]['log']['detail'], [{'unsupported_names': '1'}])
        orders = next(s for s in messages[-1]['catalog']['streams'] if s['name'] == 'orders')
        self.assertEqual(orders['json_schema']['properties']['updated']['source_type'], 'mongodb:date')
        self.assertEqual(orders['json_schema']['properties']['_id'], {'source_type': 'mongodb:objectId', 'nullable': False})

    def test_tls_and_credentials_fail_closed(self):
        if WRONG_CA:
            messages, code = run_actor('check', {**self.config, 'ssl_ca': WRONG_CA})
            self.assertNotEqual(code, 0); self.assertEqual(trace(messages), 'auth_failed')
        messages, code = run_actor('check', {**self.config, 'password': 'wrong-'+secrets.token_hex(4)})
        self.assertNotEqual(code, 0); self.assertEqual(trace(messages), 'auth_failed')
        if os.environ.get('MONGODB_TEST_IPV6') == '1':
            # The certificate names localhost and 127.0.0.1, not ::1. A direct connection keeps
            # replica-set discovery from switching to the advertised localhost member.
            config = {**self.config, 'hosts': '[::1]', 'direct_connection': True}
            config.pop('replica_set')
            messages, code = run_actor('check', config)
            self.assertNotEqual(code, 0); self.assertEqual(trace(messages), 'auth_failed')

    def test_incremental_values_ties_and_resume(self):
        stream = self.stream('orders')
        messages = self.read(stream)
        got = records(messages)
        self.assertEqual([r['primary_key'] for r in got], [{'$oid': str(i)} for i in self.ids])
        first = got[0]['data']
        self.assertEqual(first['updated'], {'$date': '2026-11-22T10:00:00.123Z'})
        self.assertEqual(decimal.Decimal(first['amount']['$numberDecimal']), decimal.Decimal('12345678901234567890.0123456789'))
        self.assertEqual(first['big'], {'$numberLong': str(2**53+1)})
        self.assertEqual(first['name'], 'café ✓')
        self.assertEqual(first['tags'], ['a', 'b'])
        self.assertTrue(states(messages)[-1]['completed'])
        middle = states(messages)[1]
        self.assertEqual(middle['primary_key'], {'$oid': str(self.ids[1])})
        again = records(self.read(stream, middle))
        self.assertEqual([r['primary_key'] for r in again], [{'$oid': str(i)} for i in self.ids[2:]])
        final = states(messages)[-1]
        self.assertEqual(records(self.read(stream, final)), [])
        import bson
        late = bson.ObjectId()
        self.mongo[DATABASE].orders.insert_one({'_id': late, 'updated': T0+datetime.timedelta(seconds=2), 'name': 'new'})
        try:
            self.assertEqual([r['primary_key'] for r in records(self.read(stream, final))], [{'$oid': str(late)}])
        finally:
            self.mongo[DATABASE].orders.delete_one({'_id': late})

    def test_numeric_cursor_across_int_and_long(self):
        stream = self.stream('counters', cursor='seq')
        got = records(self.read(stream))
        self.assertEqual([r['cursor'] for r in got], [10, 20, 30, 40, 50, 60])
        # Resume after the third document (seq 30): an int and two longs follow.
        again = records(self.read(stream, states(self.read(stream))[2]))
        self.assertEqual([r['primary_key'] for r in again], [4, 5, 6])
        self.assertEqual([r['cursor'] for r in again], [40, 50, 60])

    def test_full_refresh_and_single_document_pages(self):
        messages = self.read(self.stream('orders', mode='full_refresh'))
        self.assertEqual(len(records(messages)), 5)
        import source_mongodb as mongodb
        mongo = mongodb.client(self.config)
        try:
            for name, mode, cursor, expected in (('orders', 'incremental', 'updated', 5), ('counters', 'full_refresh', '_id', 6)):
                got = [m for m in mongodb.read(mongo, self.stream(name, mode=mode, cursor=cursor), {}, self.config, batch_size=1) if m['type'] == 'RECORD']
                self.assertEqual(len(got), expected, name)
                self.assertEqual(len({json.dumps(r['record']['primary_key']) for r in got}), expected, name)
        finally:
            mongo.close()

    def test_documents_that_would_be_skipped_fail_instead(self):
        stream = self.stream('strays', cursor='at')
        self.mongo[DATABASE].strays.insert_one({'_id': 3, 'note': 'no cursor'})
        self.mongo[DATABASE].strays.insert_one({'_id': 4, 'at': 'not a date'})
        try:
            messages, code = run_actor('read', self.config, stream, {})
            self.assertNotEqual(code, 0); self.assertEqual(trace(messages), 'schema_drift')
            self.assertEqual(records(messages), [])
        finally:
            self.mongo[DATABASE].strays.delete_many({'_id': {'$in': [3, 4]}})
        self.assertEqual(len(records(self.read(stream))), 2)

    def test_oversized_document_fails(self):
        messages, code = run_actor('read', self.config, self.stream('blobs', cursor='at'), {})
        self.assertNotEqual(code, 0)
        self.assertEqual(trace(messages), 'record_too_large')
        self.assertEqual(len(records(messages)), 1)
        self.assertEqual(base64.b64decode(records(messages)[0]['data']['body']['$binary']['base64']), b'x')

    def test_reset_checkpoint_carries_into_the_next_incremental_run(self):
        # A reset runs the incremental stream as full_refresh with the same cursor field.
        incremental = self.stream('orders')
        reset = {**incremental, 'sync_mode': 'full_refresh'}
        final = states(self.read(reset))[-1]
        self.assertTrue(final['completed'])
        self.assertIsNotNone(final['cursor'])
        import bson
        late = bson.ObjectId()
        self.mongo[DATABASE].orders.insert_one({'_id': late, 'updated': T0+datetime.timedelta(seconds=3), 'name': 'after reset'})
        try:
            got = [r['primary_key'] for r in records(self.read(incremental, final))]
            self.assertIn({'$oid': str(late)}, got)
        finally:
            self.mongo[DATABASE].orders.delete_one({'_id': late})

    def test_changed_binding_requires_reset(self):
        stream = self.stream('orders')
        final = states(self.read(stream))[-1]
        messages, code = run_actor('read', {**self.config, 'user': 'someone_else'}, stream, final)
        self.assertNotEqual(code, 0)
        self.assertEqual(trace(messages), 'source_changed')


if __name__ == '__main__':
    unittest.main()
