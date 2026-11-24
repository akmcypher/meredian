"""SQL Server actor against a real SQL Server database engine over verified TLS.

Opt in with SQLSERVER_TEST_PORT (and SQLSERVER_TEST_CA, SQLSERVER_TEST_WRONG_CA,
SQLSERVER_TEST_SA_PASSWORD). The local fixture is Azure SQL Edge on ARM64
(the SQL Server engine); see deploy/connect/README.md, "SQL Server". Evidence from it
is engine evidence, not SQL Server 2022 or managed-service certification.
"""
import base64
import decimal
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import unittest
import uuid

ACTOR = Path(__file__).parent/'source_sqlserver.py'
PORT = os.environ.get('SQLSERVER_TEST_PORT')
CA = os.environ.get('SQLSERVER_TEST_CA')
WRONG_CA = os.environ.get('SQLSERVER_TEST_WRONG_CA')
SA = os.environ.get('SQLSERVER_TEST_SA_PASSWORD')
DATABASE = 'meridian_connect_test'


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


@unittest.skipUnless(PORT and CA and SA, 'set SQLSERVER_TEST_PORT, SQLSERVER_TEST_CA and SQLSERVER_TEST_SA_PASSWORD')
class SQLServerLive(unittest.TestCase):
    @classmethod
    def admin(cls, database='master'):
        import pytds
        return pytds.connect(dsn='localhost', port=int(PORT), database=database, user='sa', password=SA,
                             cafile=CA, validate_host=True, enc_login_only=False, autocommit=True,
                             login_timeout=30, as_dict=True)

    @classmethod
    def setUpClass(cls):
        cls.password = 'R'+secrets.token_hex(12)+'a!9'
        with cls.admin() as conn, conn.cursor() as c:
            c.execute(f"IF DB_ID('{DATABASE}') IS NOT NULL BEGIN ALTER DATABASE [{DATABASE}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{DATABASE}] END")
            c.execute(f'CREATE DATABASE [{DATABASE}]')
            c.execute(f'ALTER DATABASE [{DATABASE}] SET ALLOW_SNAPSHOT_ISOLATION ON')
            c.execute("IF SUSER_ID('meridian_reader') IS NOT NULL DROP LOGIN meridian_reader")
            c.execute(f"CREATE LOGIN meridian_reader WITH PASSWORD='{cls.password}', CHECK_POLICY=OFF")
        with cls.admin(DATABASE) as conn, conn.cursor() as c:
            c.execute('CREATE USER meridian_reader FOR LOGIN meridian_reader')
            c.execute('ALTER ROLE db_datareader ADD MEMBER meridian_reader')
            c.execute("""CREATE TABLE dbo.orders (
                tenant int NOT NULL, id bigint NOT NULL, updated datetime2(7) NOT NULL,
                amount decimal(38,10) NULL, big bigint NULL, price money NULL, ratio float NULL,
                flag bit NULL, guid uniqueidentifier NULL, payload varbinary(64) NULL,
                label nvarchar(40) NULL, note varchar(max) NULL, at_time time(7) NULL,
                at_offset datetimeoffset(7) NULL, born date NULL, doc xml NULL,
                CONSTRAINT pk_orders PRIMARY KEY (tenant, id))""")
            c.execute('CREATE TABLE dbo.blobs (id int NOT NULL PRIMARY KEY, changed datetime2(7) NOT NULL, body varbinary(max) NULL)')
            c.execute('CREATE TABLE dbo.versions (id varbinary(16) NOT NULL PRIMARY KEY, rv rowversion NOT NULL, name nvarchar(20) NULL)')
            c.execute('CREATE TABLE dbo.unsupported (id int NOT NULL PRIMARY KEY, changed datetime2 NOT NULL, place geography NULL)')
            c.execute('CREATE TABLE dbo.nokey (id int NOT NULL, changed datetime2 NOT NULL)')
            # A SQL_* collation orders varchar differently from nvarchar ('co-op' sorts first
            # as varchar, after 'con' as Unicode).
            c.execute("""CREATE TABLE dbo.codes (code varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL PRIMARY KEY,
                changed datetime2 NOT NULL)""")
            c.execute("INSERT dbo.codes VALUES ('co-op','2026-11-22'),('con','2026-11-22'),('coop','2026-11-22'),('cop','2026-11-22')")
        cls.config = {'host': 'localhost', 'port': int(PORT), 'database': DATABASE,
                      'user': 'meridian_reader', 'password': cls.password, 'ssl_ca': CA}
        cls.seed()

    @classmethod
    def seed(cls):
        with cls.admin(DATABASE) as conn, conn.cursor() as c:
            # Three rows share one datetime2 tick: the cursor tie must be broken by the key.
            for tenant, ident, updated in [(1, 1, '2026-11-22T10:00:00.1234567'), (1, 2, '2026-11-22T10:00:00.1234567'),
                                           (2, 1, '2026-11-22T10:00:00.1234567'), (1, 3, '2026-11-22T10:00:00.1234568'),
                                           (1, 9007199254740993, '2026-11-22T10:00:01.0000001')]:
                c.execute("""INSERT dbo.orders VALUES (%s,%s,%s,CAST('12345678901234567890123456.0123456789' AS decimal(38,10)),
                    9007199254740993, 922337203685477.5807, 0.1, 1, '6F9619FF-8B86-D011-B42D-00C04FC964FF',
                    0x00FF10, N'café ✓', 'x', '23:59:59.9999999', '2026-11-22T10:00:00.1234567+05:30',
                    '2026-11-22', '<a b="1"/>')""", (tenant, ident, updated))
            c.execute("INSERT dbo.blobs VALUES (1,'2026-11-22T00:00:00',0x01), (2,'2026-11-22T00:00:01',CAST(REPLICATE(CAST(0x41 AS varbinary(max)),600000) AS varbinary(max)))")
            c.execute("INSERT dbo.versions (id,name) VALUES (0x0001,'a'),(0x00FF,'b'),(0x0100,'c')")

    def stream(self, name, mode='incremental', cursor='updated'):
        messages, code = run_actor('discover', self.config)
        self.assertEqual(code, 0, messages[-1:])
        found = next(s for s in messages[-1]['catalog']['streams'] if s['name'] == name)
        keys = found['primary_key'] if isinstance(found['primary_key'], list) else [found['primary_key']]
        return {'namespace': 'dbo', 'name': name, 'primary_keys': keys, 'primary_key': ','.join(keys),
                'cursor_field': cursor, 'sync_mode': mode, 'schema': found['json_schema']}

    def read(self, stream, state=None):
        messages, code = run_actor('read', self.config, stream, state or {})
        self.assertEqual(code, 0, messages[-1:])
        return messages

    def test_check_and_discovery_over_verified_tls(self):
        messages, code = run_actor('check', self.config)
        self.assertEqual(code, 0, messages)
        self.assertEqual(messages[-1]['connectionStatus']['status'], 'SUCCEEDED')
        messages, _ = run_actor('discover', self.config)
        streams = {s['name']: s for s in messages[-1]['catalog']['streams']}
        # Tables without a primary key are not offered.
        self.assertEqual(set(streams), {'orders', 'blobs', 'versions', 'unsupported', 'codes'})
        self.assertEqual(streams['orders']['primary_key'], ['tenant', 'id'])
        props = streams['orders']['json_schema']['properties']
        self.assertEqual(props['updated']['source_type'], 'datetime2(7)')
        self.assertEqual(props['amount']['source_type'], 'decimal(38,10)')
        self.assertEqual(props['label']['source_type'], 'nvarchar(40)')
        self.assertEqual(props['note']['source_type'], 'varchar(max)')

    def test_wrong_ca_and_wrong_password_are_auth_failures(self):
        if WRONG_CA:
            messages, code = run_actor('check', {**self.config, 'ssl_ca': WRONG_CA})
            self.assertNotEqual(code, 0); self.assertEqual(trace(messages), 'auth_failed')
        messages, code = run_actor('check', {**self.config, 'password': 'wrong-'+secrets.token_hex(4)})
        self.assertNotEqual(code, 0); self.assertEqual(trace(messages), 'auth_failed')

    def test_hostname_must_match_the_certificate(self):
        # The fixture certificate lists DNS:localhost and IP:127.0.0.1 only, and the server
        # also listens on [::1]. The same server must be accepted through its IP SAN and
        # refused through a loopback address the certificate does not name.
        messages, code = run_actor('check', {**self.config, 'host': '127.0.0.1'})
        self.assertEqual(code, 0, messages[-1:])
        if os.environ.get('SQLSERVER_TEST_IPV6') == '1':
            messages, code = run_actor('check', {**self.config, 'host': '::1'})
            self.assertNotEqual(code, 0)
            self.assertEqual(trace(messages), 'auth_failed')

    def test_incremental_read_types_ties_and_resume(self):
        stream = self.stream('orders')
        messages = self.read(stream)
        got = records(messages)
        self.assertEqual([r['primary_key'] for r in got],
                         [[1, 1], [1, 2], [2, 1], [1, 3], [1, '9007199254740993']])
        first = got[0]['data']
        self.assertEqual(first['updated'], '2026-11-22T10:00:00.1234567')
        self.assertEqual(first['at_time'], '23:59:59.9999999')
        self.assertEqual(first['at_offset'], '2026-11-22T10:00:00.1234567+05:30')
        self.assertEqual(decimal.Decimal(first['amount']), decimal.Decimal('12345678901234567890123456.0123456789'))
        self.assertEqual(first['big'], '9007199254740993')
        self.assertEqual(decimal.Decimal(str(first['price'])), decimal.Decimal('922337203685477.5807'))
        self.assertEqual(uuid.UUID(first['guid']), uuid.UUID('6F9619FF-8B86-D011-B42D-00C04FC964FF'))
        self.assertEqual(base64.b64decode(first['payload']), b'\x00\xff\x10')
        self.assertEqual(first['label'], 'café ✓')
        self.assertTrue(states(messages)[-1]['completed'])
        # Resume from the middle of the tied cursor group: only later rows are read.
        middle = states(messages)[1]
        self.assertEqual(middle['primary_key'], [1, 2])
        again = records(self.read(stream, middle))
        self.assertEqual([r['primary_key'] for r in again], [[2, 1], [1, 3], [1, '9007199254740993']])
        # Nothing new after the final checkpoint; a later insert is picked up.
        final = states(messages)[-1]
        self.assertEqual(records(self.read(stream, final)), [])
        with self.admin(DATABASE) as conn, conn.cursor() as c:
            c.execute("INSERT dbo.orders (tenant,id,updated) VALUES (3,1,'2026-11-22T10:00:01.0000002')")
        try:
            later = records(self.read(stream, final))
            self.assertEqual([r['primary_key'] for r in later], [[3, 1]])
        finally:
            with self.admin(DATABASE) as conn, conn.cursor() as c:
                c.execute('DELETE dbo.orders WHERE tenant=3')

    def test_full_refresh_pages_by_key(self):
        stream = self.stream('orders', mode='full_refresh')
        messages, code = run_actor('read', self.config, stream, {})
        self.assertEqual(code, 0, messages[-1:])
        self.assertEqual(len(records(messages)), 5)

    def test_binary_keys_and_rowversion_cursor_resume(self):
        stream = self.stream('versions', cursor='rv')
        messages = self.read(stream)
        self.assertEqual([base64.b64decode(r['primary_key']) for r in records(messages)], [b'\x00\x01', b'\x00\xff', b'\x01\x00'])
        again = records(self.read(stream, states(messages)[0]))
        self.assertEqual([base64.b64decode(r['primary_key']) for r in again], [b'\x00\xff', b'\x01\x00'])

    def test_single_row_pages_seek_through_every_key_type(self):
        import source_sqlserver as sqlserver
        conn = sqlserver.connection(self.config)
        try:
            for name, cursor, expected in (('versions', 'rv', 3), ('orders', 'updated', 5), ('codes', 'changed', 4)):
                for mode in ('full_refresh', 'incremental'):
                    stream = self.stream(name, mode=mode, cursor=cursor)
                    got = [m for m in sqlserver.read(conn, stream, {}, self.config, batch_size=1) if m['type'] == 'RECORD']
                    self.assertEqual(len(got), expected, (name, mode))
                    self.assertEqual(len({json.dumps(r['record']['primary_key']) for r in got}), expected, (name, mode))
        finally:
            conn.close()

    def test_oversized_row_fails_before_the_value_is_read(self):
        # Row sizes are checked for the whole page before any value is fetched, so the
        # page holding the oversized row emits nothing and the next attempt restarts it.
        messages, code = run_actor('read', self.config, self.stream('blobs', cursor='changed'), {})
        self.assertNotEqual(code, 0)
        self.assertEqual(trace(messages), 'record_too_large')
        self.assertEqual(len(records(messages)), 0)

    def test_unsupported_type_and_schema_drift_fail_closed(self):
        messages, code = run_actor('read', self.config, self.stream('unsupported', cursor='changed'), {})
        self.assertNotEqual(code, 0); self.assertEqual(trace(messages), 'schema_drift')
        stream = self.stream('orders')
        with self.admin(DATABASE) as conn, conn.cursor() as c:
            c.execute('ALTER TABLE dbo.orders ADD extra int NULL')
        try:
            messages, code = run_actor('read', self.config, stream, {})
            self.assertNotEqual(code, 0); self.assertEqual(trace(messages), 'schema_drift')
        finally:
            with self.admin(DATABASE) as conn, conn.cursor() as c:
                c.execute('ALTER TABLE dbo.orders DROP COLUMN extra')

    def test_snapshot_isolation_is_required(self):
        with self.admin() as conn, conn.cursor() as c:
            c.execute(f'ALTER DATABASE [{DATABASE}] SET ALLOW_SNAPSHOT_ISOLATION OFF')
        try:
            messages, code = run_actor('read', self.config, self.stream('orders'), {})
            self.assertNotEqual(code, 0)
            self.assertEqual(len(records(messages)), 0)
            self.assertIsNotNone(trace(messages))
        finally:
            with self.admin() as conn, conn.cursor() as c:
                c.execute(f'ALTER DATABASE [{DATABASE}] SET ALLOW_SNAPSHOT_ISOLATION ON')


if __name__ == '__main__':
    unittest.main()
