"""Oracle actor against a real Oracle Database over verified TLS (TCPS).

Opt in with ORACLE_TEST_PORT, ORACLE_TEST_CA and ORACLE_TEST_SYSTEM_PASSWORD (optionally
ORACLE_TEST_WRONG_CA and ORACLE_TEST_IPV6=1). The local fixture is Oracle Database Free
(gvenzl/oracle-free) with a TCPS listener; see deploy/connect/README.md, "Oracle". Evidence
from it is engine evidence, not Enterprise Edition, RAC or managed-service certification.
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

ACTOR = Path(__file__).parent/'source_oracle.py'
PORT = os.environ.get('ORACLE_TEST_PORT')
CA = os.environ.get('ORACLE_TEST_CA')
WRONG_CA = os.environ.get('ORACLE_TEST_WRONG_CA')
SYSTEM = os.environ.get('ORACLE_TEST_SYSTEM_PASSWORD')
SERVICE = os.environ.get('ORACLE_TEST_SERVICE', 'FREEPDB1')
OWNER, READER = 'MERIDIAN_SRC', 'MERIDIAN_READER'
TABLES = ('ORDERS', 'BLOBS', 'VERSIONS', 'UNSUPPORTED', 'NOKEY')


def run_actor(command, config, catalog=None, state=None):
    with tempfile.TemporaryDirectory() as tmp:
        args = [sys.executable, str(ACTOR), command]
        for name, value in (('config', config), ('catalog', catalog), ('state', state)):
            if value is None: continue
            path = Path(tmp)/f'{name}.json'; path.write_text(json.dumps(value)); args += ['--'+name, str(path)]
        proc = subprocess.run(args, capture_output=True, text=True, timeout=300, encoding='utf-8',
                              env={**os.environ, 'PYTHONPATH': str(ACTOR.parent)})
    return [json.loads(line) for line in proc.stdout.splitlines() if line.strip()], proc.returncode


def records(messages): return [m['record'] for m in messages if m['type'] == 'RECORD']
def states(messages): return [m['state'] for m in messages if m['type'] == 'STATE']
def trace(messages): return messages[-1]['trace']['code'] if messages and messages[-1]['type'] == 'TRACE' else None


@unittest.skipUnless(PORT and CA and SYSTEM, 'set ORACLE_TEST_PORT, ORACLE_TEST_CA and ORACLE_TEST_SYSTEM_PASSWORD')
class OracleLive(unittest.TestCase):
    @classmethod
    def admin(cls, *statements):
        sys.path.insert(0, str(ACTOR.parent))
        import source_oracle as oracle
        conn = oracle.connection({'host': 'localhost', 'port': int(PORT), 'service_name': SERVICE,
                                  'user': 'system', 'password': SYSTEM, 'ssl_ca': CA})
        try:
            with conn.cursor() as c:
                for sql in statements:
                    if isinstance(sql, tuple): c.execute(*sql)
                    else: c.execute(sql)
            conn.commit()
        finally:
            conn.close()

    @classmethod
    def drop_users(cls):
        for user in (READER, OWNER):
            cls.admin(f"""BEGIN EXECUTE IMMEDIATE 'DROP USER {user} CASCADE';
                EXCEPTION WHEN OTHERS THEN IF SQLCODE != -1918 THEN RAISE; END IF; END;""")

    @classmethod
    def setUpClass(cls):
        cls.password = 'R'+secrets.token_hex(12)
        cls.drop_users()
        t = OWNER
        cls.admin(
            f'CREATE USER {OWNER} IDENTIFIED BY "{secrets.token_hex(12)}x" QUOTA UNLIMITED ON USERS',
            f'CREATE USER {READER} IDENTIFIED BY "{cls.password}"',
            f'GRANT CREATE SESSION TO {READER}',
            f"""CREATE TABLE {t}.ORDERS (TENANT NUMBER(10) NOT NULL, ID NUMBER(20) NOT NULL,
                UPDATED TIMESTAMP(9) NOT NULL, AMOUNT NUMBER(38,10), BIG NUMBER(20), RATIO BINARY_DOUBLE,
                PRICE NUMBER, LABEL NVARCHAR2(40), NOTE CLOB, AT_TZ TIMESTAMP(9) WITH TIME ZONE,
                AT_LOCAL TIMESTAMP(6) WITH LOCAL TIME ZONE, BORN DATE, PAYLOAD RAW(16),
                CONSTRAINT PK_ORDERS PRIMARY KEY (TENANT, ID))""",
            f'CREATE TABLE {t}.BLOBS (ID NUMBER(10) PRIMARY KEY, CHANGED TIMESTAMP NOT NULL, BODY BLOB)',
            f'CREATE TABLE {t}.VERSIONS (ID RAW(16) PRIMARY KEY, CHANGED DATE NOT NULL, NAME VARCHAR2(20))',
            f'CREATE TABLE {t}.UNSUPPORTED (ID NUMBER(10) PRIMARY KEY, CHANGED TIMESTAMP NOT NULL, SPAN INTERVAL DAY TO SECOND)',
            f'CREATE TABLE {t}.NOKEY (ID NUMBER(10) NOT NULL, CHANGED TIMESTAMP NOT NULL)',
            *[f'GRANT SELECT ON {t}.{name} TO {READER}' for name in TABLES])
        # Three rows share one nanosecond: the cursor tie must be broken by the key.
        tick = '2026-11-22 10:00:00.123456789'
        rows = [(1, 1, tick), (1, 2, tick), (2, 1, tick), (1, 3, '2026-11-22 10:00:00.123456790'),
                (1, 9007199254740993, '2026-11-22 10:00:01.000000001')]
        cls.admin(*[(f"""INSERT INTO {t}.ORDERS VALUES (:1, :2, TO_TIMESTAMP(:3, 'YYYY-MM-DD HH24:MI:SS.FF9'),
                12345678901234567890123456.0123456789, 9007199254740993, 0.1, 0.5, N'café ✓', 'x',
                TIMESTAMP '2026-11-22 10:00:00.123456789 +05:30', TIMESTAMP '2026-11-22 10:00:00.5 +00:00',
                DATE '2026-11-22', HEXTORAW('00FF10'))""", r) for r in rows],
            f"INSERT INTO {t}.BLOBS VALUES (1, TIMESTAMP '2026-11-22 00:00:00', HEXTORAW('01'))",
            f"""DECLARE b BLOB; BEGIN INSERT INTO {t}.BLOBS VALUES (2, TIMESTAMP '2026-11-22 00:00:01', EMPTY_BLOB())
                RETURNING BODY INTO b; FOR i IN 1..20 LOOP DBMS_LOB.WRITEAPPEND(b, 30000, UTL_RAW.COPIES(HEXTORAW('41'), 30000)); END LOOP; END;""",
            *[f"INSERT INTO {t}.VERSIONS VALUES (HEXTORAW('{h}'), DATE '2026-11-22' + {i}, '{n}')"
              for i, (h, n) in enumerate((('0001', 'a'), ('00FF', 'b'), ('0100', 'c')))])
        cls.config = {'host': 'localhost', 'port': int(PORT), 'service_name': SERVICE, 'user': READER,
                      'password': cls.password, 'schema': OWNER, 'ssl_ca': CA}

    @classmethod
    def tearDownClass(cls):
        cls.drop_users()

    def stream(self, name, mode='incremental', cursor='UPDATED'):
        messages, code = run_actor('discover', self.config)
        self.assertEqual(code, 0, messages[-1:])
        found = next(s for s in messages[-1]['catalog']['streams'] if s['name'] == name)
        keys = found['primary_key'] if isinstance(found['primary_key'], list) else [found['primary_key']]
        return {'namespace': OWNER, 'name': name, 'primary_keys': keys, 'primary_key': ','.join(keys),
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
        self.assertEqual(set(streams), {'ORDERS', 'BLOBS', 'VERSIONS', 'UNSUPPORTED'})
        self.assertEqual(streams['ORDERS']['primary_key'], ['TENANT', 'ID'])
        props = streams['ORDERS']['json_schema']['properties']
        self.assertEqual(props['UPDATED']['source_type'], 'TIMESTAMP(9)')
        self.assertEqual(props['AMOUNT']['source_type'], 'NUMBER(38,10)')
        self.assertEqual(props['LABEL']['source_type'], 'NVARCHAR2(40)')
        self.assertEqual(props['AT_TZ']['sql_type'], 'TIMESTAMP WITH TIME ZONE')

    def test_wrong_ca_and_wrong_password_are_auth_failures(self):
        if WRONG_CA:
            messages, code = run_actor('check', {**self.config, 'ssl_ca': WRONG_CA})
            self.assertNotEqual(code, 0); self.assertEqual(trace(messages), 'auth_failed')
        messages, code = run_actor('check', {**self.config, 'password': 'wrong'+secrets.token_hex(4)})
        self.assertNotEqual(code, 0); self.assertEqual(trace(messages), 'auth_failed')

    def test_hostname_must_match_the_certificate(self):
        # python-oracledb matches the server certificate against the DNS host name, so the
        # fixture (CN and DNS SAN localhost) is refused through a loopback address.
        if os.environ.get('ORACLE_TEST_IPV6') == '1':
            messages, code = run_actor('check', {**self.config, 'host': '::1'})
            self.assertNotEqual(code, 0); self.assertEqual(trace(messages), 'auth_failed')
        messages, code = run_actor('check', {**self.config, 'host': '127.0.0.1'})
        self.assertNotEqual(code, 0); self.assertEqual(trace(messages), 'auth_failed')

    def test_incremental_read_types_ties_and_resume(self):
        stream = self.stream('ORDERS')
        messages = self.read(stream)
        got = records(messages)
        self.assertEqual([r['primary_key'] for r in got], [[1, 1], [1, 2], [2, 1], [1, 3], [1, '9007199254740993']])
        first = got[0]['data']
        self.assertEqual(first['UPDATED'], '2026-11-22T10:00:00.123456789')
        self.assertEqual(first['AT_TZ'], '2026-11-22T10:00:00.123456789+05:30')
        self.assertEqual(first['AT_LOCAL'], '2026-11-22T10:00:00.500000000')
        self.assertEqual(first['BORN'], '2026-11-22T00:00:00')
        self.assertEqual(decimal.Decimal(first['AMOUNT']), decimal.Decimal('12345678901234567890123456.0123456789'))
        self.assertEqual(first['BIG'], '9007199254740993')
        self.assertEqual(first['RATIO'], 0.1)
        self.assertEqual(first['PRICE'], '0.5')
        self.assertEqual(first['NOTE'], 'x')
        self.assertEqual(base64.b64decode(first['PAYLOAD']), b'\x00\xff\x10')
        self.assertEqual(first['LABEL'], 'café ✓')
        self.assertTrue(states(messages)[-1]['completed'])
        # Resume from the middle of the tied cursor group: only later rows are read.
        middle = states(messages)[1]
        self.assertEqual(middle['primary_key'], [1, 2])
        again = records(self.read(stream, middle))
        self.assertEqual([r['primary_key'] for r in again], [[2, 1], [1, 3], [1, '9007199254740993']])
        # Nothing new after the final checkpoint; a later insert is picked up.
        final = states(messages)[-1]
        self.assertEqual(records(self.read(stream, final)), [])
        self.admin(f"INSERT INTO {OWNER}.ORDERS (TENANT, ID, UPDATED) VALUES (3, 1, TIMESTAMP '2026-11-22 10:00:01.000000002')")
        try:
            self.assertEqual([r['primary_key'] for r in records(self.read(stream, final))], [[3, 1]])
        finally:
            self.admin(f'DELETE FROM {OWNER}.ORDERS WHERE TENANT = 3')

    def test_full_refresh_pages_by_key(self):
        messages = self.read(self.stream('ORDERS', mode='full_refresh'))
        self.assertEqual(len(records(messages)), 5)

    def test_reset_checkpoint_carries_into_the_next_incremental_run(self):
        # A reset runs the incremental stream as full_refresh with the same cursor field.
        incremental = self.stream('ORDERS')
        final = states(self.read({**incremental, 'sync_mode': 'full_refresh'}))[-1]
        self.assertTrue(final['completed'])
        self.assertIsNotNone(final['cursor'])
        self.admin(f"INSERT INTO {OWNER}.ORDERS (TENANT, ID, UPDATED) VALUES (4, 1, TIMESTAMP '2026-11-22 10:00:05')")
        try:
            self.assertIn([4, 1], [r['primary_key'] for r in records(self.read(incremental, final))])
        finally:
            self.admin(f'DELETE FROM {OWNER}.ORDERS WHERE TENANT = 4')

    def test_raw_keys_and_date_cursor_resume(self):
        stream = self.stream('VERSIONS', cursor='CHANGED')
        messages = self.read(stream)
        self.assertEqual([base64.b64decode(r['primary_key']) for r in records(messages)], [b'\x00\x01', b'\x00\xff', b'\x01\x00'])
        again = records(self.read(stream, states(messages)[0]))
        self.assertEqual([base64.b64decode(r['primary_key']) for r in again], [b'\x00\xff', b'\x01\x00'])
        by_key = self.stream('VERSIONS', mode='full_refresh', cursor='CHANGED')
        first = states(self.read(by_key))[0]
        self.assertEqual([base64.b64decode(r['primary_key']) for r in records(self.read(by_key, first))], [b'\x00\xff', b'\x01\x00'])

    def test_single_row_pages_seek_through_every_key_type(self):
        sys.path.insert(0, str(ACTOR.parent))
        import source_oracle as oracle
        conn = oracle.connection(self.config)
        try:
            for name, cursor, expected in (('VERSIONS', 'CHANGED', 3), ('ORDERS', 'UPDATED', 5)):
                stream = self.stream(name, cursor=cursor)
                got = [m for m in oracle.read(conn, stream, {}, self.config, batch_size=1) if m['type'] == 'RECORD']
                self.assertEqual(len(got), expected, name)
                self.assertEqual(len({json.dumps(r['record']['primary_key']) for r in got}), expected, name)
        finally:
            conn.close()

    def test_oversized_row_fails_before_the_value_is_read(self):
        # Row sizes are checked for the whole page before any value is fetched, so the
        # page holding the oversized row emits nothing and the next attempt restarts it.
        messages, code = run_actor('read', self.config, self.stream('BLOBS', cursor='CHANGED'), {})
        self.assertNotEqual(code, 0)
        self.assertEqual(trace(messages), 'record_too_large')
        self.assertEqual(len(records(messages)), 0)

    def test_unsupported_type_and_schema_drift_fail_closed(self):
        messages, code = run_actor('read', self.config, self.stream('UNSUPPORTED', cursor='CHANGED'), {})
        self.assertNotEqual(code, 0); self.assertEqual(trace(messages), 'schema_drift')
        stream = self.stream('ORDERS')
        self.admin(f'ALTER TABLE {OWNER}.ORDERS ADD EXTRA NUMBER(10)')
        try:
            messages, code = run_actor('read', self.config, stream, {})
            self.assertNotEqual(code, 0); self.assertEqual(trace(messages), 'schema_drift')
        finally:
            self.admin(f'ALTER TABLE {OWNER}.ORDERS DROP COLUMN EXTRA')

    def test_revoked_grant_is_schema_drift_not_an_outage(self):
        stream = self.stream('VERSIONS', cursor='CHANGED')
        self.admin(f'REVOKE SELECT ON {OWNER}.VERSIONS FROM {READER}')
        try:
            messages, code = run_actor('read', self.config, stream, {})
            self.assertNotEqual(code, 0); self.assertEqual(trace(messages), 'schema_drift')
        finally:
            self.admin(f'GRANT SELECT ON {OWNER}.VERSIONS TO {READER}')


if __name__ == '__main__':
    unittest.main()
