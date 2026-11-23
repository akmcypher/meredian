"""Opt-in local PostgreSQL CDC retirement checks; no service startup or database creation.

Run with CONNECT_RETIREMENT_LIVE=1 and PGPASSWORD set for the local postgres role.
Only 127.0.0.1:58622/connect_source is accessed. Each test owns one UUID-derived
pgoutput slot; no tables, publications, databases or unrelated slots are changed.
"""
import os
import unittest
import uuid

from psycopg import connect, OperationalError
from psycopg.rows import dict_row

import source_postgres_cdc as pg


@unittest.skipUnless(os.environ.get('CONNECT_RETIREMENT_LIVE') == '1',
                     'set CONNECT_RETIREMENT_LIVE=1 to run local PostgreSQL retirement checks')
class RetirementLive(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = {'host': '127.0.0.1', 'port': 58622, 'dbname': 'connect_source',
                      'user': 'postgres', 'sslmode': 'disable', 'connect_timeout': 3,
                      'autocommit': True, 'row_factory': dict_row,
                      'options': '-c statement_timeout=10000 -c lock_timeout=2000'}
        try:
            with connect(**{**cls.config, 'dbname': 'postgres'}) as probe:
                if not probe.execute('SELECT 1 FROM pg_database WHERE datname=%s',
                                     ['connect_source']).fetchone():
                    raise unittest.SkipTest('local connect_source database does not exist')
            with connect(**cls.config) as probe:
                if probe.execute('SHOW wal_level').fetchone()['wal_level'] != 'logical':
                    raise unittest.SkipTest('local PostgreSQL wal_level is not logical')
                role = probe.execute('SELECT rolsuper OR rolreplication AS allowed FROM pg_roles WHERE rolname=current_user').fetchone()
                if not role['allowed']:
                    raise unittest.SkipTest('local PostgreSQL role cannot manage replication slots')
                capacity = probe.execute("SELECT count(*) < current_setting('max_replication_slots')::int AS available FROM pg_replication_slots").fetchone()
                if not capacity['available']:
                    raise unittest.SkipTest('no local replication slot capacity for the isolated fixture')
        except OperationalError:
            raise unittest.SkipTest('local PostgreSQL at 127.0.0.1:58622 is unavailable or credentials are missing') from None

    def setUp(self):
        self.stream = {'id': 'retirement-live-' + str(uuid.uuid4())}
        self.slot = pg.slot_name(self.stream)
        self.admin = connect(**self.config)
        self.addCleanup(self.admin.close)
        # The UUID-derived name must be absent before this test takes ownership.
        self.assertIsNone(self.slot_row())
        created = self.admin.execute("SELECT slot_name FROM pg_create_logical_replication_slot(%s,'pgoutput')", [self.slot]).fetchone()
        self.assertEqual(created['slot_name'], self.slot)
        self.addCleanup(self.remove_owned_slot)

    def slot_row(self):
        return self.admin.execute('SELECT slot_name,plugin,database,active FROM pg_replication_slots WHERE slot_name=%s', [self.slot]).fetchone()

    def remove_owned_slot(self):
        # Exact test-owned slot only, including when an assertion/production cleanup fails.
        row = self.slot_row()
        if row is not None:
            self.assertEqual((row['plugin'], row['database'], row['active']),
                             ('pgoutput', 'connect_source', False))
            self.admin.execute('SELECT pg_drop_replication_slot(%s)', [self.slot])

    def test_cleanup_drops_deterministic_slot_and_is_idempotent(self):
        self.assertEqual(self.slot_row()['slot_name'], pg.slot_name(dict(self.stream)))
        pg.cleanup(self.admin, self.stream)
        self.assertIsNone(self.slot_row())
        pg.cleanup(self.admin, self.stream)
        self.assertIsNone(self.slot_row())

    def test_source_lock_rejects_concurrent_cleanup_then_release_allows_it(self):
        with connect(**self.config) as reader:
            pg.source_lock(reader, self.stream)
            with self.assertRaises(pg.Stop) as blocked:
                pg.cleanup(self.admin, self.stream)
            self.assertEqual((str(blocked.exception), blocked.exception.reason),
                             ('source_unavailable', 'cdc_stream_busy'))
            self.assertIsNotNone(self.slot_row())
        # Closing the separate reader session releases its advisory lock.
        pg.cleanup(self.admin, self.stream)
        self.assertIsNone(self.slot_row())
        pg.cleanup(self.admin, self.stream)
        self.assertIsNone(self.slot_row())


if __name__ == '__main__':
    unittest.main()
