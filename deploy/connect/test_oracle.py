"""Oracle seek, projection, type and transport contracts without a server."""
import ssl
import unittest
from unittest.mock import patch

import source_oracle as oracle

CONFIG = {'host': 'db.example', 'service_name': 'FREEPDB1', 'user': 'READER', 'password': 'private'}
COLUMNS = {'TENANT': {'sql_type': 'NUMBER'}, 'ID': {'sql_type': 'NUMBER'},
           'UPDATED': {'sql_type': 'TIMESTAMP'}, 'AT': {'sql_type': 'TIMESTAMP WITH TIME ZONE'},
           'DAY': {'sql_type': 'DATE'}, 'TAG': {'sql_type': 'RAW'}}


class Contracts(unittest.TestCase):
    def test_base_types_strip_precision_and_keep_time_zone_kinds(self):
        self.assertEqual(oracle.base_type('TIMESTAMP(6)'), 'TIMESTAMP')
        self.assertEqual(oracle.base_type('TIMESTAMP(9) WITH TIME ZONE'), 'TIMESTAMP WITH TIME ZONE')
        self.assertEqual(oracle.base_type('TIMESTAMP(3) WITH LOCAL TIME ZONE'), 'TIMESTAMP WITH LOCAL TIME ZONE')
        self.assertEqual(oracle.base_type('NUMBER'), 'NUMBER')

    def test_temporal_projections_keep_nine_fractional_digits_and_offsets(self):
        self.assertEqual(oracle.project('UPDATED', COLUMNS['UPDATED']),
                         'TO_CHAR("UPDATED", \'YYYY-MM-DD"T"HH24:MI:SS.FF9\') AS "UPDATED"')
        self.assertIn('TZH:TZM', oracle.project('AT', COLUMNS['AT']))
        self.assertEqual(oracle.project('ID', COLUMNS['ID']), '"ID"')

    def test_composite_seek_binds_every_value_back_to_its_column_type(self):
        predicate, args = oracle.seek(['UPDATED', 'TENANT', 'TAG'], ['2026-11-22T10:00:00.123456789', 7, '00ff'], COLUMNS)
        self.assertEqual(predicate,
            '("UPDATED" > TO_TIMESTAMP(:p0, \'YYYY-MM-DD"T"HH24:MI:SS.FF9\')) OR '
            '("UPDATED" = TO_TIMESTAMP(:p1, \'YYYY-MM-DD"T"HH24:MI:SS.FF9\') AND "TENANT" > :p2) OR '
            '("UPDATED" = TO_TIMESTAMP(:p3, \'YYYY-MM-DD"T"HH24:MI:SS.FF9\') AND "TENANT" = :p4 AND "TAG" > HEXTORAW(:p5))')
        self.assertEqual(args, {'p0': '2026-11-22T10:00:00.123456789', 'p1': '2026-11-22T10:00:00.123456789', 'p2': 7,
                                'p3': '2026-11-22T10:00:00.123456789', 'p4': 7, 'p5': '00ff'})
        self.assertIn("TO_DATE(:p0", oracle.seek(['DAY'], ['2026-11-22T00:00:00'], COLUMNS)[0])
        self.assertIn("TO_TIMESTAMP_TZ(:p0", oracle.seek(['AT'], ['x'], COLUMNS)[0])

    def test_identifiers_are_quoted_and_validated(self):
        self.assertEqual(oracle.quote('ORDERS'), '"ORDERS"')
        for bad in ('A"B', 'X; DROP TABLE Y', '1ABC', 'SYS$X'):
            with self.assertRaisesRegex(ValueError, 'schema_drift'): oracle.quote(bad)

    def test_raw_positions_round_trip_from_landed_base64(self):
        self.assertEqual(oracle.position('AP8=', COLUMNS['TAG']), '00ff')
        with self.assertRaisesRegex(ValueError, 'source_changed'): oracle.position('not base64!', COLUMNS['TAG'])

    def test_connection_is_always_tls_with_verification(self):
        with patch('oracledb.connect') as connect:
            oracle.connection({**CONFIG, 'protocol': 'tcp', 'ssl_server_dn_match': False})
        kwargs = connect.call_args.kwargs
        self.assertEqual(kwargs['protocol'], 'tcps')
        self.assertTrue(kwargs['ssl_server_dn_match'])
        self.assertEqual(kwargs['port'], 2484)
        context = kwargs['ssl_context']
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertGreaterEqual(context.minimum_version, ssl.TLSVersion.TLSv1_2)
        session = connect.return_value.cursor.return_value.__enter__.return_value.execute.call_args.args[0]
        for setting in ("TIME_ZONE = '+00:00'", "NLS_COMP = 'BINARY'", "NLS_SORT = 'BINARY'", "NLS_NUMERIC_CHARACTERS = '.,'"):
            self.assertIn(setting, session)

    def test_reset_and_incremental_share_one_binding(self):
        # Resets and backfills run incremental streams as full_refresh; the incremental run
        # after them must accept their checkpoint.
        found = {'json_schema': {'type': 'object', 'properties': COLUMNS}}
        stream = {'namespace': 'S', 'name': 'T', 'cursor_field': 'UPDATED', 'sync_mode': 'incremental',
                  'primary_key': 'TENANT,ID', 'primary_keys': ['TENANT', 'ID']}
        self.assertEqual(oracle.schema_hash(found, CONFIG, stream),
                         oracle.schema_hash(found, CONFIG, {**stream, 'sync_mode': 'full_refresh'}))

    def test_connect_descriptors_and_bad_services_are_refused(self):
        for bad in ({'host': '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)))'}, {'host': 'user@db'},
                    {'service_name': 'a b'}, {'service_name': ''}, {'port': 'x'}):
            with self.assertRaisesRegex(ValueError, 'connector_failed'):
                oracle.connection({**CONFIG, **bad})


if __name__ == '__main__': unittest.main()
