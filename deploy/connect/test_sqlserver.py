"""SQL Server pagination, precision and transport contracts without a server."""
import base64
import copy
import decimal
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import source_sqlserver as sqlserver

CONFIG = {'host': 'db.example', 'database': 'sales', 'user': 'reader', 'password': 'private'}
COLUMNS = {'tenant': {'sql_type': 'int', 'source_type': 'int', 'nullable': False, 'max_length': 4},
           'id': {'sql_type': 'bigint', 'source_type': 'bigint', 'nullable': False, 'max_length': 8},
           'updated': {'sql_type': 'datetime2', 'source_type': 'datetime2(7)', 'nullable': False, 'max_length': 8}}
FOUND = {'namespace': 'dbo', 'name': 'orders', 'primary_key': ['tenant', 'id'],
         'json_schema': {'type': 'object', 'properties': COLUMNS}}
STREAM = {'namespace': 'dbo', 'name': 'orders', 'primary_keys': ['tenant', 'id'],
          'primary_key': 'tenant,id', 'cursor_field': 'updated', 'sync_mode': 'incremental',
          'schema': FOUND['json_schema']}


class Connection:
    autocommit = True
    isolation_level = 2
    def __init__(self): self.rolled_back = False
    def rollback(self): self.rolled_back = True


class SQLServerContracts(unittest.TestCase):
    def read(self, data, checkpoint=None, stream=None, size=500):
        conn = Connection()
        guard = sqlserver.size_column(COLUMNS)
        metadata = [{**row, guard: 64} for row in data]
        with patch.object(sqlserver, 'discover', return_value=[FOUND]), patch.object(sqlserver, 'query', side_effect=[metadata, []]) as query, patch.object(sqlserver, 'rows', return_value=iter(data)):
            result = list(sqlserver.read(conn, stream or STREAM, checkpoint or {}, CONFIG, batch_size=size))
        self.assertTrue(conn.rolled_back)
        self.assertTrue(conn.autocommit)
        return result, query

    def test_composite_seek_is_expanded_and_values_are_bound(self):
        predicate, args = sqlserver.seek(['updated', 'tenant', 'id'], ['t', 7, 9], COLUMNS)
        self.assertEqual(predicate, '([updated] > %s) OR ([updated] = %s AND [tenant] > %s) OR ([updated] = %s AND [tenant] = %s AND [id] > %s)')
        self.assertEqual(args, ['t', 't', 7, 't', 7, 9])
        # Character keys compare under the column's collation as varchar, never as nvarchar.
        code = {'sql_type': 'varchar', 'collation': 'SQL_Latin1_General_CP1_CI_AS'}
        predicate, _ = sqlserver.seek(['code'], ['co-op'], {'code': code})
        self.assertEqual(predicate, '([code] > CAST(%s COLLATE SQL_Latin1_General_CP1_CI_AS AS varchar(8000)))')
        with self.assertRaisesRegex(ValueError, 'schema_drift'):
            sqlserver.seek(['code'], ['x'], {'code': {**code, 'collation': 'X; DROP TABLE y'}})

    def test_reset_checkpoint_carries_into_the_next_incremental_run(self):
        # Resets and backfills run incremental streams as full_refresh; the hash and the
        # cursor in their checkpoint must stay usable by the incremental run that follows.
        full = {**STREAM, 'sync_mode': 'full_refresh'}
        self.assertEqual(sqlserver.schema_hash(FOUND, CONFIG, full), sqlserver.schema_hash(FOUND, CONFIG, STREAM))
        messages, _ = self.read([{'tenant': 1, 'id': 1, 'updated': '2026-11-22T00:00:00.0000001'}], stream=full)
        self.assertEqual(messages[-1]['state']['cursor'], '2026-11-22T00:00:00.0000001')
        with self.assertRaisesRegex(ValueError, 'schema_drift'): sqlserver.quote('id]; DELETE orders')

    def test_datetime2_projection_retains_seven_fractional_digits(self):
        self.assertEqual(sqlserver.project('updated', COLUMNS['updated']), 'CONVERT(nvarchar(48),[updated],126) AS [updated]')
        value = {'tenant': 1, 'id': 9007199254740993, 'updated': '2026-11-22T10:20:30.1234567'}
        messages, _ = self.read([value])
        self.assertEqual(messages[0]['record']['data']['id'], '9007199254740993')
        self.assertEqual(messages[0]['record']['cursor'], value['updated'])
        self.assertEqual(messages[1]['state']['cursor'], value['updated'])
        self.assertEqual(messages[1]['state']['primary_key'], [1, '9007199254740993'])
        self.assertTrue(messages[-1]['state']['completed'])

    def test_every_row_has_its_own_atomic_checkpoint_before_next_record(self):
        data = [{'tenant': 1, 'id': n, 'updated': f'2026-11-22T00:00:00.{n:07d}'} for n in (1, 2)]
        messages, _ = self.read(data)
        self.assertEqual([m['type'] for m in messages], ['RECORD', 'STATE', 'RECORD', 'STATE', 'STATE'])
        self.assertEqual(messages[1]['state']['primary_key'], [1, 1])
        self.assertEqual(messages[3]['state']['primary_key'], [1, 2])

    def test_cursor_that_is_a_key_is_not_repeated_in_order(self):
        stream = {**STREAM, 'cursor_field': 'id'}
        _, query = self.read([], stream=stream)
        self.assertIn('ORDER BY [id],[tenant]', query.call_args.args[1])

    def test_binary_checkpoint_is_restored_to_bytes(self):
        value = b'\x00\xff\x01'
        self.assertEqual(sqlserver.restore(base64.b64encode(value).decode(), {'sql_type': 'rowversion'}), value)
        with self.assertRaisesRegex(ValueError, 'source_changed'):
            sqlserver.restore('not base64!', {'sql_type': 'varbinary'})

    def test_changed_endpoint_or_principal_cannot_reuse_a_checkpoint(self):
        binding = sqlserver.schema_hash(FOUND, CONFIG, STREAM)
        checkpoint = {'cursor': '2026-11-22', 'primary_key': [1, 1], 'schema_hash': binding}
        for field in ('host', 'database', 'user'):
            with self.subTest(field=field), patch.object(sqlserver, 'discover', return_value=[FOUND]):
                with self.assertRaisesRegex(ValueError, 'source_changed'):
                    list(sqlserver.read(Connection(), STREAM, checkpoint, {**CONFIG, field: 'changed'}))
        self.assertEqual(binding, sqlserver.schema_hash(FOUND, {**CONFIG, 'password': 'rotated'}, STREAM))

    def test_wrong_key_arity_and_unbound_old_checkpoint_fail_closed(self):
        for checkpoint in ({'cursor': 't', 'primary_key': [1]}, {'cursor': 't', 'primary_key': [1, 2]}):
            with patch.object(sqlserver, 'discover', return_value=[FOUND]), self.assertRaisesRegex(ValueError, 'source_changed'):
                list(sqlserver.read(Connection(), STREAM, checkpoint, CONFIG))

    def test_oversized_row_fails_before_fetching_or_emitting_data(self):
        marker = sqlserver.size_column(COLUMNS)
        with patch.object(sqlserver, 'discover', return_value=[FOUND]), patch.object(sqlserver, 'query', return_value=[{marker: sqlserver.MAX_ROW_BYTES+1}]), patch.object(sqlserver, 'rows') as rows:
            with self.assertRaisesRegex(ValueError, 'record_too_large'):
                list(sqlserver.read(Connection(), STREAM, {}, CONFIG))
            rows.assert_not_called()

    def test_nullable_cursor_unsupported_type_and_schema_drift_are_rejected(self):
        for column_change in ({'nullable': True}, {'sql_type': 'sql_variant'}):
            found = copy.deepcopy(FOUND)
            found['json_schema']['properties']['updated'].update(column_change)
            stream = {**STREAM, 'schema': found['json_schema']}
            with patch.object(sqlserver, 'discover', return_value=[found]), self.assertRaisesRegex(ValueError, 'schema_drift'):
                list(sqlserver.read(Connection(), stream, {}, CONFIG))
        with patch.object(sqlserver, 'discover', return_value=[]), self.assertRaisesRegex(ValueError, 'schema_drift'):
            list(sqlserver.read(Connection(), STREAM, {}, CONFIG))

    def test_tls_cannot_be_downgraded_and_ca_file_remains_local(self):
        with tempfile.TemporaryDirectory() as directory:
            ca = Path(directory, 'ca.pem'); ca.write_text('test CA')
            with patch.object(sqlserver.pytds, 'connect') as connect:
                sqlserver.connection({**CONFIG, 'ssl_ca': str(ca), 'enc_login_only': True, 'validate_host': False})
            settings = connect.call_args.kwargs
            self.assertEqual(settings['cafile'], str(ca))
            self.assertTrue(settings['validate_host'])
            self.assertFalse(settings['enc_login_only'])
            self.assertEqual(settings['timeout'], 60)


def certificate(cn, dns=(), ips=()):
    import datetime, ipaddress
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID
    from OpenSSL import crypto
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
    now = datetime.datetime.now(datetime.timezone.utc)
    builder = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
               .serial_number(1).not_valid_before(now).not_valid_after(now+datetime.timedelta(days=1)))
    names = [x509.DNSName(d) for d in dns]+[x509.IPAddress(ipaddress.ip_address(i)) for i in ips]
    if names: builder = builder.add_extension(x509.SubjectAlternativeName(names), critical=False)
    return crypto.X509.from_cryptography(builder.sign(key, hashes.SHA256()))


class HostValidation(unittest.TestCase):
    def test_driver_hook_is_replaced(self):
        import pytds.tls
        self.assertIs(pytds.tls.validate_host, sqlserver.validate_host)

    def test_san_names_including_ones_the_driver_misparsed(self):
        cert = certificate('ignored', dns=['Sales.db.example', 'DNS-01.example', '*.prod.example'])
        for host in (b'sales.db.example', b'dns-01.example', b'a.prod.example', b'SALES.DB.EXAMPLE.'):
            self.assertTrue(sqlserver.validate_host(cert, host), host)
        for host in (b'ales.db.example', b'ignored', b'a.b.prod.example', b'prod.example', b'evil.example'):
            self.assertFalse(sqlserver.validate_host(cert, host), host)

    def test_ip_addresses_need_an_ip_san(self):
        cert = certificate('127.0.0.1', dns=['localhost'], ips=['127.0.0.1'])
        self.assertTrue(sqlserver.validate_host(cert, b'127.0.0.1'))
        self.assertFalse(sqlserver.validate_host(cert, b'::1'))
        self.assertFalse(sqlserver.validate_host(certificate('127.0.0.1'), b'127.0.0.1'))

    def test_common_name_only_without_sans_and_no_bare_wildcards(self):
        self.assertTrue(sqlserver.validate_host(certificate('db.example'), b'db.example'))
        self.assertFalse(sqlserver.validate_host(certificate('*.example'), b'db.example'))
        self.assertFalse(sqlserver.validate_host(certificate('*'), b'db'))


if __name__ == '__main__': unittest.main()
