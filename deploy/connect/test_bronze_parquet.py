"""Bronze Parquet parts (S6, bronze_parquet.py): lossless round trip, schema-derived column types,
NDJSON fallback, and the shared fixture the TypeScript decoder (src/lib/bronze-parquet.ts) reads."""
import json, os, pathlib, sys, unittest
sys.path.insert(0, os.path.dirname(__file__))
import bronze_parquet as bp

FIXTURE = pathlib.Path(__file__).resolve().parents[2] / 'src' / 'test' / 'fixtures' / 'bronze-part'
SCHEMA = {'type': 'object', 'properties': {
    'id': {'source_type': 'integer', 'nullable': False},
    'big': {'source_type': 'bigint', 'nullable': True},
    'ok': {'source_type': 'boolean', 'nullable': True},
    'amount': {'source_type': 'numeric', 'nullable': True},
    'name': {'source_type': 'character varying', 'nullable': True},
    'at': {'source_type': 'timestamp with time zone', 'nullable': True},
    'doc': {'source_type': 'jsonb', 'nullable': True},
    'score': {'source_type': 'double precision', 'nullable': True},
}}


def envelope(i, data, operation='upsert'):
    return {'protocol': 'meridian-connect/1', 'connection_id': '11111111-1111-4111-8111-111111111111',
            'run_id': '22222222-2222-4222-8222-222222222222', 'stream_id': '33333333-3333-4333-8333-333333333333',
            'schema': SCHEMA, 'extracted_at': 1794759162000, 'cursor': f'2026-11-17T00:00:{i:02d}+00:00',
            'primary_key': i, 'operation': operation, 'source_namespace': 'public', 'source_stream': 'orders',
            'data': data}


def records():
    return [
        envelope(1, {'id': 1, 'big': 9007199254740991, 'ok': True, 'amount': '12.50', 'name': 'Zoë ✓',
                     'at': '2026-11-17T00:00:01+00:00', 'doc': {'tags': ['a', '名'], 'n': None}, 'score': 1.5}),
        envelope(2, {'id': 2, 'big': '9007199254740993', 'ok': None, 'amount': None, 'name': '',
                     'at': None, 'doc': [1, 2.25, None], 'score': 'NaN'}),
        envelope(3, {'id': 3, 'big': None, 'ok': False, 'amount': '-0.000000001', 'name': None,
                     'at': '2026-11-17T00:00:03+00:00', 'doc': None, 'score': 2}, operation='delete'),
    ]


class BronzeParquet(unittest.TestCase):
    def test_round_trip_is_lossless(self):
        envs = records()
        body = bp.encode(envs)
        self.assertTrue(bp.is_parquet(body))
        self.assertEqual(bp.decode(body), envs)

    def test_column_types_come_from_the_schema(self):
        import pyarrow as pa, pyarrow.parquet as pq
        schema = pq.read_schema(pa.BufferReader(bp.encode(records())))
        types = {f.name: str(f.type) for f in schema}
        self.assertEqual(types, {'_meridian_operation': 'string', '_meridian_primary_key': 'string',
                                 '_meridian_cursor': 'string', 'id': 'int64', 'big': 'string', 'ok': 'bool',
                                 'amount': 'string', 'name': 'string', 'at': 'string', 'doc': 'string',
                                 'score': 'string'})
        meta = json.loads(schema.metadata[bp.META])
        self.assertEqual(meta['columns'], {'id': 'int64', 'big': 'json', 'ok': 'bool', 'amount': 'string',
                                           'name': 'string', 'at': 'string', 'doc': 'json', 'score': 'json'})
        self.assertEqual(meta['layout'], bp.LAYOUT)

    def test_parts_that_cannot_be_columns_fall_back_to_ndjson(self):
        envs = records()
        envs[1]['data'].pop('name')
        self.assertIsNone(bp.encode(envs), 'records with different fields')
        envs = records(); envs[0]['data']['id'] = '1'
        self.assertIsNone(bp.encode(envs), 'a value that does not fit its typed column')
        envs = records()
        for e in envs: e['data']['_meridian_x'] = 1
        self.assertIsNone(bp.encode(envs), 'a reserved field name')
        self.assertIsNone(bp.encode([]), 'an empty part stays NDJSON')

    def test_kinds(self):
        for t, k in [('integer', 'int64'), ('int(11) unsigned', 'int64'), ('tinyint(1)', 'bool'), ('tinyint', 'int64'),
                     ('bigint', 'json'), ('varchar(32)', 'string'), ('decimal(40,9)', 'string'), ('datetime(6)', 'string'),
                     ('json', 'json'), ('ARRAY', 'json'), ('kafka_offset', 'json'), (None, 'json')]:
            self.assertEqual(bp.kind_of({'source_type': t}), k, t)

    def test_a_file_without_the_layout_is_refused(self):
        import io, pyarrow as pa, pyarrow.parquet as pq
        out = io.BytesIO(); pq.write_table(pa.table({'a': [1]}), out)
        with self.assertRaises(ValueError): bp.decode(out.getvalue())

    def test_the_shared_fixture_decodes_to_its_envelopes(self):
        # Regenerate with: python deploy/connect/test_bronze_parquet.py --write-fixture
        expected = json.loads((FIXTURE.with_suffix('.json')).read_text(encoding='utf-8'))
        self.assertEqual(bp.decode(str(FIXTURE.with_suffix('.parquet'))), expected)
        self.assertEqual(expected, records())


if __name__ == '__main__':
    if '--write-fixture' in sys.argv:
        FIXTURE.parent.mkdir(parents=True, exist_ok=True)
        FIXTURE.with_suffix('.parquet').write_bytes(bp.encode(records()))
        FIXTURE.with_suffix('.json').write_text(json.dumps(records(), ensure_ascii=False, indent=1) + '\n', encoding='utf-8')
    else:
        unittest.main()
