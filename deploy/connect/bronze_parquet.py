"""Bronze parts as Parquet (layout meridian-bronze-parquet/1).

A Parquet part holds the same meridian-connect/1 envelopes as an NDJSON part, laid out in columns:

- The fields that are the same for every envelope of a part (protocol, connection, run, stream,
  schema, extraction time, source namespace and stream) are stored once, in the file's key-value
  metadata under `meridian.bronze`, together with the layout name and the kind of each data column.
- `_meridian_operation` (text), `_meridian_primary_key` and `_meridian_cursor` (JSON text) carry the
  per-record envelope fields.
- Every field of `data` is its own column, in the order of the first record. Its type comes from
  the stream schema's `source_type`, so every part of a stream has the same Parquet schema:
  booleans, integers that always fit a JSON number, and types the actors always encode as text
  (strings, decimals, dates and times, UUIDs, bytes) are typed columns; anything else (JSON,
  arrays, bigint, floats, unknown types) is JSON text.

Decoding gives back the exact envelopes that were encoded. A part that cannot be laid out this way
(records with different fields, a field named `_meridian_*`, or a value that doesn't fit its
column, such as a bigint past 2^53, which travels as text) is not encoded: `encode` returns None
and the caller writes NDJSON instead. Readers accept both formats.
"""
import io
import json
import re

LAYOUT = 'meridian-bronze-parquet/1'
META = b'meridian.bronze'
OPERATION, PRIMARY_KEY, CURSOR = '_meridian_operation', '_meridian_primary_key', '_meridian_cursor'
RESERVED = '_meridian_'
CONSTANT = ('protocol', 'connection_id', 'run_id', 'stream_id', 'schema', 'extracted_at',
            'source_namespace', 'source_stream')
KINDS = ('bool', 'int64', 'string', 'json')
MAGIC = b'PAR1'


def is_parquet(head):
    """True when the bytes start with the Parquet magic number."""
    return head[:4] == MAGIC


def _text(v):
    return json.dumps(v, separators=(',', ':'), ensure_ascii=False)


# source_type -> column kind (PostgreSQL data_type, MySQL column_type, file and Kafka metadata).
_BOOL = re.compile(r'^(boolean|tinyint\(1\))$')
_INT = re.compile(r'^(smallint|integer|(tinyint|smallint|mediumint|int)(\(\d+\))?( unsigned)?)$')
_STRING = re.compile(r'^(text|string|uuid|bytea|date|interval|time.*|datetime.*|character( varying)?(\(\d+\))?'
                     r'|(var)?char\(\d+\)|(tiny|medium|long)?text|(var)?binary\(\d+\)|(tiny|medium|long)?blob'
                     r'|numeric(\(.*\))?|decimal(\(.*\))?( unsigned)?)$')


def kind_of(prop):
    """The column kind for a stream schema property."""
    t = str((prop or {}).get('source_type') or '').strip().lower()
    if _BOOL.match(t):
        return 'bool'
    if _INT.match(t):
        return 'int64'
    if _STRING.match(t):
        return 'string'
    return 'json'


def _fits(kind, v):
    if v is None or kind == 'json':
        return True
    if kind == 'bool':
        return isinstance(v, bool)
    if kind == 'int64':
        return isinstance(v, int) and not isinstance(v, bool) and -(1 << 63) <= v < (1 << 63)
    return isinstance(v, str)


def encode(envelopes):
    """Parquet bytes for a list of envelopes of one part, or None when the part can't be laid out
    as columns (then the caller writes NDJSON)."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    if not envelopes:
        return None
    first = envelopes[0]
    names = list(first['data'])
    if any(n.startswith(RESERVED) for n in names):
        return None
    fields = set(names)
    for e in envelopes:
        if not isinstance(e['data'], dict) or set(e['data']) != fields:
            return None
        if any(e[k] != first[k] for k in CONSTANT):
            return None
    props = (first.get('schema') or {}).get('properties') or {}
    kinds = {n: kind_of(props.get(n)) for n in names}
    if any(not _fits(kinds[n], e['data'][n]) for e in envelopes for n in names):
        return None
    types = {'bool': pa.bool_(), 'int64': pa.int64(), 'string': pa.string()}
    arrays = [
        pa.array([e['operation'] for e in envelopes], pa.string()),
        pa.array([_text(e['primary_key']) for e in envelopes], pa.string()),
        pa.array([_text(e.get('cursor')) for e in envelopes], pa.string()),
    ]
    for n in names:
        values = [e['data'][n] for e in envelopes]
        if kinds[n] == 'json':
            arrays.append(pa.array([None if v is None else _text(v) for v in values], pa.string()))
        else:
            arrays.append(pa.array(values, types[kinds[n]]))
    meta = {'layout': LAYOUT, **{k: first[k] for k in CONSTANT}, 'columns': kinds}
    table = pa.Table.from_arrays(arrays, names=[OPERATION, PRIMARY_KEY, CURSOR, *names])
    table = table.replace_schema_metadata({META: _text(meta).encode()})
    out = io.BytesIO()
    pq.write_table(table, out, compression='snappy', row_group_size=len(envelopes))
    return out.getvalue()


def decode(source):
    """The envelopes of a Parquet part (a path, file object or bytes), in order."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    if isinstance(source, (bytes, bytearray)):
        source = pa.BufferReader(source)
    f = pq.ParquetFile(source)
    raw = (f.schema_arrow.metadata or {}).get(META)
    if raw is None:
        raise ValueError('invalid_bronze_parquet')
    meta = json.loads(raw)
    if meta.get('layout') != LAYOUT or meta.get('protocol') != 'meridian-connect/1':
        raise ValueError('invalid_bronze_parquet')
    kinds = meta['columns']
    if any(k not in KINDS for k in kinds.values()):
        raise ValueError('invalid_bronze_parquet')
    table = f.read()
    if table.column_names != [OPERATION, PRIMARY_KEY, CURSOR, *kinds]:
        raise ValueError('invalid_bronze_parquet')
    cols = {n: table.column(n).to_pylist() for n in table.column_names}
    constant = {k: meta[k] for k in CONSTANT}
    out = []
    for i in range(table.num_rows):
        data = {}
        for n, kind in kinds.items():
            v = cols[n][i]
            data[n] = json.loads(v) if kind == 'json' and v is not None else v
        out.append({**constant, 'cursor': json.loads(cols[CURSOR][i]),
                    'primary_key': json.loads(cols[PRIMARY_KEY][i]),
                    'operation': cols[OPERATION][i], 'data': data})
    return out
