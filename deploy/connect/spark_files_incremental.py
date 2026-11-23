"""Bounded cross-run object/range state for Spark file ingestion.

This module does no Spark or control-plane work. The driver advances ``done`` only
when an immutable Bronze part has been accepted by the existing fenced API.
"""
import json
import source_files as sf

STATE_BYTES = 48 * 1024


class FileRun:
    def __init__(self, ledger, run_id):
        self.ledger = ledger
        self.run_id = run_id
        self.plan = []
        self.done = {}
        self.digests = {}
        self.objects = {}
        for key, obj in ledger['objects'].items():
            self.digests[key] = obj['sha256']
            if obj['done']:
                continue
            indices = []
            for entry in obj['ranges']:
                i = len(self.plan)
                self.plan.append(entry['range'])
                self.done[str(i)] = {'after': entry['after'], 'done': False}
                indices.append(i)
            self.objects[key] = indices
        if len(self.plan) > 4096:
            raise ValueError('connector_failed')
        if not self.done:
            self.done = {'0': {'after': None, 'done': True}}
        # Capacity is checked before the caller can submit any executor work.
        self.checkpoint(False)

    def checkpoint(self, completed):
        ledger = json.loads(json.dumps(self.ledger))
        for key, indices in self.objects.items():
            remaining = [{'range': self.plan[i], 'after': self.done[str(i)]['after']}
                         for i in indices if not self.done[str(i)]['done']]
            obj = ledger['objects'][key]
            obj['done'] = not remaining
            if remaining:
                obj['ranges'] = remaining
            else:
                obj.pop('ranges', None)
        state = {'version': 1, 'cursor': None, 'primary_key': None,
                 'completed': completed, 'files': ledger,
                 'spark': {'source': ledger['source'], 'run': self.run_id,
                           'mode': 'incremental', 'partitions': len(self.done),
                           'consistency': 'single'}}
        if len(json.dumps(state, ensure_ascii=True).encode()) > STATE_BYTES:
            raise ValueError('record_too_large')
        return {'type': 'STATE', 'state': state}


def prepare(fs_, config, stream, state, run_id, range_bytes):
    """Verify object identities and return only unfinished immutable ranges.

    Every new range is planned from the same bounded disk snapshot whose digest
    is persisted. Known unfinished objects keep their original boundaries even
    if the operator changes the preferred range size for a later run.
    """
    source = sf.source_identity(config, stream['schema'])
    ledger = state.get('files')
    if ledger is None:
        if state:
            raise sf.Changed()
        ledger = {'version': 2, 'source': source, 'objects': {}}
    else:
        ledger = json.loads(json.dumps(ledger))
    if (ledger.get('version') != 2 or ledger.get('source') != source
            or not isinstance(ledger.get('objects'), dict)):
        raise sf.Changed()
    fmt, bucket, _ = sf.settings(config)
    files = sf.listing(fs_, config)
    present = {key for key, _, _ in files}
    objects = ledger['objects']
    if any(not obj['done'] and key not in present for key, obj in objects.items()):
        raise sf.Changed()
    for key, size, mtime in files:
        mod = sf.modified(mtime)
        with sf.object_snapshot(fs_, f'{bucket}/{key}', size, mod) as (snapshot, digest):
            old = objects.get(key)
            if old is not None:
                if old['sha256'] != digest or old['size'] != size:
                    raise sf.Changed()
                # Unfinished ranges already bind this digest/schema; keep their exact plan.
                continue
            obj = {'sha256': digest, 'size': size, 'modified': mod, 'done': False}
            objects[key] = obj
            if not size and fmt not in ('parquet', 'avro'):
                obj['done'] = True
                continue
            found = sf.schema_of(snapshot, fmt)
            if fmt == 'jsonl':
                found['x-files']['columns'] = sf.columns_of(stream['schema'])
            if found != stream['schema']:
                raise ValueError('schema_drift')
            snapshot.seek(0)
            ranges = sf.plan_object(config, key, size, mod, range_bytes, snapshot, limit=4096)
            obj['ranges'] = [{'range': rng, 'after': None} for rng in ranges]
            obj['done'] = not ranges
            if obj['done']:
                obj.pop('ranges', None)
    return FileRun(ledger, run_id)
