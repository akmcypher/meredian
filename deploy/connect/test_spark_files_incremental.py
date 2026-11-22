"""Real-file contracts for distributed incremental object/range checkpoints."""
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow.fs as fs
import source_files as sf


class IncrementalSparkFiles(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / 'bucket' / 'p'
        self.root.mkdir(parents=True)
        self.local = fs.SubTreeFileSystem(self.tmp.name, fs.LocalFileSystem())
        self.config = {'bucket': 'bucket', 'prefix': 'p', 'format': 'csv',
                       'access_key_id': 'worker-secret', 'secret_access_key': 'private'}
        self.write('b.csv', range(20))
        self.schema = sf.discover(self.local, self.config)[0]['json_schema']
        self.stream = {'sync_mode': 'incremental', 'schema': self.schema}

    def write(self, key, ids):
        (self.root / key).write_text('id,name\n' + ''.join(f'{i},value{i}\n' for i in ids), encoding='utf8')

    def prepare(self, state=None, config=None, run='run-one', size=32):
        from spark_files_incremental import prepare
        return prepare(self.local, config or self.config, self.stream,
                       state or {}, run, size)

    def finish(self, work):
        for entry in work.done.values():
            entry['done'] = True
        return work.checkpoint(True)['state']

    def test_initial_plan_is_content_bound_and_completed_objects_are_not_reloaded(self):
        work = self.prepare()
        self.assertGreater(len(work.plan), 1)
        raw = (self.root / 'b.csv').read_bytes()
        self.assertEqual(work.digests['p/b.csv'], hashlib.sha256(raw).hexdigest())
        state = self.finish(work)
        self.assertEqual(state['files']['version'], 2)
        self.assertTrue(state['files']['objects']['p/b.csv']['done'])
        self.assertNotIn('worker-secret', json.dumps(state))
        self.assertNotIn('private', json.dumps(state))
        self.assertEqual(self.prepare(state, run='next').plan, [])

    def test_late_key_is_read_after_completed_object(self):
        state = self.finish(self.prepare())
        self.write('a.csv', [100, 101])
        late = self.prepare(state, run='next')
        self.assertEqual({r[0] for r in late.plan}, {'p/a.csv'})
        self.assertTrue(late.checkpoint(False)['state']['files']['objects']['p/b.csv']['done'])

    def test_partial_and_out_of_order_ranges_resume_across_new_run(self):
        work = self.prepare()
        candidates = [(i, list(sf.read_range(self.local, self.config, self.schema, r)))
                      for i, r in enumerate(work.plan)]
        target, rows = next((i, rows) for i, rows in candidates if len(rows) > 1)
        for i, entry in work.done.items():
            entry['done'] = int(i) != target
        work.done[str(target)]['after'] = rows[0][1]
        state = work.checkpoint(False)['state']
        resumed = self.prepare(state, run='different-run', size=1)
        self.assertEqual(resumed.plan, [work.plan[target]])
        self.assertEqual(resumed.done['0'], {'after': rows[0][1], 'done': False})
        self.assertEqual(self.prepare(self.finish(resumed), run='third').plan, [])

    def test_same_size_same_mtime_replacement_is_refused(self):
        state = self.finish(self.prepare())
        path = self.root / 'b.csv'
        before = path.stat()
        path.write_bytes(path.read_bytes().replace(b'value0', b'other0'))
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        with self.assertRaises(sf.Changed):
            self.prepare(state)

    def test_deleted_completed_object_remains_known_but_missing_partial_fails(self):
        partial = self.prepare().checkpoint(False)['state']
        complete = self.finish(self.prepare())
        (self.root / 'b.csv').unlink()
        with self.assertRaises(sf.Changed):
            self.prepare(partial)
        resumed = self.prepare(complete)
        self.assertEqual(resumed.plan, [])
        self.assertTrue(resumed.checkpoint(True)['state']['files']['objects']['p/b.csv']['done'])
        self.write('b.csv', [999])
        with self.assertRaises(sf.Changed):
            self.prepare(complete)

    def test_source_changes_and_legacy_layout_require_reset_but_credentials_may_rotate(self):
        state = self.finish(self.prepare())
        rotated = {**self.config, 'secret_access_key': 'rotated'}
        self.assertEqual(self.prepare(state, config=rotated).plan, [])
        for changed in ({'prefix': 'elsewhere'}, {'provider': 'adls', 'account_name': 'new'}):
            with self.subTest(changed=changed), self.assertRaises(sf.Changed):
                self.prepare(state, config={**self.config, **changed})
        for legacy in ({'version': 1, 'completed': True},
                       {'files': {'version': 1, 'source': sf.source_identity(self.config, self.schema), 'objects': {}}}):
            with self.subTest(legacy=legacy), self.assertRaises(sf.Changed):
                self.prepare(legacy)
        self.assertGreater(len(self.prepare({}).plan), 0)

    def test_empty_text_object_completes_durably(self):
        (self.root / 'empty.csv').write_bytes(b'')
        work = self.prepare()
        state = self.finish(work)
        self.assertTrue(state['files']['objects']['p/empty.csv']['done'])
        self.assertEqual(self.prepare(state).plan, [])

    def test_all_formats_keep_global_positions_and_checkpoint_completion(self):
        import csv
        import pyarrow as pa
        import pyarrow.parquet as pq
        from fastavro import writer
        from spark_files_incremental import prepare
        for fmt in ('csv', 'tsv', 'jsonl', 'parquet', 'avro'):
            with self.subTest(format=fmt):
                root = Path(self.tmp.name) / 'bucket' / fmt
                root.mkdir()
                records = [{'id': i, 'name': 'row'+str(i)} for i in range(20)]
                path = root / ('a.'+fmt)
                if fmt in ('csv', 'tsv'):
                    with path.open('w', encoding='utf8', newline='') as f:
                        out = csv.writer(f, delimiter=',' if fmt == 'csv' else '\t')
                        out.writerow(['id', 'name'])
                        out.writerows((r['id'], r['name']) for r in records)
                elif fmt == 'jsonl':
                    path.write_text(''.join(json.dumps(r)+'\n' for r in records), encoding='utf8')
                elif fmt == 'parquet':
                    pq.write_table(pa.Table.from_pylist(records), path, row_group_size=3)
                else:
                    with path.open('wb') as f:
                        writer(f, {'type': 'record', 'name': 'Row', 'fields':
                                   [{'name': 'id', 'type': 'long'}, {'name': 'name', 'type': 'string'}]},
                               records, sync_interval=12)
                config = {'bucket': 'bucket', 'prefix': fmt, 'format': fmt,
                          'csv_quoted_newlines': fmt == 'csv'}
                stream = {'sync_mode': 'incremental', 'schema': sf.discover(self.local, config)[0]['json_schema']}
                work = prepare(self.local, config, stream, {}, 'run', 24)
                found = [data for rng in work.plan for data, _ in sf.read_range(self.local, config, stream['schema'], rng)]
                self.assertEqual([str(r['id']) for r in found], [str(i) for i in range(20)])
                self.assertEqual(len({(r['_file'], r['_position']) for r in found}), 20)
                state = self.finish(work)
                self.assertEqual(prepare(self.local, config, stream, state, 'next', 1).plan, [])

    def test_state_budget_is_checked_before_range_execution(self):
        from spark_files_incremental import prepare
        with patch('spark_files_incremental.STATE_BYTES', 100):
            with self.assertRaisesRegex(ValueError, 'record_too_large'):
                prepare(self.local, self.config, self.stream, {}, 'run', 1)

    def test_range_planning_stops_at_bound_before_allocating_an_unbounded_plan(self):
        with self.assertRaisesRegex(ValueError, 'record_too_large'):
            sf.plan_object(self.config, 'huge.csv', 10**15, None, 1, limit=4096)

    def test_driver_uses_range_ledger_for_incremental_runs(self):
        import spark_ingest as spark
        captured = []
        job = {'config': self.config, 'claim': {'run_id': 'run-one'}}
        def drive(job, done, checkpoint, load):
            done['0'] = {'after': 12, 'done': False}
            captured.append(checkpoint(False)['state'])
        with patch.object(sf, 'filesystem', return_value=self.local), patch.object(spark, 'drive', side_effect=drive):
            spark.main_files(job, self.stream, {}, None)
        self.assertIn('file_digests', job)
        self.assertEqual(captured[0]['files']['version'], 2)
        resumed = self.prepare(captured[0], run='another-run')
        self.assertEqual(resumed.done['0']['after'], 12)

    def test_driver_preserves_terminal_checkpoint_capacity_error(self):
        import spark_ingest as spark
        job = {'config': self.config, 'claim': {'run_id': 'run-one'}}
        with patch.object(sf, 'filesystem', return_value=self.local), patch.object(spark, 'drive', side_effect=ValueError('record_too_large')):
            with self.assertRaises(spark.Stop) as failure:
                spark.main_files(job, self.stream, {}, None)
        self.assertEqual(failure.exception.code, 'record_too_large')

    def test_reset_keeps_configured_full_refresh_mode_for_the_next_normal_sync(self):
        import spark_ingest as spark
        for operation in ('reset', 'backfill'):
            job = {'claim': {'run_id': 'run', 'source_version': 'spark-files-v1',
                            'operation': operation, 'streams': [{**self.stream, 'sync_mode': 'full_refresh',
                                                              'configured_sync_mode': 'full_refresh'}]}}
            with self.subTest(operation=operation), patch('sys.stdin', io.StringIO(json.dumps(job))), patch.object(spark, 'main_files') as run:
                spark.main()
                self.assertEqual(run.call_args.args[1]['sync_mode'], 'full_refresh')

    def test_reset_and_backfill_bootstrap_incremental_ledger(self):
        import spark_ingest as spark
        for operation in ('reset', 'backfill'):
            job = {'claim': {'run_id': 'run', 'source_version': 'spark-files-v1',
                            'operation': operation, 'streams': [{**self.stream, 'sync_mode': 'full_refresh', 'configured_sync_mode': 'incremental'}]}}
            with self.subTest(operation=operation), patch('sys.stdin', io.StringIO(json.dumps(job))), patch.object(spark, 'main_files') as run:
                spark.main()
                self.assertEqual(run.call_args.args[1]['sync_mode'], 'incremental')

    def test_executor_refuses_equal_metadata_replacement_before_upload(self):
        from spark_ingest import load_range, Failed
        work = self.prepare()
        path = self.root / 'b.csv'
        before = path.stat()
        path.write_bytes(path.read_bytes().replace(b'value0', b'other0'))
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        job = {'config': self.config, 'claim': {'streams': [self.stream]},
               'plan': work.plan, 'file_digests': work.digests}
        with patch.object(sf, 'filesystem', return_value=self.local), patch('spark_ingest.upload') as upload:
            with self.assertRaisesRegex(Failed, 'meridian:source_changed'):
                load_range(job, 0, None)
            upload.assert_not_called()


class SharedSnapshot(unittest.TestCase):
    def test_snapshot_hash_matches_parsed_bytes_and_closes_after_exit(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'object.csv'
            path.write_bytes(b'id\n1\n')
            local = fs.LocalFileSystem()
            info = local.get_file_info(str(path))
            with sf.object_snapshot(local, str(path), info.size, sf.modified(info.mtime)) as (snapshot, digest):
                path.write_bytes(b'id\n9\n')
                self.assertEqual(snapshot.read(), b'id\n1\n')
                self.assertEqual(digest, hashlib.sha256(b'id\n1\n').hexdigest())
            self.assertTrue(snapshot.closed)

    def test_snapshot_rejects_changed_listed_size(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'object.csv'
            path.write_bytes(b'id\n1\n')
            local = fs.LocalFileSystem()
            info = local.get_file_info(str(path))
            with self.assertRaises(sf.Changed):
                with sf.object_snapshot(local, str(path), info.size + 1, sf.modified(info.mtime)):
                    self.fail('invalid snapshot must not be exposed')


if __name__ == '__main__':
    unittest.main()
