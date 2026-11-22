"""Concurrent driver commits may never publish another task's rejected progress."""
import copy
import sys
import threading
import types
import unittest
from unittest.mock import patch

import spark_ingest as spark


class DriverCommitFailure(unittest.TestCase):
    def test_failed_commit_aborts_other_tasks_before_their_checkpoint_can_include_it(self):
        failed = threading.Event()
        accepted = []
        done = {'0': {'after': None, 'done': False}, '1': {'after': None, 'done': False}}
        job = {'claim': {'run_id': 'run', 'lease_token': 'lease',
                         'streams': [{'id': 'stream', 'sequence': 0}]}}

        class RDD:
            def __init__(self, values):
                self.values = values
            def map(self, function):
                self.function = function
                return self
            def collect(self):
                return [self.function(value) for value in self.values]

        class Context:
            defaultParallelism = 2
            def getConf(self):
                return {}
            def parallelize(self, values, _):
                return RDD(values)

        class Session:
            sparkContext = Context()
            def stop(self):
                pass
        class Builder:
            def appName(self, _):
                return self
            def getOrCreate(self):
                return Session()

        def load(i, _):
            if i == 0:
                self.assertTrue(failed.wait(3), 'second task must fail before first finishes reading')
            return [{'after': 1, 'part': str(i), 'rows': 1, 'bytes': 1,
                     'sha256': 'a' * 64, 'executor': 'worker'}]

        def api(_, payload):
            if payload['part'] == '1':
                failed.set()
                raise spark.Stop('destination_failed')
            accepted.append(copy.deepcopy(payload['state']))
            return {'sequence': payload['sequence'] + 1}

        module = types.ModuleType('pyspark.sql')
        module.SparkSession = types.SimpleNamespace(builder=Builder())
        with patch.dict(sys.modules, {'pyspark': types.ModuleType('pyspark'), 'pyspark.sql': module}), \
                patch.object(spark, 'require_secure'), patch.object(spark, 'api', side_effect=api):
            with self.assertRaises(spark.Stop) as failure:
                spark.drive(job, done, lambda complete: {'type': 'STATE', 'state':
                            {'completed': complete, 'progress': copy.deepcopy(done)}}, load)
        self.assertEqual(failure.exception.code, 'destination_failed')
        self.assertEqual(accepted, [], 'a rejected part must never become durable through another commit')


if __name__ == '__main__':
    unittest.main()
