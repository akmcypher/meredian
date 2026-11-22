"""Source cleanup boundaries; transport is controlled, SQL is parameterized."""
import unittest
from unittest.mock import MagicMock
import source_postgres_cdc as pg

class Retirement(unittest.TestCase):
    def test_cleanup_cannot_race_a_reader_holding_the_source_lock(self):
        admin=MagicMock()
        admin.execute.return_value.fetchone.return_value={'acquired':False,'plugin':'pgoutput','active':False}
        with self.assertRaises(pg.Stop): pg.cleanup(admin,{'id':'stream-1'})
        self.assertNotIn('pg_drop_replication_slot',str(admin.execute.call_args_list))
    def test_cleanup_drops_only_the_streams_deterministic_slot(self):
        stream={'id':'stream-1'}; admin=MagicMock()
        admin.execute.return_value.fetchone.side_effect=[{'acquired':True},{'slot_name':pg.slot_name(stream),'plugin':'pgoutput','database':'source','active':False}]
        pg.cleanup(admin,stream)
        self.assertEqual(admin.execute.call_args.args,('SELECT pg_drop_replication_slot(%s)',[pg.slot_name(stream)]))
    def test_missing_slot_is_idempotent(self):
        admin=MagicMock(); admin.execute.return_value.fetchone.side_effect=[{'acquired':True},None]
        pg.cleanup(admin,{'id':'stream-1'})
        self.assertEqual(admin.execute.call_count,2)
    def test_wrong_plugin_or_active_slot_is_not_dropped(self):
        for row in ({'plugin':'other','active':False},{'plugin':'pgoutput','active':True}):
            admin=MagicMock(); admin.execute.return_value.fetchone.side_effect=[{'acquired':True},row]
            with self.assertRaises(pg.Stop): pg.cleanup(admin,{'id':'stream-1'})
            self.assertEqual(admin.execute.call_count,2)

if __name__=='__main__': unittest.main()
