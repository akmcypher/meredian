"""Offline checks of the CDC actors (deploy/connect/README.md, "Change data capture"): PostgreSQL pgoutput decoding
and positions, MySQL replica ids and value rules. Live behaviour is in
src/test/connect-cdc-postgres.e2e.test.ts and src/test/connect-cdc-mysql.e2e.test.ts."""
import struct, unittest
from source_postgres_cdc import NULL, UNCHANGED, Stop, decode, lsn_int, lsn_text, slot_name

def text(s): return s.encode()+b'\x00'
def tup(*values):
    out=struct.pack('>h',len(values))
    for v in values:
        if v is None: out+=b'n'
        elif v is UNCHANGED: out+=b'u'
        else: b=v.encode(); out+=b't'+struct.pack('>i',len(b))+b
    return out

class Decode(unittest.TestCase):
    def test_begin_and_commit_positions(self):
        self.assertEqual(decode(b'B'+struct.pack('>QQi',0x16B374D848,0,7)),('B',{'final':0x16B374D848}))
        self.assertEqual(decode(b'C'+struct.pack('>BQQQ',0,1,0x16B374D900,0)),('C',{'end':0x16B374D900}))
    def test_relation(self):
        body=struct.pack('>I',16384)+text('public')+text('orders')+b'd'+struct.pack('>h',2)
        body+=b'\x01'+text('id')+struct.pack('>Ii',23,-1)+b'\x00'+text('note')+struct.pack('>Ii',25,-1)
        self.assertEqual(decode(b'R'+body),('R',{'oid':16384,'namespace':'public','name':'orders','columns':[('id',23),('note',25)]}))
    def test_insert_update_delete(self):
        kind,m=decode(b'I'+struct.pack('>I',1)+b'N'+tup('1',None))
        self.assertEqual((kind,m['new']),('I',[b'1',NULL]))
        # Update with the old key (key changed) and an unchanged TOAST value in the new row.
        kind,m=decode(b'U'+struct.pack('>I',1)+b'K'+tup('1',None)+b'N'+tup('2',UNCHANGED))
        self.assertEqual((m['full'],m['old'],m['new']),(False,[b'1',NULL],[b'2',UNCHANGED]))
        kind,m=decode(b'U'+struct.pack('>I',1)+b'N'+tup('2','x'))
        self.assertEqual((m['old'],m['new']),(None,[b'2',b'x']))
        kind,m=decode(b'D'+struct.pack('>I',1)+b'O'+tup('2','x'))
        self.assertEqual((kind,m['full'],m['old']),('D',True,[b'2',b'x']))
    def test_truncate(self):
        self.assertEqual(decode(b'T'+struct.pack('>iB',2,0)+struct.pack('>II',5,6)),('T',{'oids':[5,6]}))
    def test_unknown_tuple_kind_fails_closed(self):
        with self.assertRaises(Stop): decode(b'I'+struct.pack('>I',1)+b'N'+struct.pack('>h',1)+b'b')

class Positions(unittest.TestCase):
    def test_lsn_round_trip_and_order(self):
        for t in ['0/0','0/16B3748','16/B374D848','FFFFFFFF/FFFFFFFF']: self.assertEqual(lsn_text(lsn_int(t)),t)
        self.assertLess(lsn_int('0/FFFFFFFF'),lsn_int('1/0'))
    def test_slot_name_is_stable_and_valid(self):
        a=slot_name({'id':'7d1c2a4e-0000-4000-8000-000000000001'})
        self.assertEqual(a,slot_name({'id':'7d1c2a4e-0000-4000-8000-000000000001'}))
        self.assertRegex(a,r'^meridian_[0-9a-f]{24}$')
        self.assertNotEqual(a,slot_name({'id':'7d1c2a4e-0000-4000-8000-000000000002'}))

class MySQL(unittest.TestCase):
    def test_server_id_is_stable_and_out_of_the_usual_range(self):
        import source_mysql_cdc as m
        a=m.server_id({'id':'7d1c2a4e-0000-4000-8000-000000000001'})
        self.assertEqual(a,m.server_id({'id':'7d1c2a4e-0000-4000-8000-000000000001'}))
        self.assertTrue(0x40000000<=a<0x7FFFFFFF)
    def test_binlog_json_matches_the_copy(self):
        import source_mysql_cdc as m
        columns={'doc':{'source_type':'json'},'ok':{'source_type':'tinyint(1)'},'n':{'source_type':'int'}}
        row={'doc':{b'k':[b'v',1,None]},'ok':1,'n':2**60}
        self.assertEqual(m.typed(row,columns),{'doc':{'k':['v',1,None]},'ok':True,'n':str(2**60)})
        self.assertEqual(m.typed({'doc':None,'ok':None,'n':None},columns),{'doc':None,'ok':None,'n':None})
    def test_positions_order_by_file_then_offset(self):
        import source_mysql_cdc as m
        self.assertLess(m.order({'file':'binlog.000009','position':999}),m.order({'file':'binlog.000010','position':4}))

if __name__=='__main__': unittest.main()
