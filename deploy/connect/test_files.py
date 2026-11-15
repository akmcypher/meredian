"""source_files.py on a local filesystem standing in for the bucket (the S3 path is covered live by
src/test/connect-spark-s3.e2e.test.ts). The property every split must keep: for any range size, the
ranges of a file read exactly the records one whole-file range reads, with the same keys."""
import json, os, subprocess, sys, tempfile, unittest
from pathlib import Path
import pyarrow as pa, pyarrow.parquet as pq
from pyarrow import fs
sys.path.insert(0,str(Path(__file__).parent))
import source_files as sf
CSV=('﻿id,name,note\r\n1,ann,"comma, inside"\r\n2,bob,plain\r\n\r\n3,"cé",""\r\n'
     +''.join(f'{i},n{i},"q ""{i}"""\r\n' for i in range(4,60))+'60,last,no newline at end')
JSONL=''.join(json.dumps({'id':i,'v':'é'*(i%5),'big':2**60+i,'nested':{'a':[i]}})+'\n' for i in range(80))
MULTI='id,text\n1,"line one\nline two"\n2,"x"\n3,"a\n\nb"\n'
class Files(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir=tempfile.mkdtemp(prefix='files-'); root=Path(cls.dir)/'bucket'
        (root/'csv').mkdir(parents=True); (root/'jsonl').mkdir(); (root/'pq').mkdir(); (root/'multi').mkdir(); (root/'tsv').mkdir()
        (root/'csv'/'a.csv').write_bytes(CSV.encode()); (root/'csv'/'b.csv').write_bytes(CSV.replace('\r\n','\n').encode())
        (root/'csv'/'skip.txt').write_text('ignored')
        (root/'tsv'/'a.tsv').write_text('id\tv\n1\tx\n2\ty\n')
        (root/'jsonl'/'a.jsonl').write_bytes(JSONL.encode())
        (root/'multi'/'a.csv').write_bytes(MULTI.encode())
        t=pa.table({'id':pa.array(range(1000),pa.int64()),'amount':pa.array([i/4 for i in range(1000)]),'name':pa.array([f'r{i}' for i in range(1000)])})
        pq.write_table(t,root/'pq'/'a.parquet',row_group_size=64); pq.write_table(t.slice(0,10),root/'pq'/'b.parquet',row_group_size=3)
        cls.fs=fs.SubTreeFileSystem(cls.dir,fs.LocalFileSystem())
    def config(self, fmt, prefix, **kw): return {'bucket':'bucket','prefix':prefix,'format':fmt,**kw}
    def records(self, config, range_bytes):
        schema=sf.discover(self.fs,config)[0]['json_schema']
        # The stored schema is jsonb, which keeps no key order: readers must not depend on it.
        schema={**schema,'properties':dict(reversed(list(schema['properties'].items())))}
        return [r for rng in sf.plan(self.fs,config,range_bytes) for r in sf.read_range(self.fs,config,schema,rng)]
    def assertSplitInvariant(self, config, sizes):
        whole=self.records(config,10**12)
        self.assertTrue(whole)
        keys=[(d['_file'],p) for d,p in whole]
        self.assertEqual(len(keys),len(set(keys)))
        for size in sizes:
            with self.subTest(range_bytes=size):
                self.assertEqual(self.records(config,size),whole)
        return whole
    def test_csv_ranges_read_every_record_once_for_any_range_size(self):
        whole=self.assertSplitInvariant(self.config('csv','csv'),range(1,len(CSV)+3))
        a=[d for d,_ in whole if d['_file']=='csv/a.csv']
        self.assertEqual(len(a),60)
        self.assertEqual(a[0]['id'],'1'); self.assertEqual(a[0]['note'],'comma, inside')
        self.assertEqual(a[2]['name'],'cé'); self.assertEqual(a[3]['note'],'q "4"')
        self.assertEqual(a[-1]['note'],'no newline at end')
        # Positions are byte offsets of the record in the object.
        raw=CSV.encode(); self.assertTrue(raw[a[1]['_position']:].startswith(b'2,bob'))
        self.assertEqual({d['_file'] for d,_ in whole},{'csv/a.csv','csv/b.csv'})
    def test_tsv_and_jsonl(self):
        self.assertEqual([d['v'] for d,_ in self.records(self.config('tsv','tsv'),3)],['x','y'])
        whole=self.assertSplitInvariant(self.config('jsonl','jsonl'),[1,7,64,500,4096])
        self.assertEqual([d['id'] for d,_ in whole],list(range(80)))
        self.assertEqual(whole[5][0]['big'],str(2**60+5))
        self.assertEqual(whole[5][0]['nested'],{'a':[5]})
    def test_parquet_row_group_ranges(self):
        config=self.config('parquet','pq')
        whole=self.assertSplitInvariant(config,[1,2000,5000,10**6])
        self.assertEqual(len(whole),1010)
        a=[(d,p) for d,p in whole if d['_file']=='pq/a.parquet']
        self.assertEqual([p for _,p in a],list(range(1000)))
        self.assertEqual(a[999][0],{'id':999,'amount':249.75,'name':'r999','_file':'pq/a.parquet','_position':999,'_modified':a[0][0]['_modified']})
        self.assertGreater(len(sf.plan(self.fs,config,1)),16)
    def test_quoted_newlines_keep_one_range_per_file(self):
        config=self.config('csv','multi',csv_quoted_newlines=True)
        self.assertEqual(len(sf.plan(self.fs,config,1)),1)
        self.assertEqual([d['text'] for d,_ in self.records(config,1)],['line one\nline two','x','a\n\nb'])
    def test_discovery_schema_and_key(self):
        s=sf.discover(self.fs,self.config('csv','csv'))[0]
        self.assertEqual((s['namespace'],s['name'],s['primary_key'],s['cursor_field']),('bucket','csv',['_file','_position'],'_modified'))
        self.assertEqual(list(s['json_schema']['properties'])[:3],['id','name','note'])
        self.assertEqual(s['json_schema']['x-files'],{'format':'csv','columns':['id','name','note']})
        self.assertEqual(sf.stream_of({'bucket':'my-bucket.v2','prefix':'raw/2026-orders','format':'csv'}),('my_bucket_v2','_2026_orders'))
    def test_changed_object_and_changed_header_are_refused(self):
        root=Path(self.dir)/'bucket'/'drift'; root.mkdir()
        (root/'a.csv').write_text('id,v\n1,x\n'); (root/'b.csv').write_text('id,w\n1,x\n')
        config=self.config('csv','drift')
        schema=sf.discover(self.fs,config)[0]['json_schema']; ranges=sf.plan(self.fs,config,10**6)
        list(sf.read_range(self.fs,config,schema,ranges[0]))
        with self.assertRaisesRegex(ValueError,'schema_drift'): list(sf.read_range(self.fs,config,schema,ranges[1]))
        (root/'a.csv').write_text('id,v\n1,x\n2,y\n')
        with self.assertRaises(sf.Changed): list(sf.read_range(self.fs,config,schema,ranges[0]))
    def test_unsafe_settings_are_refused(self):
        for bad in ({'bucket':'b','format':'xml'},{'bucket':'a/b','format':'csv'},{'bucket':'b','prefix':'x/../y','format':'csv'}):
            with self.subTest(bad), self.assertRaisesRegex(ValueError,'connector_failed'): sf.settings(bad)
        with self.assertRaisesRegex(ValueError,'connector_failed'): sf.filesystem({'endpoint':'http://127.0.0.1:9'})
        with self.assertRaisesRegex(ValueError,'connector_failed'): sf.filesystem({'endpoint':'ftp://x'})
    def test_single_worker_read_resumes_after_its_checkpoint(self):
        config=self.config('csv','csv',range_bytes=40)
        stream={'name':'csv','namespace':'bucket','schema':sf.discover(self.fs,config)[0]['json_schema']}
        out=list(sf.read(self.fs,config,stream,{}))
        recs=[m['record'] for m in out if m['type']=='RECORD']
        self.assertEqual(len(recs),120); self.assertTrue(out[-1]['state']['completed'])
        cp=recs[70]['primary_key']
        rest=[m['record']['primary_key'] for m in sf.read(self.fs,config,stream,{'primary_key':cp}) if m['type']=='RECORD']
        self.assertEqual(rest,[r['primary_key'] for r in recs[71:]])
if __name__=='__main__': unittest.main()
