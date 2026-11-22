"""source_files.py on a local filesystem standing in for the bucket (the S3 path is covered live by
src/test/connect-spark-s3.e2e.test.ts). The property every split must keep: for any range size, the
ranges of a file read exactly the records one whole-file range reads, with the same keys."""
import json, os, subprocess, sys, tempfile, unittest
from pathlib import Path
import pyarrow as pa, pyarrow.parquet as pq
from unittest.mock import patch
from pyarrow import fs
sys.path.insert(0,str(Path(__file__).parent))
import source_files as sf
from fastavro import writer
CSV=('﻿id,name,note\r\n1,ann,"comma, inside"\r\n2,bob,plain\r\n\r\n3,"cé",""\r\n'
     +''.join(f'{i},n{i},"q ""{i}"""\r\n' for i in range(4,60))+'60,last,no newline at end')
JSONL=''.join(json.dumps({'id':i,'v':'é'*(i%5),'big':2**60+i,'nested':{'a':[i]}})+'\n' for i in range(80))
MULTI='id,text\n1,"line one\nline two"\n2,"x"\n3,"a\n\nb"\n'
AVRO_SCHEMA={'type':'record','name':'Event','fields':[{'name':'id','type':'long'},
    {'name':'name','type':['null','string'],'default':None},{'name':'nested','type':{'type':'array','items':'int'}}]}
def avro_file(path, rows, schema=AVRO_SCHEMA):
    with open(path,'wb') as f: writer(f,schema,rows,codec='deflate',sync_interval=40)

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
    def test_avro_blocks_have_stable_keys_and_full_refresh_resume(self):
        root=Path(self.dir)/'bucket'/'avro'; root.mkdir()
        avro_file(root/'a.avro',[{'id':i,'name':None if i%2 else 'é','nested':[i]} for i in range(40)])
        config=self.config('avro','avro')
        whole=self.assertSplitInvariant(config,[1,50,200,100000])
        self.assertEqual([p for _,p in whole],list(range(40)))
        self.assertEqual(whole[2][0]['nested'],[2])
        self.assertGreater(len(sf.plan(self.fs,config,1)),1)
        schema=sf.discover(self.fs,config)[0]['json_schema']
        stream={'name':'avro','namespace':'bucket','schema':schema}
        rows=[m['record']['data']['id'] for m in sf.read(self.fs,config,stream,{'primary_key':['avro/a.avro',19]}) if m['type']=='RECORD']
        self.assertEqual(rows,list(range(20,40)))
        avro_file(root/'b.avro',[],schema={'type':'record','name':'Event','fields':[{'name':'id','type':'string'}]})
        with self.assertRaisesRegex(ValueError,'schema_drift'):
            list(sf.read(self.fs,config,stream,{}))
    def test_adls_filesystem_uses_worker_credentials_and_rejects_unknown_provider(self):
        config={'provider':'adls','account_name':'account','account_key':'worker-secret'}
        with patch('pyarrow.fs.AzureFileSystem') as azure:
            sf.filesystem(config)
            azure.assert_called_once_with(account_name='account',account_key='worker-secret')
        with patch('pyarrow.fs.AzureFileSystem') as azure:
            sf.filesystem({'provider':'adls','account_name':'account','tenant_id':'tenant','client_id':'client','client_secret':'secret'})
            azure.assert_called_once_with(account_name='account',tenant_id='tenant',client_id='client',client_secret='secret')
        for bad in ({'provider':'unknown'},{'provider':'adls'},{'provider':'adls','account_name':'https://bad'},
                    {'provider':'adls','account_name':'account','endpoint':'http://bad'}):
            with self.subTest(bad),self.assertRaisesRegex(ValueError,'connector_failed'): sf.filesystem(bad)
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

class IncrementalFiles(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)/'bucket'; self.root.mkdir()
        self.fs=fs.SubTreeFileSystem(self.tmp.name,fs.LocalFileSystem())
        self.config={'bucket':'bucket','format':'csv'}
        (self.root/'b.csv').write_text('id,v\n1,x\n2,y\n')
        self.stream={'name':'bucket','namespace':'bucket','sync_mode':'incremental',
                     'schema':sf.discover(self.fs,self.config)[0]['json_schema']}
    def read(self, checkpoint=None):
        return list(sf.read(self.fs,self.config,self.stream,checkpoint or {}))
    def rows(self, out): return [m['record']['data'] for m in out if m['type']=='RECORD']
    def checkpoint(self, out): return json.loads(json.dumps(out[-1]['state']))
    def test_completed_objects_are_skipped_but_late_keys_are_not(self):
        first=self.read(); cp=self.checkpoint(first)
        self.assertEqual([r['id'] for r in self.rows(first)],['1','2'])
        self.assertEqual(self.rows(self.read(cp)),[])
        (self.root/'a.csv').write_text('id,v\n3,z\n')
        later=self.read(cp)
        self.assertEqual([r['id'] for r in self.rows(later)],['3'])
        self.assertEqual(self.rows(self.read(self.checkpoint(later))),[])
    def test_every_record_has_a_durable_resume_position(self):
        out=self.read(); records=[i for i,m in enumerate(out) if m['type']=='RECORD']
        for i in records:
            self.assertEqual(out[i+1]['type'],'STATE')
            cp=json.loads(json.dumps(out[i+1]['state']))
            rest=self.rows(self.read(cp))
            self.assertEqual([r['id'] for r in rest],['2'] if i==records[0] else [])
    def test_same_size_replacement_with_preserved_mtime_fails_closed(self):
        cp=self.checkpoint(self.read()); path=self.root/'b.csv'; stat=path.stat()
        path.write_text('id,v\n1,z\n2,y\n'); os.utime(path,ns=(stat.st_atime_ns,stat.st_mtime_ns))
        with self.assertRaises(sf.Changed): self.read(cp)
    def test_deleted_completed_object_is_remembered_and_reappearance_checked(self):
        cp=self.checkpoint(self.read()); path=self.root/'b.csv'; raw=path.read_bytes(); path.unlink()
        missing=self.read(cp)
        self.assertEqual(self.rows(missing),[])
        path.write_bytes(raw)
        self.assertEqual(self.rows(self.read(self.checkpoint(missing))),[])
        path.write_text('id,v\n9,q\n')
        with self.assertRaises(sf.Changed): self.read(self.checkpoint(missing))
    def test_missing_partial_object_fails_instead_of_silently_completing(self):
        out=self.read(); cp=next(m['state'] for m in out if m['type']=='STATE')
        (self.root/'b.csv').unlink()
        with self.assertRaises(sf.Changed): self.read(cp)
    def test_empty_objects_are_committed_and_legacy_checkpoints_need_reset(self):
        (self.root/'b.csv').write_text('id,v\n')
        out=self.read(); self.assertEqual(self.rows(out),[])
        cp=self.checkpoint(out)
        self.assertTrue(cp['files']['objects']['b.csv']['done'])
        self.assertEqual(self.rows(self.read(cp)),[])
        with self.assertRaises(sf.Changed): self.read({'version':1,'primary_key':['b.csv',10]})
    def test_source_binding_changes_fail_but_rotating_secrets_does_not(self):
        cp=self.checkpoint(self.read())
        self.config['secret_access_key']='local-only-value'
        self.assertEqual(self.rows(self.read(cp)),[])
        self.assertNotIn('local-only-value',json.dumps(cp))
        self.config['csv_quoted_newlines']=True
        with self.assertRaises(sf.Changed): self.read(cp)
    def test_adls_identity_is_bound_but_secrets_may_rotate(self):
        self.config.update(provider='adls',account_name='first',account_key='worker-secret')
        cp=self.checkpoint(self.read())
        self.assertNotIn('worker-secret',json.dumps(cp))
        self.config.update(account_key='rotated',sas_token='rotated-sas')
        self.assertEqual(self.rows(self.read(cp)),[])
        self.config['account_name']='second'
        with self.assertRaises(sf.Changed): self.read(cp)
        self.config.update(account_name='first',provider='s3')
        with self.assertRaises(sf.Changed): self.read(cp)
    def test_avro_incremental_resume_append_and_replacement(self):
        self.config.update(format='avro')
        avro_file(self.root/'b.avro',[{'id':1,'name':None,'nested':[]},{'id':2,'name':'a','nested':[2]}])
        self.stream['schema']=sf.discover(self.fs,self.config)[0]['json_schema']
        out=self.read(); partial=next(m['state'] for m in out if m['type']=='STATE')
        self.assertEqual([r['id'] for r in self.rows(self.read(partial))],[2])
        cp=self.checkpoint(out)
        avro_file(self.root/'a.avro',[{'id':3,'name':None,'nested':[]}])
        self.assertEqual([r['id'] for r in self.rows(self.read(cp))],[3])
        avro_file(self.root/'b.avro',[{'id':9,'name':None,'nested':[]}])
        with self.assertRaises(sf.Changed): self.read(cp)
    def test_destination_response_loss_and_restart_do_not_duplicate_committed_rows(self):
        from destination_bronze import write
        committed=[]; payloads=[]; saved={}; attempts=0
        def post(config,payload):
            nonlocal saved,attempts
            payloads.append(json.loads(json.dumps(payload)))
            if payload['sequence']==0:
                attempts+=1
                if attempts==1:
                    committed.extend(payload['records']); saved=payload['state']['state']
                    raise ConnectionError('ACK lost')
                self.assertEqual(payloads[0],payloads[1])
                return {'sequence':1}
            raise ValueError('record_too_large')  # fail definitively before the second commit
        dest={'sequence':0,'state':{},'schema':self.stream['schema'],'run_id':'r','stream_id':'s','lease_token':'t'}
        with patch('destination_bronze.post',post),patch('destination_bronze.time.sleep'):
            with self.assertRaisesRegex(ValueError,'record_too_large'):
                list(write(dest,sf.read(self.fs,self.config,self.stream,{})))
        def resumed(config,payload):
            committed.extend(payload['records']); return {'sequence':payload['sequence']+1}
        with patch('destination_bronze.post',resumed):
            list(write({**dest,'sequence':1,'state':saved},sf.read(self.fs,self.config,self.stream,saved)))
        self.assertEqual([m['record']['data']['id'] for m in committed],['1','2'])
    def test_all_formats_keep_incremental_resume_positions(self):
        for fmt,raw,extra in [('jsonl',b'{"id":1}\n{"id":2}\n',{}),('tsv',b'id\tv\n1\tx\n2\ty\n',{}),
                              ('csv',b'id,v\n1,"a\nb"\n2,c\n',{'csv_quoted_newlines':True}),('parquet',None,{})]:
            with self.subTest(fmt):
                prefix=self.root/fmt; prefix.mkdir(); path=prefix/f'a.{fmt}'
                if raw is None: pq.write_table(pa.table({'id':[1,2]}),path,row_group_size=1)
                else: path.write_bytes(raw)
                config={**self.config,'format':fmt,'prefix':fmt,**extra}
                stream={**self.stream,'schema':sf.discover(self.fs,config)[0]['json_schema']}
                out=list(sf.read(self.fs,config,stream,{})); cp=next(m['state'] for m in out if m['type']=='STATE')
                self.assertEqual([str(r['id']) for r in self.rows(out)],['1','2'])
                self.assertEqual([str(r['id']) for r in self.rows(list(sf.read(self.fs,config,stream,cp)))],['2'])
    def test_ledger_exhaustion_emits_no_rows_for_an_untracked_object(self):
        cp=self.checkpoint(self.read())
        entry=cp['files']['objects']['b.csv']
        cp['files']['objects'].update({f'{i:04}-'+('x'*100):entry for i in range(400)})
        messages=sf.read(self.fs,self.config,self.stream,cp)
        with self.assertRaisesRegex(ValueError,'record_too_large'): next(messages)
    def test_parsed_bytes_match_snapshot_even_if_source_changes_and_temp_files_are_removed(self):
        original=sf.read_range; directories=[]; temporary=sf.tempfile.TemporaryDirectory
        def snapshot_range(*args,**kw):
            (self.root/'b.csv').write_text('id,v\n9,z\n')
            yield from original(*args,**kw)
        def directory(*args,**kw):
            result=temporary(*args,**kw); directories.append(result.name); return result
        with patch.object(sf,'read_range',snapshot_range),patch.object(sf.tempfile,'TemporaryDirectory',directory):
            out=self.read()
        self.assertEqual([r['id'] for r in self.rows(out)],['1','2'])
        self.assertTrue(directories)
        self.assertTrue(all(not Path(d).exists() for d in directories))
        with self.assertRaises(sf.Changed): self.read(self.checkpoint(out))
    def test_new_jsonl_object_with_changed_schema_fails_before_rows(self):
        root=self.root/'jsonl'; root.mkdir()
        (root/'b.jsonl').write_text('{"id":1}\n')
        config={**self.config,'format':'jsonl','prefix':'jsonl'}
        stream={**self.stream,'schema':sf.discover(self.fs,config)[0]['json_schema']}
        out=list(sf.read(self.fs,config,stream,{})); cp=self.checkpoint(out)
        for raw in ('{"id":2,"extra":"unexpected"}\n','{"id":"wrong type"}\n'):
            with self.subTest(raw):
                (root/'a.jsonl').write_text(raw)
                messages=sf.read(self.fs,config,stream,cp)
                with self.assertRaisesRegex(ValueError,'schema_drift'): next(messages)
    def test_zero_byte_text_objects_are_durable_empty_objects(self):
        cp=self.checkpoint(self.read())
        (self.root/'a.csv').write_bytes(b'')
        out=self.read(cp); self.assertEqual(self.rows(out),[])
        cp=self.checkpoint(out)
        self.assertTrue(cp['files']['objects']['a.csv']['done'])
        self.assertEqual(self.rows(self.read(cp)),[])
    def test_jsonl_member_order_is_not_schema_drift(self):
        root=self.root/'jsonl'; root.mkdir()
        (root/'b.jsonl').write_text('{"id":1,"name":"a"}\n')
        config={**self.config,'format':'jsonl','prefix':'jsonl'}
        stream={**self.stream,'schema':sf.discover(self.fs,config)[0]['json_schema']}
        cp=self.checkpoint(list(sf.read(self.fs,config,stream,{})))
        (root/'a.jsonl').write_text('{"name":"b","id":2}\n')
        out=list(sf.read(self.fs,config,stream,cp))
        self.assertEqual([r['id'] for r in self.rows(out)],[2])

if __name__=='__main__': unittest.main()
