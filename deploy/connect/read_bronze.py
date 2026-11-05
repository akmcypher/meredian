"""External owner-scoped Bronze reader. Downloads verified batches; never forwards the engine
token to storage. Optional --query-count executes a real DataFusion SQL acceptance query."""
import argparse, hashlib, json, pathlib, urllib.request, urllib.parse

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs): return None

def read(args):
    base=args.app.rstrip('/'); parsed=urllib.parse.urlparse(base)
    if parsed.scheme!='https' and not (parsed.scheme=='http' and parsed.hostname in ('127.0.0.1','localhost','::1')):
        raise ValueError('Use HTTPS except for a loopback development app')
    token=pathlib.Path(args.token_file).read_text().strip()
    output=pathlib.Path(args.output); output.mkdir(parents=True,exist_ok=False)
    opener=urllib.request.build_opener(NoRedirect())
    after=-1; total=0; count=0; identity=None; seen=set()
    while True:
        body=json.dumps({'bronze':{'streamId':args.stream,'runId':args.run,'afterSequence':after}}).encode()
        req=urllib.request.Request(base+'/api/public/serve-manifest',data=body,
            headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
        with opener.open(req,timeout=30) as response: raw=response.read(4*1024*1024+1)
        if len(raw)>4*1024*1024: raise ValueError('manifest_too_large')
        page=json.loads(raw)['bronze']
        if page['streamId']!=args.stream or page['runId']!=args.run: raise ValueError('snapshot_mismatch')
        if identity is not None and identity!=page['datasetId']: raise ValueError('dataset_changed')
        identity=page['datasetId']
        if not 1<=len(page['parts'])<=25: raise ValueError('invalid_page')
        for part in page['parts']:
            seq=part['sequence']
            if not isinstance(seq,int) or seq<=after or seq in seen: raise ValueError('invalid_sequence')
            if part['datasetId']!=identity or part['namespace']!='connect' or part['format']!='ndjson':
                raise ValueError('invalid_part')
            if part['producedBy']['runId']!=args.run: raise ValueError('run_mismatch')
            expected=part['location']['bytes']
            if not isinstance(expected,int) or not 0<=expected<=8*1024*1024: raise ValueError('part_too_large')
            digest=hashlib.sha256(); size=0; target=output/f'{seq:010d}.ndjson'
            # A storage request deliberately carries no Authorization header from the app.
            with opener.open(part['location']['url'],timeout=30) as data, target.open('xb') as dest:
                while chunk:=data.read(65536):
                    size+=len(chunk)
                    if size>expected: raise ValueError('part_size_mismatch')
                    digest.update(chunk);dest.write(chunk)
            if size!=expected or digest.hexdigest()!=part['location']['sha256']: raise ValueError('part_digest_mismatch')
            with target.open('rb') as data: rows=sum(1 for line in data if line.strip())
            if rows!=part['rows']: raise ValueError('part_row_count_mismatch')
            total+=rows;count+=1;seen.add(seq)
        cursor=page['nextSequence']
        if cursor is None: break
        if not isinstance(cursor,int) or cursor<=after or cursor!=page['parts'][-1]['sequence']:
            raise ValueError('invalid_cursor')
        after=cursor
    result={'datasetId':identity,'runId':args.run,'parts':count,'rows':total}
    if args.query_count:
        from datafusion import SessionContext
        ctx=SessionContext()
        if total:
            ctx.register_json('bronze',str(output),file_extension='.ndjson')
        else:
            import pyarrow as pa
            ctx.register_record_batches('bronze',[[pa.record_batch({'_empty':pa.array([],type=pa.int8())})]])
        result['sqlRows']=ctx.sql('SELECT count(*) AS n FROM bronze').collect()[0].to_pylist()[0]['n']
        if result['sqlRows']!=total: raise ValueError('external_query_count_mismatch')
    (output/'COMPLETE.json').write_text(json.dumps(result),encoding='utf-8')
    return result

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    for flag in ('app','token-file','stream','run','output'): parser.add_argument('--'+flag,required=True)
    parser.add_argument('--query-count',action='store_true')
    print(json.dumps(read(parser.parse_args())))
