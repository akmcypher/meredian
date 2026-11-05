"""Real HTTP boundary tests for the external reader, not control-plane certification."""
import argparse, hashlib, json, pathlib, tempfile, threading, unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from read_bronze import read

class BronzeReader(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=pathlib.Path(self.temp.name)
        (self.root/'token').write_text('short-lived-fixture-token')
        self.body='{"name":"é"}\n'.encode();self.bad_hash=False;self.redirect=False;self.headers=[]
        case=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args): pass
            def do_POST(self):
                request=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                case.headers.append((self.path,self.headers.get('Authorization')))
                part={'datasetId':'dataset','namespace':'connect','format':'ndjson','sequence':0,
                    'producedBy':{'runId':'run'},'rows':0 if not case.body else 1,
                    'location':{'bytes':len(case.body),'sha256':'0'*64 if case.bad_hash else hashlib.sha256(case.body).hexdigest(),
                        'url':case.url+'/data'}}
                self.send_response(200);self.end_headers()
                self.wfile.write(json.dumps({'bronze':{'datasetId':'dataset','runId':'run','streamId':'stream',
                    'parts':[part],'nextSequence':None}}).encode())
            def do_GET(self):
                case.headers.append((self.path,self.headers.get('Authorization')))
                if case.redirect:
                    self.send_response(302);self.send_header('Location',case.url+'/redirected');self.end_headers();return
                self.send_response(200);self.end_headers();self.wfile.write(case.body)
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.url=f'http://127.0.0.1:{self.server.server_port}'
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
    def tearDown(self):
        self.server.shutdown();self.server.server_close();self.thread.join();self.temp.cleanup()
    def args(self,query=False):
        return argparse.Namespace(app=self.url,token_file=str(self.root/'token'),stream='stream',run='run',
            output=str(self.root/'output'),query_count=query)
    def test_utf8_bytes_and_no_token_forwarding_to_storage(self):
        result=read(self.args())
        self.assertEqual(result['rows'],1)
        self.assertEqual(self.headers,[('/api/public/serve-manifest','Bearer short-lived-fixture-token'),('/data',None)])
        self.assertEqual((self.root/'output'/'0000000000.ndjson').read_bytes(),self.body)
    def test_bad_digest_never_marks_download_complete(self):
        self.bad_hash=True
        with self.assertRaisesRegex(ValueError,'digest'):read(self.args())
        self.assertFalse((self.root/'output'/'COMPLETE.json').exists())
    def test_redirect_is_not_followed(self):
        self.redirect=True
        with self.assertRaises(HTTPError):read(self.args())
        self.assertEqual([h[0] for h in self.headers],['/api/public/serve-manifest','/data'])
    def test_real_sql_over_an_empty_verified_run(self):
        self.body=b''
        result=read(self.args(query=True))
        self.assertEqual((result['rows'],result['sqlRows']),(0,0))
    def test_real_sql_over_downloaded_utf8_rows(self):
        result=read(self.args(query=True))
        self.assertEqual((result['rows'],result['sqlRows']),(1,1))

if __name__=='__main__':unittest.main()
