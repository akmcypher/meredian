"""Kafka secured-cluster certification (Phase 3 K1) against deploy/connect/certify-secure.compose.yml:
SASL/SCRAM-SHA-512 and SASL/PLAIN over TLS, and mutual TLS. The actor runs as the agent runs it
(a child process with a config file in a private directory, file paths relative to it). Each
accepted mode reads real records end to end; every wrong credential, untrusted CA, hostname
mismatch or missing client certificate is refused without reading anything."""
import json, os, shutil, socket, subprocess, sys, tempfile, unittest, uuid
from pathlib import Path
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
ROOT=Path(__file__).parent
CERTS=Path(os.environ.get('KAFKA_SECURE_CERTS',ROOT/'certify-secure'/'out'))
SASL=os.environ.get('KAFKA_SECURE_SASL','localhost:39291')
MTLS=os.environ.get('KAFKA_SECURE_MTLS','localhost:39292')
SCRAM={'security_protocol':'SASL_SSL','sasl_mechanism':'SCRAM-SHA-512','sasl_username':'meridian-scram','sasl_password':'meridian-test-scram','ssl_ca_location':'ca.pem'}
PLAIN={'security_protocol':'SASL_SSL','sasl_mechanism':'PLAIN','sasl_username':'meridian-plain','sasl_password':'meridian-test-plain','ssl_ca_location':'ca.pem'}
MUTUAL={'security_protocol':'SSL','ssl_ca_location':'ca.pem','ssl_keystore_location':'client.p12','ssl_keystore_password':'meridian-test-store'}
def reachable():
    if not (CERTS/'ready').exists(): return False
    try: socket.create_connection(('127.0.0.1',int(SASL.rsplit(':',1)[1])),timeout=1).close(); return True
    except OSError: return False
def librdkafka(cfg, base):
    """The same security settings as librdkafka properties, for the test's own producer/admin."""
    sys.path.insert(0,str(ROOT)); import source_kafka
    return source_kafka.client_config(source_kafka.resolve_files(cfg,str(base)))
@unittest.skipUnless(reachable(),'secured Kafka not running (deploy/connect/certify-secure.compose.yml)')
class SecuredKafka(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir=Path(tempfile.mkdtemp(prefix='kafka-secure-'))
        for f in ('ca.pem','rogue-ca.pem','client.p12','rogue-client.p12'): shutil.copy(CERTS/f,cls.dir/f)
        cls.topic=f'secure_{uuid.uuid4().hex[:10]}'
        admin=AdminClient(librdkafka({'bootstrap_servers':SASL,**SCRAM},cls.dir))
        admin.create_topics([NewTopic(cls.topic,num_partitions=2,replication_factor=1)])[cls.topic].result(30)
        p=Producer(librdkafka({'bootstrap_servers':SASL,**SCRAM},cls.dir))
        for i in range(10): p.produce(cls.topic,json.dumps({'id':i}).encode(),partition=i%2)
        p.flush(30)
    @classmethod
    def tearDownClass(cls):
        try: AdminClient(librdkafka({'bootstrap_servers':SASL,**SCRAM},cls.dir)).delete_topics([cls.topic])[cls.topic].result(30)
        except Exception: pass
        shutil.rmtree(cls.dir,ignore_errors=True)
    def actor(self, command, config, stream=None, state=None):
        cfg=self.dir/f'{uuid.uuid4().hex}.json'; cfg.write_text(json.dumps(config))
        args=[sys.executable,str(ROOT/'source_kafka.py'),command,'--config',str(cfg)]
        for name,value in (('catalog',stream),('state',state)):
            if value is not None:
                f=self.dir/f'{uuid.uuid4().hex}.json'; f.write_text(json.dumps(value)); args+=['--'+name,str(f)]
        out=subprocess.run(args,capture_output=True,text=True,timeout=120)
        return out.returncode,[json.loads(l) for l in out.stdout.splitlines() if l.strip()]
    def code(self, messages):
        return next((m['trace']['code'] for m in messages if m['type']=='TRACE'),None)
    def read_all(self, config):
        rc,cat=self.actor('discover',config)
        self.assertEqual(rc,0,cat)
        stream=next(s for s in cat[0]['catalog']['streams'] if s['json_schema']['x-kafka-topic']==self.topic)
        stream={'name':stream['name'],'namespace':stream['namespace'],'schema':stream['json_schema']}
        rc,out=self.actor('read',config,stream,{})
        self.assertEqual(rc,0,out)
        return sorted(m['record']['data']['id'] for m in out if m['type']=='RECORD'),[m for m in out if m['type']=='STATE'][-1]['state']
    def test_scram_sha_512_over_tls_reads_every_record(self):
        ids,state=self.read_all({'bootstrap_servers':SASL,**SCRAM})
        self.assertEqual(ids,list(range(10)))
        self.assertEqual(state['positions'],{'0':'5','1':'5'})
    def test_plain_over_tls_reads_every_record(self):
        self.assertEqual(self.read_all({'bootstrap_servers':SASL,**PLAIN})[0],list(range(10)))
    def test_mutual_tls_reads_every_record(self):
        self.assertEqual(self.read_all({'bootstrap_servers':MTLS,**MUTUAL})[0],list(range(10)))
    def test_one_partition_task_reads_only_its_partition(self):
        config={'bootstrap_servers':SASL,**SCRAM}
        rc,plan=self.actor('discover',config)
        stream=next(s for s in plan[0]['catalog']['streams'] if s['json_schema']['x-kafka-topic']==self.topic)
        stream={'name':stream['name'],'namespace':stream['namespace'],'schema':stream['json_schema'],'shard':'1'}
        rc,shards=self.actor('plan',config,stream)
        self.assertEqual(shards[-1]['plan']['shards'],['0','1'])
        rc,out=self.actor('read',config,stream,{})
        self.assertEqual(rc,0,out)
        self.assertEqual({m['record']['data']['_kafka']['partition'] for m in out if m['type']=='RECORD'},{1})
        self.assertEqual([m for m in out if m['type']=='STATE'][-1]['state']['positions'],{'1':'5'})
    def test_wrong_credentials_untrusted_ca_and_hostname_mismatch_are_auth_failed(self):
        for name,config in {
            'wrong scram password':{'bootstrap_servers':SASL,**SCRAM,'sasl_password':'wrong'},
            'wrong plain password':{'bootstrap_servers':SASL,**PLAIN,'sasl_password':'wrong'},
            'unknown user':{'bootstrap_servers':SASL,**SCRAM,'sasl_username':'nobody'},
            'untrusted broker CA':{'bootstrap_servers':SASL,**SCRAM,'ssl_ca_location':'rogue-ca.pem'},
            # The broker certificate names localhost only.
            'hostname mismatch':{'bootstrap_servers':SASL.replace('localhost','127.0.0.1'),**SCRAM},
        }.items():
            with self.subTest(name):
                rc,out=self.actor('check',config)
                self.assertNotEqual(rc,0)
                self.assertEqual(self.code(out),'auth_failed')
                self.assertFalse(any(m['type']=='CONNECTION_STATUS' for m in out))
    def test_mtls_without_a_trusted_client_certificate_reads_nothing(self):
        # The broker drops the connection; the client cannot tell that from a network fault, so the
        # run is retried (source_unavailable) and then fails. It never connects or reads.
        for name,config in {
            'no client certificate':{'bootstrap_servers':MTLS,'security_protocol':'SSL','ssl_ca_location':'ca.pem'},
            'client certificate from another CA':{'bootstrap_servers':MTLS,**MUTUAL,'ssl_keystore_location':'rogue-client.p12'},
            'plaintext to a TLS listener':{'bootstrap_servers':SASL},
        }.items():
            with self.subTest(name):
                rc,out=self.actor('check',config)
                self.assertNotEqual(rc,0)
                self.assertIn(self.code(out),('auth_failed','source_unavailable'))
                self.assertFalse(any(m['type']=='CONNECTION_STATUS' for m in out))
    def test_unsafe_settings_are_refused_or_flagged(self):
        # PLAIN without TLS would send the password in clear text: refused unless explicitly allowed.
        rc,out=self.actor('check',{'bootstrap_servers':SASL,'security_protocol':'SASL_PLAINTEXT','sasl_mechanism':'PLAIN','sasl_username':'a','sasl_password':'b'})
        self.assertEqual(self.code(out),'connector_failed')
        for bad in ({'security_protocol':'SASL_SSL','sasl_mechanism':'GSSAPI','sasl_username':'a','sasl_password':'b'},
                    {'security_protocol':'SASL_SSL','sasl_mechanism':'SCRAM-SHA-512','sasl_username':'a'},
                    {'security_protocol':'TLS'},
                    {'security_protocol':'SSL','ssl_endpoint_identification_algorithm':'maybe'}):
            with self.subTest(bad):
                self.assertEqual(self.code(self.actor('check',{'bootstrap_servers':SASL,**bad})[1]),'connector_failed')
        # Turning hostname verification off works but is never silent.
        rc,out=self.actor('check',{'bootstrap_servers':SASL.replace('localhost','127.0.0.1'),**SCRAM,'ssl_endpoint_identification_algorithm':'none'})
        self.assertEqual(rc,0,out)
        self.assertIn('tls_hostname_verification_disabled',[m['log']['code'] for m in out if m['type']=='LOG'])
    def test_registry_credentials_never_travel_over_plain_http(self):
        rc,out=self.actor('check',{'bootstrap_servers':SASL,**SCRAM,'schema_registry_url':'http://127.0.0.1:9','schema_registry_username':'u','schema_registry_password':'p'})
        self.assertEqual(self.code(out),'connector_failed')
    def test_secrets_never_appear_in_actor_output(self):
        for config in ({'bootstrap_servers':SASL,**SCRAM,'sasl_password':'wrong-secret-value'},{'bootstrap_servers':SASL,**SCRAM}):
            rc,out=self.actor('check',config)
            text=json.dumps(out)
            self.assertNotIn(config['sasl_password'],text)
            self.assertNotIn('meridian-test-store',text)
if __name__=='__main__': unittest.main()
