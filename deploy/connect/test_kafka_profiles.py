"""Managed Kafka profiles (Phase 3 K4). Offline: each profile's expansion, conflict refusal and the
MSK IAM SigV4 token (signed locally with fixed test keys, no AWS call). Live, against
deploy/connect/certify-secure.compose.yml when it runs: the profiles that use SCRAM, PLAIN or client
certificates connect and read like their managed service would (private CA supplied as a file)."""
import base64, json, os, shutil, subprocess, sys, tempfile, unittest, urllib.parse, uuid
from pathlib import Path
ROOT=Path(__file__).parent
sys.path.insert(0,str(ROOT))
import source_kafka as k
from test_kafka_security import CERTS, SASL, MTLS, reachable
def expand(config): return k.client_config(k.apply_profile({'bootstrap_servers':'b:9093',**config}))
class Profiles(unittest.TestCase):
    def test_confluent_cloud_is_sasl_ssl_plain_from_an_api_key(self):
        c=expand({'profile':'confluent_cloud','api_key':'KEY','api_secret':'SECRET'})
        self.assertEqual((c['security.protocol'],c['sasl.mechanism'],c['sasl.username'],c['sasl.password']),('SASL_SSL','PLAIN','KEY','SECRET'))
        self.assertEqual(c['ssl.endpoint.identification.algorithm'],'https')
    def test_event_hubs_uses_the_connection_string_as_password(self):
        cs='Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=v'
        c=expand({'profile':'azure_event_hubs','connection_string':cs})
        self.assertEqual((c['sasl.mechanism'],c['sasl.username'],c['sasl.password']),('PLAIN','$ConnectionString',cs))
        with self.assertRaises(ValueError): expand({'profile':'azure_event_hubs','connection_string':'not-a-connection-string'})
    def test_msk_scram_redpanda_and_aiven(self):
        self.assertEqual(expand({'profile':'aws_msk_scram','sasl_username':'u','sasl_password':'p'})['sasl.mechanism'],'SCRAM-SHA-512')
        self.assertEqual(expand({'profile':'redpanda_cloud','sasl_username':'u','sasl_password':'p'})['sasl.mechanism'],'SCRAM-SHA-256')
        self.assertEqual(expand({'profile':'redpanda_cloud','sasl_mechanism':'SCRAM-SHA-512','sasl_username':'u','sasl_password':'p'})['sasl.mechanism'],'SCRAM-SHA-512')
        self.assertEqual(expand({'profile':'aiven','ssl_ca_location':'/x/ca.pem','ssl_keystore_location':'/x/c.p12','ssl_keystore_password':'p'})['security.protocol'],'SSL')
    def test_contradictions_and_weakening_are_refused(self):
        for bad in ({'profile':'nope'},
                    {'profile':'confluent_cloud','api_key':'a','api_secret':'b','security_protocol':'SASL_PLAINTEXT'},
                    {'profile':'confluent_cloud','api_key':'a','sasl_username':'other','api_secret':'b'},
                    {'profile':'confluent_cloud','api_key':'a','api_secret':'b','ssl_endpoint_identification_algorithm':'none'},
                    {'profile':'aws_msk_scram','sasl_mechanism':'PLAIN','sasl_username':'u','sasl_password':'p'},
                    {'profile':'redpanda_cloud','sasl_mechanism':'PLAIN','sasl_username':'u','sasl_password':'p'},
                    {'profile':'aiven','security_protocol':'PLAINTEXT'},
                    {'profile':'aws_msk_iam','aws_region':'not a region'},
                    {'profile':'aws_msk_iam','aws_region':'us-east-1','aws_access_key_id':'only-half'}):
            with self.subTest(bad), self.assertRaises(ValueError): expand(bad)
        # OAUTHBEARER cannot be selected directly: it only comes with the IAM token callback.
        with self.assertRaises(ValueError): expand({'security_protocol':'SASL_SSL','sasl_mechanism':'OAUTHBEARER','_msk_iam':True})
    def test_public_ca_uses_the_system_store_unless_a_ca_is_given(self):
        c=expand({'profile':'confluent_cloud','api_key':'a','api_secret':'b'})
        if os.name!='nt': self.assertEqual(c['ssl.ca.location'],'probe')
        else: self.assertNotIn('ssl.ca.location',c)
        self.assertEqual(expand({'profile':'confluent_cloud','api_key':'a','api_secret':'b','ssl_ca_location':'/ca.pem'})['ssl.ca.location'],'/ca.pem')
    def test_msk_iam_token_is_a_sigv4_presigned_connect_request(self):
        c=expand({'profile':'aws_msk_iam','aws_region':'eu-west-1','aws_access_key_id':'meridian-test-key-id','aws_secret_access_key':'test-only-secret'})
        self.assertEqual((c['security.protocol'],c['sasl.mechanism']),('SASL_SSL','OAUTHBEARER'))
        token,expiry=c['oauth_cb']('')
        url=base64.urlsafe_b64decode(token+'='*(-len(token)%4)).decode()
        parts=urllib.parse.urlparse(url); q=urllib.parse.parse_qs(parts.query)
        self.assertEqual(parts.netloc,'kafka.eu-west-1.amazonaws.com')
        self.assertEqual(q['Action'],['kafka-cluster:Connect'])
        self.assertTrue(q['X-Amz-Credential'][0].startswith('meridian-test-key-id/'))
        self.assertIn('X-Amz-Signature',q)
        self.assertNotIn('test-only-secret',url)
        import time
        self.assertGreater(expiry,time.time())
@unittest.skipUnless(reachable(),'secured Kafka not running (deploy/connect/certify-secure.compose.yml)')
class ProfilesLive(unittest.TestCase):
    """Profiles whose wire protocol the local secured broker speaks, end to end through the actor."""
    @classmethod
    def setUpClass(cls):
        cls.dir=Path(tempfile.mkdtemp(prefix='kafka-profile-'))
        for f in ('ca.pem','client.p12'): shutil.copy(CERTS/f,cls.dir/f)
    @classmethod
    def tearDownClass(cls): shutil.rmtree(cls.dir,ignore_errors=True)
    def check(self, config):
        cfg=self.dir/f'{uuid.uuid4().hex}.json'; cfg.write_text(json.dumps(config))
        out=subprocess.run([sys.executable,str(ROOT/'source_kafka.py'),'check','--config',str(cfg)],capture_output=True,text=True,timeout=120)
        return out.returncode,[json.loads(l) for l in out.stdout.splitlines() if l.strip()]
    def test_profiles_connect(self):
        for name,config in {
            'confluent_cloud':{'bootstrap_servers':SASL,'profile':'confluent_cloud','api_key':'meridian-plain','api_secret':'meridian-test-plain','ssl_ca_location':'ca.pem'},
            'aws_msk_scram':{'bootstrap_servers':SASL,'profile':'aws_msk_scram','sasl_username':'meridian-scram','sasl_password':'meridian-test-scram','ssl_ca_location':'ca.pem'},
            'redpanda_cloud':{'bootstrap_servers':SASL,'profile':'redpanda_cloud','sasl_mechanism':'SCRAM-SHA-512','sasl_username':'meridian-scram','sasl_password':'meridian-test-scram','ssl_ca_location':'ca.pem'},
            'aiven (mTLS)':{'bootstrap_servers':MTLS,'profile':'aiven','ssl_ca_location':'ca.pem','ssl_keystore_location':'client.p12','ssl_keystore_password':'meridian-test-store'},
        }.items():
            with self.subTest(name):
                rc,out=self.check(config)
                self.assertEqual(rc,0,out)
                self.assertEqual(out[-1]['connectionStatus']['status'],'SUCCEEDED')
    def test_a_wrong_api_secret_is_auth_failed(self):
        rc,out=self.check({'bootstrap_servers':SASL,'profile':'confluent_cloud','api_key':'meridian-plain','api_secret':'wrong','ssl_ca_location':'ca.pem'})
        self.assertNotEqual(rc,0)
        self.assertEqual(out[-1]['trace']['code'],'auth_failed')
    def test_a_profile_cannot_be_weakened(self):
        rc,out=self.check({'bootstrap_servers':SASL,'profile':'confluent_cloud','api_key':'a','api_secret':'b','ssl_endpoint_identification_algorithm':'none'})
        self.assertEqual(out[-1]['trace']['code'],'connector_failed')
if __name__=='__main__': unittest.main()
