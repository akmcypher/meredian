import json
import unittest
from unittest.mock import patch
from destination_bronze import write
from sdk import state,record,validate,dead_letter
class DestinationContract(unittest.TestCase):
 def test_ack_state_only_after_commit_and_retries_identical(self):
  payloads=[]
  def commit(config,payload):
   payloads.append(payload)
   if len(payloads)==1:raise ConnectionError('response lost')
   return {'sequence':1}
  messages=[record({'name':'rows','namespace':'public'},{'id':1},1,1,0),state(1,1)]
  with patch('destination_bronze.post',commit),patch('destination_bronze.time.sleep'):
   self.assertEqual(list(write({'sequence':0,'run_id':'run','lease_token':'lease','stream_id':'stream'},messages)),[messages[-1]])
  self.assertEqual(payloads[0],payloads[1])
 def test_no_state_ack_on_failure(self):
  with patch('destination_bronze.post',side_effect=ConnectionError()),patch('destination_bronze.time.sleep'):
   with self.assertRaises(ConnectionError):list(write({'sequence':0,'run_id':'run','lease_token':'lease','stream_id':'stream'},[state()]))
 def test_dead_letters_ride_with_their_checkpoint(self):
  payloads=[]
  def commit(config,payload): payloads.append(payload); return {'sequence':1}
  stream={'name':'rows','namespace':'public'}
  messages=[record(stream,{'id':1},None,1,0),dead_letter(stream,'0:7','deserialization_failed',{'value_b64':'AA=='}),state(positions={0:8})]
  with patch('destination_bronze.post',commit):list(write({'sequence':0,'run_id':'run','lease_token':'lease','stream_id':'stream'},messages))
  self.assertEqual(payloads[0]['dead_letters'],[messages[1]]);self.assertEqual(payloads[0]['state']['state']['positions'],{'0':'8'})
 def test_named_rejection_is_not_retried(self):
  calls=[]
  def commit(config,payload): calls.append(payload); raise ValueError('dead_letter_budget')
  with patch('destination_bronze.post',commit),patch('destination_bronze.time.sleep'):
   with self.assertRaisesRegex(ValueError,'dead_letter_budget'):list(write({'sequence':0,'run_id':'run','lease_token':'lease','stream_id':'stream'},[state()]))
  self.assertEqual(len(calls),1)
 def test_offsets_leave_as_exact_decimal_strings(self):
  for o in (2**53-1,2**53,2**53+1,2**64-1):
   self.assertEqual(state(positions={0:o})['state']['positions'],{'0':str(o)})
   self.assertEqual(int(json.loads(json.dumps(state(positions={0:o})))['state']['positions']['0']),o)
 def test_oversized_single_message_fails_explicitly_before_any_commit(self):
  calls=[]
  stream={'name':'rows','namespace':'public'}
  huge=record(stream,{'blob':'x'*(7*1024*1024)},None,1,0)
  with patch('destination_bronze.post',lambda c,p: calls.append(p) or {'sequence':1}):
   with self.assertRaisesRegex(ValueError,'record_too_large'):list(write({'sequence':0,'run_id':'run','lease_token':'lease','stream_id':'stream'},[huge,state()]))
  self.assertEqual(calls,[])
 def test_reject_unknown_protocol(self):
  with self.assertRaises(ValueError):validate({'type':'EXECUTE_ARBITRARY'})
if __name__=='__main__':unittest.main()
