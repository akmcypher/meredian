"""MongoDB value encoding, config and cursor-type contracts without a server."""
import datetime
import decimal
import json
import unittest
import uuid
from unittest.mock import patch

import bson
from bson.binary import Binary, UuidRepresentation

import source_mongodb as mongodb

CONFIG = {'hosts': 'db.example', 'database': 'shop', 'user': 'reader', 'password': 'private'}


class Values(unittest.TestCase):
    def test_extended_json_keeps_types_and_safe_integers(self):
        oid = bson.ObjectId('6566a1b2c3d4e5f601234567')
        when = datetime.datetime(2026, 11, 22, 10, 0, 0, 123000, tzinfo=datetime.timezone.utc)
        doc = {'_id': oid, 'at': when, 'small': 2**53-1, 'big': bson.Int64(2**53+1), 'neg': -(2**60),
               'dec': bson.Decimal128(decimal.Decimal('12345678901234567890.0123456789')),
               'ratio': 0.5, 'nan': float('nan'), 'flag': True, 'none': None,
               'uuid': Binary.from_uuid(uuid.UUID('6f9619ff-8b86-d011-b42d-00c04fc964ff'), UuidRepresentation.STANDARD),
               'nested': {'list': [1, {'x': bson.Int64(2**62)}]}}
        out = mongodb.value(doc)
        self.assertEqual(out['_id'], {'$oid': '6566a1b2c3d4e5f601234567'})
        self.assertEqual(out['at'], {'$date': '2026-11-22T10:00:00.123Z'})
        self.assertEqual(out['small'], 2**53-1)
        self.assertEqual(out['big'], {'$numberLong': str(2**53+1)})
        self.assertEqual(out['neg'], {'$numberLong': str(-(2**60))})
        self.assertEqual(out['dec'], {'$numberDecimal': '12345678901234567890.0123456789'})
        self.assertEqual(out['nan'], {'$numberDouble': 'NaN'})
        self.assertIs(out['flag'], True)
        self.assertEqual(out['uuid'], {'$binary': {'base64': 'b5YZ/4uG0BG0LQDAT8lk/w==', 'subType': '04'}})
        self.assertEqual(out['nested'], {'list': [1, {'x': {'$numberLong': str(2**62)}}]})
        json.dumps(out, allow_nan=False)  # strict JSON, no NaN literals

    def test_positions_round_trip_to_bson(self):
        oid = bson.ObjectId()
        when = datetime.datetime(2026, 1, 2, 3, 4, 5, 6000, tzinfo=datetime.timezone.utc)
        for v in (oid, when, bson.Int64(2**60), 7, 'text', bson.Decimal128('1.5')):
            back = mongodb.restore(json.loads(json.dumps(mongodb.value(v))))
            self.assertEqual(back, v)
            self.assertEqual(mongodb.bson_type(back), mongodb.bson_type(v))


class Config(unittest.TestCase):
    def test_credentials_and_urls_never_come_from_the_host_field(self):
        for hosts in ('user:pw@db.example', 'db.example/admin', 'db.example?tls=false', ''):
            with self.assertRaisesRegex(ValueError, 'connector_failed'):
                mongodb.client({**CONFIG, 'hosts': hosts})
        with self.assertRaisesRegex(ValueError, 'connector_failed'):
            mongodb.client({**CONFIG, 'srv': True, 'hosts': 'a.example:27017'})

    def test_tls_is_always_on_and_not_relaxable(self):
        with patch('pymongo.MongoClient') as mc:
            mongodb.client({**CONFIG, 'tlsAllowInvalidCertificates': True, 'tls': False, 'ssl_ca': 'ca.pem'})
        kwargs = mc.call_args.kwargs
        self.assertIs(kwargs['tls'], True)
        self.assertEqual(kwargs['tlsCAFile'], 'ca.pem')
        self.assertNotIn('tlsAllowInvalidCertificates', kwargs)
        self.assertNotIn('tlsAllowInvalidHostnames', kwargs)
        self.assertEqual(kwargs['readConcernLevel'], 'majority')


class Families(unittest.TestCase):
    def stream(self, **props):
        return {'schema': {'properties': {k: {'source_type': 'mongodb:'+v} for k, v in props.items()}}}

    def test_cursor_needs_one_comparable_family(self):
        self.assertEqual(mongodb.family(self.stream(n='int|long'), 'n'), 'number')
        self.assertEqual(mongodb.family(self.stream(at='date'), 'at'), 'date')
        for t in ('date|string', 'object', 'null', 'array'):
            with self.assertRaisesRegex(ValueError, 'schema_drift'):
                mongodb.family(self.stream(x=t), 'x')
        with self.assertRaisesRegex(ValueError, 'schema_drift'):
            mongodb.family(self.stream(), 'missing')

    def test_spec(self):
        self.assertEqual(mongodb.VERSION, 'mongodb-v1')


if __name__ == '__main__': unittest.main()
