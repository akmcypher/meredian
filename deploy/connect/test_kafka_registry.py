"""Real Avro writer/reader resolution with an in-memory registry transport."""
import copy
import io
import json
import struct
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import unittest
import urllib.error
from unittest.mock import patch

import fastavro
import source_kafka as kafka

ADDRESS = {'type': 'record', 'name': 'Address', 'namespace': 'example',
           'fields': [{'name': 'city', 'type': 'string'}]}
ORDER = {'type': 'record', 'name': 'Order', 'namespace': 'example',
         'fields': [{'name': 'id', 'type': 'int'}, {'name': 'address', 'type': 'example.Address'}]}


def document(schema, **extra):
    return {'schema': json.dumps(schema), **extra}


def wire(schema_id, schema, value, dependencies=()):
    names = {}
    for dependency in dependencies:
        fastavro.parse_schema(dependency, names)
    parsed = fastavro.parse_schema(schema, names)
    body = io.BytesIO()
    fastavro.schemaless_writer(body, parsed, value)
    return b'\x00'+struct.pack('>I', schema_id)+body.getvalue()


class RegistryEvolution(unittest.TestCase):
    def decode(self, documents, reader=None):
        def get(_registry, path):
            found = documents[path]
            if isinstance(found, Exception): raise found
            return copy.deepcopy(found)
        self.transport = patch.object(kafka.Registry, 'get', get)
        self.transport.start()
        self.addCleanup(self.transport.stop)
        return kafka.decoder({'schema_registry_url': 'https://registry.invalid'},
            {'schema': {'x-avro-reader-schema': reader} if reader else {}})[0]

    def test_exact_referenced_versions_do_not_bleed_between_writer_ids(self):
        v2 = {**ADDRESS, 'fields': ADDRESS['fields']+[{'name': 'country', 'type': 'string', 'default': 'IN'}]}
        docs = {'/schemas/ids/1': document(ORDER, references=[{'name': 'example.Address', 'subject': 'a/b', 'version': 1}]),
                '/schemas/ids/2': document(ORDER, references=[{'name': 'example.Address', 'subject': 'a/b', 'version': 2}]),
                '/subjects/a%2Fb/versions/1': document(ADDRESS),
                '/subjects/a%2Fb/versions/2': document(v2)}
        decode = self.decode(docs)
        first = wire(1, ORDER, {'id': 1, 'address': {'city': 'Pune'}}, [ADDRESS])
        second = wire(2, ORDER, {'id': 2, 'address': {'city': 'Pune', 'country': 'IN'}}, [v2])
        self.assertEqual(decode(first)['address'], {'city': 'Pune'})
        self.assertEqual(decode(second)['address'], {'city': 'Pune', 'country': 'IN'})
        self.assertEqual(decode(first)['address'], {'city': 'Pune'})

    def test_nested_references_resolve_before_their_parents(self):
        wrapped = {'type': 'record', 'name': 'Wrapper', 'namespace': 'example',
                   'fields': [{'name': 'order', 'type': 'example.Order'}]}
        docs = {'/schemas/ids/3': document(wrapped, references=[{'name': 'example.Order', 'subject': 'orders', 'version': 1}]),
                '/subjects/orders/versions/1': document(ORDER, references=[{'name': 'example.Address', 'subject': 'addresses', 'version': 1}]),
                '/subjects/addresses/versions/1': document(ADDRESS)}
        value = {'order': {'id': 7, 'address': {'city': 'Delhi'}}}
        self.assertEqual(self.decode(docs)(wire(3, wrapped, value, [ADDRESS, ORDER])), value)

    def test_reader_defaults_aliases_and_promotions_work_across_versions(self):
        old = {'type': 'record', 'name': 'Event', 'fields': [{'name': 'old_id', 'type': 'int'}]}
        reader = {'type': 'record', 'name': 'Event', 'fields': [
            {'name': 'id', 'aliases': ['old_id'], 'type': 'long'},
            {'name': 'region', 'type': ['null', 'string'], 'default': None}]}
        decode = self.decode({'/schemas/ids/4': document(old)}, reader)
        self.assertEqual(decode(wire(4, old, {'old_id': 8})), {'id': 8, 'region': None})

    def test_empty_record_has_a_valid_header_only_wire_value(self):
        empty = {'type': 'record', 'name': 'Empty', 'fields': []}
        self.assertEqual(self.decode({'/schemas/ids/5': document(empty)})(wire(5, empty, {})), {})

    def test_incompatible_reader_is_a_schema_incompatible_dead_letter(self):
        writer = {'type': 'record', 'name': 'R', 'fields': [{'name': 'id', 'type': 'string'}]}
        reader = {**writer, 'fields': [{'name': 'id', 'type': 'int'}]}
        decode = self.decode({'/schemas/ids/6': document(writer)}, reader)
        with self.assertRaisesRegex(kafka.Decode, 'schema_incompatible'):
            decode(wire(6, writer, {'id': 'x'}))

    def test_missing_reference_is_replayable_but_registry_outage_is_not_poison(self):
        ref = '/subjects/addresses/versions/1'
        docs = {'/schemas/ids/7': document(ORDER, references=[{'name': 'example.Address', 'subject': 'addresses', 'version': 1}]),
                ref: urllib.error.HTTPError('https://registry.invalid', 404, 'missing', {}, None)}
        decode = self.decode(docs)
        raw = wire(7, ORDER, {'id': 1, 'address': {'city': 'Pune'}}, [ADDRESS])
        with self.assertRaisesRegex(kafka.Decode, 'schema_not_found'): decode(raw)
        docs[ref] = urllib.error.HTTPError('https://registry.invalid', 503, 'unavailable', {}, None)
        with self.assertRaises(urllib.error.HTTPError): decode(raw)
        docs[ref] = document(ADDRESS)
        self.assertEqual(decode(raw)['id'], 1)

    def test_cyclic_and_malformed_schema_graphs_fail_as_safe_decode_errors(self):
        for doc in [document(ORDER, references=[{'name': 'example.Order', 'subject': 'loop', 'version': 1}]),
                    {'schema': 'invalid json'},
                    document(ORDER, references=[{'name': 'example.Address', 'subject': 'addresses', 'version': 'latest'}])]:
            with self.subTest(doc=doc):
                decode = self.decode({'/schemas/ids/8': doc, '/subjects/loop/versions/1': doc})
                with self.assertRaisesRegex(kafka.Decode, 'deserialization_failed'):
                    decode(b'\x00'+struct.pack('>I', 8)+b'\x00')

    def test_reference_depth_is_bounded(self):
        docs = {}
        for i in range(35):
            schema = {'type': 'record', 'name': f'R{i}', 'fields': []}
            docs[f'/subjects/r{i}/versions/1'] = document(schema, references=[
                {'name': f'R{i+1}', 'subject': f'r{i+1}', 'version': 1}])
        docs['/schemas/ids/9'] = docs['/subjects/r0/versions/1']
        decode = self.decode(docs)
        with self.assertRaisesRegex(kafka.Decode, 'deserialization_failed'):
            decode(b'\x00'+struct.pack('>I', 9))

    def test_conflicting_inline_names_and_mismatched_reference_roots_fail_closed(self):
        a = {'type': 'record', 'name': 'A', 'namespace': 'example',
             'fields': [{'name': 'address', 'type': ADDRESS}]}
        b = {'type': 'record', 'name': 'B', 'namespace': 'example',
             'fields': [{'name': 'address', 'type': {**ADDRESS, 'fields': [{'name': 'value', 'type': 'boolean'}]}}]}
        root = {'type': 'record', 'name': 'Root', 'fields': [
            {'name': 'a', 'type': 'example.A'}, {'name': 'b', 'type': 'example.B'}]}
        for second_name in ('example.B', 'example.Address'):
            docs = {'/schemas/ids/10': document(root, references=[
                {'name': 'example.A', 'subject': 'a', 'version': 1},
                {'name': second_name, 'subject': 'b', 'version': 1}]),
                '/subjects/a/versions/1': document(a), '/subjects/b/versions/1': document(b)}
            with self.subTest(second_name=second_name):
                decode = self.decode(docs)
                with self.assertRaisesRegex(kafka.Decode, 'deserialization_failed'):
                    decode(b'\x00'+struct.pack('>I', 10)+b'\x00\x00')

    def test_cached_reference_cannot_be_reused_under_an_inline_name(self):
        wrapped = {'type': 'record', 'name': 'B', 'namespace': 'example',
                   'fields': [{'name': 'address', 'type': ADDRESS}]}
        root = {'type': 'record', 'name': 'Root', 'fields': []}
        docs = {'/schemas/ids/11': document(root, references=[
            {'name': 'example.B', 'subject': 'b', 'version': 1},
            {'name': 'example.Address', 'subject': 'b', 'version': 1}]),
            '/subjects/b/versions/1': document(wrapped)}
        with self.assertRaisesRegex(kafka.Decode, 'deserialization_failed'):
            self.decode(docs)(b'\x00'+struct.pack('>I', 11))


class RegistryTransport(unittest.TestCase):
    def test_redirect_is_not_followed_to_another_registry_location(self):
        seen = []
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                seen.append(self.path)
                if self.path == '/schemas/ids/1':
                    self.send_response(302)
                    self.send_header('Location', '/unexpected')
                    self.end_headers()
                else:
                    self.send_response(200); self.end_headers(); self.wfile.write(b'{}')
            def log_message(self, *args): pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            registry = kafka.Registry({'schema_registry_url': f'http://127.0.0.1:{server.server_port}'})
            with self.assertRaises(urllib.error.HTTPError): registry.schema(1)
            self.assertEqual(seen, ['/schemas/ids/1'])
        finally:
            server.shutdown(); server.server_close(); thread.join()


if __name__ == '__main__': unittest.main()
