"""Broker membership selects work; fenced Postgres tasks acknowledge landed data.

The membership consumer is paused and polled on its own thread. Existing isolated
partition actors do extraction and commits; broker offsets are never committed.
"""
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import urllib.error

import source_kafka as kafka


class Membership:
    def __init__(self, changed):
        self.lock = threading.RLock()
        self.owned = set()
        self.joined = threading.Event()
        self.changed = changed
        self.failure = None
    def snapshot(self):
        with self.lock:
            return set(self.owned)
    def _set(self, owned):
        with self.lock:
            self.owned = owned
        self.changed(set(owned))
    def assign(self, consumer, partitions):
        try:
            consumer.assign(partitions)
            if partitions:
                consumer.pause(partitions)
            self._set({(p.topic, p.partition) for p in partitions})
            self.joined.set()
        except Exception as error:
            self.failure = error
            self._set(set())
    def revoke(self, consumer, partitions):
        # The source factory pins the eager range assignor: every revoke ends ownership.
        self._set(set())
        consumer.unassign()
    def lost(self, consumer, partitions):
        self.revoke(consumer, partitions)


class SharedGroup:
    def __init__(self, directory, request, execute, worker, slots=1, once=False,
                 connection_id=None, identity=None, consumer_factory=None,
                 poll_interval=.2, context_interval=2, join_timeout=45):
        self.directory = Path(directory).resolve()
        self.request = request
        self.execute = execute
        self.worker = worker
        self.slots = max(1, min(64, slots))
        self.once = once
        self.connection_id = connection_id
        self.identity = identity or {}
        self.consumer_factory = consumer_factory or kafka.consumer
        self.poll_interval = poll_interval
        self.context_interval = context_interval
        self.join_timeout = join_timeout
        self.active_lock = threading.RLock()
        self.active = {}
        self.membership = Membership(self._changed)
        self.halt = threading.Event()
        self.poll_failure = None
        self.worker_failure = None
        self.context = None
        self.consumer = None
        self.consumer_thread = None
        self.desired_topics = ()
        self.config_path = None
        self.counter = 0

    def _body(self, action, **fields):
        body = {'action': action, **fields}
        if self.connection_id is not None and action in ('group_context', 'claim_group_task'):
            body['connectionId'] = self.connection_id
        return body

    def _context(self):
        context = self.request(self._body('group_context')).get('context')
        if not context or context.get('source_version') != 'kafka-v1':
            raise ValueError('connector_failed')
        if self.context and context['credential_ref'] != self.context['credential_ref']:
            raise ValueError('source_changed')
        self.context = context
        topics = tuple(sorted({s['topic'] for s in context['streams']}))
        with self.membership.lock:
            self.desired_topics = topics
        return context

    @staticmethod
    def _allowed(claim, owned):
        if claim.get('operation') in ('check', 'discover'):
            return True
        streams = claim.get('streams') or []
        if not claim.get('task') or len(streams) != 1:
            return False
        stream = streams[0]
        topic = stream.get('schema', {}).get('x-kafka-topic')
        shard = stream.get('shard', '')
        if shard == '':
            # Whole-topic data tasks from an older mode cannot be shared in flight.
            planning = stream.get('splittable') or claim.get('operation') == 'replay'
            return bool(planning and any(t == topic for t, _ in owned))
        return str(shard).isdigit() and (topic, int(shard)) in owned

    def _changed(self, owned):
        with self.active_lock:
            for item in self.active.values():
                if not self._allowed(item['claim'], owned):
                    item['cancel'].set()

    def _poll(self):
        subscribed = None
        try:
            while not self.halt.is_set():
                with self.membership.lock:
                    topics = self.desired_topics
                if topics != subscribed:
                    if topics:
                        self.consumer.subscribe(list(topics), on_assign=self.membership.assign,
                                                on_revoke=self.membership.revoke,
                                                on_lost=self.membership.lost)
                    else:
                        self.membership._set(set())
                        self.consumer.unsubscribe()
                    subscribed = topics
                message = self.consumer.poll(self.poll_interval)
                errors = getattr(self.consumer, 'errors', None)
                if errors is not None and errors.seen is not None:
                    raise kafka.KafkaException(errors.seen)
                if self.membership.failure:
                    raise self.membership.failure
                if message is not None and message.error():
                    if message.error().code() != kafka.KafkaError._PARTITION_EOF:
                        raise kafka.KafkaException(message.error())
        except Exception as error:
            self.poll_failure = error
        finally:
            self.membership._set(set())
            try: self.consumer.close()
            except Exception as error:
                if self.poll_failure is None: self.poll_failure = error

    def _assignments(self):
        owned = self.membership.snapshot()
        assignments = {}
        for stream in self.context['streams']:
            parts = sorted({p for topic, p in owned if topic == stream['topic']})
            if parts:
                assignments[stream['id']] = parts
        if any(len(parts)>1024 for parts in assignments.values()) or len(json.dumps(assignments).encode())>48*1024:
            raise ValueError('record_too_large')
        return assignments

    def _abandon(self, item):
        claim = item['claim']
        try:
            self.request(self._body('abandon_task', runId=claim['run_id'], token=claim['lease_token']))
            return True
        except urllib.error.HTTPError as error:
            if error.code == 409:
                return True  # already expired/completed/fenced; never disturb a newer lease
            raise
        except (OSError, TimeoutError):
            return False  # stop children; retry or let their existing leases expire

    def _work(self, item):
        try:
            self.execute(item['claim'], cancel=item['cancel'], config_override=self.config_path)
        except Exception as error:
            self.worker_failure = error
            item['cancel'].set()
        finally:
            item['finished'].set()

    def _reap(self):
        with self.active_lock:
            items = list(self.active.items())
        for token, item in items:
            if item['cancel'].is_set() and not item['released']:
                if not self._abandon(item):
                    continue
                item['released'] = True
            if item['finished'].is_set():
                item['thread'].join()
                with self.active_lock:
                    self.active.pop(token, None)

    def _dispatch(self, claim):
        token = claim['lease_token']
        item = {'claim': claim, 'cancel': threading.Event(), 'finished': threading.Event(),
                'released': False}
        # Membership updates happen before their cancellation observer. Holding this lock
        # through registration ensures a revoke cannot miss a newly registered worker.
        with self.membership.lock:
            allowed = self._allowed(claim, self.membership.owned)
            if allowed:
                with self.active_lock:
                    if token in self.active:
                        raise ValueError('connector_failed')
                    self.active[token] = item
        if not allowed:
            self._abandon(item)
            streams = claim.get('streams') or []
            if (claim.get('operation') not in ('check', 'discover', 'replay') and
                    len(streams) == 1 and not streams[0].get('shard') and
                    not streams[0].get('splittable')):
                # Switching a partially read whole-topic task needs an explicit new run;
                # repeatedly refunding this unsupported task would spin forever.
                raise ValueError('source_changed')
            return False
        thread = threading.Thread(target=self._work, args=(item,), daemon=True)
        item['thread'] = thread
        thread.start()
        return True

    def run(self):
        context = self._context()
        if self.once and not context['pending']:
            return
        raw_path = self.directory / (context['credential_ref']+'.json')
        with raw_path.open() as source:
            config = kafka.apply_profile(kafka.resolve_files(json.load(source), str(raw_path.parent)))
        if not kafka.consumer_group(config):
            raise ValueError('connector_failed')
        with tempfile.TemporaryDirectory(prefix='meridian-group-') as scratch:
            self.config_path = str(Path(scratch) / 'source.json')
            Path(self.config_path).write_text(json.dumps({**config, 'consumer_group': False}))
            os.chmod(self.config_path, 0o600)
            joined_at = time.monotonic()
            refreshed = joined_at
            try:
                while not self.halt.is_set():
                    self._reap()
                    if self.poll_failure:
                        raise self.poll_failure
                    if self.worker_failure:
                        raise self.worker_failure
                    now = time.monotonic()
                    if now-refreshed >= self.context_interval:
                        context = self._context()
                        refreshed = now
                    with self.active_lock:
                        active = len(self.active)
                    if self.once and not context['pending'] and not active:
                        break
                    if self.consumer is None and self.desired_topics:
                        self.consumer = self.consumer_factory(config)
                        self.consumer_thread = threading.Thread(target=self._poll, daemon=True)
                        self.consumer_thread.start()
                        joined_at = now
                    if (self.consumer is not None and not self.membership.joined.is_set()
                            and now-joined_at > self.join_timeout):
                        raise ValueError('source_unavailable')
                    assignments = self._assignments()
                    if active < self.slots:
                        self.counter += 1
                        response = self.request(self._body('claim_group_task', assignments=assignments,
                                                worker=f'{self.worker}:{self.counter}', **self.identity))
                        claim = response.get('claim')
                        if claim:
                            self._dispatch(claim)
                            continue
                    self.halt.wait(self.poll_interval)
            finally:
                self.halt.set()
                with self.active_lock:
                    items = list(self.active.values())
                    for item in items:
                        item['cancel'].set()
                for item in items:
                    if not self._abandon(item):
                        break
                for item in items:
                    item['thread'].join(timeout=10)
                if self.consumer_thread:
                    self.consumer_thread.join(timeout=10)
