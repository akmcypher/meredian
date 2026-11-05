# Read a completed Bronze run from an external engine

Connect publishes append-only raw NDJSON envelopes. It does not apply upsert/delete events to a
relational table. A stream has a stable DatasetRef identity; each committed batch is an immutable
version with its own schema, checksum and provenance. A run snapshot includes every committed
batch of that completed run, including a possible empty final checkpoint batch.
Connect does not persist an exact attempt number per historical batch, so its public attempt
field is explicitly `null`; the final run attempt is never substituted as if it were observed.

This reader is owner-only. Sharing raw Bronze with organization peers, applying row policies or
masks in an arbitrary engine, and lakehouse materialization are not supported by this path.
Organization membership must remain valid when the source or engine belongs to an organization.

## Operator setup

Configure the control plane's Ed25519 signing key using `node scripts/engine-keys.mjs`. Keep the
`ENGINE_TOKEN_SIGNING_KEY` setting only in the server's `.env` or secret store. Never send that
private key or a Supabase service-role key to an external engine. Restart the app after setting it.

A trusted install operator registers an engine in Supabase Studio's SQL editor. Use the source
owner's actual auth-user UUID; the endpoint is registry metadata, not contacted by this pull reader:

```sql
insert into public.execution_engines(owner_id, name, kind, endpoint, enabled)
values ('OWNER-UUID', 'Owner Bronze reader', 'datafusion', 'https://reader.example.test', true)
returning id;
```

In a private directory, mint a five-minute token for that registered engine. Bun loads the control
plane's `.env`. Redirect stdout into a private file and give only this short-lived file to the reader:

```sh
umask 077
bun scripts/ops/bronze-token.ts --engine ENGINE-UUID > engine.token
```

On Windows, use a private user directory and its restricted inherited file permissions. Do not
commit the token. Minting needs operator access; ordinary users cannot register arbitrary engines.
The CLI selects the registered engine owner, rather than accepting an arbitrary token subject.

## Read and verify

Find the stream UUID and a succeeded sync/reset/backfill run UUID in the Connect run metadata
(or Studio's `connect_streams` and `connect_runs`). The output directory must not already exist:

```sh
python deploy/connect/read_bronze.py --app https://YOUR-CONTROL-PLANE \
  --token-file engine.token --stream STREAM-UUID --run RUN-UUID --output bronze-run
```

The reader accepts HTTPS, or HTTP on loopback for local development. It refuses redirects,
pages the manifest, verifies byte lengths, SHA-256 and row counts, and never forwards the engine
token to storage. `COMPLETE.json` is written only after all batches verify. If a read fails, discard
the incomplete local directory and retry with a fresh token and a new output directory.

For a real SQL count using the optional independent DataFusion engine:

```sh
python -m pip install -r deploy/connect/requirements-test.txt
python deploy/connect/read_bronze.py --app http://localhost:8080 \
  --token-file engine.token --stream STREAM-UUID --run RUN-UUID \
  --output verified-bronze-run --query-count
```

Production workers need only `requirements.txt`; the query runtime is an optional reader/test dependency.
The summary reports parts, verified raw rows, and SQL rows. Empty completed runs return zero rows.

## API contract and revocation

`POST /api/public/serve-manifest` with `Authorization: Bearer ENGINE-TOKEN` accepts:

```json
{ "bronze": { "streamId": "STREAM-UUID", "runId": "RUN-UUID", "afterSequence": -1 } }
```

The `bronze` response contains `datasetId`, `streamId`, `runId`, up to 25 `parts` and `nextSequence`.
Use that cursor for the next page; `null` means complete. Each part is a DatasetRef with namespace
`connect`, format `ndjson`, layer `bronze`, and the original schema and extraction provenance.
Stream schemas larger than 64 KiB cannot be served through this bounded endpoint.
Unknown, wrong-owner, unfinished or unavailable snapshots return 404 without revealing metadata.
Invalid/expired tokens or disabled/unregistered engines return 401; an engine/subject scope mismatch
returns 403. Disabling a source or connection also prevents new read URLs from being issued.

Tokens and read URLs expire after at most five minutes. Disabling an engine or revoking membership
blocks subsequent manifest requests, but already-issued storage URLs can remain usable until their
expiry. This is bounded capability expiry, not instantaneous revocation of an already-shared URL.
Migration 0078 also rejects banned, deleted and unverified token subjects inside both manifest RPCs,
including engine JWTs issued before account deactivation. Apply this forward migration with the
updated API; existing object URLs still expire on their original schedule.
Keep storage URLs private and use shorter runs/pages or renew tokens when a large download exceeds
the token lifetime. Existing Gold `names` queries retain their original response contract.

Source, connection and publication organization scopes must agree. Rescoping an existing source
or connection fails closed for old publications; create a new connection for the new scope.

## Replay upgrade qualification

The durable deferral counter starts at zero when migration 0071 is installed. Before upgrading an
existing preview deployment, cancel outstanding replay runs and requeue their still-open letters
after migration. Earlier response-only deferrals cannot be reconstructed from historical runs.
Fresh installations need no replay upgrade action. Deferred letters remain open and a run with
any durable deferral finishes as `replay_incomplete`, including after a lost claim response.

## Exact engine scope

An organization engine may consume only snapshots in that same organization, and the subject
must own the source and retain membership. Membership in two organizations does not permit an
engine in one to read a source in the other. A personal engine serves only its owner's personal
snapshots. Personal and organization scopes are never implicitly interchangeable. The database
RPC receives the verified token's registered engine identity and enforces these rules before
returning object locations.

Migration 0073 replaces the service-only RPC signature. Deploy the application and migration
together; the old application fails closed until its updated RPC caller is deployed.
