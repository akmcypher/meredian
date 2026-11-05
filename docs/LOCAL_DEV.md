# Local development

## Prerequisites

- Bun (https://bun.sh)
- Docker
- Supabase CLI (`npm install -g supabase`)
- Python 3.13 for the Connect actors and their certification tests

## First run

```bash
git clone <repository-url> meridian
cd meridian
supabase start            # pulls images once and applies supabase/migrations
supabase status           # shows the API URL, anon key and service_role key
cp .env.example .env      # then paste the values from `supabase status`
bun install
bun run dev -- --port 8080
```

The supplied configuration enables signup with required email confirmation. Restart a running Auth service after changing it. The sign-in page reads the actual backend policy. See [authentication and onboarding](AUTHENTICATION.md) for callback URLs, SMTP, migration 0077. Use `bootstrap_platform_admin` only for controlled platform administration, never ordinary registration.

## Built server

Build with the public Supabase URL/key configured, then supply server environment variables at
runtime. Plain Node does not load `.env` automatically. For local verification with Node 20.6+:

```bash
bun run build
node --env-file=.env .output/server/index.mjs
```

For deployment, inject the required values through your service manager or container environment
and use `bun run start`. Keep `SUPABASE_SERVICE_ROLE_KEY` and `ENGINE_TOKEN_SIGNING_KEY` server-side.
Supply runtime values explicitly. An HTTP 200 alone does not establish user-journey acceptance; sign in, run a real worker, and inspect the published output.

## Connectors

Database and stream connectors run as a separate worker next to the app. See
`deploy/connect/README.md` to build and run it, and `docs/CONNECTOR_DEVELOPER_GUIDE.md` to
write and certify your own.

Create a Python environment and install the pinned actor dependencies before running them:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r deploy/connect/requirements.txt
```

On Windows use `.venv\Scripts\python.exe` instead of `.venv/bin/python`.

## Tests

```bash
RUN_TENANT_E2E=0 bun x vitest run      # unit tests
bun run test:e2e:local                 # live tests against the local Supabase stack
bun run test:python:local              # Community Connect Python suites
```

Before Python certification, install `deploy/connect/requirements-test.txt` in the venv; it includes
the optional native engine used by external-read acceptance. Production workers use only
`requirements.txt`. The Python runner requires a local-only `.env`. PostgreSQL suites need the local database;
MySQL and Kafka suites need their fixture containers described in `deploy/connect/README.md`.
A skipped suite is not connector certification. CI runs fast checks and an isolated database/connector integration job.

## Storage reconciliation

`bun scripts/ops/reconcile-ingest-storage.ts --min-age-hours 168` reports candidates and writes
an append-only audit log. It never deletes objects. `--apply` is rejected because concurrent
object registration and deletion do not yet share writer exclusion. Retain orphaned objects
until a safe deletion lifecycle is delivered; a dry-run list is not permission to delete it.

See `BRONZE_CONSUMPTION.md` for registered-engine setup and verified external reads of a completed run.
