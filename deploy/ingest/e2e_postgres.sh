#!/usr/bin/env bash
# Repeatable end-to-end test: real PostgreSQL → Meridian partitioned ingestion with a hard crash + resume.
# Prereqs: a Postgres you can reach ($SRC_URL), a Meridian source token ($MERIDIAN_TOKEN) whose run is planned
# (Partitioned ingest → pick the source → hash on seq, 24 partitions, PK "tenant, seq"), node 20 + `npm i pg`.
set -euo pipefail
: "${SRC_URL:?}" "${MERIDIAN_URL:?}" "${MERIDIAN_TOKEN:?}"
ROWS="${ROWS:-600000}"
psql "$SRC_URL" -v ON_ERROR_STOP=1 -q <<SQL
drop table if exists events_e2e;
create table events_e2e(tenant int not null, seq bigint not null, kind text, payload jsonb, primary key(tenant, seq));
insert into events_e2e select g%7, g, (array['click','view','buy'])[1+g%3], jsonb_build_object('n',g) from generate_series(1,$ROWS) g;
SQL
W="node $(dirname "$0")/partitioned_worker.mjs --kind postgres --url $SRC_URL --table public.events_e2e --threads 6"
$W --worker-id crash-a & PID=$!; sleep 30; kill -9 $PID || true; echo "worker A hard-killed"
sleep 5; $W --worker-id resume-b
echo "Now publish in Meridian and check: row_count = $ROWS and sum(seq) = $((ROWS*(ROWS+1)/2))."
