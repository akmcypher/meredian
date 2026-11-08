# Meridian Community

Open-source data ingestion with a web control plane. Bring data in from REST APIs, files,
databases and streams, and retain immutable raw ingestion batches and their manifests.

## Project Naming Notice

**Meridian** is the provisional name of this open-source community project. The project name,
branding, and associated visual identity may change as the project evolves and before any future
commercial or enterprise release.

The current name is used for development, community collaboration, and preview distribution.
It does not imply affiliation with, endorsement by, or sponsorship from any other organization
using a similar name.

## Meridian Community Preview 0.1 — Disclaimer

Meridian Community Preview 0.1 is pre-release software provided for evaluation, development, testing and community feedback.

**Not production-certified:** Passing automated tests and CI checks does not constitute production certification, independent security certification, performance benchmarking or a guarantee of reliability or availability.

**Data and backup risks:** Do not use the preview as the sole repository for critical, regulated or irreplaceable data. Maintain independent backups and verify restoration before relying on the system. No specific recovery time or recovery point is guaranteed.

**Security and deployment boundaries:** Connector actors currently share an agent process boundary. Per-connector and per-tenant runtime isolation, production high availability and signed-image certification are not yet established. Use one installation per trust boundary. A platform administrator may have technical access to organization data and connection secrets.

**Feature limitations:** Connector capabilities vary. PostgreSQL and MySQL extraction use cursor-plus-primary-key incremental reads, not full CDC. REST pulls are manual. Native uploads and REST/Push batches are not automatically available through the Connect Bronze reader. Consult the documentation before relying on a specific capability.

**Hosted services:** Public signup, production email delivery, hosted Realtime and complete hosted deployment acceptance require separate configuration and verification. Local CI results do not certify these services.

**No warranty:** The software is provided under its applicable open-source licence, on an "AS IS" basis, without warranties of merchantability, fitness for a particular purpose or uninterrupted operation, to the extent permitted by applicable law.

Users are responsible for evaluating the software, securing their deployment, complying with applicable laws and maintaining appropriate backups and operational controls.

## What it does

- **Sources:** manual REST API pulls, a Push API endpoint, native file upload (CSV, TSV,
  Parquet, Avro, JSON, JSONL and NDJSON), and connector
  actors for PostgreSQL, MySQL and Kafka built on the Connect SDK, with a conformance kit for
  writing your own. The PostgreSQL and MySQL actors are incremental table extractors (cursor
  plus primary key), not change data capture: deletes, and updates that do not advance the
  cursor, are not captured.
- **Large inputs:** the partition-worker path splits records and retries partitioned runs before
  atomic publication. Native uploads create bounded legacy user tables with a 5,000-row preview cap;
  they are not Connect actors or Bronze snapshots.
- **Recoverable runs:** each batch commit is durable and idempotent, and stores its source
  checkpoint with it, so a failed run resumes from the last commit. Extraction is at-least-once
  into append-only bronze storage; Kafka positions below retention stop the run until the owner
  accepts the gap.
- **Scheduling:** Connect actors can run every N minutes through the running agent, or be triggered
  by your scheduler. REST pulls are manual.
- **Outputs:** append-only raw NDJSON batches and Connect manifests. Registered owner-scoped engines
  consume verified Bronze DatasetRef snapshots through short-lived read URLs; see
  [Bronze consumption](docs/BRONZE_CONSUMPTION.md).
  REST/Push raw batches and native-upload legacy tables are not automatically bridged to that reader.
- **Security built in:** organization-scoped access with row-level security, secrets that never
  reach the browser, per-source tokens, request limits and a hash-chained admin audit trail.

## Run it

Start with [QUICKSTART.md](QUICKSTART.md) and the [user guide](docs/USER_GUIDE.md).

On Windows 11 (x64 or ARM64), download `meridian-<version>-windows.zip` from the release, start
Docker Desktop, and run `.\meridian.cmd install` then `.\meridian.cmd start` in the extracted folder.
The launcher fetches pinned, hash-verified Node, Bun and Supabase CLI builds if they are missing; see
[Windows installer](docs/LOCAL_DEV.md#windows-installer-community-preview).

Otherwise see [docs/LOCAL_DEV.md](docs/LOCAL_DEV.md). You need Bun, Docker and the Supabase CLI.

## Contributing and security

See [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md). Licensed under
[Apache-2.0](LICENSE).

Storage reconciliation is dry-run only. Destructive cleanup is disabled until concurrent writers
and deletion share a safe lifecycle. See the [release notes](docs/RELEASE_NOTES.md) for what is
included and known limitations.
