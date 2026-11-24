# Using Meridian Community

Meridian collects data from APIs, files, databases and Kafka. A workspace groups your resources; being its owner does not make you a platform administrator. This is a preview; see the [release notes](RELEASE_NOTES.md).

## Accounts and workspaces

Open Meridian and select **No account? Sign up**. Enter your name and email, then a password with at least 12 characters including uppercase, lowercase and a digit. Confirm it and submit. Keep the original browser open: the verification link uses that browser's PKCE verifier. Follow the delivered email link there. An expired, already-used or different-browser link requires a new verification email, not administrator access or disabled confirmation.

After confirmation, Meridian creates your private workspace. Complete its name/details and onboarding choices. A source preference is a preference, not an already connected source. Sign out and sign in to check that your progress persists. **Forgot password?** sends a recovery email; follow it in the initiating browser, choose a new password and sign in again. Report missing email to the deployment operator: repeated retries cannot fix an unconfigured sender.

Workspace owners/admins can edit workspace settings within their permissions. Switch only to workspaces you belong to. User invitation workflows are not implemented; do not treat platform administrator controls as ordinary workspace user management.

## Native files

Open **Data**, choose an upload, inspect its preview, name the table and choose **Create table**. CSV/TSV, Parquet, Avro, JSON/JSONL/NDJSON are supported. Native previews are bounded to 5,000 rows. Check inferred columns and data before saving. These tables are separate from Connect's raw Bronze snapshots. Table names must be unique within a workspace.

## REST pulls

In **Ingestion**, create a REST source with a name, target table, HTTPS URL and required request headers. If records are nested, supply their JSON path, such as `data.items`. Preview the result, select a key where required and choose **Save & load**. Inspect its run status and published output. Later pulls are manual via **Run now**; there is no promised REST scheduler. If the remote service returns 401/403, correct its credentials; for 429, respect the provider's limit. Never put headers or tokens into screenshots or support messages.

## Push ingestion

Create a Push source in **Ingestion**, retain its one-time token in your sender's secret store and use the endpoint/request example displayed for that source. Keep the token server-side. Send within the 8MiB/5,000-row request limits and inspect the resulting runs before retrying a failed delivery. Do not assume raw Push batches are registered Connect Bronze snapshots.

## PostgreSQL and MySQL

These are **incremental extractors, not CDC**. Use a read-only source account reachable from the connector agent. Tables need a primary key and a non-null monotonic cursor for incremental extraction; updates that do not advance the cursor and row deletions are not captured.

Create the appropriate source in **Ingestion**, then open **Connectors → Meridian Connect**. Register the connection against that source and an agent credential reference. The operator supplies the referenced private credential file to an agent (deploy/connect/README.md); credentials are not entered into ordinary connection JSON. Queue **Check credentials**, then **Discover streams**. Choose the discovered table, key, cursor and sync mode, save the stream and choose **Sync now**. Without a running agent these jobs stay queued; the web application does not run one itself.

PostgreSQL credential files use psycopg settings such as `host`, `port`, `dbname`, `user`, `password`, `sslmode`; MySQL uses the actor's documented configuration in its specification. Obtain these from your database operator and supply them through the installation's protected secret directory. Do not weaken source network/TLS policy to make a check pass.

## Kafka

Ask the operator for reachable brokers, topic read permissions and, for wire-format Avro, a compatible schema registry. Follow the same connection check/discovery/stream/sync workflow. JSON, Debezium JSON, wire-format Avro, tombstones and delete events are understood. Offsets are tracked durably per partition.

An `offset_out_of_range` error means retention removed positions before the saved checkpoint. Inspect the reported gap and approve it only if you accept the missing positions; retrying cannot recover deleted broker data. Dead-letter replay retries selected failed records without rewinding the source cursor. See KAFKA.md for formats and exact limitations. Consumer-group scaling/rebalance, production SASL/TLS and applied warehouse merges are not certified.

## Runs, recovery and output

Watch replication runs for status, attempts, record/byte counts and durable checkpoints. Check and discovery are separate jobs from data sync. Cancellation fences new commits; a source process may still finish a blocking read before exiting. A failed agent lease can be reclaimed and resumes from committed state. Reset/backfill retains append-only history; it does not overwrite a warehouse table.

Connect publishes immutable raw NDJSON envelopes, not applied upserts/deletes. Under **Committed bronze batches**, choose **Inspect batch** to verify its checksum and preview up to 100 raw records. Expand **Manifest and checkpoint** for metadata. Raw inspection uses the source owner's storage access; another workspace member cannot bypass that by viewing connection metadata. A failed integrity check shows an error instead of records. External owner-only reads require a registered engine and short-lived token: follow BRONZE_CONSUMPTION.md. This is an operator task. REST/Push/native files do not automatically bridge into this reader.

## Safe troubleshooting

- **Queued indefinitely:** ask the operator to check the agent, its source token, credential reference and network reachability.
- **Verify your email before continuing:** complete confirmation for the current account; do not request elevated roles.
- **Workspace is inactive / access unavailable:** ask the workspace operator to review membership/status; switching browser IDs does not grant access.
- **Schema drift:** inspect the source schema and update the stream through supported controls before rerunning; records are not silently coerced into a new schema.
- **Conflicting batch/checksum:** stop retry amplification and retain evidence. Never edit committed objects or checkpoint rows manually.

Share timestamps, resource/run IDs and safe error codes. Never share passwords, tokens, raw customer data or signed object URLs in support messages.
