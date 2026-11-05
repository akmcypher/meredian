# Meridian Community quick start

Meridian ingests APIs, files, PostgreSQL, MySQL and Kafka into scoped workspaces with recoverable
raw batches. It is a preview; see the [release notes](docs/RELEASE_NOTES.md).

## Install

`scripts/self-host.mjs` starts a local installation with its own backend. It needs Docker,
Node 26, Bun 1.4 and Supabase CLI 2.122. For development, follow [LOCAL_DEV.md](docs/LOCAL_DEV.md).
Never run install or reset commands against an existing database you want to keep.

Each account that signs up becomes the owner of its own workspace, not a platform administrator.
The installation operator creates the platform administrator with the service-only
`bootstrap_platform_admin` function. Never expose its service credential to users or the browser.

Read the [user guide](docs/USER_GUIDE.md) for source walkthroughs, inspecting data, retries and
limitations. Run one installation per trust boundary.
