# Local development

## Windows installer (Community preview)

On Windows 11 (x64 or ARM64) you can install without setting up the toolchain by hand. Download
`meridian-<version>-windows.zip` and its `.sigstore.json` from the GitHub release, then optionally
verify it with cosign (command at the top of `.github/workflows/release.yml`). Extract it to a short
path such as `C:\meridian`, and run, from that folder:

```powershell
winget install --exact --id Docker.DockerDesktop   # once; start Docker Desktop and wait for "Engine running"
.\meridian.cmd install     # checks Docker, gets Node 26 / Bun 1.4 / Supabase CLI 2.122.0, builds
.\meridian.cmd start       # backend + web app in the background; opens http://localhost:8080
.\meridian.cmd status      # tools, Docker, installation and health
.\meridian.cmd stop        # stops the app and backend; data is kept
.\meridian.cmd uninstall   # permanently deletes this installation (asks for its id; back up first)
```

Docker Desktop for Windows publishes container ports on every network interface by default, and
`self-host.mjs` refuses to run a backend that is reachable from the network. Docker Desktop ignores
the per-network loopback option the installer sets (and the engine `"ip"` setting), so once, in
Docker Desktop > Settings > Resources > Network, set **Port binding behavior** to "Localhost by
default" and choose "Apply & restart" (`install` checks this and explains it). It applies to every
container that does not name an address, and the restart stops running containers; those with a
restart policy come back.

The launcher (`installer/windows/meridian.ps1`, run through `meridian.cmd`) wraps
`scripts/self-host.mjs`; `bootstrap-admin`, `backup`, `restore`, `upgrade` and `check` are passed
through. It uses Node, Bun and the Supabase CLI from PATH when the required version is there;
otherwise it downloads pinned releases, checks them against SHA-256 hashes in the script, and keeps
them in `%LOCALAPPDATA%\Meridian\tools` without changing PATH or installing anything system-wide.
Docker Desktop is never installed for you. State and generated credentials stay in `.meridian\`
inside the extracted folder; the zip contains only committed files, no secrets. `-Port` and
`-BackendPort` on `install` choose other ports (defaults 8080 and 58321–58325). Logs are in
`.meridian\logs\` (`.\meridian.cmd logs`). From a source checkout, run
`installer\windows\meridian.cmd` the same way.

Status: locally demonstrated on 2026-11-08 on Windows 11 ARM64 (Docker Desktop 4.97): clean extract,
pinned tool downloads, install, start, sign-up with email confirmation, administrator bootstrap,
stop, restart and uninstall. Not yet run on x64; release CI on x64 only smoke-tests the launcher. This is a single-machine preview, not a hosted or
certified deployment. The launcher is not Authenticode-signed; the zip is signed with Sigstore.

## macOS and Linux installer (Community preview)

On macOS (Apple silicon or Intel) and on Ubuntu or Debian (x64 or ARM64) the same launcher does
what the Windows one does. Download `meridian-<version>-macos.tar.gz` or
`meridian-<version>-linux.tar.gz` and its `.sigstore.json` from the GitHub release, optionally
verify it with cosign (command at the top of `.github/workflows/release.yml`), extract it, and run,
from that folder:

```sh
tar -xzf meridian-<version>-linux.tar.gz && cd meridian-<version>-linux
./meridian install     # checks Docker, gets Node 26 / Bun 1.4 / Supabase CLI 2.122.0, builds
./meridian start       # backend + web app in the background; opens http://localhost:8080
./meridian status      # tools, Docker, installation and health
./meridian stop        # stops the app and backend; data is kept
./meridian uninstall   # permanently deletes this installation (asks for its id; back up first)
```

Docker comes first and is never installed for you:

- **macOS:** Docker Desktop for your Mac (`brew install --cask docker` or the download from
  docs.docker.com), opened once until it reports "Engine running". If Settings > Resources >
  Network offers **Port binding behavior**, set it to "Localhost by default"; `install` refuses a
  Docker Desktop that is recorded as publishing on all interfaces.
- **Ubuntu/Debian:** Docker Engine and the compose plugin from Docker's apt repository
  (docs.docker.com/engine/install/ubuntu/), with your account in the `docker` group
  (`sudo usermod -aG docker "$USER"`, then sign in again; that group is equivalent to root). The
  backend's Docker network publishes on 127.0.0.1 only. Compose is only needed for the connector
  agents in `deploy/connect`. Minimal images (WSL, cloud) may also need curl, `libatomic1`
  (Node.js on ARM64 needs it: `sudo apt install libatomic1`), and `unzip` or `python3` to unpack
  Bun.

After `install` and after every `start`, the launcher connects to the backend and app ports on
this machine's own network addresses; if any answers, it stops the backend and explains the fix.
That checks what the network actually sees, on top of the Docker-reported bindings that
`self-host.mjs` verifies. New installs also refuse ports already in use (`--port` and
`--backend-port` choose others; defaults 8080 and 58321–58325).

The launcher (`installer/posix/meridian`, POSIX `sh`) wraps `scripts/self-host.mjs`;
`bootstrap-admin`, `backup`, `restore`, `upgrade` and `check` are passed through. It uses Node, Bun
and the Supabase CLI from PATH when the required version is there; otherwise it downloads pinned
releases, checks them against SHA-256 hashes in the script, and keeps them in
`~/Library/Application Support/Meridian/tools` (macOS) or `~/.local/share/meridian/tools` (Linux;
`MERIDIAN_TOOLS_DIR` overrides). It never uses sudo, installs nothing system-wide and does not edit
PATH or shell profiles. `./meridian tools` only downloads and verifies the toolchain;
`./meridian doctor` checks prerequisites without downloading. State and generated credentials stay
in `.meridian/` inside the extracted folder; the tarballs contain only committed files, no secrets.
Logs are in `.meridian/logs/` (`./meridian logs`). From a source checkout, run
`installer/posix/meridian` the same way. Packages are built with
`node scripts/package-posix.mjs <output-dir> <version>`.

Status: locally demonstrated on 2026-11-14 on Ubuntu 24.04 ARM64 (WSL 2, Docker Engine 29.9.3, non-root
user in the `docker` group) from a tarball built by `scripts/package-posix.mjs`: clean extract, pinned
tool downloads, install, start, the app loading in a browser, sign-up with email confirmation,
administrator bootstrap, status, stop, restart, the network-exposure check refusing a port answering
on the machine's address, and uninstall (containers, volumes, network, generated files and tool
cache removed). Not yet run on Linux x64 or on Debian. **macOS is not locally demonstrated**: the
maintainers had no Mac; its pinned downloads were checked (SHA-256 and archive layout) and the
release workflow's `macos-smoke` job (not yet run) checks `help`, `doctor` and `tools` on a macOS runner. This is a
single-machine preview, not a hosted or certified deployment. The tarballs are signed with Sigstore.

### Homebrew (macOS and Linux)

Once the tap is live, `brew install <owner>/tap/meridian` (`<owner>` is the GitHub account that
publishes this repository) installs the same tarball read-only in
Homebrew's prefix, plus a `meridian` command. On first use the launcher copies it to
`~/Library/Application Support/Meridian/app` (macOS) or `~/.local/share/meridian/app` (Linux;
`MERIDIAN_HOME` overrides the parent) and keeps state, dependencies and the build there, so
`meridian install`, `meridian start` and the other commands work as above. After
`brew upgrade meridian`, run `meridian stop` and then `meridian upgrade`, which starts the backend,
applies new migrations and rebuilds; `start` refuses until that is done. Run `meridian uninstall`
before `brew uninstall meridian`. `scripts/homebrew-formula.mjs` renders the formula from a
release's tarballs, and the public repository's `homebrew.yml` verifies their Sigstore bundles and
commits it to `<owner>/homebrew-tap` when a release is published.

Status: the formula was locally demonstrated on 2026-11-15 with Homebrew on Linux (Ubuntu 24.04
ARM64 under WSL 2, Docker Engine), using a local tap and tarballs built by `package-posix.mjs`:
`brew install`, `brew test`, `meridian install`, `start`, `status`, `stop`, `brew upgrade` to a
newer version (start then refused until `meridian upgrade`, which kept the installation and its
data), restart, `meridian uninstall` and `brew uninstall`. Homebrew on Linux needs its documented
prerequisites (`build-essential`). Not yet available to users: the tap repository and its
`HOMEBREW_TAP_TOKEN` secret do not exist yet and no release has been published. **Homebrew on
macOS is not locally demonstrated.**

## Developing from source

### Prerequisites

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
