Meridian Community for Windows 11 (x64 and ARM64) - local preview installation

This folder is the Meridian source plus a launcher. It runs everything on this computer: a local
backend in Docker and the web app on http://localhost:8080. It is a preview for evaluation, not a
hosted or certified deployment. Run one installation per trust boundary.

1. Install Docker Desktop and start it once (it enables WSL 2):
     winget install --exact --id Docker.DockerDesktop
   Then in Docker Desktop > Settings > Resources > Network set Port binding behavior to
   "Localhost by default" and Apply & restart, so backend ports are only reachable from this
   computer (install checks this).
2. Open PowerShell or Command Prompt in this folder and run:
     .\meridian.cmd install
   Node.js 26, Bun 1.4 and Supabase CLI 2.122.0 are used from PATH if present; otherwise pinned,
   SHA-256-verified copies are downloaded to %LOCALAPPDATA%\Meridian\tools.
3. .\meridian.cmd start        (opens http://localhost:8080)
4. Sign up, confirm the email in the local mail viewer the launcher prints, then:
     .\meridian.cmd bootstrap-admin you@example.com

Other commands: status, stop, logs, backup, restore <dir>, upgrade, uninstall, doctor, help.
uninstall permanently deletes this installation's data; back up first.

Generated credentials live in .meridian\ in this folder. Keep it private.
Full documentation: docs\LOCAL_DEV.md and QUICKSTART.md.
