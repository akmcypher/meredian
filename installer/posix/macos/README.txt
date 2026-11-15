Meridian Community for macOS (Apple silicon and Intel) - local preview installation

This folder is the Meridian source plus a launcher. It runs everything on this Mac: a local
backend in Docker and the web app on http://localhost:8080. It is a preview for evaluation, not a
hosted or certified deployment. Run one installation per trust boundary.

The macOS launcher has not been run end to end by the Meridian maintainers yet (it shares its code
with the Linux launcher, which has). Please report what you find.

1. Install Docker Desktop for your Mac (Apple silicon or Intel build) and open it once:
     https://docs.docker.com/desktop/setup/install/mac-install/
     or, with Homebrew: brew install --cask docker
   If Docker Desktop > Settings > Resources > Network offers "Port binding behavior", set it to
   "Localhost by default" and Apply & restart, so backend ports are only reachable from this Mac.
   Install and start also check from the network side that no backend port is reachable.
2. Open Terminal in this folder and run:
     ./meridian install
   (If the file lost its executable bit, run: sh ./meridian install)
   Node.js 26, Bun 1.4 and Supabase CLI 2.122.0 are used from PATH if present; otherwise pinned,
   SHA-256-verified copies are downloaded to ~/Library/Application Support/Meridian/tools.
   Nothing is installed system-wide and nothing asks for your password.
3. ./meridian start        (opens http://localhost:8080)
4. Sign up, confirm the email in the local mail viewer the launcher prints, then:
     ./meridian bootstrap-admin you@example.com

Other commands: status, stop, logs, backup, restore <dir>, upgrade, uninstall, doctor, help.
uninstall permanently deletes this installation's data; back up first.

Generated credentials live in .meridian/ in this folder. Keep it private.
Full documentation: docs/LOCAL_DEV.md and QUICKSTART.md.
