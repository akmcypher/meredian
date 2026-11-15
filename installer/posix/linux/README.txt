Meridian Community for Ubuntu and Debian (x64 and ARM64) - local preview installation

This folder is the Meridian source plus a launcher. It runs everything on this computer: a local
backend in Docker and the web app on http://localhost:8080. It is a preview for evaluation, not a
hosted or certified deployment. Run one installation per trust boundary.

1. Install Docker Engine and the compose plugin from Docker's apt repository:
     https://docs.docker.com/engine/install/ubuntu/   (Debian: .../engine/install/debian/)
     sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
   Let your account use Docker without sudo, then sign out and back in:
     sudo usermod -aG docker "$USER"
   (docker group membership is equivalent to root on this machine.)
   The launcher creates the backend on a Docker network that publishes ports on 127.0.0.1 only,
   and install and start check from the network side that no backend port is reachable.
2. Open a terminal in this folder and run:
     ./meridian install
   (If the file lost its executable bit, run: sh ./meridian install)
   Node.js 26, Bun 1.4 and Supabase CLI 2.122.0 are used from PATH if present; otherwise pinned,
   SHA-256-verified copies are downloaded to ~/.local/share/meridian/tools. Nothing is installed
   system-wide and the launcher never uses sudo. It needs curl (or wget), tar, and unzip or
   python3 to unpack Bun. On ARM64, Node.js also needs libatomic1, which minimal images (WSL,
   cloud) leave out: sudo apt install libatomic1
3. ./meridian start        (opens http://localhost:8080 when a desktop browser is available)
4. Sign up, confirm the email in the local mail viewer the launcher prints, then:
     ./meridian bootstrap-admin you@example.com

Other commands: status, stop, logs, backup, restore <dir>, upgrade, uninstall, doctor, help.
uninstall permanently deletes this installation's data; back up first.

Generated credentials live in .meridian/ in this folder. Keep it private.
Full documentation: docs/LOCAL_DEV.md and QUICKSTART.md.
