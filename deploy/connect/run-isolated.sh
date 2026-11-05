#!/usr/bin/env bash
# STDIO actor runner: credentials/config are mounted files, never CLI secret values.
set -euo pipefail
: "${CONNECTOR_IMAGE:?}" "${CONFIG_DIR:?}"
[[ "$CONNECTOR_IMAGE" =~ @sha256:[a-f0-9]{64}$ ]] || { echo 'Built immutable digest required'; exit 1; }
exec docker run --rm -i --read-only --cap-drop ALL --security-opt no-new-privileges \
  --memory 512m --cpus 1 --pids-limit 64 --user 65532:65532 \
  --tmpfs /tmp:rw,noexec,nosuid,size=16m -v "$CONFIG_DIR:/config:ro" \
  --network "${CONNECT_NETWORK:-bridge}" "$CONNECTOR_IMAGE" "$@"
