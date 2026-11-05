#!/usr/bin/env bash
set -euo pipefail
: "${SOURCE_IMAGE:?Supply built immutable source image}" "${DESTINATION_IMAGE:?Supply built immutable destination image}" "${COSIGN_PUBLIC_KEY:?}"
for image in "$SOURCE_IMAGE" "$DESTINATION_IMAGE"; do
  [[ "$image" =~ @sha256:[a-f0-9]{64}$ ]] || { echo 'Immutable digest required'; exit 1; }
  cosign verify --key "$COSIGN_PUBLIC_KEY" "$image" >/dev/null
  trivy image --exit-code 1 --severity HIGH,CRITICAL "$image"
  syft "$image" -o spdx-json > "/tmp/$(echo "$image" | tr '/:@' '___').sbom.json"
done
