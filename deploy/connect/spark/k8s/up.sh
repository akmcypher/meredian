#!/bin/sh
# Builds the Spark ingestion image, loads it into the kind cluster (kind.yaml) and applies the
# manifests. Creates the cluster's Spark secret once, from 32 random bytes, without putting it in
# an argument; the key files live only in a private temporary directory. `key` is mounted into the
# driver; `key.b64` (its base64 form, which is what Spark uses) reaches executors by secretKeyRef.
#   sh deploy/connect/spark/k8s/up.sh
set -eu
here=$(cd "$(dirname "$0")" && pwd)
ctx=${KUBE_CONTEXT:-kind-meridian-spark}
k() { kubectl --context "$ctx" "$@"; }
docker build -q -f "$here/Dockerfile" -t meridian-spark-ingest-k8s:local "$here/../.."
kind load docker-image meridian-spark-ingest-k8s:local --name "${KIND_CLUSTER:-meridian-spark}"
k apply -f "$here/namespace.yaml" -f "$here/rbac.yaml"
if ! k -n meridian-spark get secret spark-auth >/dev/null 2>&1; then
  umask 077
  dir=$(mktemp -d)
  trap 'rm -rf "$dir"' EXIT
  od -An -tx1 -N32 /dev/urandom | tr -d ' \n' >"$dir/key"
  base64 <"$dir/key" | tr -d '\n' >"$dir/key.b64"
  k -n meridian-spark create secret generic spark-auth --from-file=key="$dir/key" \
    --from-file=key.b64="$dir/key.b64"
fi
k apply -f "$here/spark.yaml"
k -n meridian-spark rollout restart deploy/spark-driver
k -n meridian-spark rollout status deploy/spark-driver --timeout=180s
