#!/bin/sh
# Writes this container's Spark configuration with RPC authentication and encryption turned on,
# then runs the given command (a master, a worker, or nothing for spark-submit).
#
# The shared secret is read from $SPARK_AUTH_SECRET_FILE (default /run/meridian-spark/key, created
# once by the spark-secret service in compose.yml) and written into spark-defaults.conf in
# $SPARK_CONF_DIR with mode 0600, so it never appears on a command line or in `docker inspect`.
# Every node and every application must share it: standalone mode has no per-application secret.
#
#   spark.authenticate            only holders of the secret can join the cluster or an application
#   spark.network.crypto.enabled  AES encryption of RPC and block transfers (credentials, lease
#                                 tokens and record data travel to executors this way)
#   spark.io.encryption.enabled   encryption of shuffle and spill files on local disk
set -eu
key_file=${SPARK_AUTH_SECRET_FILE:-/run/meridian-spark/key}
conf_dir=${SPARK_CONF_DIR:?SPARK_CONF_DIR must be set}
[ -s "$key_file" ] || { echo "secure-conf: missing Spark secret at $key_file" >&2; exit 1; }
umask 077
mkdir -p "$conf_dir"
tmp="$conf_dir/.spark-defaults.$$"
{
  echo "spark.authenticate true"
  printf 'spark.authenticate.secret %s\n' "$(cat "$key_file")"
  echo "spark.network.crypto.enabled true"
  echo "spark.io.encryption.enabled true"
} > "$tmp"
mv "$tmp" "$conf_dir/spark-defaults.conf"
[ "$#" -eq 0 ] || exec "$@"
