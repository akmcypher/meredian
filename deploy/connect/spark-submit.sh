#!/bin/sh
# Starts spark_ingest.py with the job (claim, source config, ingest token) the agent writes to
# stdin. spark-submit does not pass its stdin to the Python driver, so the job goes through a file
# that only this user can read; the driver deletes it as soon as it has read it.
#   CONNECT_SPARK_SUBMIT="sh /opt/meridian/connect/spark-submit.sh --master spark://host:7077 [--conf ...]"
set -eu
umask 077
job=$(mktemp "${TMPDIR:-/tmp}/meridian-job.XXXXXX")
trap 'rm -f "$job"' EXIT
cat > "$job"
"${SPARK_HOME:-/opt/spark}/bin/spark-submit" "$@" "$(dirname "$0")/spark_ingest.py" --job-file "$job"
