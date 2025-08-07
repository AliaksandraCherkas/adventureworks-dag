#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration ---
# The name of the metadata key to read from. The DAG must set this.
INSTANCE_METADATA_KEY="cloud-sql-instance-name"
PROXY_LOG_FILE="/var/log/cloud-sql-proxy.log"

echo "Starting Cloud SQL Proxy initialization script."

# --- Step A: Fetch Instance Connection Name from Metadata ---
# We still need to get the value from the VM's metadata.
echo "Fetching instance connection name from metadata key: ${INSTANCE_METADATA_KEY}"
INSTANCE_CONNECTION_NAME_VALUE=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/attributes/${INSTANCE_METADATA_KEY}" -H "Metadata-Flavor: Google")

if [[ -z "${INSTANCE_CONNECTION_NAME_VALUE}" ]]; then
  echo "ERROR: Metadata key '${INSTANCE_METADATA_KEY}' not found or is empty." >&2
  exit 1
fi

# --- Step B: Export the Fetched Value as an Environment Variable ---
# This makes the value available as an environment variable within this script's session.
export CLOUD_SQL_CONNECTION_NAME="${INSTANCE_CONNECTION_NAME_VALUE}"
echo "Successfully exported CLOUD_SQL_CONNECTION_NAME environment variable."

# --- Download and Install the Proxy ---
echo "Downloading Cloud SQL Auth Proxy..."
wget https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.10.1/cloud-sql-proxy.linux.amd64 -O /usr/local/bin/cloud-sql-proxy
chmod +x /usr/local/bin/cloud-sql-proxy

# --- Start the Proxy Using the Environment Variable ---
echo "Starting Cloud SQL Proxy using the environment variable. Logging to ${PROXY_LOG_FILE}"
# The script now references the environment variable we just set.
nohup /usr/local/bin/cloud-sql-proxy --private-ip "${CLOUD_SQL_CONNECTION_NAME}" > "${PROXY_LOG_FILE}" 2>&1 &

sleep 3
echo "✅ Cloud SQL Proxy process started successfully."