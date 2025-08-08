#!/bin/bash
set -e

# ---  Fetch the secret from Secret Manager ---
# Fetch the latest version of the secret and store it in a variable.

export CLOUD_SQL_CONNECTION_NAME=$(gcloud secrets versions access latest --secret="cloud-sql-connection-name" --project="adventureworks-project-466602")

# --- End of New Section ---

# Download the Cloud SQL Auth Proxy
# This part can be optimized by checking if the proxy already exists
if [ ! -f /usr/local/bin/cloud-sql-proxy ]; then
    wget https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.10.1/cloud-sql-proxy.linux.amd64 -O /usr/local/bin/cloud-sql-proxy
    chmod +x /usr/local/bin/cloud-sql-proxy
fi

# Check if the connection name was retrieved successfully
if [ -z "$CLOUD_SQL_CONNECTION_NAME" ]; then
    echo "Error: CLOUD_SQL_CONNECTION_NAME could not be retrieved from Secret Manager."
    exit 1
fi

# Run the proxy in the background. The --private-ip flag is crucial.
# We redirect stdout/stderr to a log file for later debugging if needed.
nohup /usr/local/bin/cloud-sql-proxy --private-ip ${CLOUD_SQL_CONNECTION_NAME} > /var/log/cloud-sql-proxy.log 2>&1 &

echo "Cloud SQL Auth Proxy started successfully."