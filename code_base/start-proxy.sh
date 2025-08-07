#!/bin/bash
set -e

# Download the Cloud SQL Auth Proxy
wget https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.10.1/cloud-sql-proxy.linux.amd64 -O /usr/local/bin/cloud-sql-proxy
chmod +x /usr/local/bin/cloud-sql-proxy

# Run the proxy in the background. The --private-ip flag is crucial.
# We redirect stdout/stderr to a log file for later debugging if needed.
nohup /usr/local/bin/cloud-sql-proxy --private-ip adventureworks-project-466602:us-central1:cld-sql-adventureworks > /var/log/cloud-sql-proxy.log 2>&1 &
