#!/bin/bash
echo "--- Running Who Am I Debug Script ---"
SA_EMAIL=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email" -H "Metadata-Flavor: Google")
echo "This cluster is configured to run as Service Account: ${SA_EMAIL}"
echo "------------------------------------"