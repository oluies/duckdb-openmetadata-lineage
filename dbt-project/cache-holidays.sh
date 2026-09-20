#!/bin/bash
# Download the Azure Open Datasets public-holidays Parquet (~330 KB) once, so the dbt demo
# runs offline. Called by `make setup`; safe to re-run.
set -eu
. "$(dirname "$0")/../scripts/env.sh"
cd "$DEMO_ROOT"
mkdir -p data/cache
[ -s cache/holidays.parquet ] && { echo "cache/holidays.parquet already cached"; exit 0; }
duckdb -no-init -c "
  SET azure_transport_option_type = 'curl';
  SET ca_cert_file = '${DEMO_CA_BUNDLE:-/etc/ssl/cert.pem}';
  CREATE SECRET (TYPE azure, PROVIDER config, ACCOUNT_NAME 'azureopendatastorage');
  COPY (FROM 'azure://holidaydatacontainer/Processed/*.parquet')
  TO 'cache/holidays.parquet' (FORMAT parquet, COMPRESSION zstd);
  SELECT count(*) AS holidays_cached FROM 'cache/holidays.parquet';"
