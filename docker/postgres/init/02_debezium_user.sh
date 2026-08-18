#!/bin/bash
# Creates the replication user Debezium connects as, and the publication it
# reads from.
#
# Two things here are easy to get wrong and both stop CDC dead:
#   1. The user needs REPLICATION, not just SELECT. Debezium opens a
#      replication slot, which is a privileged operation.
#   2. The publication must exist and list the tables. Debezium can create one
#      itself, but that requires superuser and hides the configuration. An
#      explicit publication is auditable and is what you would do in production.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE ROLE ${DEBEZIUM_USER}
        WITH REPLICATION LOGIN PASSWORD '${DEBEZIUM_PASSWORD}';

    GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO ${DEBEZIUM_USER};
    GRANT USAGE ON SCHEMA erp TO ${DEBEZIUM_USER};
    GRANT SELECT ON ALL TABLES IN SCHEMA erp TO ${DEBEZIUM_USER};
    ALTER DEFAULT PRIVILEGES IN SCHEMA erp
        GRANT SELECT ON TABLES TO ${DEBEZIUM_USER};

    -- Only the ERP tables are captured. commerce.* is served by the REST API,
    -- so publishing it would ingest the same data twice by two routes.
    CREATE PUBLICATION northpeak_erp
        FOR TABLE erp.orders, erp.order_items, erp.payments;
EOSQL

echo "Debezium role '${DEBEZIUM_USER}' and publication 'northpeak_erp' created."
