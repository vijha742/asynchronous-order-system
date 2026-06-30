#!/bin/bash

DB_HOST="localhost"
DB_PORT="5435"
DB_NAME="inventorydb"
DB_USER="inventoryuser"
DB_PASS="inventorypass"

export PGPASSWORD="$DB_PASS"

psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" <<EOF
DO \$\$
DECLARE
    i INT;
    q INT;
    p BIGINT;
BEGIN
    FOR i IN 1..1000 LOOP
        q := floor(random() * 991 + 10)::int;
        p := floor(random() * 100000 + 1)::bigint;
        INSERT INTO item (product_id, quantity, price)
        VALUES (i, q, p)
        ON CONFLICT (product_id) DO UPDATE
        SET quantity = EXCLUDED.quantity,
            price = EXCLUDED.price;
    END LOOP;
END \$\$;
EOF

unset PGPASSWORD
