"""Create, inspect, meter, suspend, or drop resources owned by this experiment."""
import argparse
import json
import os
from common import DB, DT_WH, EVIDENCE, account_identity, verify_ownership, connect, dump, query


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=('setup', 'inspect', 'capture', 'suspend', 'cleanup'))
    args = parser.parse_args()
    ownership = EVIDENCE / 'owned_resources.json'
    with connect(schema=None) as conn:
        if args.action == 'setup':
            if ownership.exists():
                raise RuntimeError('An ownership manifest already exists. Inspect or clean up; use a fresh directory for another experiment.')
            generation = os.environ.get('SNOWFLAKE_LAB_WAREHOUSE_GENERATION')
            if generation not in (None, '1', '2'):
                raise ValueError('SNOWFLAKE_LAB_WAREHOUSE_GENERATION must be 1 or 2')
            owned = {'identity': account_identity(conn), 'database': None, 'warehouse': None}
            dump(ownership.name, owned)
            # Deliberately no OR REPLACE and no IF NOT EXISTS: never adopt preexisting objects.
            query(conn, f'CREATE TRANSIENT DATABASE {DB} DATA_RETENTION_TIME_IN_DAYS=0', 'create_database')
            owned['database'] = DB
            dump(ownership.name, owned)
            query(conn, f'CREATE SCHEMA {DB}.DT', 'create_schema')
            suffix = f" GENERATION='{generation}'" if generation else ''
            query(conn, f"CREATE WAREHOUSE {DT_WH} WAREHOUSE_SIZE='XSMALL' AUTO_SUSPEND=60 "
                  f"AUTO_RESUME=TRUE INITIALLY_SUSPENDED=TRUE ENABLE_QUERY_ACCELERATION=FALSE{suffix}", 'create_warehouse')
            owned['warehouse'] = DT_WH
            dump(ownership.name, owned)
            dump('warehouse_configuration.json', query(conn, f"SHOW WAREHOUSES LIKE '{DT_WH}'", 'warehouse_configuration'))
        elif args.action == 'inspect':
            query(conn, f"SHOW DATABASES LIKE '{DB}'", 'database')
            query(conn, f"SHOW WAREHOUSES LIKE '{DT_WH}'", 'warehouse')
        elif args.action == 'capture':
            verify_ownership(conn)
            query(conn, f'USE WAREHOUSE {DT_WH}')
            dump('warehouse_metering.json', query(conn, f"SELECT * FROM TABLE({DB}.INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY("
                f"DATE_RANGE_START=>DATEADD('DAY',-1,CURRENT_TIMESTAMP()))) WHERE WAREHOUSE_NAME='{DT_WH}'", 'warehouse_metering'))
        elif args.action in ('suspend', 'cleanup'):
            owned = verify_ownership(conn, require_resources=False)
            if owned['database'] is None and owned['warehouse'] is None:
                print('No resources are currently owned; no resource action was taken.')
                return
            if args.action == 'suspend':
                if owned['database']:
                    tables = query(conn, f'SHOW DYNAMIC TABLES IN DATABASE {DB}', 'dynamic_tables')
                    for row in tables['rows']:
                        d = dict(zip(tables['columns'], row))
                        # All names come from this script's owned database; quote for SHOW output safety.
                        schema = d['schema_name'].replace('"', '""')
                        table = d['name'].replace('"', '""')
                        query(conn, f'ALTER DYNAMIC TABLE {DB}."{schema}"."{table}" SUSPEND', 'suspend_' + d['name'])
                if owned['warehouse']:
                    query(conn, f'ALTER WAREHOUSE {DT_WH} SUSPEND', 'suspend_warehouse', expected_error=True)
            else:
                if owned['database']:
                    query(conn, f'DROP DATABASE IF EXISTS {DB}', 'drop_database')
                    owned['database'] = None
                    dump(ownership.name, owned)
                if owned['warehouse']:
                    query(conn, f'DROP WAREHOUSE IF EXISTS {DT_WH}', 'drop_warehouse')
                    owned['warehouse'] = None
                    dump(ownership.name, owned)
                database = query(conn, f"SHOW DATABASES LIKE '{DB}'", 'verify_database_absent')
                warehouse = query(conn, f"SHOW WAREHOUSES LIKE '{DT_WH}'", 'verify_warehouse_absent')
                # LIKE treats underscores as wildcards; compare the exact returned names.
                for result, name in ((database, DB), (warehouse, DT_WH)):
                    index = [c.lower() for c in result['columns']].index('name')
                    assert not any(row[index] == name for row in result['rows'])
                dump('cleanup.json', {'database_absent': True, 'warehouse_absent': True})


if __name__ == '__main__':
    main()
