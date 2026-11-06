"""Small, complete Snowpipe Streaming -> Dynamic Tables demonstration.

Uses an existing key-pair connection; never creates credentials or grants roles.
Every cloud action is explicit. Setup refuses collisions. Cleanup verifies ownership.
"""
import argparse
from datetime import datetime, timedelta, timezone
import io
import json
import math
import os
from pathlib import Path
import re
import time
import tomllib
import uuid

HERE = Path(__file__).resolve().parent


def identifier(value):
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,100}", value):
        raise argparse.ArgumentTypeError("Use letters, digits and underscores; begin with a letter")
    return value.upper()


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['setup', 'seed', 'read', 'moving-window', 'precision', 'cleanup'])
    parser.add_argument('--connection', default=os.getenv('SNOWFLAKE_CONNECTION_NAME'))
    parser.add_argument('--database', type=identifier, default='SENSOR_TIMESERIES_DEMO')
    parser.add_argument('--connections-file', type=Path, default=Path.home()/'.snowflake/connections.toml')
    parser.add_argument('--state-dir', type=Path, default=HERE/'.state')
    args = parser.parse_args()
    if not args.connection:
        parser.error('Set SNOWFLAKE_CONNECTION_NAME or pass --connection')
    cfg = tomllib.loads(args.connections_file.expanduser().read_text())[args.connection]
    if not all(cfg.get(k) for k in ('account', 'user', 'private_key_file')):
        parser.error('This demo requires an existing account/user/private_key_file key-pair profile')
    import snowflake.connector
    db, wh = args.database, args.database+'_WH'
    params = {k: cfg[k] for k in ('account', 'user', 'role', 'authenticator', 'private_key_file', 'private_key_file_pwd') if k in cfg}
    params['private_key_file'] = str(Path(params['private_key_file']).expanduser())
    args.state_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = args.state_dir/(db+'.json')

    def save(state):
        tmp = manifest_path.with_suffix('.tmp')
        tmp.write_text(json.dumps(state, indent=2)+'\n')
        tmp.replace(manifest_path)

    with snowflake.connector.connect(**params, session_parameters={
            'TIMEZONE': 'UTC', 'QUERY_TAG': 'snowflake-timeseries-quickstart'},
            login_timeout=30, network_timeout=120) as conn:
        def query(sql):
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()

        identity = list(query('SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT(), CURRENT_REGION()')[0])
        if args.action == 'setup':
            if manifest_path.exists():
                raise RuntimeError('Ownership state already exists. Inspect it or run cleanup first.')
            state = {'identity': identity, 'database': db, 'warehouse': wh,
                     'marker': 'sensor-demo-'+uuid.uuid4().hex, 'created': []}
            save(state)
            for kind, name, ddl in [
                ('DATABASE', db, f"CREATE TRANSIENT DATABASE {db} DATA_RETENTION_TIME_IN_DAYS=0 COMMENT='{state['marker']}'"),
                ('WAREHOUSE', wh, f"CREATE WAREHOUSE {wh} WAREHOUSE_SIZE='XSMALL' AUTO_SUSPEND=60 AUTO_RESUME=TRUE INITIALLY_SUSPENDED=TRUE COMMENT='{state['marker']}'")
            ]:
                query(ddl)
                state['created'].append([kind, name])
                save(state)
            query(f'CREATE SCHEMA {db}.SENSORS')
        else:
            state = json.loads(manifest_path.read_text())
            if (state.get('identity'), state['database'], state['warehouse']) != (identity, db, wh):
                raise RuntimeError('Connection or resource names do not match the ownership manifest')

        allowed = [['DATABASE', db], ['WAREHOUSE', wh]]
        if any(pair not in allowed for pair in state['created']):
            raise RuntimeError('Unexpected resource in ownership manifest')
        if len(state['created']) != len({tuple(pair) for pair in state['created']}):
            raise RuntimeError('Duplicate resource in ownership manifest')
        if not re.fullmatch(r'sensor-demo-[0-9a-f]{32}', state['marker']):
            raise RuntimeError('Invalid ownership marker')

        def owned_object_exists(kind, name):
            with conn.cursor() as cur:
                cur.execute(f"SHOW {kind}S LIKE '{name}'")
                cols = [d[0] for d in cur.description]
                matches = [dict(zip(cols, r)) for r in cur.fetchall() if r[cols.index('name')] == name]
            if matches and (len(matches) != 1 or matches[0]['comment'] != state['marker']):
                raise RuntimeError('Ownership marker differs; refusing to use '+name)
            return bool(matches)

        if args.action == 'cleanup':
            if state.get('cleaned'):
                print('Already cleaned; no resource action taken.')
                return
            # Match the exact name, not merely SHOW's LIKE wildcard pattern.
            for kind, name in reversed(list(state['created'])):
                if owned_object_exists(kind, name):
                    query(f'DROP {kind} {name}')
                state['created'].remove([kind, name])
                save(state)
            state['cleaned'] = True
            save(state)
            print('Removed the demo resources. Keep or archive the local ownership file.')
            return

        if state.get('cleaned'):
            raise RuntimeError('These resources were cleaned up; use a new database name for a fresh run')
        if state['created'] != allowed:
            raise RuntimeError('Setup was incomplete; inspect state and clean up before using these resources')
        for kind, name in allowed:
            if not owned_object_exists(kind, name):
                raise RuntimeError('Owned resource is missing: '+name)
        query(f'USE DATABASE {db}')
        query(f'USE SCHEMA {db}.SENSORS')
        query(f'USE WAREHOUSE {wh}')

        def run_file(name):
            sql = (HERE/name).read_text().replace('__WAREHOUSE__', wh)
            for cur in conn.execute_stream(io.StringIO(sql)):
                if cur.description:
                    print(cur.fetchall())

        if args.action == 'setup':
            run_file('tables.sql')
            print('Ready. Run seed, then read. Run cleanup when finished.')
        elif args.action == 'moving-window':
            run_file('moving_window.sql')
        elif args.action == 'precision':
            run_file('precision.sql')
        elif args.action == 'seed':
            from snowflake.ingest.streaming import StreamingIngestClient
            batch_id = uuid.uuid4().hex[:12]
            sensor = 'demo-'+batch_id
            base = utcnow().replace(second=0, microsecond=0)-timedelta(minutes=1)
            readings = [(1, '10'), (2, '20'), (3, '30'), (1, '10'), (4, '40'), (5, 'bad-value')]
            rows = [{'event_id': f'{batch_id}-{i}', 'sensor_id': sensor,
                     'event_ts': (base+timedelta(seconds=i)).isoformat(), 'value': value,
                     'sent_at': (utcnow()+timedelta(microseconds=j)).isoformat()}
                    for j, (i, value) in enumerate(readings)]
            props = {'authorization_type': 'JWT', 'account': cfg['account'], 'user': cfg['user'],
                     'private_key_file': params['private_key_file'],
                     'url': f"https://{cfg['account']}.snowflakecomputing.com:443"}
            if cfg.get('role'):
                props['role'] = cfg['role']
            if cfg.get('private_key_file_pwd'):
                raise RuntimeError('Demo streaming helper currently requires an unencrypted key file protected by filesystem permissions; do not change an existing key for this demo')
            client = StreamingIngestClient(client_name='sensor_demo_'+batch_id,
                db_name=db, schema_name='SENSORS', pipe_name='SENSOR_PIPE', properties=props)
            try:
                client.get_elastic_channel().append_rows_with_wait(rows, batch_id).result(timeout=120)
            finally:
                client.close()
            expected = (5, 4, 25.0, math.sqrt(125), math.sqrt(500/3))
            deadline = time.monotonic()+240
            while time.monotonic() < deadline:
                result = query(f"SELECT received_readings, valid_readings, average, population_sd, sample_sd FROM sensor_minute WHERE sensor_id='{sensor}'")
                if len(result) == 1 and all(v is not None and math.isclose(float(v), e, rel_tol=1e-8, abs_tol=1e-8) for v, e in zip(result[0], expected)):
                    print('PASS: duplicate excluded, invalid value retained as NULL; '+str(result[0]))
                    return
                time.sleep(3)
            raise TimeoutError('Expected summary did not appear within 240 seconds')
        elif args.action == 'read':
            with conn.cursor() as cur:
                cur.execute('SELECT * FROM sensor_minute ORDER BY minute_start, sensor_id')
                print([d[0] for d in cur.description])
                for row in cur:
                    print(row)


if __name__ == '__main__':
    main()
