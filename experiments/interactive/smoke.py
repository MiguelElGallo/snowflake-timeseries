"""Zero-copy Interactive Warehouse capability smoke test; not a benchmark.

Reads only existing fixture summaries. Creates and drops one owned warehouse.
"""
import argparse
from datetime import datetime, timedelta, timezone
import json
import math
import os
from pathlib import Path
import re
import time
import tomllib
import uuid


def identifier(value):
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,100}', value):
        raise argparse.ArgumentTypeError('Use a simple unquoted SQL identifier')
    return value.upper()


def table_name(value):
    parts = value.split('.')
    if len(parts) != 3:
        raise argparse.ArgumentTypeError('Use DATABASE.SCHEMA.TABLE')
    return '.'.join(identifier(part) for part in parts)


def expected_rows(base):
    """Independent expected counts/mean/SD for the published DT fixture."""
    return {
        ('equal', base): [2, 2, 7.0, 0.0, 0.0],
        ('null', base): [2, 0, None, None, None],
        ('single', base): [2, 1, 13.0, 0.0, None],
        ('late', base): [2, 2, 54.5, 44.5, math.sqrt(3960.5)],
        ('late', base + timedelta(minutes=1)): [1, 1, 20.0, 0.0, None],
        ('late', base + timedelta(minutes=2)): [1, 1, 30.0, 0.0, None],
        ('late', base + timedelta(minutes=3)): [1, 1, 40.0, 0.0, None],
        ('boundary', base - timedelta(minutes=1)): [1, 1, 1.0, 0.0, None],
        ('boundary', base): [3, 3, 3.0, math.sqrt(2/3), 1.0],
        ('boundary', base + timedelta(minutes=1)): [2, 2, 5.5, 0.5, math.sqrt(0.5)],
    }


def validate(record, expected, warehouse):
    actual = {}
    for row in record['rows']:
        item = dict(zip(record['columns'], row))
        assert item['EXECUTING_WAREHOUSE'] == warehouse
        key = (item['SENSOR_ID'], item['MINUTE_START'])
        assert key not in actual, ('duplicate summary key', key)
        actual[key] = [item[k] for k in ('N_EVENTS', 'N_VALID', 'MEAN', 'STD_POP', 'STD_SAMP')]
    assert actual.keys() == expected.keys(), ('key coverage', list(actual), list(expected))
    for key, want in expected.items():
        got = actual[key]
        assert got[:2] == want[:2], (key, got, want)
        for a, b in zip(got[2:], want[2:]):
            if a is None or b is None:
                assert a is None and b is None, (key, got, want)
            else:
                assert math.isclose(float(a), b, rel_tol=0, abs_tol=1e-8), (key, got, want)
    return {'status': 'passed', 'summary_rows': len(actual), 'absolute_tolerance': 1e-8}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('run', 'cleanup', 'local'))
    parser.add_argument('--source-table', type=table_name)
    parser.add_argument('--warehouse', type=identifier)
    parser.add_argument('--fixture-base', default='2000-01-01T12:00:00')
    parser.add_argument('--connection', default=os.environ.get('SNOWFLAKE_CONNECTION_NAME'))
    parser.add_argument('--connections-file', type=Path, default=Path.home()/'.snowflake/connections.toml')
    parser.add_argument('--output-dir', type=Path, default=Path(__file__).resolve().parent/'artifacts')
    args = parser.parse_args()
    if args.action == 'local':
        base = datetime(2000, 1, 1, 12)
        expected = expected_rows(base)
        columns = ['SENSOR_ID', 'MINUTE_START', 'N_EVENTS', 'N_VALID', 'MEAN', 'STD_POP', 'STD_SAMP', 'EXECUTING_WAREHOUSE']
        record = {'columns': columns, 'rows': [[*key, *metrics, 'DEMO_WH'] for key, metrics in expected.items()]}
        validate(record, expected, 'DEMO_WH')
        # A missing NULL or a changed metric must not pass.
        bad = {**record, 'rows': [list(row) for row in record['rows']]}
        bad['rows'][0][4] = None
        try:
            validate(bad, expected, 'DEMO_WH')
        except AssertionError:
            print('PASS: ten expected summary rows and NULL mismatch rejection')
            return
        raise AssertionError('NULL mismatch was accepted')
    if not args.connection:
        parser.error('Set SNOWFLAKE_CONNECTION_NAME or --connection')
    if args.action == 'run' and not args.source_table:
        parser.error('--source-table is required for run')
    base = datetime.fromisoformat(args.fixture_base)
    if base.tzinfo is not None or base.second != 0 or base.microsecond != 0:
        parser.error('--fixture-base must be a naive UTC minute boundary')
    cfg = tomllib.loads(args.connections_file.expanduser().read_text())[args.connection]
    keys = ('account', 'user', 'authenticator', 'role', 'private_key_file', 'private_key_password', 'private_key_file_pwd', 'host')
    params = {k: cfg[k] for k in keys if k in cfg}
    if params.get('private_key_file'):
        params['private_key_file'] = str(Path(params['private_key_file']).expanduser())
    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest = args.output_dir/'ownership.json'
    result_path = args.output_dir/'result.json'

    def save(path, data):
        tmp = path.with_suffix('.tmp')
        tmp.write_text(json.dumps(data, indent=2, default=str)+'\n')
        tmp.replace(path)

    import snowflake.connector
    with snowflake.connector.connect(**params, session_parameters={
        'TIMEZONE': 'UTC', 'USE_CACHED_RESULT': False, 'QUERY_TAG': 'timeseries_interactive_smoke'},
        login_timeout=30, network_timeout=120) as conn:
        def query(sql, label):
            start = time.monotonic()
            rec = {'label': label, 'sql': sql, 'started_utc': datetime.now(timezone.utc).isoformat()}
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    rec.update(status='ok', query_id=cur.sfqid,
                               columns=[d[0] for d in cur.description] if cur.description else [],
                               rows=cur.fetchall() if cur.description else [])
            except Exception as exc:
                rec.update(status='error', error=str(exc), query_id=getattr(exc, 'sfqid', None))
                raise
            finally:
                rec['client_elapsed_seconds'] = time.monotonic()-start
                with (args.output_dir/'queries.jsonl').open('a') as output:
                    output.write(json.dumps(rec, default=str)+'\n')
                print(label, rec['status'], round(rec['client_elapsed_seconds'], 3), flush=True)
            return rec

        def identity():
            return list(query('SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT(), CURRENT_REGION()', 'identity')['rows'][0])

        def warehouse_record(name):
            rec = query(f"SHOW WAREHOUSES LIKE '{name}'", 'warehouse_state')
            rows = [dict(zip(rec['columns'], r)) for r in rec['rows']]
            exact = [r for r in rows if r['name'] == name]
            assert len(exact) <= 1
            return exact[0] if exact else None

        live_identity = identity()
        if args.action == 'run':
            if manifest.exists():
                raise RuntimeError('Use a fresh output directory; existing ownership must be inspected or cleaned up')
            name = args.warehouse or 'TIMESERIES_SERVE_'+uuid.uuid4().hex[:10].upper()
            if warehouse_record(name) is not None:
                raise RuntimeError('Warehouse name already exists; refusing to adopt or replace it')
            state = {'identity': live_identity, 'warehouse': name, 'owned': False,
                     'marker': 'interactive-smoke-'+uuid.uuid4().hex}
            save(manifest, state)
        else:
            state = json.loads(manifest.read_text())
            name = identifier(state['warehouse'])
            if args.warehouse and args.warehouse != name:
                raise RuntimeError('Warehouse argument does not match ownership')
        if state['identity'] != live_identity:
            raise RuntimeError('Live organization/account/region differs from ownership')
        if not re.fullmatch(r'interactive-smoke-[0-9a-f]{32}', state['marker']):
            raise RuntimeError('Invalid ownership marker')

        def cleanup():
            if not state['owned']:
                if not state.get('create_attempted'):
                    return {'status': 'not_owned_no_action'}
                # CREATE may have committed before a transport failure; recover only our unique marker.
                current = warehouse_record(name)
                if current is None:
                    state['create_attempted'] = False
                    save(manifest, state)
                    return {'status': 'passed', 'warehouse_absent': True, 'create_did_not_leave_resource': True}
                if current['comment'] != state['marker']:
                    raise RuntimeError('Warehouse exists with a different marker after create attempt; refusing cleanup')
                state['owned'] = True
                save(manifest, state)
            if identity() != state['identity']:
                raise RuntimeError('Account identity changed; refusing cleanup')
            current = warehouse_record(name)
            if current is not None:
                if current['comment'] != state['marker']:
                    raise RuntimeError('Warehouse ownership marker differs; refusing cleanup')
                query(f'DROP WAREHOUSE {name}', 'drop_owned_warehouse')
            state['owned'] = False
            state['create_attempted'] = False
            save(manifest, state)
            assert warehouse_record(name) is None, 'Warehouse still exists after cleanup'
            return {'status': 'passed', 'warehouse_absent': True}

        if args.action == 'cleanup':
            print(cleanup())
            return

        result = {'status': 'started', 'kind': 'capability_smoke_not_benchmark',
                  'source_table': args.source_table, 'fixture_base_utc': base, 'queries': [],
                  'minimum_first_resume_credits_at_published_xs_rate': 0.6,
                  'cost_is_billing_model_not_observed_invoice': True}
        error = None
        try:
            state['create_attempted'] = True
            save(manifest, state)
            query(f"CREATE INTERACTIVE WAREHOUSE {name} WAREHOUSE_SIZE='XSMALL' "
                  f"MIN_CLUSTER_COUNT=1 MAX_CLUSTER_COUNT=1 AUTO_SUSPEND=86400 "
                  f"AUTO_RESUME=TRUE INITIALLY_SUSPENDED=TRUE COMMENT='{state['marker']}'", 'create_interactive_warehouse')
            state['owned'] = True
            save(manifest, state)
            current = warehouse_record(name)
            assert current and current['type'] == 'INTERACTIVE' and current['comment'] == state['marker']
            result['warehouse_configuration'] = current
            fallback = query(f"SHOW PARAMETERS LIKE 'FALLBACK_WAREHOUSE' FOR WAREHOUSE {name}", 'fallback_configuration')
            assert len(fallback['rows']) == 1, 'Could not verify fallback configuration'
            for row in fallback['rows']:
                assert dict(zip(fallback['columns'], row))['value'] in ('', None), 'Unexpected fallback warehouse'
            query(f'USE WAREHOUSE {name}', 'select_interactive_warehouse')
            session = query('SELECT CURRENT_WAREHOUSE(), CURRENT_SESSION()', 'executing_session')
            assert session['rows'][0][0] == name
            expected = expected_rows(base)
            for label, sensors in [('null_and_equal', ('equal','null','single')),
                                   ('late_arrival', ('late',)), ('minute_boundaries', ('boundary',))]:
                names = ','.join("'"+sensor+"'" for sensor in sensors)
                rec = query(f'''SELECT SENSOR_ID,MINUTE_START,N_EVENTS,N_VALID,MEAN,STD_POP,STD_SAMP,
                    CURRENT_WAREHOUSE() EXECUTING_WAREHOUSE FROM {args.source_table}
                    WHERE RUN_ID='fixture' AND SENSOR_ID IN ({names})
                      AND MINUTE_START>='{(base-timedelta(minutes=1)).isoformat()}'::TIMESTAMP_NTZ
                      AND MINUTE_START<'{(base+timedelta(minutes=4)).isoformat()}'::TIMESTAMP_NTZ
                    ORDER BY SENSOR_ID,MINUTE_START''', label)
                validation = validate(rec, {key: metrics for key, metrics in expected.items() if key[0] in sensors}, name)
                result['queries'].append({'label': label, 'validation': validation, 'observation': rec})
                save(result_path, result)
            result['status'] = 'passed'
        except BaseException as exc:
            error = exc
            result.update(status='failed_or_unavailable', error=str(exc), error_type=type(exc).__name__)
        finally:
            try:
                result['cleanup'] = cleanup()
            except BaseException as exc:
                result['cleanup'] = {'status': 'failed', 'error': str(exc)}
                if error is None:
                    error = exc
                    result['status'] = 'cleanup_failed'
            result['completed_utc'] = datetime.now(timezone.utc).isoformat()
            save(result_path, result)
        if error is not None:
            raise error


if __name__ == '__main__':
    main()
