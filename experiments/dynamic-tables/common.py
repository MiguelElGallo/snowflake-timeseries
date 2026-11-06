"""Local configuration, evidence recording, and Snowflake clients.

Credentials are read only when a cloud command runs. Nothing authenticates on import.
"""
import json
import os
import re
import secrets
import threading
import time
import tomllib
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
EVIDENCE = Path(os.environ.get('SNOWFLAKE_LAB_ARTIFACT_DIR', str(ROOT / 'artifacts'))).expanduser().resolve()
EVIDENCE.mkdir(parents=True, exist_ok=True)
_lock = threading.Lock()


def identifier(value):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,199}', value):
        raise ValueError('Use an unquoted SQL identifier containing only letters, digits and underscores')
    return value.upper()


def settings():
    path = EVIDENCE / 'lab_config.json'
    saved = json.loads(path.read_text()) if path.exists() else {}
    database = identifier(os.environ.get('SNOWFLAKE_LAB_DATABASE', saved.get('database', 'TIMESERIES_' + secrets.token_hex(5))))
    warehouse = identifier(os.environ.get('SNOWFLAKE_LAB_WAREHOUSE', saved.get('warehouse', database + '_WH')))
    result = {'database': database, 'warehouse': warehouse}
    if saved and result != saved:
        raise RuntimeError('Resource names differ from the artifact directory. Use a fresh artifact directory for another run.')
    if not saved:
        path.write_text(json.dumps(result, indent=2) + '\n')
    return result


_settings = settings()
DB, DT_WH = _settings['database'], _settings['warehouse']


def config():
    name = os.environ.get('SNOWFLAKE_CONNECTION_NAME')
    if not name:
        raise RuntimeError('Set SNOWFLAKE_CONNECTION_NAME to your own key-pair connection profile')
    path = Path(os.environ.get('SNOWFLAKE_CONNECTIONS_FILE', '~/.snowflake/connections.toml')).expanduser()
    profiles = tomllib.loads(path.read_text())
    if name not in profiles:
        raise RuntimeError('The named Snowflake profile was not found in the configured connections file')
    c = dict(profiles[name])
    required = {'account', 'user', 'role', 'private_key_file'}
    if not required <= c.keys():
        raise RuntimeError('Profile requires account, user, role and private_key_file for JWT streaming')
    c['private_key_file'] = str(Path(c['private_key_file']).expanduser())
    passphrase = os.environ.get('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE',
        c.get('private_key_password', c.get('private_key_file_pwd', c.get('private_key_passphrase'))))
    if passphrase is not None:
        c['private_key_password'] = passphrase
    return c


def account_identity(conn):
    result = query(conn, 'SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT(), CURRENT_REGION()', 'account_identity')
    return dict(zip(('organization', 'account', 'region'), result['rows'][0]))


def verify_ownership(conn, require_resources=True):
    path = EVIDENCE / 'owned_resources.json'
    if not path.exists():
        raise RuntimeError('Run admin.py setup first; this stage requires an ownership manifest')
    owned = json.loads(path.read_text())
    if owned.get('identity') != account_identity(conn):
        raise RuntimeError('The live Snowflake organization/account/region differs from the ownership manifest; no resource action is allowed')
    if owned.get('database') not in (None, DB) or owned.get('warehouse') not in (None, DT_WH):
        raise RuntimeError('Ownership manifest and configured resource names differ')
    if require_resources and (owned.get('database') != DB or owned.get('warehouse') != DT_WH):
        raise RuntimeError('This experiment does not currently own both resources; inspect partial setup or use a fresh experiment')
    return owned


def connect(schema='DT', warehouse=None, connection_config=None):
    import snowflake.connector
    c = config() if connection_config is None else connection_config
    keys = ('account', 'user', 'authenticator', 'private_key_file', 'private_key_password', 'role', 'host')
    params = {k: c[k] for k in keys if k in c}
    params.setdefault('authenticator', 'SNOWFLAKE_JWT')
    conn = snowflake.connector.connect(**params,
        session_parameters={'QUERY_TAG': DB, 'TIMEZONE': 'UTC', 'USE_CACHED_RESULT': False},
        login_timeout=30, network_timeout=120)
    if schema is not None:
        try:
            verify_ownership(conn)
            query(conn, f'USE SCHEMA {DB}.{identifier(schema)}')
            query(conn, f'USE WAREHOUSE {identifier(warehouse or DT_WH)}')
        except BaseException:
            conn.close()
            raise
    return conn


def append(name, obj):
    with _lock, (EVIDENCE / name).open('a') as f:
        f.write(json.dumps(obj, default=str) + '\n')


def dump(name, obj):
    target = EVIDENCE / name
    tmp = target.with_suffix(target.suffix + '.tmp')
    tmp.write_text(json.dumps(obj, indent=2, default=str) + '\n')
    tmp.replace(target)


def start_stage(stage):
    """A stage writes deterministic run IDs; refuse accidental repeat ingestion."""
    path = EVIDENCE / ('stage_' + stage + '_started.json')
    with path.open('x') as f:
        json.dump({'stage': stage, 'started_utc': datetime.now(timezone.utc).isoformat()}, f)


def query(conn, sql, label=None, expected_error=False):
    started = time.monotonic()
    rec = {'label': label, 'sql': sql, 'started_utc': datetime.now(timezone.utc).isoformat()}
    error = None
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rec.update(query_id=cur.sfqid, columns=[d[0] for d in cur.description] if cur.description else [],
                       rows=cur.fetchall() if cur.description else [], status='ok')
    except Exception as exc:
        error = exc
        rec.update(status='error', error=str(exc), query_id=getattr(exc, 'sfqid', None))
    rec['elapsed_s'] = time.monotonic() - started
    append('queries.jsonl', rec)
    if label:
        print(label, rec['status'], round(rec['elapsed_s'], 3), flush=True)
    if error is not None and not expected_error:
        raise error
    return rec


def streaming_client(pipe, schema='DT'):
    c = config()
    # Authenticate this exact profile before giving its account/keys to the Streaming client.
    with connect(schema=None, connection_config=c) as conn:
        verify_ownership(conn)
    from snowflake.ingest.streaming import StreamingIngestClient
    props = {'authorization_type': 'JWT', 'account': c['account'], 'user': c['user'],
             'private_key_file': c['private_key_file'], 'role': c['role'],
             'url': os.environ.get('SNOWFLAKE_STREAMING_URL',
                 f"https://{c.get('host', c['account'] + '.snowflakecomputing.com')}:443")}
    if 'private_key_password' in c:
        props['private_key_passphrase'] = c['private_key_password']
    return StreamingIngestClient(client_name='timeseries_' + schema.lower() + '_' + pipe.lower(),
        db_name=DB, schema_name=schema, pipe_name=pipe, properties=props)
