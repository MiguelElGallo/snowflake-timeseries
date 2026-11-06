"""Staged live DT experiment. Never import this file to run cloud mutations.

Commands: local, setup, correctness, paced, cleanup. See README for ordered stages.
"""
import argparse
from collections import deque
from datetime import datetime, timedelta, timezone
import json
import math
import statistics
import threading
import time

from common import DB, DT_WH, EVIDENCE, append, connect, dump, query, streaming_client, start_stage
from dt_oracle import METRICS, canonical, compare, fixture, grouped, self_check, stats, timestamp

FIELDS = 'RUN_ID,EVENT_ID,SENSOR_ID,EVENT_TS,MINUTE_START,VALUE,SOURCE_VALUE,BATCH_SEQ,SENT_AT'
AGG = ('COUNT(*) N_EVENTS,COUNT(VALUE) N_VALID,AVG(VALUE) MEAN,'
       'STDDEV_POP(VALUE) STD_POP,STDDEV_SAMP(VALUE) STD_SAMP,'
       'MIN(VALUE) MIN_VALUE,MAX(VALUE) MAX_VALUE')
TABLES = ('MINUTE', 'ROLLING_ASOF', 'ROLLING_NOW', 'ROLLING_RANGE', 'PRECISION_NUMBER', 'ROLLING_NOW_UTC',
          'ROLLING_NOW_IMPLICIT_CONTROL', 'CANONICAL')
PANEL = ('0', '1', '17', '999')


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def setup():
    conn = connect()
    dump('dt_session.json', query(conn, '''SELECT CURRENT_ACCOUNT() ACCOUNT,CURRENT_REGION() REGION,
        CURRENT_USER() USERNAME,CURRENT_ROLE() ACTIVE_ROLE''', 'dt_session_context'))
    definitions = [
        ('RAW', '''CREATE TABLE RAW (
          RUN_ID VARCHAR,EVENT_ID VARCHAR,SENSOR_ID VARCHAR,EVENT_TS TIMESTAMP_NTZ(6),
          MINUTE_START TIMESTAMP_NTZ(6),VALUE FLOAT,SOURCE_VALUE VARCHAR,
          BATCH_SEQ NUMBER,SENT_AT TIMESTAMP_NTZ(6)) ROW_TIMESTAMP=TRUE CHANGE_TRACKING=TRUE'''),
        ('PIPE', '''CREATE PIPE SENSOR_PIPE AS COPY INTO RAW FROM (
          SELECT $1:run_id::VARCHAR,$1:event_id::VARCHAR,$1:sensor_id::VARCHAR,
          TO_TIMESTAMP_NTZ($1:event_ts::VARCHAR),
          DATE_TRUNC('MINUTE',TO_TIMESTAMP_NTZ($1:event_ts::VARCHAR)),
          TRY_TO_DOUBLE($1:value::VARCHAR),$1:value::VARCHAR,$1:batch_seq::NUMBER,
          TO_TIMESTAMP_NTZ($1:sent_at::VARCHAR)
          FROM TABLE(DATA_SOURCE(TYPE=>'STREAMING')))'''),
        ('CLOCK', 'CREATE TABLE ASOF_CLOCK (RUN_ID VARCHAR, AS_OF TIMESTAMP_NTZ(6)) CHANGE_TRACKING=TRUE'),
        ('CANONICAL', f'''CREATE DYNAMIC TABLE CANONICAL TARGET_LAG=DOWNSTREAM
          WAREHOUSE={DT_WH} REFRESH_MODE=INCREMENTAL INITIALIZE=ON_CREATE AS
          SELECT {FIELDS} FROM RAW QUALIFY ROW_NUMBER() OVER (
          PARTITION BY RUN_ID,EVENT_ID ORDER BY BATCH_SEQ DESC,SENT_AT DESC)=1'''),
        ('MINUTE', f'''CREATE DYNAMIC TABLE MINUTE TARGET_LAG='60 seconds'
          WAREHOUSE={DT_WH} REFRESH_MODE=INCREMENTAL INITIALIZE=ON_CREATE AS
          SELECT RUN_ID,SENSOR_ID,MINUTE_START,{AGG},MIN(BATCH_SEQ) MIN_SEQ,
          MAX(BATCH_SEQ) MAX_SEQ FROM CANONICAL GROUP BY RUN_ID,SENSOR_ID,MINUTE_START'''),
        ('ROLLING_ASOF', f'''CREATE DYNAMIC TABLE ROLLING_ASOF TARGET_LAG=DOWNSTREAM
          WAREHOUSE={DT_WH} REFRESH_MODE=INCREMENTAL INITIALIZE=ON_CREATE AS
          SELECT c.RUN_ID,c.SENSOR_ID,k.AS_OF,{AGG},MAX(c.BATCH_SEQ) MAX_SEQ
          FROM CANONICAL c JOIN ASOF_CLOCK k ON c.RUN_ID=k.RUN_ID
          WHERE c.EVENT_TS>DATEADD('SECOND',-60,k.AS_OF) AND c.EVENT_TS<=k.AS_OF
          GROUP BY c.RUN_ID,c.SENSOR_ID,k.AS_OF'''),
        ('ROLLING_NOW', f'''CREATE DYNAMIC TABLE ROLLING_NOW TARGET_LAG='60 seconds'
          WAREHOUSE={DT_WH} REFRESH_MODE=INCREMENTAL INITIALIZE=ON_CREATE AS
          SELECT RUN_ID,SENSOR_ID,{AGG} FROM CANONICAL
          WHERE RUN_ID='idle' AND EVENT_TS>DATEADD('SECOND',-60,CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
          AND EVENT_TS<=CURRENT_TIMESTAMP()::TIMESTAMP_NTZ GROUP BY RUN_ID,SENSOR_ID'''),
    ]
    win = "PARTITION BY RUN_ID,SENSOR_ID ORDER BY EVENT_TS RANGE BETWEEN INTERVAL '60 seconds' PRECEDING AND CURRENT ROW"
    range_metrics = ','.join(f'{expr} OVER ({win}) {name}' for expr, name in (
        ('COUNT(EVENT_ID)', 'N_EVENTS'), ('COUNT(VALUE)', 'N_VALID'), ('AVG(VALUE)', 'MEAN'),
        ('STDDEV_POP(VALUE)', 'STD_POP'), ('STDDEV_SAMP(VALUE)', 'STD_SAMP'),
        ('MIN(VALUE)', 'MIN_VALUE'), ('MAX(VALUE)', 'MAX_VALUE')))
    definitions.append(('ROLLING_RANGE', f'''CREATE DYNAMIC TABLE ROLLING_RANGE TARGET_LAG=DOWNSTREAM
        WAREHOUSE={DT_WH} REFRESH_MODE=INCREMENTAL INITIALIZE=ON_CREATE AS
        SELECT RUN_ID,EVENT_ID,SENSOR_ID,EVENT_TS,{range_metrics} FROM CANONICAL WHERE RUN_ID='fixture' '''))
    result = {}
    for name, ddl in definitions:
        result[name] = query(conn, ddl, 'dt_setup_' + name, expected_error=name.startswith('ROLLING'))
    dump('dt_setup.json', result)
    dump('dt_show.json', query(conn, 'SHOW DYNAMIC TABLES IN SCHEMA DT', 'dt_resolved_modes'))
    conn.close()


def capabilities():
    result = json.loads((EVIDENCE / 'dt_setup.json').read_text())
    if (EVIDENCE / 'dt_followup_setup.json').exists():
        result.update(json.loads((EVIDENCE / 'dt_followup_setup.json').read_text()))
    return {name: record['status'] == 'ok' for name, record in result.items()}


def history(conn, name):
    results = {}
    enabled = capabilities()
    query(conn, f'USE DATABASE {DB}')
    # USE DATABASE resets the current schema to PUBLIC, even for the same database.
    query(conn, f'USE SCHEMA {DB}.DT')
    for table in TABLES:
        if enabled.get(table):
            results[table] = query(conn, f'''SELECT * FROM TABLE({DB}.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
                NAME=>'{DB}.DT.{table}',RESULT_LIMIT=>10000)) ORDER BY REFRESH_START_TIME''', 'history_' + name + '_' + table)
    dump('dt_history_' + name + '.json', results)
    return results


def wait_raw(conn, run, expected, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = query(conn, f'SELECT COUNT(*) FROM RAW WHERE RUN_ID={sql_string(run)}')
        n = r['rows'][0][0]
        if n == expected:
            return r
        assert n <= expected, (n, expected)
        time.sleep(1)
    raise TimeoutError(('raw visibility', run, expected, n))


def send(channel, events, tag):
    sent = utcnow().isoformat()
    events = [dict(e, sent_at=sent) for e in events]
    start = time.monotonic()
    channel.append_rows_with_wait(events, tag).result(timeout=120)
    append('dt_fixture_sends.jsonl', {'tag': tag, 'events': events, 'start_mono': start,
                                    'ack_mono': time.monotonic()})
    return events


def rows_to_map(record, keys):
    cols = record['columns']
    out = {}
    for row in record['rows']:
        d = dict(zip(cols, row))
        key = tuple(d[k] for k in keys)
        assert key not in out, ('duplicate output key', key)
        out[key] = [d[m] for m in METRICS]
    return out


def fixture_check(conn, events, label, asofs):
    enabled = capabilities()
    result = {}
    for table, kind, keys in (
        ('MINUTE', 'minute', ('RUN_ID', 'SENSOR_ID', 'MINUTE_START')),
        ('ROLLING_ASOF', 'asof', ('RUN_ID', 'SENSOR_ID', 'AS_OF')),
        ('ROLLING_RANGE', 'range', ('RUN_ID', 'EVENT_ID'))):
        if not enabled.get(table):
            result[table] = {'status': 'unsupported', 'definition_evidence': 'dt_setup.json'}
            continue
        refresh = query(conn, f'ALTER DYNAMIC TABLE {table} REFRESH', label + '_manual_' + table)
        observation = query(conn, f"SELECT * FROM {table} WHERE RUN_ID='fixture'", label + '_read_' + table)
        actual = rows_to_map(observation, keys); expected = grouped(events, kind, asofs)
        numerical_keys = {k for k in expected if (k[1] == 'numeric' if kind != 'range' else k[1].startswith('numeric_'))}
        # Core correctness remains strict. High-offset SD is separately diagnostic.
        validation = compare({k: v for k, v in actual.items() if k not in numerical_keys},
                             {k: v for k, v in expected.items() if k not in numerical_keys})
        try:
            numeric = {'status': 'passed', 'validation': compare({k: actual[k] for k in numerical_keys if k in actual},
                                                                {k: expected[k] for k in numerical_keys})}
        except AssertionError as exc:
            numeric = {'status': 'failed', 'error': repr(exc),
                       'actual': {str(k): actual.get(k) for k in numerical_keys},
                       'expected': {str(k): expected[k] for k in numerical_keys}, 'tolerance_absolute': 1e-4}
        result[table] = {'status': 'passed' if numeric['status'] == 'passed' else 'numerical_diagnostic_failed',
                         'validation': validation, 'large_offset_diagnostic': numeric,
                         'refresh_query_id': refresh['query_id'], 'observation_query_id': observation['query_id']}
    ids = query(conn, "SELECT RUN_ID,EVENT_ID,BATCH_SEQ FROM CANONICAL WHERE RUN_ID='fixture'", label + '_canonical_keys')
    actual = {(r[0], r[1]): r[2] for r in ids['rows']}
    expected = {(e['run_id'], e['event_id']): e['batch_seq'] for e in canonical(events)}
    assert actual == expected, (actual, expected)
    result['canonical'] = {'status': 'passed', 'keys': len(expected), 'query_id': ids['query_id']}
    dump('dt_correctness_' + label + '.json', result)
    return result


def correctness():
    dump('dt_local_oracle.json', self_check())
    conn = connect(); client = streaming_client('SENSOR_PIPE'); channel = client.get_elastic_channel()
    base, initial, late, replay = fixture()
    asofs = [('fixture', base + timedelta(seconds=s)) for s in (0, 60, 60.000001, 120, 180)]
    values = ','.join(f"('fixture',{sql_string(t.isoformat())}::TIMESTAMP_NTZ)" for _, t in asofs)
    query(conn, 'INSERT INTO ASOF_CLOCK VALUES ' + values, 'fixture_clock')
    events = send(channel, initial, 'fixture-initial')
    wait_raw(conn, 'fixture', len(events))
    checks = [fixture_check(conn, events, 'initial', asofs)]
    # More than two event-time minutes behind an already observed event at t=180.
    events.extend(send(channel, [late], 'fixture-cross-bucket-late'))
    wait_raw(conn, 'fixture', len(events))
    checks.append(fixture_check(conn, events, 'late', asofs))
    # Restart the client before replay. Semantic event data unchanged; marker advances.
    client.close(); client = streaming_client('SENSOR_PIPE'); channel = client.get_elastic_channel()
    events.extend(send(channel, [replay], 'fixture-replay-after-restart'))
    wait_raw(conn, 'fixture', len(events))
    checks.append(fixture_check(conn, events, 'replay', asofs))
    # Advance to one distinct clock: duplicate clock rows would multiply events.
    query(conn, 'DELETE FROM ASOF_CLOCK', 'clock_reset')
    query(conn, f"INSERT INTO ASOF_CLOCK VALUES ('fixture',{sql_string((base+timedelta(seconds=300)).isoformat())}::TIMESTAMP_NTZ)", 'clock_empty_window')
    checks.append(fixture_check(conn, events, 'asof_empty', [('fixture', base + timedelta(seconds=300))]))
    # Idle expiry and timezone controls run separately in dt_followup.py.
    history(conn, 'correctness')
    client.close(); conn.close()
    numeric_failures = [record for check in checks for record in check.values()
                        if record.get('status') == 'numerical_diagnostic_failed']
    dump('dt_correctness_complete.json', {'status': 'passed' if not numeric_failures else 'completed_with_numerical_failures',
        'numerical_failures': numeric_failures, 'completed_utc': utcnow(),
        'unsupported_arms': [k for k, ok in capabilities().items() if not ok]})


def generated_event(run, sensor, seq, base):
    return {'run_id': run, 'event_id': f'{sensor}:{seq}', 'sensor_id': str(sensor),
            'event_ts': (base + timedelta(seconds=seq - 1)).isoformat(),
            'value': 20 + sensor % 10 + (seq - 1) % 60, 'batch_seq': seq}


def percentile(values, p):
    if not values:
        return None
    ordered = sorted(values); index = (len(ordered) - 1) * p
    lo = math.floor(index); hi = math.ceil(index)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (index - lo)


def summarise(values):
    return {'n': len(values), 'p50': percentile(values, .5), 'p95': percentile(values, .95),
            'p99': percentile(values, .99), 'max': max(values) if values else None}


def _benchmark_run(conn, channel, run, seconds, sensors, poll_seconds=5, burst=False):
    base = utcnow().replace(microsecond=0)
    origin = time.monotonic()
    shared = {'sent': {}, 'ack': {}, 'errors': [], 'done': False}
    lock = threading.Lock()
    stop = threading.Event()
    manifest = {'run_id': run, 'base_event_utc': base, 'seconds': seconds, 'sensors': sensors,
                'events': sensors * seconds, 'intended_rate_events_s': None if burst else sensors,
                'mode': 'accelerated_burst' if burst else 'paced_1hz_per_sensor', 'origin_mono': origin,
                'panel': [s for s in PANEL if int(s) < sensors], 'poll_seconds': poll_seconds,
                'value_formula': '20 + sensor % 10 + (sequence - 1) % 60',
                'acknowledgement_timing': 'upper bound observed when FIFO future is reaped; includes producer reaping delay'}
    dump('dt_manifest_' + run + '.json', manifest)

    def producer():
        pending = deque()
        def finish(item):
            seq, future, actual, scheduled = item
            future.result(timeout=120)
            ack = time.monotonic()
            with lock:
                shared['ack'][seq] = ack
            append('dt_batches_' + run + '.jsonl', {'sequence': seq, 'scheduled_mono': scheduled,
                'sent_mono': actual, 'ack_mono': ack, 'producer_drift_s': actual - scheduled,
                'ack_observed_upper_bound_s': ack - actual, 'rows': sensors})
        try:
            for seq in range(1, seconds + 1):
                if stop.is_set():
                    break
                scheduled = origin if burst else origin + seq - 1
                stop.wait(max(0, scheduled - time.monotonic()))
                if stop.is_set():
                    break
                sent_at = utcnow().isoformat()
                events = [dict(generated_event(run, sensor, seq, base), sent_at=sent_at) for sensor in range(sensors)]
                actual = time.monotonic()
                with lock:
                    shared['sent'][seq] = actual
                future = channel.append_rows_with_wait(events, f'{run}-{seq}')
                append('dt_sends_' + run + '.jsonl', {'sequence': seq, 'scheduled_mono': scheduled,
                    'sent_mono': actual, 'sent_utc': sent_at, 'rows': sensors})
                pending.append((seq, future, actual, scheduled))
                while pending and (pending[0][1].done() or len(pending) >= 8):
                    finish(pending.popleft())
            while pending:
                finish(pending.popleft())
        except Exception as exc:
            with lock:
                shared['errors'].append(repr(exc))
            append('dt_producer_errors.jsonl', {'run_id': run, 'error': repr(exc)})
        finally:
            with lock:
                shared['done'] = True

    thread = threading.Thread(target=producer, name='dt-producer', daemon=True)
    thread.start()
    first_seen = {}; observed_revisions = set(); poll_count = 0; nonqualifying = 0
    panel = manifest['panel']; panel_sql = ','.join(sql_string(s) for s in panel)
    drain_deadline = None
    try:
        while True:
            with lock:
                done = shared['done']; errors = list(shared['errors']); sent = dict(shared['sent'])
            assert not errors, errors
            # Keep all minute buckets for this run: no final bucket can disappear from observation.
            r = query(conn, f"SELECT * FROM MINUTE WHERE RUN_ID={sql_string(run)} AND SENSOR_ID IN ({panel_sql})")
            observed = time.monotonic(); poll_count += 1
            rows = [dict(zip(r['columns'], row)) for row in r['rows']]
            newly_seen = 0
            for row in rows:
                low, high = int(row['MIN_SEQ']), int(row['MAX_SEQ'])
                sensor = int(row['SENSOR_ID']); minute = row['MINUTE_START']
                expected_start = max(1, math.ceil((minute - base).total_seconds()) + 1)
                metrics = [row[m] for m in METRICS]
                try:
                    assert low == expected_start and high <= seconds, (row, expected_start)
                    expected_events = [generated_event(run, sensor, seq, base) for seq in range(low, high + 1)]
                    assert all(timestamp(e['event_ts']).replace(second=0, microsecond=0) == minute for e in expected_events)
                    compare({('prefix',): metrics}, {('prefix',): stats(expected_events)})
                except AssertionError as exc:
                    # Elastic channels may expose out-of-order partial prefixes during ingestion.
                    nonqualifying += 1
                    append('dt_nonqualifying_' + run + '.jsonl', {'query_id': r['query_id'],
                        'observed_mono': observed, 'row': row, 'reason': repr(exc)})
                    continue
                revision = (row['SENSOR_ID'], minute.isoformat(), high)
                if revision not in observed_revisions:
                    observed_revisions.add(revision)
                    append('dt_revisions_' + run + '.jsonl', {'sensor_id': row['SENSOR_ID'], 'minute': minute,
                        'min_seq': low, 'max_seq': high, 'metrics': metrics,
                        'observed_mono': observed, 'query_id': r['query_id']})
                for seq in range(low, high + 1):
                    key = (row['SENSOR_ID'], seq)
                    if key not in first_seen and seq in sent:
                        latency = observed - sent[seq]
                        first_seen[key] = latency; newly_seen += 1
                        append('dt_event_visibility_' + run + '.jsonl', {'sensor_id': row['SENSOR_ID'],
                            'sequence': seq, 'first_correct_containing_revision_mono': observed,
                            'sent_mono': sent[seq], 'latency_s': latency, 'query_id': r['query_id'],
                            'containing_revision_max_seq': high})
            append('dt_polls_' + run + '.jsonl', {'observed_mono': observed, 'query_id': r['query_id'],
                'query_elapsed_s': r['elapsed_s'], 'new_panel_events': newly_seen, 'panel_events_seen': len(first_seen)})
            if poll_count == 1 or poll_count % 12 == 0 or done:
                dump('dt_result_' + run + '.json', dict(manifest, status='in_progress',
                    panel_observed_events=len(first_seen), panel_expected_events=len(panel) * seconds,
                    censored_events=len(panel) * seconds - len(first_seen),
                    summary_first_inclusion_latency_s=summarise(list(first_seen.values())),
                    nonqualifying_observations=nonqualifying, polling_queries=poll_count, updated_utc=utcnow()))
            if poll_count % 12 == 0:
                print('DT progress', run, 'sent_batches', len(sent), 'panel_seen', len(first_seen), flush=True)
                dump('dt_progress.json', {'run_id': run, 'sent_batches': len(sent), 'expected_batches': seconds,
                     'panel_seen': len(first_seen), 'utc': utcnow(), 'phase': 'drain' if done else 'ingesting'})
            if done:
                if drain_deadline is None:
                    drain_deadline = time.monotonic() + 240
                if len(first_seen) == len(panel) * seconds:
                    break
                assert time.monotonic() < drain_deadline, ('summary observation timeout', run, len(first_seen))
            time.sleep(poll_seconds)
    finally:
        stop.set(); thread.join(timeout=130)
        assert not thread.is_alive(), 'producer failed to terminate'
    with lock:
        assert len(shared['ack']) == seconds and not shared['errors'], shared['errors']
        acknowledgements = [shared['ack'][s] - shared['sent'][s] for s in shared['ack']]
        drift = [shared['sent'][s] - (origin if burst else origin + s - 1) for s in shared['sent']]
    wait_raw(conn, run, seconds * sensors)
    # Final state validates every sensor and every expected bucket, with NULL-safe metrics.
    expected = {}
    for sensor in range(sensors):
        ev = [generated_event(run, sensor, seq, base) for seq in range(1, seconds + 1)]
        expected.update(grouped(ev))
    final_deadline = time.monotonic() + 240
    while True:
        final = query(conn, f'SELECT * FROM MINUTE WHERE RUN_ID={sql_string(run)}', 'final_all_sensors_' + run)
        try:
            validation = compare(rows_to_map(final, ('RUN_ID', 'SENSOR_ID', 'MINUTE_START')), expected)
            break
        except AssertionError as exc:
            append('dt_final_validation_pending_' + run + '.jsonl', {'query_id': final['query_id'], 'reason': repr(exc)})
            if time.monotonic() >= final_deadline:
                raise
            time.sleep(5)
    raw = query(conn, f'''SELECT COUNT(*),COUNT(DISTINCT EVENT_ID),COUNT(DISTINCT SENSOR_ID)
        FROM RAW WHERE RUN_ID={sql_string(run)}''', 'raw_counts_' + run)
    assert list(raw['rows'][0]) == [seconds * sensors, seconds * sensors, sensors]
    canonical_keys = query(conn, f'''SELECT COUNT(*),COUNT(DISTINCT EVENT_ID),
        COUNT_IF(EVENT_ID IS NULL OR SENSOR_ID IS NULL OR BATCH_SEQ IS NULL OR EVENT_TS IS NULL
          OR EVENT_ID<>SENSOR_ID||':'||BATCH_SEQ OR BATCH_SEQ<1 OR BATCH_SEQ>{seconds}
          OR TRY_TO_NUMBER(SENSOR_ID) IS NULL OR TRY_TO_NUMBER(SENSOR_ID)<0 OR TRY_TO_NUMBER(SENSOR_ID)>={sensors}
          OR EVENT_TS<>DATEADD('SECOND',BATCH_SEQ-1,{sql_string(base.isoformat())}::TIMESTAMP_NTZ)
          OR VALUE IS NULL OR VALUE<>(20+MOD(TO_NUMBER(SENSOR_ID),10)+MOD(BATCH_SEQ-1,60)))
        FROM CANONICAL WHERE RUN_ID={sql_string(run)}''', 'canonical_domain_' + run)
    assert list(canonical_keys['rows'][0]) == [seconds * sensors, seconds * sensors, 0]
    latencies = list(first_seen.values())
    warm = [v for (_, seq), v in first_seen.items() if seq <= 120]
    steady = [v for (_, seq), v in first_seen.items() if seq > 120]
    result = dict(manifest, status='passed', completed_utc=utcnow(), final_validation=validation,
        summary_first_inclusion_latency_s=summarise(latencies), warmup_first_120s=summarise(warm),
        post_warmup_latency_s=summarise(steady), acknowledgement_observed_upper_bound_s=summarise(acknowledgements),
        producer_drift_s=summarise(drift), panel_observed_events=len(first_seen),
        panel_expected_events=len(panel) * seconds, censored_events=len(panel) * seconds - len(first_seen),
        observed_revisions=len(observed_revisions), polling_queries=poll_count, nonqualifying_observations=nonqualifying,
        final_validation_query_id=final['query_id'], canonical_domain_query_id=canonical_keys['query_id'],
        raw_count_query_id=raw['query_id'], wall_clock_seconds=time.monotonic() - origin)
    dump('dt_result_' + run + '.json', result)
    history(conn, run)
    return result


def benchmark_run(conn, channel, run, seconds, sensors, poll_seconds=5, burst=False):
    try:
        return _benchmark_run(conn, channel, run, seconds, sensors, poll_seconds, burst)
    except BaseException as exc:
        path = EVIDENCE / ('dt_result_' + run + '.json')
        partial = json.loads(path.read_text()) if path.exists() else {'run_id': run,
            'panel_expected_events': len([s for s in PANEL if int(s) < sensors]) * seconds}
        partial.update(status='failed', error=repr(exc), failed_utc=utcnow(),
                       latency_interpretation='Incomplete/censored run; do not report as successful-only performance')
        dump(path.name, partial)
        raise


def paced(args):
    conn = connect(); client = streaming_client('SENSOR_PIPE'); channel = client.get_elastic_channel()
    results = []
    try:
        for i in range(args.repeats):
            run = f'paced_{i+1}'
            results.append(benchmark_run(conn, channel, run, args.seconds, args.sensors))
            if i + 1 < args.repeats:
                deadline = time.monotonic() + args.idle_seconds
                while time.monotonic() < deadline:
                    dump('dt_progress.json', {'phase': 'between_run_idle', 'after_run': run, 'utc': utcnow(),
                         'remaining_seconds': max(0, deadline - time.monotonic())})
                    time.sleep(min(5, max(0, deadline - time.monotonic())))
        results.append(benchmark_run(conn, channel, 'burst_recovery', args.burst_seconds, args.sensors, burst=True))
        dump('dt_paced_complete.json', {'status': 'passed', 'runs': results, 'completed_utc': utcnow()})
    finally:
        client.close(); conn.close()


def cleanup():
    conn = connect()
    for table in TABLES:
        if capabilities().get(table):
            query(conn, f'ALTER DYNAMIC TABLE {table} SUSPEND', 'suspend_' + table)
    dump('dt_suspended.json', query(conn, 'SHOW DYNAMIC TABLES IN SCHEMA DT'))
    conn.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('stage', choices=('local', 'setup', 'correctness', 'paced', 'cleanup'))
    p.add_argument('--seconds', type=int, default=120)
    p.add_argument('--repeats', type=int, default=1)
    p.add_argument('--sensors', type=int, default=10)
    p.add_argument('--idle-seconds', type=int, default=60)
    p.add_argument('--burst-seconds', type=int, default=30)
    args = p.parse_args()
    if min(args.seconds, args.repeats, args.sensors, args.burst_seconds) < 1 or args.idle_seconds < 0:
        p.error('seconds, repeats, sensors and burst-seconds must be positive; idle-seconds must be nonnegative')
    if args.stage in ('setup', 'correctness', 'paced'):
        start_stage(args.stage)
    if args.stage == 'local': print(self_check())
    elif args.stage == 'setup': setup()
    elif args.stage == 'correctness': correctness()
    elif args.stage == 'paced': paced(args)
    elif args.stage == 'cleanup': cleanup()


if __name__ == '__main__':
    main()
