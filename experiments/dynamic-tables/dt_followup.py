"""Reviewed follow-ups: explicit UTC current window and materialized SD precision."""
from datetime import timedelta
import json
import time
from common import DB, DT_WH, EVIDENCE, append, connect, dump, query, streaming_client, start_stage
from dt_oracle import compare, stats
from dt_run import AGG, history, rows_to_map, send, utcnow, wait_raw

UTC_NOW = "CONVERT_TIMEZONE('UTC',CURRENT_TIMESTAMP())::TIMESTAMP_NTZ"
DDL = {
    'PRECISION_NUMBER': f'''CREATE DYNAMIC TABLE PRECISION_NUMBER TARGET_LAG=DOWNSTREAM
      WAREHOUSE={DT_WH} REFRESH_MODE=INCREMENTAL INITIALIZE=ON_CREATE AS
      SELECT RUN_ID,SENSOR_ID,COUNT(*) N_EVENTS,COUNT(VALUE) N_VALID,
      SUM(VALUE::NUMBER(38,6)) SUM_NUMBER,AVG(VALUE::NUMBER(38,6)) MEAN,
      STDDEV_POP(VALUE::NUMBER(38,6)) STD_POP,STDDEV_SAMP(VALUE::NUMBER(38,6)) STD_SAMP,
      MIN(VALUE) MIN_VALUE,MAX(VALUE) MAX_VALUE,
      STDDEV_POP(VALUE-1000000000000) POP_CENTERED,
      STDDEV_SAMP(VALUE-1000000000000) SAMP_CENTERED
      FROM CANONICAL WHERE RUN_ID='fixture' AND SENSOR_ID='numeric' GROUP BY RUN_ID,SENSOR_ID''',
    'ROLLING_NOW_UTC': f'''CREATE DYNAMIC TABLE ROLLING_NOW_UTC TARGET_LAG='60 seconds'
      WAREHOUSE={DT_WH} REFRESH_MODE=INCREMENTAL INITIALIZE=ON_CREATE AS
      SELECT RUN_ID,SENSOR_ID,{AGG} FROM CANONICAL
      WHERE RUN_ID='idle_utc' AND EVENT_TS>DATEADD('SECOND',-60,{UTC_NOW})
      AND EVENT_TS<={UTC_NOW} GROUP BY RUN_ID,SENSOR_ID''',
    'ROLLING_NOW_IMPLICIT_CONTROL': f'''CREATE DYNAMIC TABLE ROLLING_NOW_IMPLICIT_CONTROL TARGET_LAG='60 seconds'
      WAREHOUSE={DT_WH} REFRESH_MODE=INCREMENTAL INITIALIZE=ON_CREATE AS
      SELECT RUN_ID,SENSOR_ID,{AGG} FROM CANONICAL
      WHERE RUN_ID='idle_utc' AND EVENT_TS>DATEADD('SECOND',-60,CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
      AND EVENT_TS<=CURRENT_TIMESTAMP()::TIMESTAMP_NTZ GROUP BY RUN_ID,SENSOR_ID'''
}


def finalize(conn):
    setup=json.loads((EVIDENCE/'dt_followup_setup.json').read_text())
    precision_result=json.loads((EVIDENCE/'dt_precision_materialized.json').read_text())
    expiry=json.loads((EVIDENCE/'dt_idle_expiry.json').read_text())
    assert expiry['status']=='passed'
    for name in DDL:
        if setup[name]['status']=='ok':
            query(conn,f'ALTER DYNAMIC TABLE {DB}.DT.{name} SUSPEND','followup_suspend_'+name)
    checks = [json.loads((EVIDENCE / f'dt_correctness_{label}.json').read_text())
              for label in ('initial','late','replay','asof_empty')]
    findings=[record for check in checks for record in check.values()
              if record.get('status')=='numerical_diagnostic_failed']
    dump('dt_correctness_complete.json',{'status':'completed_with_numerical_findings'
         if findings or precision_result['status'] != 'passed' else 'passed',
         'ordinary_correctness':'passed','idle_expiry':'passed_explicit_utc_variant',
         'numeric_original':'failed' if findings else 'passed','numeric_number_centered_materialized':precision_result['status'],
         'numerical_failures':findings,'completed_utc':utcnow(),
         'original_idle_definition':'implicit-timezone control preserved; inspect paired observations'})


def main():
    start_stage('followup')
    conn = connect(); client = None
    try:
        setup = {name: query(conn, ddl, 'followup_setup_' + name, expected_error=name=='PRECISION_NUMBER')
                 for name, ddl in DDL.items()}
        dump('dt_followup_setup.json', setup)
        values = [{'value': x} for x in (10**12-2,10**12-1,10**12,10**12+1,10**12+2)]
        expected = stats(values)
        precision_result = {'status':'unsupported','definition_evidence':setup['PRECISION_NUMBER']}
        if setup['PRECISION_NUMBER']['status']=='ok':
            precision = query(conn, 'SELECT * FROM PRECISION_NUMBER', 'precision_materialized')
            try:
                validation = compare(rows_to_map(precision, ('RUN_ID','SENSOR_ID')), {('fixture','numeric'): expected})
                row = dict(zip(precision['columns'], precision['rows'][0]))
                assert row['SUM_NUMBER'] == 5 * 10**12
                assert abs(row['POP_CENTERED'] - expected[3]) < 1e-12
                assert abs(row['SAMP_CENTERED'] - expected[4]) < 1e-12
                precision_result = {'status':'passed','validation':validation,'observation':precision}
            except AssertionError as exc:
                precision_result = {'status':'failed','error':repr(exc),'expected':expected,'observation':precision}
        dump('dt_precision_materialized.json', precision_result)
        # Keep original failing predicate and definition intact for evidence.
        original = query(conn, "SELECT GET_DDL('DYNAMIC_TABLE','ROLLING_NOW')", 'idle_original_ddl')
        dump('dt_idle_original_ddl.json', original)
        client = streaming_client('SENSOR_PIPE'); channel = client.get_elastic_channel()
        server_clock = query(conn, f'SELECT {UTC_NOW} SERVER_UTC', 'idle_utc_server_clock')
        event_ts = server_clock['rows'][0][0]
        event = {'run_id':'idle_utc','event_id':'idle_utc1','sensor_id':'idle_utc_sensor',
                 'event_ts':event_ts.isoformat(),'value':9,'batch_seq':1,'sent_at':utcnow().isoformat()}
        send(channel,[event],'idle-explicit-utc')
        wait_raw(conn,'idle_utc',1)
        query(conn,'ALTER DYNAMIC TABLE CANONICAL REFRESH','idle_utc_canonical_ready')
        control = query(conn,f'''SELECT RUN_ID,SENSOR_ID,{AGG} FROM CANONICAL
            WHERE RUN_ID='idle_utc' AND EVENT_TS>DATEADD('SECOND',-60,{UTC_NOW})
            AND EVENT_TS<={UTC_NOW} GROUP BY RUN_ID,SENSOR_ID''','idle_utc_select_control')
        compare(rows_to_map(control,('RUN_ID','SENSOR_ID')), {('idle_utc','idle_utc_sensor'):stats([event])})
        query(conn,'ALTER DYNAMIC TABLE ROLLING_NOW_UTC REFRESH','idle_utc_initial_manual_refresh')
        initial = query(conn,'SELECT * FROM ROLLING_NOW_UTC','idle_utc_initial_observation')
        compare(rows_to_map(initial,('RUN_ID','SENSOR_ID')), {('idle_utc','idle_utc_sensor'):stats([event])})
        # Paired control shares source, fresh server clock, canonical refresh and session.
        # Its only definition difference is implicit versus explicit UTC conversion.
        query(conn,'ALTER DYNAMIC TABLE ROLLING_NOW_IMPLICIT_CONTROL REFRESH','idle_implicit_paired_manual_refresh')
        implicit_initial = query(conn,'SELECT * FROM ROLLING_NOW_IMPLICIT_CONTROL','idle_implicit_paired_initial')
        dump('dt_idle_paired_initial.json', {'utc':initial,'implicit':implicit_initial,'ordinary_utc_control':control,
            'server_clock':server_clock,'definition_only_difference':'explicit UTC conversion in CURRENT_TIMESTAMP predicate'})
        # No mutation, source event, clock tick or manual refresh occurs during this loop.
        deadline=time.monotonic()+240
        while time.monotonic()<deadline:
            time.sleep(5)
            observation=query(conn,'SELECT * FROM ROLLING_NOW_UTC')
            implicit_observation=query(conn,'SELECT * FROM ROLLING_NOW_IMPLICIT_CONTROL')
            observed=utcnow()
            append('dt_idle_utc_observations.jsonl', {'observed_utc':observed,'query_id':observation['query_id'],
                'rows':observation['rows'],'implicit_rows':implicit_observation['rows'],
                'implicit_query_id':implicit_observation['query_id']})
            if not observation['rows']:
                assert observed>=event_ts+timedelta(seconds=60)
                dump('dt_idle_expiry.json',{'status':'passed','variant':'ROLLING_NOW_UTC','event_time':event_ts,
                    'observed_expired_utc':observed,'seconds_event_to_observed_expiry':(observed-event_ts).total_seconds(),
                    'initial_query_id':initial['query_id'],'expired_query_id':observation['query_id'],
                    'control_query_id':control['query_id'],'manual_initialization_only':True,'idle_input_records':0,
                    'paired_implicit_initial_rows':implicit_initial['rows'],
                    'paired_implicit_initial_query_id':implicit_initial['query_id'],
                    'original_implicit_timezone_variant':'paired observation recorded; no universal failure assumed'})
                break
        else:
            raise TimeoutError('Explicit UTC current window did not expire automatically within 240 seconds')
        history(conn,'correctness_followup')
        finalize(conn)
    finally:
        if client is not None: client.close()
        conn.close()


if __name__=='__main__':
    import sys
    if '--finalize-only' in sys.argv:
        connection=connect()
        try: finalize(connection)
        finally: connection.close()
    else:
        main()
