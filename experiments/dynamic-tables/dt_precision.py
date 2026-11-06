"""Read-only follow-up for the observed large-offset small-variance failure."""
from common import connect, dump, query


def main():
    conn = connect()
    results = {}
    for source in ('RAW', 'CANONICAL'):
        results[source] = query(conn, f'''SELECT COUNT(*) N,
          ARRAY_AGG(VALUE) WITHIN GROUP (ORDER BY VALUE) INPUT_VALUES,
          SUM(VALUE) SUM_FLOAT,AVG(VALUE) MEAN_FLOAT,STDDEV_POP(VALUE) POP_FLOAT,STDDEV_SAMP(VALUE) SAMP_FLOAT,
          STDDEV_POP(VALUE-1000000000000) POP_CENTERED,
          STDDEV_SAMP(VALUE-1000000000000) SAMP_CENTERED,
          STDDEV_POP(VALUE::NUMBER(38,6)) POP_NUMBER,
          STDDEV_SAMP(VALUE::NUMBER(38,6)) SAMP_NUMBER,
          STDDEV_POP(TRY_TO_DECIMAL(SOURCE_VALUE,38,6)) POP_SOURCE_NUMBER
          FROM {source} WHERE RUN_ID='fixture' AND SENSOR_ID='numeric' ''', 'precision_' + source)
    results['offset_sweep'] = query(conn, '''WITH offsets AS (
        SELECT column1::FLOAT b FROM VALUES (0),(1000),(1000000),(1000000000),(1000000000000)),
      residuals AS (SELECT column1::FLOAT e FROM VALUES (-2),(-1),(0),(1),(2))
      SELECT b,STDDEV_POP(b+e) POP_DIRECT,STDDEV_SAMP(b+e) SAMP_DIRECT,
        STDDEV_POP((b+e)-b) POP_CENTERED,
        STDDEV_POP((b+e)::NUMBER(38,6)) POP_NUMBER
      FROM offsets CROSS JOIN residuals GROUP BY b ORDER BY b''', 'precision_offset_sweep')
    results['dt_output'] = query(conn, "SELECT * FROM MINUTE WHERE RUN_ID='fixture' AND SENSOR_ID='numeric'", 'precision_dt_output')
    dump('dt_precision_readonly.json', results)
    conn.close()


if __name__ == '__main__':
    main()
