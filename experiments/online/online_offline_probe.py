"""Read-only fallback: SDK definition acceptance and SQL statistical semantics.

Does not register feature objects or perform online reads. Offline SQL is not
evidence that the account can run the Postgres Online Feature Store.
"""
import json
import math
import statistics
from datetime import datetime, timezone
from importlib.metadata import version

from common import dump
from online_capability import error_record, session, sql
from online_run import FIELDS, transform_sensor_events
from snowflake.ml.feature_store import Entity, Feature, FeatureView, OnlineConfig, OnlineStoreType, StreamConfig, StreamSource
from snowflake.ml.feature_store.spec.enums import FeatureAggregationMethod


def main():
    sess=session()
    try:
        # A re-executable SELECT is sufficient for a definition-only constructor
        # test. Actual registration uses a permanent backfill table in online_run.
        backfill=sess.sql("SELECT 'PROBE'::VARCHAR AS SENSOR_ID, 'ONE'::VARCHAR AS EVENT_ID, '2020-01-01'::TIMESTAMP_NTZ AS EVENT_TS, 1.0::DOUBLE AS VALUE")
        source=StreamSource("DEFINITION_SOURCE",FIELDS)
        entity=Entity("DEFINITION_SENSOR",["SENSOR_ID"])
        cfg=StreamConfig(source,transform_sensor_events,backfill)
        definitions=[]
        for mode,window,backend in [("CONTINUOUS","60s","POSTGRES"),("CONTINUOUS","59s","POSTGRES"),
                                    ("TILES","59s","POSTGRES"),("TILES","60s","HYBRID_TABLE")]:
            rec={"mode":mode,"window":window,"backend":backend,"execution":"CLIENT_CONSTRUCTOR_ONLY"}
            try:
                fv=FeatureView("DEFINITION_ONLY",[entity],stream_config=cfg,timestamp_col="EVENT_TS",
                    refresh_freq="1m",feature_granularity="1m",
                    features=[Feature.count("VALUE",window).alias("N"),Feature.avg("VALUE",window).alias("AVG"),
                              Feature.stddev("VALUE",window).alias("STD_POP"),Feature.var("VALUE",window).alias("VAR_POP")],
                    online_config=OnlineConfig(enable=True,store_type=getattr(OnlineStoreType,backend)),
                    feature_aggregation_method=getattr(FeatureAggregationMethod,mode))
                rec.update(result="ACCEPTED",features=[str(x) for x in fv.feature_names],
                           resolved_backend=str(fv.online_config.store_type),
                           resolved_method=str(fv.feature_aggregation_method))
            except Exception as exc:
                rec.update(result="REJECTED",error=error_record(exc))
            definitions.append(rec)
        dump("online_definition_probes.json",{"sdk":version("snowflake-ml-python"),"definitions":definitions,
             "limitation":"Client acceptance is not server registration or runtime verification."})
        fixtures={"BASIC":[1.,2.,3.,4.],"ONE":[7.],"NULL_MIX":[None,2.,4.],"NULL_ALL":[None,None],
                  "OFFSET":[1e12+.25,1e12+.5,1e12+.75]}
        literals=",".join(f"('{key}',{'NULL' if val is None else repr(val)})" for key,vals in fixtures.items() for val in vals)
        result=sql(sess,f"SELECT COLUMN1 AS SENSOR_ID, COUNT(*) AS N_EVENTS, COUNT(COLUMN2) AS N_VALUES, AVG(COLUMN2::DOUBLE) AS MEAN, STDDEV_POP(COLUMN2::DOUBLE) AS STD_POP, STDDEV_SAMP(COLUMN2::DOUBLE) AS STD_SAMP, VAR_POP(COLUMN2::DOUBLE) AS VAR_POP FROM VALUES {literals} GROUP BY COLUMN1 ORDER BY COLUMN1","offline_sql_statistics")
        checks=[]
        if result["status"]=="ok":
            actual={r["SENSOR_ID"]:r for r in result["rows"]}
            if set(actual)!=set(fixtures):
                raise AssertionError("Unexpected SQL result key set")
            for key,vals in fixtures.items():
                nonnull=[x for x in vals if x is not None]
                expected={"N_EVENTS":len(vals),"N_VALUES":len(nonnull),"MEAN":statistics.fmean(nonnull) if nonnull else None,
                          "STD_POP":statistics.pstdev(nonnull) if nonnull else None,
                          "STD_SAMP":statistics.stdev(nonnull) if len(nonnull)>1 else None,
                          "VAR_POP":statistics.pvariance(nonnull) if nonnull else None}
                observed={k:actual[key][k] for k in expected}
                diffs={k:None if expected[k] is None or observed[k] is None else float(observed[k])-expected[k] for k in expected}
                ok=all(observed[k] is None if v is None else observed[k] is not None and math.isclose(float(observed[k]),v,rel_tol=0,abs_tol=.0002 if k=="MEAN" and key=="OFFSET" else 1e-9) for k,v in expected.items())
                checks.append({"sensor":key,"status":"PASS" if ok else "FAIL","expected":expected,"actual":observed,"errors":diffs})
        dump("online_offline_sql_checks.json",{"execution":"ORDINARY_OFFLINE_SQL_ONLY","checks":checks,"query":result})
        centered=sql(sess,"SELECT COUNT(*) N, AVG(COLUMN1::DOUBLE) MEAN, STDDEV_POP(COLUMN1::DOUBLE) RAW_STD_POP, STDDEV_POP(COLUMN1::DOUBLE-1000000000000.0) CENTERED_STD_POP, STDDEV_SAMP(COLUMN1::DOUBLE-1000000000000.0) CENTERED_STD_SAMP FROM VALUES (1000000000000.25),(1000000000000.5),(1000000000000.75)","offline_centered_precision_diagnostic")
        dump("online_offline_centered_precision.json",centered)
        print(json.dumps({"definitions":definitions,"sql_checks":checks},default=str))
        sql(sess,"SHOW POSTGRES INSTANCES","postgres_after_online_cleanup")
        sql(sess,"SHOW COMPUTE POOLS","compute_pools_after_online_cleanup")
        sql(sess,"SHOW ONLINE FEATURE TABLES IN ACCOUNT","online_tables_after_cleanup")
    finally:
        sess.close()


if __name__=="__main__":
    main()
