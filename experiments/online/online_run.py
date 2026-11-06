"""UNVERIFIED LIVE: Online stream statistics experiment.

No mutations unless --run is supplied. Run only after peer review and
online_capability --setup returns READY. The original trial blocked provisioning.
All HTTP responses are retained without credentials; exact event ledger is local.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import inspect
import json
import math
import statistics
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone

import jwt
import pandas as pd
import requests
from cryptography.hazmat.primitives import serialization
from snowflake.ml.feature_store import Entity, Feature, FeatureView, OnlineConfig, OnlineStoreType, StreamConfig, StreamSource
from snowflake.ml.feature_store import online_service
from snowflake.ml.feature_store.spec.enums import FeatureAggregationMethod
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import DoubleType, StringType, StructField, StructType, TimestampTimeZone, TimestampType

from common import DB, EVIDENCE, CLIENT_LOCATION, append, config, dump
from online_session import load_private_key
from online_capability import LOCATOR, MANIFEST, cleanup, error_record, feature_store, get_status, now, session, sql, status_name, verify_owner

SOURCE = "SENSOR_EVENTS"
FV_NAME = "SENSOR_CONT_60S"
VERSION = "V1"
FEATURES = ["N_EVENTS", "N_VALUES", "MEAN", "STD_POP", "VAR_POP"]
FIELDS = StructType([StructField("SENSOR_ID", StringType()), StructField("EVENT_ID", StringType()),
                     StructField("EVENT_TS", TimestampType(TimestampTimeZone.NTZ)), StructField("VALUE", DoubleType())])


def transform_sensor_events(df: pd.DataFrame) -> pd.DataFrame:
    return df[["SENSOR_ID", "EVENT_ID", "EVENT_TS", "VALUE"]]


def event(sensor, event_id, value, at=None):
    at = at or datetime.now(timezone.utc)
    return {"SENSOR_ID": sensor, "EVENT_ID": event_id,
            "EVENT_TS": at.isoformat().replace("+00:00", "Z"), "VALUE": value}


def oracle(records):
    values = [float(r["VALUE"]) for r in records if r["VALUE"] is not None]
    return {"N_EVENTS": len(records), "N_VALUES": len(values),
            "MEAN": statistics.fmean(values) if values else None,
            "STD_POP": statistics.pstdev(values) if values else None,
            "VAR_POP": statistics.pvariance(values) if values else None}


def matches(actual, expected):
    return set(actual) == set(expected) and all(
        a is None if e is None else isinstance(a, (int, float)) and not isinstance(a, bool)
        and math.isfinite(a) and math.isclose(a, e, rel_tol=0 if abs(e)>1e9 else 1e-9,
                                            abs_tol=0.0002 if abs(e)>1e9 and k=="MEAN" else 1e-9)
        for k, e in expected.items() for a in [actual[k]])


def assess_ingest(rec, count):
    body = rec.get("response", {})
    src = body.get("sources", {}).get(SOURCE, {})
    fv = src.get("feature_views", {})
    rr = src.get("records", {})
    outcomes = fv.get("results", [])
    return (rec.get("http_status") == 200 and body.get("status") == "success"
        and rr.get("total") == count and rr.get("failed") == 0 and fv.get("failed") == 0
        and len(outcomes) == 1
        and any(str(x.get("name", "")).upper() == FV_NAME and str(x.get("version", "")).upper() == VERSION
                and x.get("status") == "success" for x in outcomes)
        and all(x.get("status") == "success" for x in outcomes))


def assess_query(rec, sensors):
    body = rec.get("response", {})
    names = [str(f.get("name", "")).upper() for f in body.get("metadata", {}).get("features", [])]
    rows = body.get("results", [])
    valid = rec.get("http_status") == 200 and body.get("status") == "success" and set(names) == set(FEATURES)
    valid = valid and len(names) == len(FEATURES) and len(rows) == len(sensors)
    valid = valid and all(len(r.get("features", [])) == len(names) and len(r.get("status", [])) == len(names)
                         and all(s in ("PRESENT", "MISSING_DATA") for s in r["status"])
                         and all(s != "MISSING_DATA" or v is None for v,s in zip(r["features"],r["status"])) for r in rows)
    rec["valid"] = bool(valid)
    rec["values"] = {s: dict(zip(names, row["features"])) for s, row in zip(sensors, rows)} if valid else {}
    rec["statuses"] = {s: dict(zip(names, row["status"])) for s, row in zip(sensors, rows)} if valid else {}
    return rec


class Client:
    """Bounded HTTP client with key-pair JWT; token never enters logs."""
    def __init__(self, sess, fs):
        state = fs.get_online_service_status()
        self.urls = {kind: online_service.endpoint_url(state, kind, access=online_service.OnlineServiceAccess.PUBLIC)
                     for kind in ("query", "ingest")}
        if not all(self.urls.values()):
            raise RuntimeError("Missing public endpoint; no network configuration will be changed.")
        p = config()
        self.key = load_private_key(p)
        public = self.key.public_key().public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
        fingerprint = "SHA256:" + base64.b64encode(hashlib.sha256(public).digest()).decode()
        account = sess.sql("SELECT CURRENT_ACCOUNT() AS A").collect()[0]["A"]
        self.subject = f"{account}.{p['user']}".upper()
        self.issuer = f"{self.subject}.{fingerprint}"
        self.local = threading.local()
        self.sessions=[]
        self.sessions_lock=threading.Lock()
        self.setup_session=sess
        self.calibrate_clock()

    def calibrate_clock(self):
        clock_samples=[]
        for _ in range(3):
            before=datetime.now(timezone.utc)
            remote=self.setup_session.sql("SELECT CURRENT_TIMESTAMP() AS T").collect()[0]["T"]
            after=datetime.now(timezone.utc)
            clock_samples.append(((remote-after).total_seconds(),(remote-before).total_seconds()))
        self.clock_lower=max(x[0] for x in clock_samples)
        self.clock_upper=min(x[1] for x in clock_samples)
        if self.clock_lower>self.clock_upper:
            raise RuntimeError("Server/client clock brackets do not overlap")
        append("online_clock_bounds.jsonl",{"utc":now(),"offset_lower_s":self.clock_lower,
                                            "offset_upper_s":self.clock_upper,"samples":clock_samples})

    def close(self):
        for http in self.sessions:
            http.close()

    def request(self, kind, body, label, log=True):
        if not hasattr(self.local, "http"):
            self.local.http = requests.Session()
            with self.sessions_lock:
                self.sessions.append(self.local.http)
            self.local.token_deadline = 0
        if time.monotonic() > self.local.token_deadline:
            issued = datetime.now(timezone.utc)
            self.local.token = jwt.encode({"iss": self.issuer, "sub": self.subject, "iat": issued,
                                          "exp": issued+timedelta(minutes=15)}, self.key, algorithm="RS256")
            self.local.token_deadline = time.monotonic()+600
        rid = str(uuid.uuid4())
        rec = {"label": label, "kind": kind, "request_id": rid, "sent_utc": now(),
               "request": body, "send_monotonic": time.monotonic()}
        try:
            response = self.local.http.post(self.urls[kind].rstrip("/")+f"/api/v1/{kind}", json=body,
                headers={"Authorization": "Bearer " + self.local.token, "Content-Type": "application/json", "X-Request-ID": rid},
                timeout=(10, 30))
            rec.update(http_status=response.status_code, response=response.json(), date_header=response.headers.get("Date"))
        except Exception as exc:
            rec.update(http_status=None, error=error_record(exc))
        rec.update(received_utc=now(), receive_monotonic=time.monotonic())
        rec["asof_lower_utc"]=(datetime.fromisoformat(rec["sent_utc"])+timedelta(seconds=self.clock_lower)).isoformat()
        rec["asof_upper_utc"]=(datetime.fromisoformat(rec["received_utc"])+timedelta(seconds=self.clock_upper)).isoformat()
        rec["elapsed_s"] = rec["receive_monotonic"]-rec["send_monotonic"]
        if log:
            append("online_http.jsonl", rec)
        return rec

    def ingest(self, records, label):
        if not 1 <= len(records) <= 50:
            raise ValueError("Ingest must contain1..50 records")
        append("online_event_ledger.jsonl", {"label": label, "records": records})
        rec = self.request("ingest", {"records": {SOURCE: records}, "include_diagnostics": True}, label)
        rec["accepted"] = assess_ingest(rec,len(records))
        append("online_ingest_checks.jsonl", {"label": label, "request_id": rec["request_id"], "accepted": rec["accepted"]})
        return rec

    def query(self, sensors, label):
        if not 1 <= len(sensors) <= 10 or len(set(sensors)) != len(sensors):
            raise ValueError("Query needs1..10 distinct sensors")
        rec = self.request("query", {"name": FV_NAME, "version": VERSION, "object_type": "feature_view",
            "request_rows": [{"entity": {"SENSOR_ID": s}} for s in sensors], "features": FEATURES,
            "metadata_options": {"include_names": True, "include_effective_times": True, "include_serving_status": True},
            "include_diagnostics": True}, label)
        return assess_query(rec,sensors)


def register(sess, fs):
    # Permanent backfill ensures registration query remains valid in a fresh session.
    result = sql(sess, f"CREATE TABLE {LOCATOR}.SENSOR_BACKFILL (SENSOR_ID VARCHAR, EVENT_ID VARCHAR, EVENT_TS TIMESTAMP_NTZ, VALUE DOUBLE)", "backfill_create")
    if result["status"] != "ok":
        raise RuntimeError(result)
    sql(sess, f"INSERT INTO {LOCATOR}.SENSOR_BACKFILL VALUES ('SENTINEL','OLD','2000-01-01'::TIMESTAMP_NTZ,0.0)", "backfill_seed")
    source = StreamSource(SOURCE, FIELDS, desc="Isolated sensor statistics trial")
    fs.register_stream_source(source)
    sensor = Entity("SENSOR", ["SENSOR_ID"])
    fs.register_entity(sensor)
    backfill = sess.table(f"{LOCATOR}.SENSOR_BACKFILL").with_column("SENSOR_ID", col("SENSOR_ID").cast(StringType()))
    cfg = StreamConfig(source, transform_sensor_events, backfill)
    features = [Feature.count("EVENT_ID", "60s").alias("N_EVENTS"), Feature.count("VALUE", "60s").alias("N_VALUES"),
                Feature.avg("VALUE", "60s").alias("MEAN"), Feature.stddev("VALUE", "60s").alias("STD_POP"),
                Feature.var("VALUE", "60s").alias("VAR_POP")]
    draft = FeatureView(FV_NAME, [sensor], stream_config=cfg, timestamp_col="EVENT_TS", refresh_freq="1 minute",
        feature_granularity="1 minute", features=features,
        online_config=OnlineConfig(enable=True, store_type=OnlineStoreType.POSTGRES),
        feature_aggregation_method=FeatureAggregationMethod.CONTINUOUS)
    with sess.query_history(include_describe=True) as history:
        registered = fs.register_feature_view(draft, VERSION)
    dump("online_registration.json", {"status": "REGISTERED", "query_ids": [q.query_id for q in history.queries],
                                     "feature_names": [str(x) for x in registered.feature_names]})
    deadline = time.monotonic()+300
    while time.monotonic() < deadline:
        tables = sql(sess, f"SHOW ONLINE FEATURE TABLES IN SCHEMA {LOCATOR}", "oft_setup")
        rows = tables.get("rows", [])
        relevant = [r for r in rows if FV_NAME in r.get("name", "")]
        if any(r.get("setup_status") == "SETUP_FAILED" for r in relevant):
            raise RuntimeError(relevant)
        if relevant and all(r.get("setup_status") == "SETUP_READY" for r in relevant):
            return registered
        time.sleep(5)
    raise TimeoutError("OFT not SETUP_READY after300seconds")


def await_exact(client, expected, label, sent_mono, timeout=15):
    deadline = time.monotonic()+timeout
    last = None
    while time.monotonic() < deadline:
        last = client.query(list(expected), label)
        if last["valid"] and set(last["values"]) == set(expected) and all(matches(last["values"][s], v) for s,v in expected.items()):
            rec = {"label": label, "status": "PASS", "expected": expected, "actual": last["values"],
                   "first_correct_elapsed_s": last["receive_monotonic"]-sent_mono,
                   "query_request_id": last["request_id"]}
            append("online_checks.jsonl", rec)
            return rec
        time.sleep(0.1)
    rec = {"label": label, "status": "FAIL", "expected": expected, "last_response": last}
    append("online_checks.jsonl", rec)
    return rec


def fixture(client, label, values, ages=None, shared_timestamp=False):
    sensor = DB+"_"+label
    base = datetime.now(timezone.utc)-timedelta(seconds=5)
    rows = [event(sensor, label+str(i), v, base-timedelta(seconds=(ages[i] if ages else 0), microseconds=0 if shared_timestamp else i*1000))
            for i,v in enumerate(values)]
    ingestion = client.ingest(rows, label)
    if not ingestion["accepted"]:
        result = {"label": label, "status": "INGEST_FAILED", "stage": "ingest", "response": ingestion}
        append("online_checks.jsonl", result)
        return rows, result
    return rows, await_exact(client, {sensor: oracle(rows)}, label, ingestion["send_monotonic"])


def semantic_tests(client):
    results = []
    for label, values in [("BASIC", [1,2,3,4]), ("ONE", [7]), ("NULL_MIX", [None,2,4]),
                          ("NULL_ALL", [None,None]), ("OFFSET", [1e12+0.25,1e12+0.5,1e12+0.75]),
                          ("EQUAL", [3,3,3])]:
        rows, result = fixture(client, label, values)
        results.append(result)
        if label == "BASIC" and result["status"] != "PASS":
            return {"status": "FAILED_BASIC_GATE", "results": results}
    # Same-time distinct events and retry use separate entities to isolate identity rules.
    rows, same_time = fixture(client, "SAME_TS", [2,4], shared_timestamp=True)
    results.append(same_time)
    original, replay_base = fixture(client, "REPLAY", [1,3])
    if replay_base["status"] == "PASS":
        marker=event(original[0]["SENSOR_ID"],"REPLAY_MARKER",5,datetime.now(timezone.utc)-timedelta(seconds=1))
        replay = client.ingest(original+[marker], "EXACT_REPLAY_WITH_NEW_MARKER")
        results.append(await_exact(client, {original[0]["SENSOR_ID"]: oracle(original+[marker])}, "REPLAY_IDEMPOTENCE", replay["send_monotonic"])
                       if replay["accepted"] else {"label":"REPLAY_IDEMPOTENCE","status":"INGEST_FAILED","response":replay})
        changed = [dict(original[0], VALUE=9)]
        change = client.ingest(changed, "CHANGED_PAYLOAD_SAME_ID")
        results.append({"label": "CHANGED_PAYLOAD_SAME_ID", "status": "OBSERVED", "ingest": change,
                        "query": client.query([original[0]["SENSOR_ID"]], "CHANGED_PAYLOAD_QUERY")})
    missing = client.query([DB+"_ABSENT"], "MISSING_ENTITY")
    expected_missing = {k:None for k in FEATURES}
    ok = missing["valid"] and missing["values"].get(DB+"_ABSENT") == expected_missing and all(
        s == "MISSING_DATA" for s in missing["statuses"].get(DB+"_ABSENT", {}).values())
    results.append({"label": "MISSING_ENTITY", "status": "PASS" if ok else "FAIL", "query": missing})
    invalid = event(DB+"_INVALID", "BAD", "not-a-number")
    results.append({"label": "INVALID_VALUE", "status": "OBSERVED", "ingest": client.ingest([invalid], "INVALID_VALUE")})
    # Later event arrives first; older event is still inside the 60-second window.
    sensor=DB+"_OOO"
    base=datetime.now(timezone.utc)-timedelta(seconds=5)
    newer=event(sensor,"OOO_NEW",10,base)
    older=event(sensor,"OOO_OLD",2,base-timedelta(seconds=20))
    first=client.ingest([newer],"OUT_OF_ORDER_NEW_FIRST")
    second=client.ingest([older],"OUT_OF_ORDER_OLD_SECOND")
    results.append(await_exact(client,{sensor:oracle([newer,older])},"OUT_OF_ORDER",first["send_monotonic"])
                   if first["accepted"] and second["accepted"] else {"label":"OUT_OF_ORDER","status":"INGEST_FAILED","responses":[first,second]})
    late_marker=event(sensor,"LATE_MARKER",6,datetime.now(timezone.utc)-timedelta(seconds=1))
    stale=client.ingest([event(sensor,"STALE",1000,base-timedelta(seconds=120)),late_marker],"TWO_MINUTE_LATE_WITH_MARKER")
    results.append(await_exact(client,{sensor:oracle([newer,older,late_marker])},"STALE_EXCLUDED",stale["send_monotonic"])
                   if stale["accepted"] else {"label":"STALE_EXCLUDED","status":"INGEST_FAILED","response":stale})
    future=client.ingest([event(sensor,"FUTURE",10000,base+timedelta(seconds=120))],"FUTURE_EVENT")
    results.append({"label":"FUTURE_EVENT","status":"OBSERVED","ingest":future,
                    "strict_current_expected":oracle([newer,older,late_marker]),"query":client.query([sensor],"FUTURE_QUERY")})
    # Sample SD is explicitly a client conversion, not a native API DDOF option.
    basic=next((x for x in results if x.get("label")=="BASIC" and x.get("status")=="PASS"),None)
    if basic:
        vals=next(iter(basic["actual"].values()))
        n=vals["N_VALUES"]
        converted=vals["STD_POP"]*math.sqrt(n/(n-1)) if n>1 else None
        results.append({"label":"SAMPLE_SD_CONVERSION","status":"PASS" if math.isclose(converted,statistics.stdev([1,2,3,4]),rel_tol=1e-9) else "FAIL",
                        "actual":converted,"expected":statistics.stdev([1,2,3,4]),"location":"client"})
    dump("online_semantics.json", {"status": "COMPLETE", "results": results})
    return {"status": "COMPLETE", "results": results}


def latency_tests(client, samples=120):
    warm_start=time.monotonic()
    i=0
    while time.monotonic()-warm_start<10:
        rows=[event(DB+"_INGEST_WARM",str(i),float(i))]
        client.ingest(rows,"INGEST_WARMUP")
        client.query([DB+"_INGEST_WARM"],"READ_WARMUP")
        i+=1
    timings=[]
    for i in range(samples):
        start=time.monotonic()
        sensor=DB+f"_LAT_{i}"
        stamp=datetime.now(timezone.utc)-timedelta(seconds=3)
        records=[event(sensor,f"LAT{i}_{j}",v,stamp+timedelta(milliseconds=j)) for j,v in enumerate([1.,2.,float(i%11+4)])]
        ing=client.ingest(records,"LATENCY_INGEST")
        check=await_exact(client,{sensor:oracle(records)},"LATENCY_CORRECT",ing["send_monotonic"]) if ing["accepted"] else {"status":"INGEST_FAILED"}
        read=client.query([sensor],"LATENCY_LOOKUP")
        timings.append({"index":i,"ingest_response_s":ing["elapsed_s"],"ingest_accepted":ing["accepted"],
                        "correct_check":check,"lookup_s":read["elapsed_s"],"lookup_valid":read["valid"] and matches(read["values"].get(sensor,{}),oracle(records))})
        if i%20==0:
            print("Online latency probes",i,"/",samples,flush=True)
        time.sleep(max(0,1-(time.monotonic()-start)))
    def percentiles(values):
        values=sorted(values)
        return {"n":len(values),**{f"p{p}":values[max(0,math.ceil(p/100*len(values))-1)] for p in [50,95,99]}} if values else {"n":0}
    result={"status":"PASS" if all(x["ingest_accepted"] and x["correct_check"]["status"]=="PASS" and x["lookup_valid"] for x in timings) else "FAIL",
            "client_location":CLIENT_LOCATION,
            "ingest_response_s":percentiles([x["ingest_response_s"] for x in timings if x["ingest_accepted"]]),
            "first_correct_s":percentiles([x["correct_check"]["first_correct_elapsed_s"] for x in timings if x["correct_check"]["status"]=="PASS"]),
            "lookup_s":percentiles([x["lookup_s"] for x in timings if x["lookup_valid"]]),"timings":timings}
    dump("online_latency.json",result)
    return result


def idle_and_boundaries(client):
    base = datetime.now(timezone.utc)
    sensor = DB+"_EXPIRY"
    # Sparse timestamps let observation transitions distinguish event time from whole-minute tiling.
    rows = [event(sensor,"exp"+str(i),v,base-timedelta(seconds=age)) for i,(age,v) in enumerate([(59,1),(30,10),(5,100)])]
    ingest = client.ingest(rows,"EXPIRY_INPUT")
    if not ingest["accepted"]:
        result={"status":"NOT_RUN","reason":"Expiry input was not fully accepted","ingest":ingest}
        dump("online_idle_expiry.json",result)
        return result
    observations = []
    deadline = time.monotonic()+125
    while time.monotonic() < deadline:
        rec = client.query([sensor], "IDLE_EXPIRY")
        left = datetime.fromisoformat(rec["asof_lower_utc"])
        right = datetime.fromisoformat(rec["asof_upper_utc"])
        def active(at):
            return [e for e in rows if at-timedelta(seconds=60) < datetime.fromisoformat(e["EVENT_TS"].replace("Z","+00:00")) <= at]
        lo,hi = oracle(active(left)),oracle(active(right))
        # Empty entity may be represented entirely missing; capture separately rather than coerce counts.
        actual = rec.get("values",{}).get(sensor,{})
        ambiguous = lo != hi
        empty_missing=(lo["N_EVENTS"]==0 and set(actual)==set(FEATURES) and all(v is None for v in actual.values())
                       and all(s=="MISSING_DATA" for s in rec.get("statuses",{}).get(sensor,{}).values()))
        verdict = "INCONCLUSIVE" if ambiguous else "PASS" if rec["valid"] and matches(actual,lo) else "PASS_EMPTY_MISSING_DATA" if rec["valid"] and empty_missing else "DIFFERENT_FROM_STRICT_WINDOW"
        observations.append({"utc":rec["received_utc"],"verdict":verdict,"expected_left":lo,"expected_right":hi,"actual":actual,
                             "request_id":rec["request_id"]})
        time.sleep(1)
    passed=(any(x["verdict"]=="PASS" and x["expected_left"]["N_EVENTS"]>0 for x in observations)
            and observations[-1]["verdict"] in ("PASS","PASS_EMPTY_MISSING_DATA")
            and observations[-1]["expected_left"]["N_EVENTS"]==0
            and not any(x["verdict"]=="DIFFERENT_FROM_STRICT_WINDOW" for x in observations))
    result={"status":"PASS" if passed else "FAIL_OR_INCONCLUSIVE","ingest_accepted":ingest["accepted"],"observations":observations}
    dump("online_idle_expiry.json",result)
    return result


def fresh_client_replay(sess,fs,client):
    sensor=DB+"_FRESH_CLIENT_REPLAY"
    at=datetime.now(timezone.utc)-timedelta(seconds=3)
    rows=[event(sensor,"FRESH_A",1.,at),event(sensor,"FRESH_B",3.,at+timedelta(milliseconds=1))]
    initial=client.ingest(rows,"FRESH_CLIENT_BEFORE")
    if not initial["accepted"]:
        return {"status":"INGEST_FAILED","stage":"before_restart","response":initial}
    first=await_exact(client,{sensor:oracle(rows)},"FRESH_CLIENT_BEFORE",initial["send_monotonic"])
    if first["status"]!="PASS":
        return first
    client.close()
    fresh=Client(sess,fs)
    try:
        marker=event(sensor,"FRESH_MARKER",5.,datetime.now(timezone.utc)-timedelta(seconds=1))
        replay=fresh.ingest(rows+[marker],"FRESH_CLIENT_REPLAY_WITH_MARKER")
        if not replay["accepted"]:
            return {"status":"INGEST_FAILED","stage":"after_restart","response":replay}
        result=await_exact(fresh,{sensor:oracle(rows+[marker])},"FRESH_CLIENT_REPLAY_IDEMPOTENCE",replay["send_monotonic"])
        result["restart_scope"]="New HTTP client sessions and newly generated JWT; same Snowflake setup session, not an OS process restart"
        dump("online_restart_replay.json",result)
        return result
    finally:
        fresh.close()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--run",action="store_true")
    parser.add_argument("--phase",choices=["all","register","semantics","latency","load"],default="all")
    parser.add_argument("--keep-service",action="store_true",help="Only for continuing the reviewed sustained workload in this task")
    args=parser.parse_args()
    if not args.run:
        print("Definitions only: pass --run after implementation review and successful service capability.")
        return
    ownership=json.loads(MANIFEST.read_text())
    if ownership.get("locator") != LOCATOR or not ownership.get("created") or ownership.get("cleaned"):
        raise RuntimeError("No active service owned by this run")
    sess=session()
    client=None
    try:
        verify_owner(sess, ownership)
        fs=feature_store(sess)
        if status_name(get_status(sess))!="RUNNING":
            raise RuntimeError("Online service is not RUNNING")
        if args.phase in ("all","register"):
            register(sess,fs)
        if args.phase=="register":
            result={"status":"REGISTERED","phase":"register"}
        else:
            client=Client(sess,fs)
            if args.phase in ("all","semantics"):
                warm_deadline=time.monotonic()+10
                while time.monotonic()<warm_deadline:
                    client.query([DB+"_WARMUP"],"WARMUP")
                result=semantic_tests(client)
                required={"BASIC","ONE","NULL_MIX","NULL_ALL","EQUAL","SAME_TS","REPLAY_IDEMPOTENCE",
                          "MISSING_ENTITY","OUT_OF_ORDER","STALE_EXCLUDED","SAMPLE_SD_CONVERSION"}
                outcomes={x["label"]:x["status"] for x in result["results"] if "label" in x}
                core_ok=all(outcomes.get(x)=="PASS" for x in required)
                expiry=idle_and_boundaries(client) if core_ok else {"status":"NOT_RUN"}
                replay=fresh_client_replay(sess,fs,client) if core_ok else {"status":"NOT_RUN"}
                if core_ok:
                    client.close()
                    client=Client(sess,fs)
                gate={"status":"PASS" if core_ok and expiry["status"]==replay["status"]=="PASS" else "FAIL",
                      "locator":LOCATOR,"owner_intent_utc":ownership["intent_utc"],"required":outcomes,
                      "expiry_status":expiry["status"],"fresh_client_replay_status":replay["status"],
                      "precision_diagnostic":outcomes.get("OFFSET"),"updated_utc":now()}
                dump("online_semantics_gate.json",gate)
                result={"status":gate["status"],"phase":"semantics","gate":gate}
            if args.phase in ("all","latency","load"):
                gate=json.loads((EVIDENCE/"online_semantics_gate.json").read_text())
                if gate.get("status")!="PASS" or gate.get("locator")!=LOCATOR or gate.get("owner_intent_utc")!=ownership["intent_utc"]:
                    raise RuntimeError("This service incarnation has not passed the semantic/expiry/replay gates")
                if args.phase in ("all","latency"):
                    result=latency_tests(client)
                    if result["status"]!="PASS":
                        raise RuntimeError("Latency phase has failed/censored correctness probes; load not started")
                if args.phase in ("all","load"):
                    from online_load import run_workload
                    result=run_workload(client)
        dump("online_run_result.json",result)
    except Exception as exc:
        dump("online_run_result.json",{"status":"BLOCKED","error":error_record(exc)})
        raise
    finally:
        if client is not None:
            client.close()
        if not args.keep_service:
            dump("online_cleanup.json",cleanup(sess))
        sess.close()


if __name__=="__main__":
    main()
