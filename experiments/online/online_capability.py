"""Check/create/clean only this experiment's schema-scoped Online service.

Read-only by default. --setup is authorized only after implementation review.
--cleanup drops only a service with this run's persisted creation-intent manifest.
The original online_session helper reads an explicitly selected CLI profile.
"""
from __future__ import annotations

import argparse
import dataclasses
import inspect
import json
import time
from datetime import datetime, timezone
from importlib.metadata import version

from common import DB, EVIDENCE, FS_WH, PROFILE, SCHEMA, PRODUCER_ROLE, CONSUMER_ROLE, append, dump
from online_session import create_snowpark_session
from snowflake.ml.feature_store import CreationMode, Feature, FeatureStore
from snowflake.ml.feature_store.online_service import OnlineServiceAccess

LOCATOR = f"{DB}.{SCHEMA}"
MANIFEST = EVIDENCE / "online_owner.json"


def now():
    return datetime.now(timezone.utc).isoformat()


def error_record(exc):
    return {"type": type(exc).__name__, "message": str(exc),
            "query_id": getattr(exc, "sfqid", None),
            "sql_error_code": getattr(exc, "sql_error_code", None)}


def session():
    result = create_snowpark_session(PROFILE)
    result.sql(f"USE WAREHOUSE {FS_WH}").collect()
    result.sql(f"USE DATABASE {DB}").collect()
    result.sql(f"USE SCHEMA {SCHEMA}").collect()
    # The service-create API requires at least 1,800 seconds remaining on the
    # statement; give its admission check headroom. This changes this session only.
    result.sql(f"ALTER SESSION SET TIMEZONE='UTC', QUERY_TAG='{DB}_ONLINE', STATEMENT_TIMEOUT_IN_SECONDS=3600").collect()
    return result


def sql(sess, statement, label):
    rec = {"label": label, "sql": statement, "started_utc": now()}
    started = time.monotonic()
    with sess.query_history(include_describe=True) as history:
        try:
            rec.update(status="ok", rows=[x.as_dict() for x in sess.sql(statement).collect()])
        except Exception as exc:
            rec.update(status="error", error=error_record(exc))
    rec.update(elapsed_s=time.monotonic()-started,
               query_ids=[x.query_id for x in history.queries])
    append("online_queries.jsonl", rec)
    return rec


def get_status(sess):
    record = sql(sess, f"SELECT SYSTEM$GET_FEATURE_STORE_ONLINE_SERVICE_STATUS('{LOCATOR}') AS STATUS", "service_status")
    if record["status"] != "ok":
        raise RuntimeError(record["error"]["message"])
    return json.loads(record["rows"][0]["STATUS"])


def status_name(value):
    return str(value.get("status", "")).upper()


def account_identity(sess):
    """Bind ownership to the authenticated account, not an editable profile alias."""
    row = sess.sql("SELECT CURRENT_ORGANIZATION_NAME() AS ORGANIZATION, "
                   "CURRENT_ACCOUNT() AS ACCOUNT_LOCATOR, CURRENT_REGION() AS REGION").collect()[0].as_dict()
    keys = ("ORGANIZATION", "ACCOUNT_LOCATOR", "REGION")
    if any(not isinstance(row.get(key), str) or not row[key] for key in keys):
        raise RuntimeError("Cannot establish live account identity; refusing ownership-dependent operation.")
    return {key: row[key] for key in keys}


def verify_owner(sess, owner):
    if owner.get("locator") != LOCATOR or owner.get("run_id") != DB:
        raise RuntimeError("Ownership manifest locator/run mismatch; refusing operation.")
    if owner.get("account_identity") != account_identity(sess):
        raise RuntimeError("Ownership manifest live-account mismatch or missing identity; refusing operation.")


def feature_store(sess, create=False):
    return FeatureStore(session=sess, database=DB, name=SCHEMA,
                        default_warehouse=FS_WH,
                        creation_mode=(CreationMode.CREATE_IF_NOT_EXIST if create else CreationMode.FAIL_IF_NOT_EXIST),
                        online_service_access=OnlineServiceAccess.PUBLIC)


def inventory(sess):
    results = {}
    for label, statement in [
        ("environment", "SELECT CURRENT_REGION() REGION, CURRENT_VERSION() VERSION, CURRENT_ROLE() ROLE"),
        ("postgres_inventory", "SHOW POSTGRES INSTANCES"),
        ("online_tables_inventory", "SHOW ONLINE FEATURE TABLES IN ACCOUNT"),
    ]:
        results[label] = sql(sess, statement, label)
    return results


def setup(sess, timeout_s=1200, retry=False):
    if not PRODUCER_ROLE or not CONSUMER_ROLE:
        raise ValueError("Set SNOWFLAKE_PRODUCER_ROLE and SNOWFLAKE_CONSUMER_ROLE to authorized existing account roles")
    prior = None
    if MANIFEST.exists():
        prior = json.loads(MANIFEST.read_text())
        if not retry or prior.get("locator") != LOCATOR or prior.get("run_id") != DB or prior.get("created"):
            raise RuntimeError("Ownership manifest exists; retry is allowed only after this run's failed, uncreated attempt.")
        verify_owner(sess, prior)
    identity = account_identity(sess)
    before = get_status(sess)
    if status_name(before) != "NOT_FOUND":
        raise RuntimeError("Target schema already has an online service; preserving it without modification.")
    if prior:
        append("online_previous_attempts.jsonl", prior)
    owner = {"locator": LOCATOR, "run_id": DB, "account_identity": identity, "size": "XS", "status_before": before,
             "intent_utc": now(), "create_attempted": False, "created": False, "cleaned": False}
    dump("online_owner.json", owner)
    fs = feature_store(sess, create=True)
    # This exact locator was absent. The caller must use a new isolated lab schema.
    owner["create_attempted"] = True
    dump("online_owner.json", owner)
    try:
        with sess.query_history(include_describe=True) as history:
            result = fs.create_online_service(PRODUCER_ROLE, CONSUMER_ROLE, size="XS")
        owner.update(created=True, create_ack_utc=now(), create_result=dataclasses.asdict(result),
                     create_query_ids=[q.query_id for q in history.queries])
        dump("online_owner.json", owner)
    except Exception as exc:
        owner.update(create_error=error_record(exc), create_end_utc=now())
        try:
            owner["status_after_error"] = get_status(sess)
        except Exception as status_exc:
            owner["status_error"] = error_record(status_exc)
        dump("online_owner.json", owner)
        # Do not call this success. A partial creation may need scoped cleanup.
        return {"status": "BLOCKED", "stage": "service_create", "error": error_record(exc)}
    deadline = time.monotonic() + timeout_s
    last = None
    while time.monotonic() < deadline:
        state = get_status(sess)
        if state != last:
            print("Online service", status_name(state), flush=True)
            append("online_status.jsonl", {"utc": now(), "state": state})
            last = state
        name = status_name(state)
        if name == "RUNNING":
            owner["running_utc"] = now()
            dump("online_owner.json", owner)
            return {"status": "READY", "service": state}
        if "ERROR" in name or "FAIL" in name:
            return {"status": "BLOCKED", "stage": "provisioning", "service": state}
        time.sleep(15)
    return {"status": "BLOCKED", "stage": "provisioning_timeout", "last_state": last}


def cleanup(sess):
    if not MANIFEST.exists():
        return {"status": "NO_MANIFEST_PRESERVED"}
    owner = json.loads(MANIFEST.read_text())
    verify_owner(sess, owner)
    if status_name(owner.get("status_before", {})) != "NOT_FOUND" or not owner.get("create_attempted"):
        raise RuntimeError("Manifest does not prove initial absence and run creation attempt; refusing cleanup.")
    current = get_status(sess)
    if owner.get("cleaned"):
        if status_name(current) != "NOT_FOUND":
            raise RuntimeError("A service appeared after confirmed cleanup; preserving it without modification.")
        return {"status": "CLEAN", "service": current}
    if status_name(current) != "NOT_FOUND":
        fs = feature_store(sess)
        with sess.query_history(include_describe=True) as history:
            result = fs.drop_online_service()
        owner.update(drop_ack_utc=now(), drop_result=dataclasses.asdict(result),
                     drop_query_ids=[q.query_id for q in history.queries])
        dump("online_owner.json", owner)
    deadline = time.monotonic()+300
    while time.monotonic() < deadline:
        after = get_status(sess)
        if status_name(after) == "NOT_FOUND":
            owner.update(cleaned=True, cleaned_utc=now(), cleanup_status=after)
            dump("online_owner.json", owner)
            return {"status": "CLEAN", "service": after}
        time.sleep(10)
    return {"status": "CLEANUP_UNCONFIRMED", "service": after}


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--setup", action="store_true")
    group.add_argument("--cleanup", action="store_true")
    parser.add_argument("--retry", action="store_true", help="Retry this run's failed uncreated attempt, preserving previous evidence")
    args = parser.parse_args()
    packages = {x: version(x) for x in ["snowflake-ml-python", "snowflake-snowpark-python", "snowflake-connector-python"]}
    surface = {name: (str(inspect.signature(getattr(FeatureStore, name))) if hasattr(FeatureStore, name) else "NOT_FOUND")
               for name in ["create_online_service", "get_online_service_status", "drop_online_service", "ingest", "read_feature_view"]}
    dump("online_sdk.json", {"packages": packages, "surface": surface,
         "stddev_doc": inspect.getdoc(Feature.stddev), "variance_doc": inspect.getdoc(Feature.var),
         "stddev_source": inspect.getsourcefile(Feature.stddev), "stddev_line": inspect.getsourcelines(Feature.stddev)[1]})
    sess = session()
    try:
        if args.cleanup:
            result = cleanup(sess)
            dump("online_cleanup.json", result)
        else:
            dump("online_inventory_before.json", inventory(sess))
            result = setup(sess, retry=args.retry) if args.setup else {"status": "READ_ONLY", "service": get_status(sess)}
            dump("online_capability.json", result)
        print(json.dumps(result, default=str), flush=True)
        if result["status"] in ("BLOCKED", "CLEANUP_UNCONFIRMED"):
            raise SystemExit(1)
    finally:
        sess.close()


if __name__ == "__main__":
    main()
