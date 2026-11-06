"""Prepared Online workload, UNVERIFIED LIVE because trial provisioning failed.

Bounded offer/ack accounting; no silent producer slowdown and no automatic retry.
Each failed or ambiguous request remains failed, not counted as accepted records.
"""
from __future__ import annotations

import math
import time
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone

from common import DB, append, dump
from online_run import FEATURES, event, matches, oracle


def expected_at(records,at):
    return oracle([r for r in records if at-timedelta(seconds=60)<datetime.fromisoformat(r["EVENT_TS"].replace("Z","+00:00"))<=at])


def check_snapshot_response(rec,sensors,ledger):
    if not rec.get("valid") or set(rec.get("values",{}))!=set(sensors):
        return [{"sensor":s,"status":"FAIL","reason":"Response shape/metadata/key mapping invalid"} for s in sensors]
    low=datetime.fromisoformat(rec["asof_lower_utc"])
    high=datetime.fromisoformat(rec["asof_upper_utc"])
    results=[]
    for sensor in sensors:
        records=ledger.get(sensor,[])
        left=expected_at(records,low)
        right=expected_at(records,high)
        actual=rec["values"][sensor]
        if left!=right:
            verdict="INCONCLUSIVE"
        elif matches(actual,left):
            verdict="PASS"
        elif left["N_EVENTS"]==0 and set(actual)==set(FEATURES) and set(rec.get("statuses",{}).get(sensor,{}))==set(FEATURES) and all(v is None for v in actual.values()) and all(
            x=="MISSING_DATA" for x in rec.get("statuses",{}).get(sensor,{}).values()):
            verdict="EMPTY_MISSING_DATA"
        else:
            verdict="FAIL"
        results.append({"sensor":sensor,"status":verdict,"expected_left":left,"expected_right":right,
                        "actual":actual,"request_id":rec.get("request_id")})
    return results


def snapshot(client,ledger,sensors,label,workers=8):
    # Recheck clock skew after long workloads, before concurrent REST requests.
    if hasattr(client,"calibrate_clock"):
        client.calibrate_clock()
    checks=[]
    chunks=[sensors[i:i+10] for i in range(0,len(sensors),10)]
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures={executor.submit(client.query,chunk,label):chunk for chunk in chunks}
        for future,chunk in futures.items():
            try:
                rec=future.result()
                rows=check_snapshot_response(rec,chunk,ledger)
                # No new input during snapshot. Retry only an uncertain time boundary,
                # never replace an actual wrong payload with an unnoticed later pass.
                for _ in range(3):
                    uncertain=[x["sensor"] for x in rows if x["status"]=="INCONCLUSIVE"]
                    if not uncertain:
                        break
                    time.sleep(.2)
                    rec=client.query(uncertain,label+"_BOUNDARY_RETRY")
                    replacements={x["sensor"]:x for x in check_snapshot_response(rec,uncertain,ledger)}
                    rows=[replacements[x["sensor"]] if x["status"]=="INCONCLUSIVE" else x for x in rows]
                checks.extend(rows)
            except Exception as exc:
                checks.extend({"sensor":s,"status":"FAIL","reason":str(exc)} for s in chunk)
    keys=[x["sensor"] for x in checks]
    exact=set(keys)==set(sensors) and len(keys)==len(sensors)
    nonempty=sum(x.get("expected_left",{}).get("N_EVENTS",0)>0 for x in checks)
    result={"label":label,"utc":datetime.now(timezone.utc).isoformat(),"exact_keyset":exact,
            "nonempty_entities":nonempty,"checks":checks,
            "status":"FAIL" if not exact or any(x["status"]=="FAIL" for x in checks) else
                     "INCONCLUSIVE" if any(x["status"]=="INCONCLUSIVE" for x in checks) else "PASS"}
    append("online_load_snapshots.jsonl",result)
    return result


def produce_segment(client,label,seconds=1800,sensor_count=1000,per_sensor_tick=1,
                    workers=8,max_pending=64,interval_s=1.,drain_timeout_s=60.,progress=True):
    """Offer all logical ticks on schedule; bounded saturation is explicit shedding.

    interval_s is configurable solely so local fake-transport tests take milliseconds.
    Event timestamps always advance one logical second per tick.
    """
    start=time.monotonic()
    base=datetime.now(timezone.utc)-timedelta(seconds=1)
    ledger={DB+f"_{label}_S{i}":[] for i in range(sensor_count)}
    pending={}
    latencies=[]
    offered=sent=accepted=failed=shed=timed_out=0
    max_depth=0
    drift=[]
    failures=[]
    executor=ThreadPoolExecutor(max_workers=workers)

    def record(future):
        nonlocal accepted,failed,timed_out
        meta=pending.pop(future)
        try:
            rec=future.result()
            if rec.get("accepted"):
                accepted+=len(meta["rows"])
                latencies.append(rec["elapsed_s"])
                for row in meta["rows"]:
                    ledger[row["SENSOR_ID"]].append(row)
            else:
                failed+=len(meta["rows"])
                failures.append({"request_id":rec.get("request_id"),"http_status":rec.get("http_status"),
                                 "error":rec.get("error"),"count":len(meta["rows"]),"tick":meta["tick"]})
            append("online_load_batches.jsonl",{"segment":label,"tick":meta["tick"],"scheduled_mono":meta["scheduled"],
                "request_send_mono":rec.get("send_monotonic"),"request_end_mono":rec.get("receive_monotonic"),
                "accepted":rec.get("accepted",False),"records":len(meta["rows"]),"request_id":rec.get("request_id")})
        except Exception as exc:
            failed+=len(meta["rows"])
            failures.append({"count":len(meta["rows"]),"tick":meta["tick"],"error":str(exc)})

    try:
        for tick in range(seconds):
            scheduled=start+tick*interval_s
            time.sleep(max(0,scheduled-time.monotonic()))
            for future in list(pending):
                if future.done():
                    record(future)
            drift.append(max(0,time.monotonic()-scheduled))
            records=[]
            for repeat in range(per_sensor_tick):
                for sensor_index,sensor in enumerate(ledger):
                    value=None if (tick+sensor_index+repeat)%97==0 else (sensor_index%11)*.25+((tick+repeat)%13)-6
                    records.append(event(sensor,f"{label}_{tick}_{repeat}_{sensor_index}",value,
                        base+timedelta(seconds=tick,microseconds=repeat*int(1000000/per_sensor_tick))))
            offered+=len(records)
            for i in range(0,len(records),50):
                batch=records[i:i+50]
                for future in list(pending):
                    if future.done():
                        record(future)
                if len(pending)>=max_pending:
                    shed+=len(batch)
                    append("online_load_shed.jsonl",{"segment":label,"tick":tick,"event_ids":[r["EVENT_ID"] for r in batch]})
                    continue
                future=executor.submit(client.ingest,batch,f"LOAD_{label}_{tick}")
                pending[future]={"rows":batch,"tick":tick,"scheduled":scheduled}
                sent+=len(batch)
                max_depth=max(max_depth,len(pending))
            if tick and tick%60==0:
                # Only the recent window is needed for end-of-segment oracle checks.
                cutoff=base+timedelta(seconds=tick-150)
                for sensor in ledger:
                    ledger[sensor]=[r for r in ledger[sensor] if datetime.fromisoformat(r["EVENT_TS"].replace("Z","+00:00"))>=cutoff]
                if progress:
                    print("Online load",label,tick,"/",seconds,"accepted",accepted,"shed",shed,flush=True)
                append("online_load_progress.jsonl",{"segment":label,"tick":tick,"offered":offered,"sent":sent,"accepted":accepted,
                                                      "failed":failed,"shed":shed,"pending":len(pending)})
        production_end=time.monotonic()
        deadline=production_end+drain_timeout_s
        while pending and time.monotonic()<deadline:
            done,_=wait(list(pending),timeout=min(.2,max(0,deadline-time.monotonic())))
            for future in done:
                record(future)
        censored=[{"tick":m["tick"],"count":len(m["rows"]),"cancelled":f.cancel()} for f,m in pending.items()]
        timed_out=sum(x["count"] for x in censored)
        # Running HTTP requests already have finite 10s-connect/30s-read bounds.
        # Wait for those to finish before any correctness snapshot or cleanup.
        executor.shutdown(wait=True,cancel_futures=True)
        for future in list(pending):
            record(future)
        end=time.monotonic()
        elapsed=max(end-start,1e-9)
        offered_elapsed=max(production_end-start,interval_s*seconds)
        result={"segment":label,"status":"PASS" if failed==shed==timed_out==0 and accepted==offered else "FAILED_OR_CENSORED",
            "planned_records":seconds*sensor_count*per_sensor_tick,"offered_records":offered,"sent_records":sent,
            "accepted_records":accepted,"failed_or_ambiguous_records":failed,"shed_records":shed,
            "censored_at_drain_deadline_records":timed_out,"censored_requests":censored,
            "target_events_per_second":sensor_count*per_sensor_tick/interval_s,
            "offered_events_per_second":offered/offered_elapsed,"achieved_accepted_events_per_second":accepted/elapsed,
            "offer_elapsed_s":production_end-start,"total_elapsed_s":elapsed,"maximum_pending_requests":max_depth,
            "maximum_scheduler_drift_s":max(drift,default=0),"failures":failures,
            "ingest_response_s":{f"p{p}":sorted(latencies)[max(0,math.ceil(p/100*len(latencies))-1)] for p in [50,95,99]} if latencies else {}}
        dump(f"online_load_{label}.json",result)
        return result,ledger
    finally:
        executor.shutdown(wait=True,cancel_futures=True)


def run_workload(client,runs=3,seconds=1800,sensors=1000,idle_seconds=300,burst_seconds=30):
    stages=[]
    for index in range(runs+1):
        burst=index==runs
        label="BURST" if burst else f"R{index+1}"
        production,ledger=produce_segment(client,label,seconds=burst_seconds if burst else seconds,
                                          sensor_count=sensors,per_sensor_tick=10 if burst else 1)
        stages.append(production)
        if production["status"]!="PASS":
            result={"status":"FAILED_OR_CENSORED","stages":stages,
                "remaining_stages":"NOT RUN after capacity/transport failure; no resize or silent lower-rate retry"}
            dump("online_workload.json",result)
            return result
        check=snapshot(client,ledger,list(ledger),label+"_DRAINED")
        stages.append({"phase":"drained_correctness","status":check["status"],"nonempty_entities":check["nonempty_entities"]})
        if check["status"]!="PASS" or check["nonempty_entities"]!=sensors:
            result={"status":"FAILED_OR_CENSORED","stages":stages,
                "reason":"Not every entity had a verified nonempty rolling payload before expiry"}
            dump("online_workload.json",result)
            return result
        idle_start=time.monotonic()
        last=None
        for pause in [15,30,60,90,idle_seconds]:
            if pause>idle_seconds:
                continue
            time.sleep(max(0,idle_start+pause-time.monotonic()))
            last=snapshot(client,ledger,list(ledger),label+f"_IDLE_{pause}")
            stages.append({"phase":f"idle_{pause}","status":last["status"],"nonempty_entities":last["nonempty_entities"]})
        if last is None or last["status"]!="PASS" or last["nonempty_entities"]!=0:
            result={"status":"FAILED_OR_CENSORED","stages":stages,"reason":"Idle expiry did not verify"}
            dump("online_workload.json",result)
            return result
    result={"status":"PASS","stages":stages,"live_verification":"Describes this execution only; the original published trial could not provision the online service."}
    dump("online_workload.json",result)
    return result
