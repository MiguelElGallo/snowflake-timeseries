"""Local fake-transport tests; never contact Snowflake or claim live throughput."""
from datetime import datetime, timedelta, timezone
import time
from unittest.mock import patch
from concurrent.futures import Future

from common import dump
import online_load
import online_run


class FakeClient:
    def __init__(self,delay=0.,accepted=True):
        self.delay=delay
        self.accepted=accepted
        self.closed=False
        self.calls=[]

    def ingest(self,records,label):
        before=time.monotonic()
        self.calls.append((records,label))
        time.sleep(self.delay)
        after=time.monotonic()
        return {"accepted":self.accepted,"elapsed_s":after-before,"send_monotonic":before,
                "receive_monotonic":after,"request_id":"FAKE_ONLY","http_status":200 if self.accepted else 207}

    def close(self):
        self.closed=True


class ImmediateExecutor:
    def __init__(self,**kwargs):
        pass
    def submit(self,fn,*args,**kwargs):
        future=Future()
        try:
            future.set_result(fn(*args,**kwargs))
        except Exception as exc:
            future.set_exception(exc)
        return future
    def shutdown(self,**kwargs):
        pass


def main():
    checks=[]
    def check(name,actual,expected):
        checks.append({"name":name,"status":"PASS" if actual==expected else "FAIL","actual":actual,"expected":expected})
    # Keep synthetic scheduler logs out of the actual experiment's evidence files.
    with patch.object(online_load,"append"),patch.object(online_load,"dump"):
        good,ledger=online_load.produce_segment(FakeClient(.001),"LOCAL_GOOD",seconds=4,sensor_count=4,interval_s=.01,progress=False)
        check("scheduler_offers_all_ticks",good["offered_records"],16)
        check("scheduler_accepts_all_fast_batches",good["accepted_records"],16)
        check("scheduler_success_status",good["status"],"PASS")
        check("scheduler_exact_ledger_count",sum(len(v) for v in ledger.values()),16)
        slow,_=online_load.produce_segment(FakeClient(.05),"LOCAL_SHED",seconds=5,sensor_count=4,interval_s=.005,max_pending=1,workers=1,progress=False)
        check("bounded_queue",slow["maximum_pending_requests"],1)
        check("shedding_is_recorded",slow["shed_records"]>0,True)
        check("shedding_cannot_pass",slow["status"],"FAILED_OR_CENSORED")
        check("offered_accounting",slow["sent_records"]+slow["shed_records"],slow["offered_records"])
        timed,_=online_load.produce_segment(FakeClient(.04),"LOCAL_TIMEOUT",seconds=1,sensor_count=4,interval_s=.01,drain_timeout_s=.001,progress=False)
        check("deadline_censors_inflight",timed["censored_at_drain_deadline_records"],4)
        check("late_success_cannot_erase_censor",timed["status"],"FAILED_OR_CENSORED")
        denied,_=online_load.produce_segment(FakeClient(accepted=False),"LOCAL_REJECT",seconds=1,sensor_count=4,interval_s=.01,progress=False)
        check("rejected_not_accepted",denied["accepted_records"],0)
        check("rejected_count_recorded",denied["failed_or_ambiguous_records"],4)
        check("rejected_cannot_pass",denied["status"],"FAILED_OR_CENSORED")
        with patch.object(online_load,"ThreadPoolExecutor",ImmediateExecutor):
            burst,_=online_load.produce_segment(FakeClient(),"LOCAL_FAST_BURST",seconds=1,sensor_count=1000,
                per_sensor_tick=10,max_pending=1,interval_s=.01,progress=False)
        check("completed_requests_do_not_consume_capacity",burst["shed_records"],0)
        check("fast_burst_all_records_accepted",burst["accepted_records"],10000)
    now=datetime.now(timezone.utc)
    records=[online_run.event("S",str(i),v,now-timedelta(seconds=5+i)) for i,v in enumerate([1,2,3])]
    rec={"valid":True,"values":{"S":online_run.oracle(records)},"statuses":{"S":{k:"PRESENT" for k in online_run.FEATURES}},
         "asof_lower_utc":(now-timedelta(milliseconds=10)).isoformat(),"asof_upper_utc":(now+timedelta(milliseconds=10)).isoformat()}
    check("snapshot_full_payload",online_load.check_snapshot_response(rec,["S"],{"S":records})[0]["status"],"PASS")
    bad=dict(rec,values={"S":dict(rec["values"]["S"],MEAN=99.)})
    check("snapshot_wrong_average_fails",online_load.check_snapshot_response(bad,["S"],{"S":records})[0]["status"],"FAIL")
    check("snapshot_missing_key_fails",online_load.check_snapshot_response(rec,["S","MISSING"],{"S":records})[0]["status"],"FAIL")
    boundary=[online_run.event("S","BOUNDARY",1.,now-timedelta(seconds=60))]
    uncertain=dict(rec,values={"S":online_run.oracle(boundary)})
    check("time_bracket_boundary_is_inconclusive",online_load.check_snapshot_response(uncertain,["S"],{"S":boundary})[0]["status"],"INCONCLUSIVE")
    absent=dict(rec,values={"S":{k:None for k in online_run.FEATURES}},statuses={"S":{k:"MISSING_DATA" for k in online_run.FEATURES}})
    check("expired_missing_representation",online_load.check_snapshot_response(absent,["S"],{"S":[]})[0]["status"],"EMPTY_MISSING_DATA")
    a_records=[dict(r,SENSOR_ID="A") for r in records]
    b_records=[dict(r,SENSOR_ID="B") for r in boundary]
    class MixedSnapshotClient:
        def query(self,sensors,label):
            if sensors==["A","B"]:
                return {"valid":True,"values":{"A":dict(online_run.oracle(a_records),MEAN=99.),"B":online_run.oracle(b_records)},
                    "statuses":{s:{k:"PRESENT" for k in online_run.FEATURES} for s in sensors},
                    "asof_lower_utc":rec["asof_lower_utc"],"asof_upper_utc":rec["asof_upper_utc"]}
            if sensors!=["B"]:
                raise AssertionError("Only inconclusive B may be retried; failed A must remain sticky")
            return {"valid":True,"values":{"B":{k:None for k in online_run.FEATURES}},
                    "statuses":{"B":{k:"MISSING_DATA" for k in online_run.FEATURES}},
                    "asof_lower_utc":(now+timedelta(seconds=1)).isoformat(),"asof_upper_utc":(now+timedelta(seconds=1.1)).isoformat()}
    with patch.object(online_load,"append"),patch.object(online_load.time,"sleep"):
        sticky=online_load.snapshot(MixedSnapshotClient(),{"A":a_records,"B":b_records},["A","B"],"LOCAL_STICKY_FAILURE")
    check("mixed_snapshot_failure_is_sticky",sticky["status"],"FAIL")
    check("mixed_snapshot_retains_original_wrong_value",next(x for x in sticky["checks"] if x["sensor"]=="A")["actual"]["MEAN"],99.)
    original=FakeClient()
    rejected_replay=FakeClient(accepted=False)
    with patch.object(online_run,"Client",return_value=rejected_replay) as constructor,patch.object(online_run,"await_exact",return_value={"status":"PASS"}) as assertion,patch.object(online_run,"dump"):
        replay=online_run.fresh_client_replay(None,None,original)
        check("fresh_client_recreated",constructor.call_count,1)
        check("fresh_client_closed",original.closed and rejected_replay.closed,True)
        check("rejected_replay_does_not_assert_old_state",assertion.call_count,1)
        check("rejected_replay_gate",replay["status"],"INGEST_FAILED")
        check("replay_contains_fresh_marker",len(rejected_replay.calls[0][0]),3)
    result={"execution":"LOCAL_FAKE_TRANSPORT_ONLY","checks":checks,"passed":sum(c["status"]=="PASS" for c in checks),
            "failed":sum(c["status"]=="FAIL" for c in checks),"limitation":"No real REST requests, online aggregates or throughput were tested."}
    dump("online_local_control_checks.json",result)
    print(f"Local scheduler/replay controls: {result['passed']} passed, {result['failed']} failed")
    if result["failed"]:
        raise SystemExit(1)


if __name__=="__main__":
    main()
