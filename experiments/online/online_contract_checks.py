"""Local checks against documented REST response shapes; no network or SQL."""
from copy import deepcopy

from common import dump
from online_run import FEATURES, FV_NAME, SOURCE, VERSION, assess_ingest, assess_query, matches


def main():
    results=[]
    good_ingest={"http_status":200,"response":{"status":"success","sources":{SOURCE:{
        "records":{"total":2,"failed":0},"feature_views":{"failed":0,"results":[
            {"name":FV_NAME,"version":VERSION,"status":"success"}]}}}}}
    good_query={"http_status":200,"response":{"status":"success","metadata":{"features":[{"name":x} for x in FEATURES]},
        "results":[{"features":[2,2,2.,1.,1.],"status":["PRESENT"]*5}]}}
    def check(name,actual,expected):
        results.append({"name":name,"status":"PASS" if actual==expected else "FAIL","actual":actual,"expected":expected})
    check("ingest_success",assess_ingest(good_ingest,2),True)
    partial=deepcopy(good_ingest);partial["http_status"]=207;partial["response"]["status"]="partial_success"
    check("ingest_207_is_not_success",assess_ingest(partial,2),False)
    failed=deepcopy(good_ingest);failed["response"]["sources"][SOURCE]["feature_views"]["failed"]=1
    check("feature_persistence_failure",assess_ingest(failed,2),False)
    rejected=deepcopy(good_ingest);rejected["response"]["sources"][SOURCE]["records"]["failed"]=1
    check("record_failure",assess_ingest(rejected,2),False)
    wrong_count=deepcopy(good_ingest);wrong_count["response"]["sources"][SOURCE]["records"]["total"]=1
    check("ingest_wrong_count",assess_ingest(wrong_count,2),False)
    wrong_fv=deepcopy(good_ingest);wrong_fv["response"]["sources"][SOURCE]["feature_views"]["results"][0]["name"]="OTHER"
    check("ingest_wrong_feature_view",assess_ingest(wrong_fv,2),False)
    check("query_success",assess_query(deepcopy(good_query),["A"])["valid"],True)
    missing=deepcopy(good_query);missing["response"]["results"][0]={"features":[None]*5,"status":["MISSING_DATA"]*5}
    checked=assess_query(missing,["ABSENT"])
    check("missing_data_is_valid_response",checked["valid"],True)
    check("missing_data_is_not_numeric_success",matches(checked["values"]["ABSENT"],dict(zip(FEATURES,[2,2,2.,1.,1.]))),False)
    present_null=deepcopy(good_query)
    present_null["response"]["results"][0]["features"]=[2,0,None,None,None]
    check("present_null_is_allowed_by_contract",assess_query(present_null,["NULL_SENSOR"])["valid"],True)
    for name,mutate in [
        ("missing_feature_metadata",lambda x:x["response"]["metadata"]["features"].pop()),
        ("duplicate_feature_metadata",lambda x:x["response"]["metadata"]["features"].__setitem__(0,{"name":FEATURES[1]})),
        ("wrong_row_count",lambda x:x["response"]["results"].append(deepcopy(x["response"]["results"][0]))),
        ("timeout_status",lambda x:x["response"]["results"][0]["status"].__setitem__(0,"TIME_OUT")),
        ("status_value_contradiction",lambda x:x["response"]["results"][0]["status"].__setitem__(0,"MISSING_DATA")),
        ("query_207",lambda x:x.__setitem__("http_status",207)),
    ]:
        value=deepcopy(good_query);mutate(value)
        check(name,assess_query(value,["A"])["valid"],False)
    two=deepcopy(good_query);two["response"]["results"].append({"features":[1,1,7.,0.,0.],"status":["PRESENT"]*5})
    parsed=assess_query(two,["A","B"])
    check("positional_entity_mapping",parsed["values"]["B"]["MEAN"],7.)
    check("large_mean_absolute_tolerance",matches({"MEAN":1e12+1},{"MEAN":1e12}),False)
    result={"execution":"LOCAL_RESPONSE_CONTRACT_CHECKS_ONLY","checks":results,
            "passed":sum(x["status"]=="PASS" for x in results),"failed":sum(x["status"]=="FAIL" for x in results)}
    dump("online_local_contract_checks.json",result)
    print(f"Local REST contract checks: {result['passed']} passed, {result['failed']} failed")
    if result["failed"]:
        raise SystemExit(1)


if __name__=="__main__":
    main()
