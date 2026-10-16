"""Measure public fixtures through the real queue, with one timed job at a time.

Run inside the deployed API container. Uses a new private draft contest and
removes only its own rows and objects after exporting all evidence. Never
changes node routing, host settings, or existing contest data.
"""
import argparse
import hashlib
import json
import logging
import time
from datetime import timedelta
from pathlib import Path

from sqlalchemy import delete, func, select

from app.database import SessionLocal
from app.models import new_id, now_utc
from app.orm_models import (
    ContestRow, ContestDivisionRow, ProblemRow, TestcaseSetRow, TestcaseRow,
    SubmissionRow, JudgeJobRow, JudgeNodeRow, EditorJudgeRunRow,
    EditorExecutionTaskRow,
)
from app.services.storage import object_storage
from app.services.store import store

LOG = logging.getLogger("serial-benchmark")


def save(path, data):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")
    tmp.replace(path)


def nodes(db):
    cutoff = now_utc() - timedelta(seconds=30)
    return db.scalars(select(JudgeNodeRow).where(
        JudgeNodeRow.schedulable.is_(True),
        JudgeNodeRow.last_heartbeat_at >= cutoff,
    )).all()


def activity(contest_id):
    with SessionLocal() as db:
        regular = db.scalar(select(func.count()).select_from(JudgeJobRow).where(
            JudgeJobRow.status.in_(["pending", "running"]),
            JudgeJobRow.contest_id != contest_id,
        ))
        editor = sum(db.scalar(select(func.count()).select_from(model).where(
            model.status.in_(states))) for model, states in [
                (EditorJudgeRunRow, ["waiting", "preparing", "judging"]),
                (EditorExecutionTaskRow, ["waiting", "running"]),
            ])
        # Node heartbeats can retain our completed job count throughout a
        # 20-second long poll. Authoritative active queue rows define overlap.
        return regular + editor


def idle(contest_id, deadline):
    quiet_since = None
    last_notice = 0.0
    while time.monotonic() < deadline:
        count = activity(contest_id)
        if not count:
            quiet_since = quiet_since or time.monotonic()
            if time.monotonic() - quiet_since >= 5:
                return
        else:
            quiet_since = None
        if time.monotonic() - last_notice >= 30:
            LOG.info("Waiting for five idle seconds; other queued/running work=%s", count)
            last_notice = time.monotonic()
        time.sleep(1)
    raise TimeoutError("No uncontended measurement window; existing work was not cancelled")


def setup(fixtures, state):
    cid, did = new_id(), new_id()
    state.update(contest_id=cid, division_id=did, storage_keys=[], problem_ids={})
    with SessionLocal() as db:
        db.add(ContestRow(contest_id=cid, title="Internal 2200MHz serial benchmark " + cid[:8],
            organization_name="ZOJ internal calibration", overview="Temporary public-fixture benchmark",
            status="draft", visibility="private", visibility_after_end="private",
            start_at=now_utc()+timedelta(days=1), end_at=now_utc()+timedelta(days=2),
            freeze_at=now_utc()+timedelta(days=2)))
        db.flush()
        db.add(ContestDivisionRow(division_id=did, contest_id=cid, code="benchmark", name="Internal benchmark"))
        db.flush()
        for index, case in enumerate(fixtures["cases"]):
            key = case["family"] + ":" + case["id"]
            pid, sid = new_id(), new_id()
            state["problem_ids"][key] = pid
            db.add(ProblemRow(problem_id=pid, contest_id=cid, division_id=did,
                problem_code=str(index+1), title=key, statement="Public benchmark fixture",
                display_order=index+1, time_limit_ms=120000, memory_limit_mb=512,
                language_resource_limits={lang:{"time_limit_ms":120000,"memory_limit_mb":512}
                                          for lang in ("c99","cpp17","python313","java8")}))
            db.flush()
            db.add(TestcaseSetRow(testcase_set_id=sid, problem_id=pid, version=1, is_active=True))
            db.flush()
            refs=[]
            for suffix, body in [("in",case["input"]),("out",case["output"])]:
                path=f"contests/{cid}/serial-benchmark/{index}.{suffix}"
                object_storage.write_bytes(path,body.encode())
                state["storage_keys"].append(path)
                refs.append(path)
            db.add(TestcaseRow(testcase_set_id=sid, display_order=1,
                input_storage_key=refs[0],output_storage_key=refs[1],
                input_sha256=hashlib.sha256(case["input"].encode()).hexdigest(),
                output_sha256=hashlib.sha256(case["output"].encode()).hexdigest()))
        db.commit()


def measure(case, repeat, state, deadline):
    idle(state["contest_id"], deadline)
    pid=state["problem_ids"][case["family"]+":"+case["id"]]
    submission=store.create_mock_submission(state["contest_id"],pid,case["language"],case["source"])
    max_other=0
    end=min(deadline,time.monotonic()+240)
    while time.monotonic()<end:
        with SessionLocal() as db:
            row, job = db.execute(select(SubmissionRow, JudgeJobRow)
                .join(JudgeJobRow, JudgeJobRow.submission_id == SubmissionRow.submission_id)
                .where(SubmissionRow.submission_id == submission.submission_id)).one()
            node=db.get(JudgeNodeRow,job.assigned_node_id) if job.assigned_node_id else None
            if job.status not in {"pending","running"}:
                result={"condition":"serial","testcases":1,"repeat":repeat,
                    "submission_id":row.submission_id,"status":row.status,
                    "runtime_ms":row.runtime_ms,"memory_kb":row.memory_kb,
                    "node":node.node_name if node else None,
                    "agent_version":node.agent_version if node else None,
                    "submitted_at":row.submitted_at.isoformat(),
                    "finished_at":row.status_updated_at.isoformat(),
                    "other_jobs_observed":max_other}
                if row.status!="accepted":
                    result["judge_message"]=row.judge_message
                    result["compile_message"]=row.compile_message
                return result
        max_other=max(max_other,activity(state["contest_id"]))
        time.sleep(0.5)
    raise TimeoutError("Synthetic submission did not finish within its bounded wait")


def cleanup(state):
    cid=state.get("contest_id")
    if not cid:return
    # Never delete a job being executed. Pending synthetic work can be removed.
    deadline=time.monotonic()+240
    while time.monotonic()<deadline:
        with SessionLocal() as db:
            running=db.scalar(select(func.count()).select_from(JudgeJobRow).where(
                JudgeJobRow.contest_id==cid,JudgeJobRow.status=="running"))
        if not running:break
        time.sleep(2)
    if running:raise RuntimeError("Synthetic job remains running; keep scoped checkpoint for cleanup")
    with SessionLocal() as db:
        contest=db.get(ContestRow,cid)
        if contest and not (contest.title=="Internal 2200MHz serial benchmark "+cid[:8]
                            and contest.visibility=="private" and contest.status=="draft"):
            raise RuntimeError("Cleanup ownership guard failed")
        problems=select(ProblemRow.problem_id).where(ProblemRow.contest_id==cid)
        sets=select(TestcaseSetRow.testcase_set_id).where(TestcaseSetRow.problem_id.in_(problems))
        db.execute(delete(JudgeJobRow).where(JudgeJobRow.contest_id==cid))
        db.execute(delete(SubmissionRow).where(SubmissionRow.contest_id==cid))
        db.execute(delete(TestcaseRow).where(TestcaseRow.testcase_set_id.in_(sets)))
        db.execute(delete(TestcaseSetRow).where(TestcaseSetRow.problem_id.in_(problems)))
        db.execute(delete(ProblemRow).where(ProblemRow.contest_id==cid))
        db.execute(delete(ContestDivisionRow).where(ContestDivisionRow.contest_id==cid))
        db.execute(delete(ContestRow).where(ContestRow.contest_id==cid))
        db.commit()
    for key in state.get("storage_keys",[]):
        if not key.startswith(f"contests/{cid}/serial-benchmark/"):
            raise RuntimeError("Object cleanup ownership guard failed")
        object_storage.delete(key)
    state["cleaned_up"]=True


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory",type=Path)
    parser.add_argument("--cleanup-only",action="store_true")
    parser.add_argument("--resume-from",type=Path)
    args=parser.parse_args();folder=args.directory
    logging.basicConfig(filename=folder/"progress.log",level=logging.INFO,
                        format="%(asctime)s %(message)s",force=True)
    state_path=folder/"state.json"
    if args.cleanup_only:
        state=json.loads(state_path.read_text());cleanup(state);save(state_path,state);return
    if state_path.exists():raise RuntimeError("Existing run checkpoint; do not restart blindly")
    state={"started_at":now_utc().isoformat(),"clock_policy":{
        "target_mhz":2200,"source":"service-owner-confirmed host policy",
        "host_policy_independently_verified":False},"runs":[],"discarded_runs":[]}
    fixtures=json.loads((folder/"fixtures.json").read_text())
    if args.resume_from:
        previous=json.loads(args.resume_from.read_text())
        assert previous.get("cleaned_up"), "Clean the old scoped contest before resuming"
        assert json.loads(args.resume_from.with_name("fixtures.json").read_text()) == fixtures
        assert all(r["status"] == "accepted" and not r["other_jobs_observed"] for r in previous["runs"])
        state.update(started_at=previous["started_at"],runs=previous["runs"],
                     discarded_runs=previous.get("discarded_runs",[]))
    completed={(r["family"],r["case_id"],r["repeat"]) for r in state["runs"]}
    total=sum(c["repeats"] for c in fixtures["cases"])
    deadline=time.monotonic()+3600
    try:
        with SessionLocal() as db:
            state["nodes_at_start"]=[{"node":n.node_name,"slots":n.total_slots,
                "agent_version":n.agent_version} for n in nodes(db)]
        idle("not-created",deadline)
        setup(fixtures,state);save(state_path,state)
        for case in fixtures["cases"]:
            for repeat in range(1,case["repeats"]+1):
                if (case["family"],case["id"],repeat) in completed:
                    continue
                for attempt in range(3):
                    result=measure(case,repeat,state,deadline)
                    result.update(case_id=case["id"],family=case["family"],attempt=attempt+1)
                    if result["status"]!="accepted" or result["runtime_ms"] is None:
                        state["discarded_runs"].append(result);save(state_path,state)
                        raise RuntimeError(f"Fixture {case['id']} did not pass: {result['status']}")
                    if result["other_jobs_observed"]:
                        state["discarded_runs"].append(result);save(state_path,state)
                        LOG.info("Discard contaminated run: %s",case["id"])
                        continue
                    state["runs"].append(result);save(state_path,state)
                    LOG.info("%s %s repeat %s: %sms %sKB %s (%s/%s)",case["family"],case["id"],
                        repeat,result["runtime_ms"],result["memory_kb"],result["node"],len(state["runs"]),total)
                    break
                else:raise RuntimeError("Repeated background traffic; stop instead of publishing mixed conditions")
        state["finished_at"]=now_utc().isoformat();state["status"]="completed"
    except Exception as error:
        state["status"]="failed";state["error"]=str(error);LOG.exception("Benchmark stopped")
    finally:
        try:cleanup(state)
        except Exception as error:state["cleanup_error"]=str(error);LOG.exception("Scoped cleanup incomplete")
        save(state_path,state)


if __name__=="__main__":main()
