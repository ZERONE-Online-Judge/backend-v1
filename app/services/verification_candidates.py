"""Server-owned candidate execution and assessments, independent of model claims."""

import copy
import hashlib
import json

from app.services import verification_ai as ai, verification_agent_tools as caps


def probes(runs):
    unique = {}
    for run in runs:
        if run.get("probe"):
            value = {k: run["probe"][k] for k in ("input", "expected_output")}
            unique[ai.digest(value)] = value
    return unique


def assessment(row, artifact, runs):
    checksum = hashlib.sha256(artifact["source"].encode()).hexdigest()
    matching = [
        r
        for r in runs
        if r.get("source_sha256") == checksum
        and r.get("language") == artifact["language"]
        and r.get("context_hash") == row.context_hash
    ]
    full = [r for r in matching if r["scope"] == "all" and r["testcase_count"] > 0]
    done = [r for r in matching if r["status"] not in ai.PENDING]
    failed = [
        r
        for r in done
        if r["scope"] != "probe" and r["status"] not in {"accepted", "system_error"}
    ]
    conflicts = [r for r in done if r["scope"] == "probe" and r["status"] != "accepted"]
    missing = set(probes(runs)) - set(probes(done))
    result = {
        "status": "unverified",
        "message": "실제 채점기로 전체 등록 테스트를 검증하지 않은 수정 후보입니다.",
        "source_sha256": checksum,
        "registered_passed": any(r["status"] == "accepted" for r in full),
        "testcase_count": max((r["testcase_count"] for r in full), default=0),
        "execution_ids": [r["submission_id"] for r in matching],
        "unreplayed_probes": len(missing),
        "probe_conflicts": len(conflicts),
    }
    if failed:
        result.update(
            status="failed",
            message="등록 테스트 실행에서 실패한 수정 후보입니다. 수정 후 다시 검증해야 합니다.",
        )
    elif any(r["status"] in ai.PENDING for r in matching):
        result.update(
            status="pending",
            message="실제 채점 결과를 기다리고 있습니다. 아직 검증이 완료되지 않았습니다.",
        )
    elif result["registered_passed"]:
        if conflicts or missing or any(r["status"] == "system_error" for r in matching):
            result.update(
                status="inconclusive",
                message="전체 등록 테스트는 통과했지만 반례 결과·미실행 범위 또는 실행 오류가 남아 추가 확인이 필요합니다. 제안 반례의 입력과 기대 출력도 독립 검토가 필요합니다.",
            )
        else:
            result.update(
                status="passed",
                message=f"저장된 동일 코드로 전체 등록 테스트 {result['testcase_count']}개 통과를 확인했습니다. 모든 입력에 대한 정답 증명은 아닙니다.",
            )
    elif done:
        result.update(
            status="inconclusive",
            message="일부 실행 기록만 있거나 실행 오류가 있어 전체 등록 테스트 통과를 확인하지 못했습니다.",
        )
    return result


def validate(row, context, state, files, call, create):
    # Persist on the pending call: polling/restarting must not create another candidate.
    if "candidate_result" not in call:
        call["candidate_result"] = create()
        args = json.loads(call.get("arguments", "{}"))
        purpose = (
            "comparison"
            if state.get("investigation_focus") == "unexpected_acceptance"
            else args.get("purpose", "repair")
        )
        state["artifacts"][call["candidate_result"]["artifact_id"]]["purpose"] = purpose
    saved = call["candidate_result"]
    aid = saved["artifact_id"]
    with ai.SessionLocal() as db:
        runs = caps.results(db, row.analysis_id)
    if "candidate_probes" not in call:
        call["candidate_probes"] = list(probes(runs).values())
    outputs = []
    error = None
    for probe in [None, *call["candidate_probes"]]:
        state["phase"] = (
            "수정 후보 반례 재검증" if probe else "수정 후보 전체 테스트 검증"
        )
        try:
            result = caps.run_code(row, state, files, context, aid, [], probe)
        except caps.ToolError as exc:
            error = str(exc)
            break
        if result.get("waiting_for_capacity") or result.get("status") in ai.PENDING:
            return None
        outputs.append(result)
        # A failed full suite already disproves acceptance; let the model repair it.
        if probe is None and result["status"] != "accepted":
            break
    with ai.SessionLocal() as db:
        runs = caps.results(db, row.analysis_id)
    verdict = assessment(row, state["artifacts"][aid], runs)
    state["phase"] = "수정 후보 실행 결과 검토"
    return {
        **saved,
        "verification": verdict,
        "executions": outputs,
        "execution_error": error,
        "next": "실제 실행 결과를 검토하세요. 실패하면 원인을 수정해 새 후보로 재검증하세요. 전체 등록 테스트 통과도 정답의 증명은 아닙니다. 동일 코드·범위 재요청은 저장된 결과를 사용합니다.",
    }


def guard_report(row, state, report, runs):
    """Bind displayed claims to exact saved code; prose snippets are never certified."""
    report = copy.deepcopy(report)
    artifacts = state.get("artifacts", {})
    assessments = {aid: assessment(row, item, runs) for aid, item in artifacts.items()}
    for fix in report["fixes"]:
        source = fix.get("code_example", "")
        match = next(
            (aid for aid, item in artifacts.items() if source == item["source"]), None
        )
        fix["verification"] = (
            assessments[match]["message"]
            if match
            else "이 설명·코드 조각 자체를 실행해 검증한 기록은 없습니다. 실제 실행 여부와 판정은 아래 저장된 수정 후보의 검증 상태를 확인하세요."
        )
    repairs = [
        value
        for aid, value in assessments.items()
        if artifacts[aid].get("purpose") != "comparison"
    ]
    for rec in report.get("recommendations", []):
        if rec["target"] == "solution":
            verified = assessments.get(rec.get("artifact_id"))
            rec["verification"] = (
                verified["message"]
                if verified
                else "실제 실행된 수정 후보와 연결되지 않은 제안입니다. 검증 완료로 간주하지 마세요."
            )
    if repairs:
        latest = repairs[-1]
        report["verdict_assessment"] = "수정 후보 실행 검증: " + latest["message"]
        if latest["status"] != "passed":
            report["summary"] = (
                "최종 수정 후보는 추가 검토가 필요합니다. " + latest["message"]
            )
    for aid, value in assessments.items():
        note = f"{aid}: {value['message']}"
        if note not in report["limitations"]:
            report["limitations"].append(note)
    return report
