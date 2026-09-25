"""Situation-aware investigation reports and server-owned evidence checks."""

from typing import Literal

from app.services import verification_ai as ai, verification_agent_tools as caps


class Section(ai.StrictModel):
    title: str
    body: str
    evidence_refs: list[str]


class Recommendation(ai.StrictModel):
    target: Literal[
        "testcases",
        "expectation",
        "solution",
        "judge",
        "infrastructure",
        "investigation",
    ]
    title: str
    action: str
    verification: str
    artifact_id: str
    evidence_refs: list[str]


class Report(ai.StrictModel):
    summary: str
    verdict_assessment: str
    conclusion: Literal[
        "test_gap",
        "expectation_error",
        "solution_error",
        "judge_issue",
        "infrastructure_issue",
        "inconclusive",
    ]
    sections: list[Section]
    recommendations: list[Recommendation]
    suggested_tests: list[ai.Counterexample]
    limitations: list[str]


def focus(evidence):
    if evidence.get("mode") == "task":
        return "user_goal"
    if evidence.get("actual_status") in {"system_error", "compile_error"}:
        return "execution_failure"
    if (
        evidence.get("actual_status") == "accepted"
        and evidence.get("expected_status") != "accepted"
    ):
        return "unexpected_acceptance"
    return "verdict_mismatch"


def normalize(report):
    if "sections" not in report:
        return ai.Report.model_validate(report).model_dump()
    source = (
        {k: v for k, v in report.items() if k not in {"report_kind", "causes", "fixes"}}
        if report.get("report_kind") == "investigation"
        else report
    )
    result = Report.model_validate(source).model_dump()
    result.update(report_kind="investigation", causes=[], fixes=[])
    return result


def validate_finish(row, state, report, runs):
    if "sections" not in report or state.get("finalizing"):
        return
    Report.model_validate(report)
    if focus(row.evidence) == "unexpected_acceptance" and any(
        r["target"] == "solution" for r in report["recommendations"]
    ):
        raise caps.ToolError(
            "오답 의도 코드가 통과한 조사는 원본을 고치는 작업이 아닙니다. 테스트 보강·기대 판정 재검토·채점 기준 조사 중 근거에 맞는 조치를 제안하세요. 대조용 풀이가 필요하면 실험용으로만 사용하세요."
        )
    known = {f["file_id"] for f in state.get("files_read", [])}
    known.update(
        "trial:" + r["submission_id"] for r in runs if r["status"] not in ai.PENDING
    )
    known.update(
        "experiment:" + r["request_id"] for r in state.get("playground_runs", [])
    )
    known.update("probe-check:" + r["check_id"] for r in state.get("probe_checks", []))
    for entry in [*report["sections"], *report["recommendations"]]:
        if any(ref not in known for ref in entry["evidence_refs"]):
            raise caps.ToolError(
                "읽은 파일 또는 완료된 실행 ID만 보고서 근거로 사용하세요."
            )
    if report["conclusion"] == "test_gap" and not supported_gap(state, runs):
        raise caps.ToolError(
            "테스트 누락을 확정할 근거가 부족합니다. 조사 대상 원본(자유 작업은 선택한 등록 풀이)의 전체 테스트 통과, 같은 코드의 반례 실패, check_probe의 등록 validator·참조 풀이 확인이 필요합니다. 자료가 없으면 inconclusive로 보고하고 필요한 추가 검증을 설명하세요."
        )


def supported_gap(state, runs):
    # Free tasks may select a registered source later through list_files.
    # Its full/probe executions must still belong to the very same artifact.
    passed = {
        r["artifact_id"]
        for r in runs
        if (
            r["artifact_id"] == "original"
            or (state.get("task_goal") and r["artifact_id"].startswith("asset:"))
        )
        and r["scope"] == "all"
        and r["status"] == "accepted"
    }
    if not passed:
        return False
    for check in state.get("probe_checks", []):
        if check.get("status") != "cross_checked":
            continue
        if any(
            r["artifact_id"] in passed
            and r["status"] == "wrong_answer"
            and r.get("probe")
            and ai.digest({k: r["probe"][k] for k in ("input", "expected_output")})
            == check["probe_hash"]
            for r in runs
        ):
            return True
    return False


def guard(row, state, report, runs):
    """A budget stop may bypass the normal finish gate, never the evidence label."""
    if report.get("conclusion") == "test_gap" and not supported_gap(state, runs):
        report["conclusion"] = "inconclusive"
        report["summary"] = (
            "테스트 누락 여부는 아직 확정하지 못했습니다. 제안 입력과 기대 출력의 교차 검증 또는 원본 재현 근거가 부족합니다."
        )
        report["verdict_assessment"] = (
            "제안 반례의 실패만으로 공식 테스트가 부족하다고 단정할 수 없습니다."
        )
    if focus(row.evidence) == "unexpected_acceptance":
        report["recommendations"] = [
            r for r in report.get("recommendations", []) if r["target"] != "solution"
        ]
    return report
