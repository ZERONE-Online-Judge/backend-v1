import base64
import copy
import json
from types import SimpleNamespace

import pytest
from app.services import verification_agent as agent, verification_ai as ai
from app.services import verification_investigation as investigation
from app.services import verification_probe_checks as checks
from app.services import verification_agent_context as memory
from app.services.verification_agent_tools import ToolError
from app.settings import Settings
from test_verification_agent import agent_context, queued, call, judged
from test_verification_ai import context, client, SubmissionStatus

REPORT = dict(
    summary="등록 테스트와 반례를 비교했습니다.",
    verdict_assessment="테스트 보강 검토",
    conclusion="test_gap",
    sections=[
        dict(
            title="재현 근거", body="원본을 실제 실행했습니다. $a+b$", evidence_refs=[]
        )
    ],
    recommendations=[
        dict(
            target="testcases",
            title="경계 테스트 추가",
            action="검증한 입력을 등록하세요.",
            verification="원본 WA·참조 풀이 일치",
            artifact_id="",
            evidence_refs=[],
        )
    ],
    suggested_tests=[],
    limitations=[],
)


def gap():
    probe = {"input": "2 3\n", "expected_output": "5\n"}
    runs = [
        dict(
            artifact_id="original", scope="all", status="accepted", submission_id="all"
        ),
        dict(
            artifact_id="original",
            scope="probe",
            status="wrong_answer",
            submission_id="probe",
            probe=probe,
        ),
    ]
    state = {
        "probe_checks": [
            dict(check_id="c", status="cross_checked", probe_hash=ai.digest(probe))
        ]
    }
    row = SimpleNamespace(
        evidence={"expected_status": "wrong_answer", "actual_status": "accepted"}
    )
    return row, state, runs


def test_test_gap_requires_matching_executed_original_and_cross_checked_probe():
    row, state, runs = gap()
    investigation.validate_finish(row, state, REPORT, runs)
    for invalid in [
        [],
        runs[:1],
        [{**runs[0], "scope": "selected"}, runs[1]],
        [runs[0], {**runs[1], "artifact_id": "candidate-1"}],
        [runs[0], {**runs[1], "probe": {"input": "2 4\n", "expected_output": "6\n"}}],
    ]:
        with pytest.raises(ToolError, match="근거가 부족"):
            investigation.validate_finish(row, state, REPORT, invalid)
    for status in ["incomplete", "conflict"]:
        state["probe_checks"][0]["status"] = status
        with pytest.raises(ToolError, match="근거가 부족"):
            investigation.validate_finish(row, state, REPORT, runs)


def test_wrong_intent_accepted_does_not_request_source_repair_or_invent_references():
    row, state, runs = gap()
    report = copy.deepcopy(REPORT)
    report["recommendations"][0]["target"] = "solution"
    with pytest.raises(ToolError, match="원본을 고치는"):
        investigation.validate_finish(row, state, report, runs)
    report = copy.deepcopy(REPORT)
    report["sections"][0]["evidence_refs"] = ["trial:not-executed"]
    with pytest.raises(ToolError, match="완료된 실행 ID"):
        investigation.validate_finish(row, state, report, runs)
    report["sections"][0]["evidence_refs"] = ["trial:all", "probe-check:c"]
    investigation.validate_finish(row, state, report, runs)


def test_budget_stop_never_certifies_unchecked_test_gap(agent_context, monkeypatch):
    _, row, ctx, state = queued(agent_context)
    row.evidence = {
        **row.evidence,
        "expected_status": "wrong_answer",
        "actual_status": "accepted",
    }
    state["finalizing"] = True
    monkeypatch.setattr(agent.caps, "results", lambda *_: [])
    result = agent.final_report(row, state, REPORT)
    assert result["conclusion"] == "inconclusive"
    assert state["phase"] == "일부 검증 후 종료"
    assert result["causes"] == result["fixes"] == []
    assert investigation.normalize(result)["sections"] == REPORT["sections"]


def test_actual_judge_results_support_modern_report_without_repair(agent_context):
    c = agent_context
    _, row, ctx, state = queued(c)
    row.evidence = {
        **row.evidence,
        "expected_status": "wrong_answer",
        "actual_status": "accepted",
    }
    state["investigation_focus"] = investigation.focus(row.evidence)
    whole = call("run_code", artifact_id="original", testcase_orders=[])
    assert agent.handle_tool(row, ctx, state, whole) is None
    judged(c, SubmissionStatus.ACCEPTED)
    full = agent.handle_tool(row, ctx, state, whole)
    probe_args = dict(input="2 3\n", expected_output="5\n")
    probe = call("run_probe", artifact_id="original", **probe_args)
    assert agent.handle_tool(row, ctx, state, probe) is None
    judged(c, SubmissionStatus.WRONG_ANSWER)
    failed = agent.handle_tool(row, ctx, state, probe)
    report = copy.deepcopy(REPORT)
    report["sections"][0]["evidence_refs"] = [
        "trial:" + full["submission_id"],
        "trial:" + failed["submission_id"],
    ]
    with pytest.raises(ToolError, match="근거가 부족"):
        agent.handle_tool(row, ctx, state, call("finish_report", **report))
    state["probe_checks"] = [
        dict(
            check_id="checked", status="cross_checked", probe_hash=ai.digest(probe_args)
        )
    ]
    assert agent.handle_tool(row, ctx, state, call("finish_report", **report))[
        "report_saved"
    ]
    assert state["phase"] == "검증 완료"
    assert state["report"]["fixes"] == [] and not state["artifacts"]


@pytest.mark.parametrize(
    "details,status",
    [
        (
            {
                "validator": {"exit_code": 0},
                "reference": {"exit_code": 0},
                "expected_matches_reference": True,
            },
            "cross_checked",
        ),
        (
            {
                "validator": {"exit_code": 1},
                "reference": {"exit_code": 0},
                "expected_matches_reference": True,
            },
            "conflict",
        ),
        (
            {
                "validator": {"exit_code": 0},
                "reference": {"exit_code": 0},
                "expected_matches_reference": False,
            },
            "conflict",
        ),
        (
            {
                "validator": {"status": "unavailable"},
                "reference": {"exit_code": 0},
                "expected_matches_reference": True,
            },
            "incomplete",
        ),
        ({"validator": None}, "incomplete"),
    ],
)
def test_probe_check_uses_immutable_sources_and_reuses_result(
    monkeypatch, details, status
):
    row = SimpleNamespace(context_hash="ctx")
    state = {"workspace": {"keep.txt": base64.b64encode(b"keep").decode()}}
    files = {
        "v": {
            "name": "validator.py",
            "storage_key": "p/validator/v",
            "text": "print('validator')",
        },
        "r": {
            "name": "reference.py",
            "storage_key": "p/verification-solutions/accepted/r",
            "text": "print(5)",
        },
    }
    args = dict(
        input="2 3\n", expected_output="5\n", validator_id="v", reference_id="r"
    )
    calls = []

    def execute(*params):
        calls.append(params)
        assert params[4] == "workspace_exec" and params[5]["command"].endswith(
            " && python3 -I check.py"
        )
        assert len(state["workspace_readonly"]) == 6
        for path in state["workspace_readonly"]:
            with pytest.raises(ToolError):
                checks.workspace.put(state, path, b"tampered")
        return dict(
            request_id="exp",
            exit_code=0,
            stdout="ZOJ_PROBE_CHECK:" + json.dumps(details),
            output_truncated=False,
        )

    monkeypatch.setattr(checks.workspace, "configured", lambda: True)
    monkeypatch.setattr(checks.workspace, "handle", execute)
    saved = checks.check(row, {}, state, files, args, {"call_id": "c"})
    assert saved["status"] == status
    assert saved["probe_hash"] == ai.digest(
        {k: args[k] for k in ("input", "expected_output")}
    )
    assert list(state["workspace"]) == ["keep.txt"]
    assert checks.check(row, {}, state, files, args, {"call_id": "again"}) == saved
    assert len(calls) == 1


def test_probe_check_rejects_unregistered_reference_and_validator(monkeypatch):
    monkeypatch.setattr(checks.workspace, "configured", lambda: True)
    for validator, reference in [("external", ""), ("", "candidate-1")]:
        with pytest.raises(ToolError, match="등록"):
            checks.check(
                SimpleNamespace(context_hash="c"),
                {},
                {},
                {},
                dict(
                    input="1",
                    expected_output="1",
                    validator_id=validator,
                    reference_id=reference,
                ),
                {"call_id": "c"},
            )


def test_luna_defaults_budget_estimate_and_measured_cache_writes(agent_context):
    defaults = Settings.model_fields
    assert defaults["verification_agent_model"].default == "gpt-6-luna"
    assert defaults["verification_agent_max_calls"].default == 24
    assert defaults["verification_agent_max_input_tokens"].default == 300000
    assert agent.price("gpt-6-luna") == (0.10, 0.01, 0.50)
    assert agent.cost_rates("gpt-6-luna", 272001) == (0.25, 0.02, 0.75)
    _, _, _, state = queued(agent_context)
    state["model"] = "gpt-6-luna"
    agent.record_usage(
        state,
        {
            "usage": {
                "input_tokens": 1000,
                "output_tokens": 100,
                "input_tokens_details": {
                    "cached_tokens": 300,
                    "cache_write_tokens": 500,
                },
            }
        },
    )
    assert state["usage"]["estimated_cost_usd"] == pytest.approx(
        (200 * 0.10 + 500 * 0.125 + 300 * 0.01 + 100 * 0.5) / 1000000
    )
    # New model tokenization is not assumed to match GPT-5.4.
    text = "검증 자료"
    assert agent.text_token_reservation(text, conservative=True) == len(
        json.dumps(text, ensure_ascii=False).encode()
    )


def test_complete_old_report_offers_explicit_upgrade_without_automatic_calls(
    agent_context, monkeypatch
):
    c = agent_context
    sid, row, ctx, state = queued(c)
    state["prompt_version"] = "verification-agent-v2.7"
    with c["sessions"]() as db:
        saved = db.get(ai.Analysis, row.analysis_id)
        saved.agent_state = state
        saved.status = "succeeded"
        saved.report = agent.fallback_report("old report")
        db.commit()
    monkeypatch.setattr(
        agent, "request_model", lambda *_: pytest.fail("GET cannot bill")
    )
    assert ai.analysis_detail(c["cid"], c["pid"], sid)["analysis"]["can_retry"]
    assert not agent.process_one()


def test_free_investigation_can_compare_a_later_selected_registered_source():
    row, state, runs = gap()
    for run in runs:
        run["artifact_id"] = "asset:registered-wrong-solution"
    assert not investigation.supported_gap(state, runs)
    state["task_goal"] = "Find missing tests by comparing registered solutions"
    assert investigation.supported_gap(state, runs)
    runs[1]["artifact_id"] = "asset:different-code"
    assert not investigation.supported_gap(state, runs)
