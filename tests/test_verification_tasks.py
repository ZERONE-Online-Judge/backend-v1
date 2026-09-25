import copy
import json
from uuid import uuid4

import pytest
from sqlalchemy import select

from app.orm_models import ProblemRow, VerificationTaskRow, VerificationTrialRow
from app.services import (
    verification_tasks as tasks,
    verification_agent as agent,
    verification_ai as ai,
)
from app.services import (
    verification_agent_tools as caps,
    verification_workspace as workspace,
)
from app.services.errors import AppError
from app.services.store import store
from app.models import SubmissionStatus
from test_verification_agent import agent_context, call, judged
from test_verification_ai import context, client, REPORT, submit


def create(c, goal="이 코드의 오류를 재현하고 수정 코드를 검증해 주세요.", **kwargs):
    return tasks.create(c["cid"], c["pid"], goal, **kwargs)


def model(calls):
    return {
        "status": "completed",
        "output": calls,
        "usage": {"input_tokens": 700, "output_tokens": 100},
    }


def load(c, tid):
    with c["sessions"]() as db:
        row = db.get(ai.Analysis, tid)
        ctx = db.get(ai.Snapshot, row.context_hash).context
        db.expunge(row)
        return row, ctx, copy.deepcopy(row.agent_state)


def test_goal_runs_without_mismatch_submission_and_keeps_shared_evidence(
    agent_context, monkeypatch
):
    c = agent_context
    task = create(c, source_asset_id=c["aid"])
    tid = task["task_id"]
    plan = [{"title": "재현과 수정 검증", "status": "in_progress"}]
    batches = [
        [
            call("update_plan", steps=plan),
            call("read_file", file_id="original", offset=0, length=1000),
            call("run_code", artifact_id="original", testcase_orders=[]),
        ],
        [
            call(
                "record_finding",
                id="constant",
                title="입력을 사용하지 않음",
                detail="원본이 항상 4를 출력합니다.",
                status="confirmed",
                evidence_refs=["original"],
            ),
            call(
                "edit_code",
                base_id="original",
                replacements=[
                    {"old": "print(4)", "new": "print(sum(map(int,input().split())))"}
                ],
            ),
        ],
        [call("run_code", artifact_id="candidate-1", testcase_orders=[])],
        [
            call("update_plan", steps=[{**plan[0], "status": "done"}]),
            call(
                "finish_task", outcome="completed", report={**REPORT, "limitations": []}
            ),
        ],
    ]

    def provider(state, _):
        assert state["task_goal"]
        assert "finish_task" in {t["name"] for t in agent.tools_for(state)}
        assert "finish_report" not in {t["name"] for t in agent.tools_for(state)}
        return model(batches.pop(0))

    monkeypatch.setattr(agent, "request_model", provider)
    assert agent.process_one() and agent.process_one()
    judged(c, SubmissionStatus.WRONG_ANSWER)
    assert agent.process_one()
    assert agent.process_one() and agent.process_one()
    assert agent.process_one() and agent.process_one()
    judged(c, SubmissionStatus.ACCEPTED)
    assert agent.process_one()
    assert agent.process_one() and agent.process_one()
    detail = tasks.detail(c["cid"], c["pid"], tid)
    a = detail["analysis"]
    assert a["status"] == "succeeded" and a["outcome"] == "completed"
    assert a["plan"][0]["status"] == "done"
    assert a["findings"][0]["status"] == "confirmed"
    assert [r["status"] for r in a["executions"]] == ["wrong_answer", "accepted"]
    assert create(c, source_asset_id=c["aid"])["task_id"] == tid
    with c["sessions"]() as db:
        assert not db.scalars(select(ai.Run)).all()
        assert "history" not in db.get(ai.Analysis, tid).agent_state


def test_question_pauses_without_model_polling_and_continuation_preserves_files(
    agent_context, monkeypatch
):
    c = agent_context
    task = create(c, "문제에 없는 동률 처리 규칙까지 포함하여 확인해 주세요.")
    tid = task["task_id"]
    row, ctx, state = load(c, tid)
    assert "original" not in caps.make_manifest(ctx, row.evidence, state["references"])
    requests = []

    def provider(*_):
        requests.append(True)
        return model(
            [
                call("workspace_write", path="probe.py", content="print(5)\n"),
                call(
                    "ask_user",
                    question="동률일 때 어떤 결과를 기대하나요?",
                    reason="저장된 문제 지문에 동률 규칙이 없습니다.",
                ),
            ]
        )

    monkeypatch.setattr(agent, "request_model", provider)
    assert agent.process_one() and agent.process_one()
    assert not agent.process_one()
    assert len(requests) == 1
    before = tasks.detail(c["cid"], c["pid"], tid)
    assert before["analysis"]["status"] == "awaiting_input"
    assert before["analysis"]["question"]["question"]
    continued = create(
        c, "동률이면 가장 작은 인덱스를 선택해야 해요.", parent_task_id=tid
    )
    assert continued["task_id"] != tid and continued["parent_task_id"] == tid
    child, _, state = load(c, continued["task_id"])
    assert state["workspace"]["probe.py"]
    assert state["calls"] == 0 and state["usage"]["input_tokens"] == 0
    assert "previous_question" in state["history"][1]["content"]
    assert before["analysis"]["usage"]["input_tokens"] == 700


def test_stop_during_provider_call_keeps_usage_and_prevents_tools(
    agent_context, monkeypatch
):
    c = agent_context
    tid = create(c)["task_id"]

    def provider(*_):
        result = tasks.cancel(c["cid"], c["pid"], tid)
        assert result["cancel_requested"]
        return model(
            [call("workspace_write", path="should-not-exist.txt", content="no")]
        )

    monkeypatch.setattr(agent, "request_model", provider)
    assert agent.process_one()
    a = tasks.detail(c["cid"], c["pid"], tid)["analysis"]
    assert a["status"] == "stopped" and a["usage"]["input_tokens"] == 700
    assert not a["workspace_files"] and not agent.process_one()


def test_task_permissions_context_and_parent_scope(agent_context):
    c = agent_context
    route = c["base"] + "/verification-tasks"
    payload = {"goal": "이 문제의 checker 동작을 확인해 주세요."}
    assert (
        client.post(route, json=payload, headers=c["headers"]["reviewer"]).status_code
        == 403
    )
    response = client.post(route, json=payload, headers=c["headers"]["owner"])
    assert response.status_code == 200, response.text
    tid = response.json()["data"]["task_id"]
    assert (
        client.get(route + "/" + tid, headers=c["headers"]["reviewer"]).status_code
        == 403
    )
    assert client.get(route, headers=c["headers"]["author"]).json()["data"]["tasks"]
    with pytest.raises(AppError) as error:
        create(c, "다른 독립 작업을 새로 시작해 주세요.")
    assert error.value.status_code == 409
    tasks.cancel(c["cid"], c["pid"], tid)
    with pytest.raises(AppError):
        create(c, parent_task_id=str(uuid4()))
    with c["sessions"]() as db:
        db.get(ProblemRow, c["pid"]).statement = "changed"
        db.commit()
    assert tasks.detail(c["cid"], c["pid"], tid)["stale"]
    with pytest.raises(AppError):
        create(c, parent_task_id=tid)


def test_findings_require_real_evidence_and_unrun_candidate_cannot_claim_completion(
    agent_context,
):
    c = agent_context
    task = create(c, source_asset_id=c["aid"])
    row, ctx, state = load(c, task["task_id"])
    with pytest.raises(caps.ToolError):
        agent.handle_tool(
            row,
            ctx,
            state,
            call(
                "record_finding",
                id="x",
                title="확인",
                detail="근거 없음",
                status="confirmed",
                evidence_refs=["trial:other-problem"],
            ),
        )
    agent.handle_tool(
        row,
        ctx,
        state,
        call(
            "edit_code",
            base_id="original",
            replacements=[{"old": "print(4)", "new": "print(5)"}],
        ),
    )
    with pytest.raises(caps.ToolError):
        agent.handle_tool(
            row, ctx, state, call("finish_task", outcome="completed", report=REPORT)
        )
    agent.handle_tool(
        row, ctx, state, call("finish_task", outcome="inconclusive", report=REPORT)
    )
    assert state["outcome"] == "inconclusive"
    assert any("실제 코드 실행" in limit for limit in state["report"]["limitations"])


def test_delete_problem_removes_tasks_and_stop_prevents_queued_model_calls(
    agent_context, monkeypatch
):
    c = agent_context
    tid = create(c)["task_id"]
    monkeypatch.setattr(agent, "request_model", lambda *_: pytest.fail("must not bill"))
    tasks.cancel(c["cid"], c["pid"], tid)
    assert not agent.process_one()
    assert store.delete_problem(c["cid"], c["pid"])
    with c["sessions"]() as db:
        assert not db.get(VerificationTaskRow, tid)
        assert not db.get(ai.Analysis, tid)


def test_historical_run_sources_are_frozen_and_scoped_to_the_problem(agent_context):
    c = agent_context
    sid = submit(c)
    tid = create(c)["task_id"]
    row, ctx, state = load(c, tid)
    listed = agent.handle_tool(row, ctx, state, call("list_verification_runs"))
    assert listed["runs"][0]["submission_id"] == sid
    result = agent.handle_tool(
        row, ctx, state, call("read_verification_run", submission_id=sid)
    )
    assert result["same_context"]
    source_id, log_id = result["file_ids"]
    assert state["extra_files"][source_id]["text"] == c["source"]
    assert "wrong answer" in state["extra_files"][log_id]["text"]
    text = agent.handle_tool(
        row, ctx, state, call("read_file", file_id=source_id, offset=0, length=1000)
    )
    assert c["source"].strip() in json.dumps(text)
    row.problem_id = str(uuid4())
    with pytest.raises(caps.ToolError):
        agent.handle_tool(
            row, ctx, state, call("read_verification_run", submission_id=sid)
        )


def test_task_tool_schema_references_resolve_from_the_parameter_root():
    for tool in agent.TASK_TOOLS:
        root = tool["parameters"]

        def check(value):
            if isinstance(value, dict):
                if "$ref" in value:
                    target = root
                    assert value["$ref"].startswith("#/")
                    for part in value["$ref"][2:].split("/"):
                        target = target[part]
                for child in value.values():
                    check(child)
            elif isinstance(value, list):
                for child in value:
                    check(child)

        check(root)
