import copy
import json

import pytest
from sqlalchemy import select

from app.models import now_utc
from app.services import verification_agent as agent, verification_ai as ai
from app.services import verification_agent_context as memory
from test_verification_agent import agent_context, queued, call, judged
from test_verification_ai import context, SubmissionStatus, REPORT, client


def test_unused_tool_schemas_are_loaded_only_when_requested(agent_context, monkeypatch):
    _, row, ctx, state = queued(agent_context)
    initial = {t["name"] for t in agent.tools_for(state)}
    assert "workspace_exec" not in initial and "read_image" not in initial
    assert {"record_finding", "run_code", "run_probe", "enable_tools"} <= initial
    agent.handle_tool(row, ctx, state, call("enable_tools", group="images"))
    assert "read_image" in {t["name"] for t in agent.tools_for(state)}
    monkeypatch.setattr(agent.workspace, "configured", lambda: True)
    agent.handle_tool(row, ctx, state, call("enable_tools", group="playground"))
    assert "workspace_exec" in {t["name"] for t in agent.tools_for(state)}


def test_cost_limit_reserves_one_final_report_call_without_increasing_limits(
    agent_context, monkeypatch
):
    _, row, ctx, state = queued(agent_context)
    original_limits = copy.deepcopy(state["limits"])
    state["usage"]["input_tokens"] = 50000
    state["usage"]["estimated_cost_usd"] = 0.17
    state["calls"] = 4
    monkeypatch.setattr(
        agent, "input_bound", lambda s: 3000 if s.get("finalizing") else 8000
    )
    requests = []

    def provider(s, output):
        requests.append(copy.deepcopy(s))
        assert s["finalizing"]
        assert {t["name"] for t in agent.tools_for(s)} == {"finish_report"}
        a, _, c = agent.cost_rates(s["model"], s["request_input_bound"])
        assert s["usage"]["estimated_cost_usd"] + (
            s["request_input_bound"] * a + output * c
        ) / 1_000_000 <= s["limits"]["max_cost_usd"]
        return {
            "status": "completed",
            "output": [call("finish_report", **REPORT)],
            "usage": {"input_tokens": 3000, "output_tokens": 1000},
        }

    monkeypatch.setattr(agent, "request_model", provider)
    agent.step(row, ctx, state)
    agent.step(row, ctx, state)
    assert len(requests) == 1
    assert state["limits"] == original_limits
    assert state["report"]["causes"]
    assert state["stop_reason"]["code"] == "cost"
    assert state["phase"] == "일부 검증 후 종료"
    assert state["usage"]["input_tokens"] == 53000


@pytest.mark.parametrize("legacy", [False, True])
def test_cumulative_tokens_do_not_stop_new_or_running_jobs_with_remaining_cost(
    agent_context, monkeypatch, legacy
):
    _, row, ctx, state = queued(agent_context)
    state["model"] = "gpt-6-luna"
    state["limits"]["max_cost_usd"] = 1.0
    state["usage"].update(
        input_tokens=400000, output_tokens=64000, estimated_cost_usd=0.10
    )
    if legacy:
        state["limits"].update(max_input_tokens=300000, max_output_tokens=32000)
        brief = json.loads(state["history"][0]["content"])
        brief["limits"] = copy.deepcopy(state["limits"])
        state["history"][0]["content"] = json.dumps(brief)
    requests = []

    def provider(s, output):
        requests.append(output)
        assert not s.get("finalizing")
        assert not s.get("stop_reason")
        assert "max_input_tokens" not in s["limits"]
        assert "max_output_tokens" not in s["limits"]
        assert "max_input_tokens" not in json.loads(s["history"][0]["content"])["limits"]
        return {
            "status": "completed",
            "output": [call("read_file", file_id="original", offset=0, length=200)],
            "usage": {"input_tokens": 3000, "output_tokens": 100},
        }

    monkeypatch.setattr(agent, "request_model", provider)
    assert agent.budget_for_request(state) is not None
    agent.step(row, ctx, state)
    assert requests == [4096]
    assert state["calls"] == 1
    assert state["usage"]["input_tokens"] == 403000
    assert state["usage"]["output_tokens"] == 64100
    assert 0.10 < state["usage"]["estimated_cost_usd"] < 1.0


def test_exhausted_budget_retains_judge_facts_and_probe_uncertainty_without_model_call(
    agent_context, monkeypatch
):
    c = agent_context
    _, row, ctx, state = queued(c)
    run = call("run_code", artifact_id="original", testcase_orders=[1])
    assert agent.handle_tool(row, ctx, state, run) is None
    judged(c, SubmissionStatus.WRONG_ANSWER)
    agent.handle_tool(row, ctx, state, run)
    probe = call(
        "run_probe", artifact_id="original", input="1 1\n", expected_output="7\n"
    )
    assert agent.handle_tool(row, ctx, state, probe) is None
    judged(c, SubmissionStatus.WRONG_ANSWER)
    agent.handle_tool(row, ctx, state, probe)
    state["usage"]["estimated_cost_usd"] = state["limits"]["max_cost_usd"]
    monkeypatch.setattr(
        agent, "request_model", lambda *_: pytest.fail("no additional bill")
    )
    agent.step(row, ctx, state)
    report = state["report"]
    assert len(report["causes"]) == 2
    assert "기대 출력의 정당성" in report["causes"][1]["explanation"]
    assert "코드의 근본 원인" in report["causes"][0]["explanation"]
    assert state["stop_reason"]["code"] == "cost"
    assert state["calls"] == 0


@pytest.mark.parametrize("usage", [None, {}, {"input_tokens": 3000}])
def test_missing_usage_reserves_request_cost_instead_of_allowing_free_calls(
    agent_context, monkeypatch, usage
):
    _, row, ctx, state = queued(agent_context)
    monkeypatch.setattr(
        agent,
        "request_model",
        lambda *_: {
            "status": "completed",
            "output": [call("read_file", file_id="original", offset=0, length=200)],
            "usage": usage,
        },
    )
    agent.step(row, ctx, state)
    a, _, c = agent.cost_rates(state["model"], state["request_input_bound"])
    assert state["usage"]["estimated_cost_usd"] == pytest.approx(
        (state["request_input_bound"] * a + 4096 * c) / 1_000_000
    )
    assert state["usage"]["includes_unconfirmed_request"] is True
    assert "input_checkpoint" not in state
    state["limits"]["max_cost_usd"] = state["usage"]["estimated_cost_usd"]
    assert agent.budget_for_request(state) is None
    assert state["budget_blocked"]["code"] == "cost"


def test_compaction_preserves_public_evidence_and_files_without_private_reasoning(
    agent_context,
):
    _, _, _, state = queued(agent_context)
    state["workspace"] = {"candidate.cpp": "dGVzdA=="}
    state["workspace_readonly"] = {"testlib.h": {"file_id": "library"}}
    reading = call("read_file", file_id="original", offset=0, length=12000)
    state["history"] += [
        {"type": "reasoning", "encrypted_content": "opaque-private-" * 5000},
        reading,
        {
            "type": "function_call_output",
            "call_id": reading["call_id"],
            "output": json.dumps(
                {"file_id": "original", "text": "print(4)\n" * 1500, "complete": True}
            ),
        },
    ]
    size = len(json.dumps(state["history"]).encode())
    assert memory.compact(state)
    text = json.dumps(state["history"])
    assert len(text.encode()) < size / 2
    assert "opaque-private" not in text and "print(4)" in text
    checkpoint = json.loads(state["history"][1]["content"])
    assert checkpoint["observations"][0]["result"]["complete"] is False
    assert checkpoint["observations"][0]["result"]["excerpt_only"] is True
    assert state["workspace"]["candidate.cpp"] == "dGVzdA=="
    assert state["workspace_readonly"]["testlib.h"]["file_id"] == "library"


def test_legacy_partial_report_explains_exact_token_stop_and_requires_explicit_upgrade(
    agent_context, monkeypatch
):
    c = agent_context
    sid, row, ctx, state = queued(c)
    state.pop("prompt_version")
    state["limits"].update(max_input_tokens=60000, max_output_tokens=16000)
    state.update(phase="일부 검증 후 종료", calls=6, request_input_bound=17934)
    state["usage"].update(
        input_tokens=55174, output_tokens=1084, estimated_cost_usd=0.0172281
    )
    with c["sessions"]() as db:
        saved = db.get(ai.Analysis, row.analysis_id)
        saved.agent_state = state
        saved.usage = state["usage"]
        saved.status = "succeeded"
        saved.report = agent.fallback_report("호출·토큰·비용 예산 한도에 도달했습니다.")
        # Model version is a cache namespace, not an automatic migration/retry.
        saved.cache_key = "previous-engine-cache-key"
        db.commit()
    monkeypatch.setattr(
        agent, "request_model", lambda *_: pytest.fail("GET must not bill")
    )
    result = ai.analysis_detail(c["cid"], c["pid"], sid)["analysis"]
    assert result["can_retry"] is True
    assert "73,108" in result["stop_reason"]["message"]
    assert "60,000" in result["report"]["limitations"][0]
    assert not agent.process_one()
    response = client.post(
        c["base"] + f"/verification-runs/{sid}/analysis", headers=c["headers"]["owner"]
    )
    assert response.status_code == 200
    assert response.json()["data"]["analysis"]["analysis_id"] != row.analysis_id
    with c["sessions"]() as db:
        assert db.get(ai.Analysis, row.analysis_id).status == "succeeded"
        assert len(db.scalars(select(ai.Analysis)).all()) == 2


def test_finalization_cannot_start_more_experiments_or_repeat_a_paid_final_call(
    agent_context,
):
    _, row, ctx, state = queued(agent_context)
    state["finalizing"] = True
    with pytest.raises(agent.caps.ToolError, match="최종 보고서"):
        agent.handle_tool(
            row,
            ctx,
            state,
            call("run_code", artifact_id="original", testcase_orders=[]),
        )
    state["final_call_made"] = True
    assert agent.prepare_request(state) is None


def test_judge_inspection_omits_repeated_default_limits_but_keeps_overrides(
    agent_context,
):
    _, _, ctx, _ = queued(agent_context)
    case = ctx["testcases"][0]
    ctx = copy.deepcopy(ctx)
    ctx["testcases"] = [{**case, "display_order": i} for i in range(1, 101)]
    ctx["testcases"][12]["time_limit_ms_override"] = 2000
    result = agent.caps.inspect_judge(ctx)
    assert result["testcase_count"] == 100
    assert result["testcase_overrides"] == [
        {
            "display_order": 13,
            "time_limit_ms_override": 2000,
            "memory_limit_mb_override": None,
        }
    ]
    assert result["overrides_omitted"] == 0
    assert "storage_key" not in json.dumps(result)


def test_budget_stop_reserves_final_call_and_rejects_more_expensive_experiments(
    agent_context,
):
    _, _, _, state = queued(agent_context)
    state["calls"] = state["limits"]["max_calls"] - 1
    assert agent.prepare_request(state) is not None
    assert state["finalizing"] is True
    assert state["stop_reason"]["code"] == "calls"
    assert state["calls"] < state["limits"]["max_calls"]


def test_task_finalization_saves_incomplete_report_without_inventing_completed_plan(
    agent_context,
):
    from app.services import verification_tasks as tasks

    c = agent_context
    created = tasks.create(
        c["cid"],
        c["pid"],
        "기준 풀이와 경계조건을 비교해 주세요.",
        source_asset_id=c["aid"],
    )
    with c["sessions"]() as db:
        row = db.get(ai.Analysis, created["task_id"])
        ctx = db.get(ai.Snapshot, row.context_hash).context
        state = agent.initial_state(db, row, ctx)
    state["finalizing"] = True
    result = agent.handle_tool(
        row, ctx, state, call("finish_task", outcome="completed", report=REPORT)
    )
    assert result["outcome"] == "inconclusive"
    assert state["plan"] == []
    assert state["report"]["causes"]


def test_local_tokenizer_reduces_korean_byte_overreservation_with_conservative_fallback(
    monkeypatch,
):
    content = ["문제의 원인과 실제 실행 결과를 확인합니다. " * 50, "<|endoftext|>"]
    reserved = agent.text_token_reservation(content)
    size = len(json.dumps(content, ensure_ascii=False).encode())
    actual = len(
        agent.tokenizer().encode(
            json.dumps(content, ensure_ascii=False), disallowed_special=()
        )
    )
    assert actual <= reserved < size

    def broken():
        raise RuntimeError("no local tokenizer cache")

    monkeypatch.setattr(agent, "tokenizer", broken)
    assert agent.text_token_reservation(content) == size


def test_successful_or_timeout_report_is_not_mislabelled_as_input_budget_stop():
    state = {
        "phase": "검증 완료",
        "usage": {"input_tokens": 55174},
        "limits": {"max_input_tokens": 60000},
        "request_input_bound": 17934,
    }
    assert agent.saved_stop_reason(state, REPORT) is None
    state["phase"] = "일부 검증 후 종료"
    assert (
        agent.saved_stop_reason(
            state, agent.fallback_report("검증 시간 한도에 도달했습니다.")
        )
        is None
    )
    state["stop_reason"] = {"code": "timeout", "message": "시간 한도"}
    assert agent.saved_stop_reason(state, REPORT)["code"] == "timeout"
