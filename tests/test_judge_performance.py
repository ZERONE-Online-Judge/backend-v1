import json

import pytest

from app.services import judge_performance as performance
from app.services import verification_agent as agent
from app.services import verification_agent_tools as caps
from test_verification_agent import agent_context, queued, call
from test_verification_ai import context


def test_reference_preserves_measured_means_and_separates_vm_and_submission_limits():
    reference = performance.reference()
    config = reference["environment"]
    assert config["host"]["cpu_sockets"] == 2
    assert config["host"]["memory_capacity_gb"] == 256
    assert config["host"]["memory_effective_rate_mt_s"] is None
    assert config["vm"]["planned_count"] == 6
    assert config["vm"]["vcpus_per_agent"] == 10
    assert config["vm"]["memory_gib_per_agent"] == 26
    assert config["agent_configuration"]["max_parallel_testcases_per_agent"] == 8
    raw = json.loads((performance.DATA / "judge_100m_benchmark.json").read_text())
    for case in raw["cases"]:
        runs = case["runs"]
        assert len(runs) == 5
        assert all(r["status"] == "accepted" for r in runs)
        assert (
            reference["benchmark"]["languages"][case["language"]]["runtime_ms"]["mean"]
            == sum(r["runtime_ms"] for r in runs) / 5
        )
    config["vm"]["planned_count"] = 100
    assert performance.reference()["environment"]["vm"]["planned_count"] == 6


def test_estimates_are_explicit_hypotheses_and_do_not_divide_by_vm_cores():
    estimate = performance.estimate("cpp17", "quadratic", 10000, 1)
    assert estimate["estimated_iterations"] == 100000000
    assert estimate["estimated_runtime_ms"]["mean"] == 165.6
    assert estimate["kind"] == "calibration_hypothesis_not_measurement"
    assert "verdict" not in estimate
    assert (
        performance.estimate("python313", "linear", 100000000, 1)[
            "estimated_runtime_ms"
        ]["mean"]
        == 14774.8
    )
    assert (
        performance.estimate("c99", "cubic", 100, 2)["estimated_iterations"] == 2000000
    )
    assert (
        performance.estimate("java8", "n_log_n", 1024, 1)["estimated_iterations"]
        == 10240
    )
    assert performance.estimate("java8", "n_log_n", 1, 1)["estimated_iterations"] == 1


@pytest.mark.parametrize(
    "n,work",
    [
        (True, 1),
        (1.5, 1),
        (0, 1),
        (10**10, 1),
        (100, float("nan")),
        (100, float("inf")),
        (100, True),
        (100, 0),
        (100, 1001),
    ],
)
def test_estimate_rejects_invalid_or_unbounded_input(n, work):
    with pytest.raises(ValueError):
        performance.estimate("cpp17", "linear", n, work)


def test_registered_tools_expose_context_without_changing_problem_limits(agent_context):
    _, row, context_data, state = queued(agent_context)
    original_problem = dict(context_data["problem"])
    result = agent.handle_tool(row, context_data, state, call("inspect_judge"))
    assert result["time_limit_ms"] == original_problem["time_limit_ms"]
    assert result["memory_limit_mb"] == original_problem["memory_limit_mb"]
    assert result["active_agents"] == 0
    assert result["active_submission_slots"] == 0
    assert result["performance_reference"]["environment"]["vm"]["planned_count"] == 6
    assert result["performance_evidence_file"] == "judge-performance"
    estimate = agent.handle_tool(
        row,
        context_data,
        state,
        call(
            "estimate_runtime",
            language="cpp17",
            complexity="quadratic",
            n=10000,
            work_per_step=1,
        ),
    )
    assert estimate["estimated_runtime_ms"]["mean"] == 165.6
    assert context_data["problem"] == original_problem
    for mode in ({}, {"task_goal": "TLE를 검증해줘"}):
        assert "estimate_runtime" in {t["name"] for t in agent.tools_for(mode)}
        assert "inspect_judge" in agent.instructions(mode)
        assert "run_code" in agent.instructions(mode)
    files = caps.make_manifest(context_data, row.evidence, state["references"])
    assert files["judge-performance"]["read_only"]
    assert "benchmark" in json.loads(files["judge-performance"]["text"])
