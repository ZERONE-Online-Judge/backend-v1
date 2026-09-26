import json

import pytest

from app.services import judge_performance as performance
from app.services import verification_agent as agent
from app.services import verification_agent_tools as caps
from test_verification_agent import agent_context, queued, call
from test_verification_ai import context


def test_reference_preserves_raw_maxima_and_separates_vm_and_submission_limits():
    reference = performance.reference()
    config = reference["environment"]
    assert config["host"]["cpu_sockets"] == 2
    assert config["host"]["memory_capacity_gb"] == 256
    assert config["host"]["memory_effective_rate_mt_s"] is None
    assert config["vm"]["planned_count"] == 6
    assert config["vm"]["vcpus_per_agent"] == 10
    assert config["vm"]["memory_gib_per_agent"] == 26
    assert config["agent_configuration"]["max_parallel_testcases_per_agent"] == 8
    raw = json.loads((performance.DATA / "judge_tle_benchmark.json").read_text())
    assert len(raw["cases"]) == 16
    assert sum(r["testcases"] for c in raw["cases"] for r in c["runs"]) == 240
    for case, published in zip(raw["cases"], reference["benchmark"]["cases"]):
        runs = case["runs"]
        assert len(runs) == 6
        assert all(r["status"] == "accepted" and r["other_jobs_observed"] == 0 for r in runs)
        assert published["runtimeMs"]["max"] == max(r["runtime_ms"] for r in runs)
        assert case["serialMaxMs"] == max(r["runtime_ms"] for r in runs if r["condition"] == "serial")
        assert case["loadMaxMs"] == max(r["runtime_ms"] for r in runs if r["condition"] == "mixed12")
    config["vm"]["planned_count"] = 100
    assert performance.reference()["environment"]["vm"]["planned_count"] == 6


def test_estimates_use_operation_specific_maximum_and_explicit_margin():
    raw = json.loads((performance.DATA / "judge_tle_benchmark.json").read_text())
    for c in raw["cases"]:
        estimate = performance.estimate(c["language"], "linear", c["iterations"], 1, c["profile"], 2, 1)
        assert estimate["planning_budget_ms"] == c["runtimeMs"]["max"] * 2
        assert estimate["scaled_observed_max_ms"] == c["runtimeMs"]["max"]
        assert estimate["kind"] == "conservative_budget_not_worst_case_bound"
        assert "verdict" not in estimate
        assert not estimate["extrapolated"]
    legacy = performance.estimate("cpp17", "quadratic", 10000, 1)
    matching = [c for c in raw["cases"] if c["language"] == "cpp17" and c["profile"] != "search"]
    worst = max(matching, key=lambda c: c["runtimeMs"]["max"]/c["iterations"])
    assert legacy["profile"] == worst["profile"]
    assert legacy["estimated_iterations"] == 100000000
    assert legacy["planning_budget_ms"] == worst["runtimeMs"]["max"] * 2
    assert performance.estimate("c99", "pairs", 10000, 1)["estimated_iterations"] == 49995000
    assert performance.estimate("c99", "pairs", 1, 1)["planning_budget_ms"] == 0
    assert performance.estimate("c99", "n_log_n", 1, 1)["estimated_iterations"] == 0
    assert performance.estimate("c99", "n_log_n", 3, 1)["estimated_iterations"] == 6
    assert performance.estimate("c99", "cubic", 100, 2, "memory", 3, 5)["estimated_iterations"] == 10000000
    assert performance.estimate("c99", "linear", 100000001, 1, "memory")["extrapolated"]


@pytest.mark.parametrize("profile,complexity", [("search", "n_log_n"), ("search", "quadratic"), ("invalid", "linear"), ("memory", "invalid")])
def test_search_call_cost_cannot_be_multiplied_by_log_again(profile, complexity):
    with pytest.raises(ValueError):
        performance.estimate("cpp17", complexity, 1000, 1, profile)


@pytest.mark.parametrize("factor,instances", [(0, 1), (11, 1), (float("nan"), 1), (True, 1), (2, 0), (2, 1.5), (2, True), (2, 1000001)])
def test_estimate_validates_margin_and_input_batches(factor, instances):
    with pytest.raises(ValueError):
        performance.estimate("cpp17", "linear", 1000, 1, "memory", factor, instances)


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
    assert estimate["planning_budget_ms"] == estimate["reference_runtime_ms"]["max"] * 2
    assert context_data["problem"] == original_problem
    for mode in ({}, {"task_goal": "TLE를 검증해줘"}):
        assert "estimate_runtime" in {t["name"] for t in agent.tools_for(mode)}
        assert "inspect_judge" in agent.instructions(mode)
        assert "run_code" in agent.instructions(mode)
    files = caps.make_manifest(context_data, row.evidence, state["references"])
    assert files["judge-performance"]["read_only"]
    assert "benchmark" in json.loads(files["judge-performance"]["text"])
