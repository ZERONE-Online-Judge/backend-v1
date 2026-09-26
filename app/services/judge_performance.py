"""Operation-specific calibration. Estimates are budgets, never verdicts or bounds."""

import copy
import json
import math
from functools import lru_cache
from pathlib import Path

DATA = Path(__file__).resolve().parent.parent / "data"


@lru_cache(maxsize=1)
def _records():
    return (
        json.loads((DATA / "judge_environment.json").read_text()),
        json.loads((DATA / "judge_tle_benchmark.json").read_text()),
    )


def reference():
    environment, measured = _records()
    return copy.deepcopy({
        "environment": environment,
        "benchmark": {
            "measured_at": measured["measuredAt"],
            "source": "https://zoj.kr" + measured["snapshotPath"],
            "method": measured["method"],
            "profiles": measured["profiles"],
            "cases": [{k: c[k] for k in ("profile", "language", "iterations", "sampleCount", "runtimeMs")} for c in measured["cases"]],
        },
        "interpretation": [
            "1억 회=1초 또는 단순 나머지 반복의 평균으로 모든 코드를 환산하지 마세요. 반복문 본문과 가장 비슷한 profile을 선택하세요.",
            "modulo는 상수 나머지 누적, dependent는 직전 값에 의존하는 곱셈·나머지, memory는 4194304개 인덱스 배열의 의존 접근, search는 1048576개 배열에서 이진 탐색 한 번입니다. search의 단위는 비교 1회가 아니라 탐색 1회입니다.",
            "보수적 예산은 측정 최댓값×반복 규모 비율×여유 계수(기본 2)입니다. 계수 2는 계획상 선택이며 실측 상한·최악 실행시간 보장이 아닙니다. 작업 종류가 다르면 이 추정도 과소평가할 수 있습니다.",
            "최악의 입력으로 실제 횟수를 계산하세요: 이중 완전 반복 N², 쌍 순회 N(N-1)/2, 문자열 비교는 접두사 길이, 반복 정렬·다중 테스트는 각 작업의 비용 합. Big-O 표기만으로 정확한 횟수는 나오지 않습니다.",
            "CPU 모델·10 vCPU·2소켓으로 단일 스레드 시간을 나누지 마세요. 실제 언어 보정 후 제한, 케이스별 제한, 최대 입력·자료구조·입출력·무한 루프를 확인하고 run_code로 반복 검증하세요.",
            "표시 시간은 격리 준비·프로세스 시작을 포함하고 큐 대기·컴파일·checker는 제외합니다. 제출 요약은 테스트별 최댓값입니다. isolate CPU/wall 제한과 측정 구간이 다릅니다. 플레이그라운드 시간을 공식 채점 근거로 대체하지 마세요.",
            "실험의 120초·512MiB는 문제 제한과 별개입니다. 작은 작업 환산은 시작 비용 때문에 부정확하고, 캐시 경계를 넘거나 메모리/알고리즘이 바뀌면 선형 환산도 성립하지 않습니다.",
        ],
    })


def evidence_text():
    environment, measured = _records()
    return json.dumps({"environment": environment, "benchmark": measured}, ensure_ascii=False)


def estimate(language, complexity, n, work_per_step, profile="conservative", safety_factor=2, instances=1):
    """Conservative scale hypothesis. Compatibility default selects slowest unit cost."""
    _, measured = _records()
    if complexity not in {"linear", "n_log_n", "quadratic", "pairs", "cubic"}:
        raise ValueError("지원하는 반복 규모를 선택하세요.")
    if type(n) is not int or not 1 <= n <= 1_000_000_000:
        raise ValueError("n은 1~1,000,000,000 정수여야 합니다.")
    if type(work_per_step) not in (int, float) or not math.isfinite(work_per_step) or not 0 < work_per_step <= 1000:
        raise ValueError("work_per_step은 0보다 크고 1000 이하인 유한수여야 합니다.")
    if type(safety_factor) not in (int, float) or not math.isfinite(safety_factor) or not 1 <= safety_factor <= 10:
        raise ValueError("safety_factor는 1~10의 유한수여야 합니다.")
    if type(instances) is not int or not 1 <= instances <= 1000000:
        raise ValueError("instances는 같은 실행 안에서 처리하는 1~1,000,000개 입력 묶음입니다.")
    cases = [c for c in measured["cases"] if c["language"] == language]
    if profile == "conservative":
        # Legacy calls must not silently retain the optimistic modulo calibration.
        cases = [c for c in cases if c["profile"] != "search"]
        case = max(cases, key=lambda c: c["runtimeMs"]["max"] / c["iterations"], default=None)
    else:
        case = next((c for c in cases if c["profile"] == profile), None)
    if case is None:
        raise ValueError("지원 언어와 연산 profile을 선택하세요.")
    if case["profile"] == "search" and complexity != "linear":
        raise ValueError("search는 이진 탐색 전체 1회 단위입니다. linear의 n에 총 탐색 호출 수를 넣으세요. log N을 다시 곱하지 마세요.")
    steps = {"linear": n, "n_log_n": n * math.ceil(math.log2(n)), "quadratic": n**2, "pairs": n*(n-1)//2, "cubic": n**3}[complexity]
    iterations = steps * work_per_step * instances
    scaled_max = case["runtimeMs"]["max"] * iterations / case["iterations"]
    return {
        "kind": "conservative_budget_not_worst_case_bound",
        "language": language, "complexity": complexity, "n": n,
        "profile": case["profile"], "profile_requested": profile,
        "unit": "binary_search_call_on_1048576_items" if case["profile"] == "search" else "profile_loop_iteration",
        "work_per_step_assumption": work_per_step, "instances_in_one_execution": instances,
        "estimated_iterations": iterations,
        "scaled_observed_max_ms": round(scaled_max, 6),
        "safety_factor": safety_factor,
        "planning_budget_ms": round(scaled_max * safety_factor, 6),
        "reference_iterations": case["iterations"],
        "reference_runtime_ms": copy.deepcopy(case["runtimeMs"]),
        "reference_source": "https://zoj.kr" + measured["snapshotPath"],
        "extrapolated": iterations > case["iterations"],
        "note": "최악 입력의 반복 횟수와 실행 시간의 최댓값은 별개입니다. 여유 계수는 보장이 아닙니다. n_log_n은 N×ceil(log2 N) 규모 가정이며 알고리즘별 정확한 비교 횟수가 아닙니다. instances는 한 프로세스 내부 입력 묶음 수이며 독립 채점 파일 개수를 합산하지 않습니다. search의 배열 크기를 바꾸면 재측정하세요. 최대 입력을 실제 채점기로 검증한 뒤 결론을 내리세요.",
    }
