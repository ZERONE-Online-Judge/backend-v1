"""Read-only service configuration and measured calibration, never a verdict oracle."""

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
        json.loads((DATA / "judge_100m_benchmark.json").read_text()),
    )


def reference():
    environment, measured = _records()
    return copy.deepcopy(
        {
            "environment": environment,
            "benchmark": {
                "measured_at": measured["measuredAt"],
                "source": "https://zoj.kr" + measured["snapshotPath"],
                "iterations": measured["iterations"],
                "operation": measured["operation"],
                "method": "언어별 5회, 한 번에 한 제출, 매번 새 프로세스, 산술평균. 전체 20회 accepted. 다른 채점 작업 없음.",
                "languages": {
                    c["language"]: {
                        "sample_count": c["sampleCount"],
                        "runtime_ms": c["runtimeMs"],
                    }
                    for c in measured["cases"]
                },
            },
            "interpretation": [
                "CPU 모델·VM 코어 수만으로 TLE 여부를 판정하지 마세요. 단일 스레드 풀이의 시간을 10코어 또는 2소켓으로 나누지 마세요.",
                "N, N*log2(N), N², N³는 증가 규모입니다. estimate_runtime의 환산은 동일한 나머지·누적 반복을 가정한 가설이며 실제 명령어 수나 모든 알고리즘의 측정값이 아닙니다.",
                "최대 입력과 실제 반복문·자료구조·입출력·무한 루프·언어별 제한을 확인한 뒤 run_code로 실패/최대 테스트와 수정 후보를 검증하세요. 경계에 가까우면 반복 실행해 편차를 확인하세요.",
                "표시 시간은 격리 실행 준비·프로세스 시작을 포함하는 경과 시간입니다. 큐 대기·컴파일·checker는 제외되며, 제출 요약은 테스트별 최댓값입니다. isolate의 CPU/wall 제한 판정과 측정 구간이 다릅니다.",
                "플레이그라운드는 다른 실행 환경입니다. 그 실행 시간을 공식 채점기의 TLE 근거로 대체하지 마세요. 벤치마크의 120초·256MB는 실험 제한이며 문제 제한을 덮어쓰지 않습니다.",
            ],
        }
    )


def evidence_text():
    environment, measured = _records()
    return json.dumps(
        {"environment": environment, "benchmark": measured}, ensure_ascii=False
    )


def estimate(language, complexity, n, work_per_step):
    """Bounded scale calculation, deliberately does not return an AC/TLE prediction."""
    _, measured = _records()
    case = next((c for c in measured["cases"] if c["language"] == language), None)
    if case is None or complexity not in {"linear", "n_log_n", "quadratic", "cubic"}:
        raise ValueError("지원 언어 또는 복잡도를 선택하세요.")
    if type(n) is not int or not 1 <= n <= 1_000_000_000:
        raise ValueError("n은 1~1,000,000,000 정수여야 합니다.")
    if (
        type(work_per_step) not in (int, float)
        or not math.isfinite(work_per_step)
        or not 0 < work_per_step <= 1000
    ):
        raise ValueError("work_per_step은 0보다 크고 1000 이하인 유한수여야 합니다.")
    steps = {
        "linear": n,
        "n_log_n": n * max(1, math.log2(n)),
        "quadratic": n**2,
        "cubic": n**3,
    }[complexity]
    iterations = steps * work_per_step
    scale = iterations / measured["iterations"]
    return {
        "kind": "calibration_hypothesis_not_measurement",
        "language": language,
        "complexity": complexity,
        "n": n,
        "work_per_step_assumption": work_per_step,
        "estimated_iterations": iterations,
        "estimated_runtime_ms": {
            k: round(v * scale, 6) for k, v in case["runtimeMs"].items()
        },
        "reference_iterations": measured["iterations"],
        "reference_runtime_ms": copy.deepcopy(case["runtimeMs"]),
        "reference_source": "https://zoj.kr" + measured["snapshotPath"],
        "note": "동일 반복 비용이 선형으로 늘어난다는 가정입니다. 고정 시작 비용까지 비례 환산하므로 짧은 실행에는 부정확합니다. Big-O만으로 TLE/AC를 단정하거나 코어 수로 나누지 말고 inspect_judge의 실제 제한과 run_code 결과를 비교하세요.",
    }
