from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import hashlib
import shutil
import subprocess
import tempfile

from app.models import ProblemAsset
from app.services.storage import object_storage
from app.services.store import store
from app.settings import settings


@dataclass
class PreparedProgram:
    name: str
    command: list[str]
    work_dir: Path


@dataclass
class GeneratedTest:
    display_order: int
    input_text: str
    output_text: str


class PackageBuildError(ValueError):
    pass


PACKAGE_ROLES = {
    "main-solution",
    "brute-solution",
    "wrong-solution",
    "checker",
    "validator",
    "generator",
    "manual-input",
    "test-script",
    "package-resource",
    "interactor",
}

_EXECUTABLE_SOURCE_SUFFIXES = {".cpp", ".c", ".py", ".java"}


def package_role(asset: ProblemAsset) -> str | None:
    for role in PACKAGE_ROLES:
        if f"/package-files/{role}/" in asset.storage_key:
            return role
    return None


def build_problem_package(contest_id: str, problem_id: str, script_text: str | None = None) -> dict:
    assets = store.problem_assets_for_problem(contest_id, problem_id)
    role_assets: dict[str, list[ProblemAsset]] = {}
    for asset in assets:
        role = package_role(asset)
        if role:
            role_assets.setdefault(role, []).append(asset)

    script = script_text or _latest_text_asset(role_assets.get("test-script", []))
    lines = _script_lines(script)
    if not lines:
        raise PackageBuildError("Test Script has no test lines.")

    main_asset = _single(role_assets, "main-solution")
    validator_asset = _single(role_assets, "validator")
    checker_asset = _optional_single(role_assets, "checker")
    generators = _named_assets(role_assets.get("generator", []))
    manual_inputs = _named_assets(role_assets.get("manual-input", []))
    package_resources = role_assets.get("package-resource", [])
    brute_assets = role_assets.get("brute-solution", [])
    wrong_assets = role_assets.get("wrong-solution", [])

    with tempfile.TemporaryDirectory(prefix="zoj-package-build-") as temp:
        work_root = Path(temp)
        main = _prepare_program(work_root / "main", main_asset, package_resources)
        validator = _prepare_program(work_root / "validator", validator_asset, package_resources)
        checker = _prepare_program(work_root / "checker", checker_asset, package_resources) if checker_asset else None
        brute_programs = [_prepare_program(work_root / f"brute-{index}", asset, package_resources) for index, asset in enumerate(brute_assets, start=1)]
        wrong_programs = [_prepare_program(work_root / f"wrong-{index}", asset, package_resources) for index, asset in enumerate(wrong_assets, start=1)]
        generator_programs = {name: _prepare_program(work_root / f"generator-{name}", asset, package_resources) for name, asset in generators.items()}

        generated: list[GeneratedTest] = []
        for order, line in enumerate(lines, start=1):
            parts = line.split()
            if parts[0] == "manual":
                if len(parts) < 2:
                    raise PackageBuildError(f"manual line requires a filename: line {order}")
                asset = manual_inputs.get(parts[1]) or manual_inputs.get(Path(parts[1]).stem)
                if not asset:
                    raise PackageBuildError(f"manual input not found: {parts[1]}")
                input_text = object_storage.read_text(asset.storage_key)
            else:
                generator = generator_programs.get(parts[0])
                if not generator:
                    raise PackageBuildError(f"generator not found: {parts[0]}")
                input_text = _run_program(generator, args=parts[1:], stdin="", label=f"generator {parts[0]} line {order}")

            _run_program(validator, args=[], stdin=input_text, label=f"validator test {order}", capture_stdout=False)
            output_text = _run_program(main, args=[], stdin=input_text, label=f"main solution test {order}")
            if checker:
                _run_checker(checker, work_root, order, input_text, output_text)
            for brute in brute_programs:
                brute_output = _run_program(brute, args=[], stdin=input_text, label=f"{brute.name} test {order}")
                if _normalize(brute_output) != _normalize(output_text):
                    raise PackageBuildError(f"{brute.name} differs from main solution on test {order}")
            generated.append(GeneratedTest(order, input_text, output_text))

        for wrong in wrong_programs:
            passed_all = True
            for test in generated:
                wrong_output = _run_program(wrong, args=[], stdin=test.input_text, label=f"{wrong.name} test {test.display_order}")
                if _normalize(wrong_output) != _normalize(test.output_text):
                    passed_all = False
                    break
            if passed_all:
                raise PackageBuildError(f"{wrong.name} unexpectedly passed all generated tests")

    testcase_set = store.create_testcase_set(contest_id, problem_id, is_active=True)
    created_cases = []
    for test in generated:
        input_key = f"contests/{contest_id}/problems/{problem_id}/generated/v{testcase_set.version}/{test.display_order:03d}.in"
        output_key = f"contests/{contest_id}/problems/{problem_id}/generated/v{testcase_set.version}/{test.display_order:03d}.out"
        object_storage.write_text(input_key, test.input_text)
        object_storage.write_text(output_key, test.output_text)
        case = store.add_testcase(
            contest_id=contest_id,
            problem_id=problem_id,
            testcase_set_id=testcase_set.testcase_set_id,
            display_order=test.display_order,
            input_storage_key=input_key,
            output_storage_key=output_key,
            input_sha256=hashlib.sha256(test.input_text.encode("utf-8")).hexdigest(),
            output_sha256=hashlib.sha256(test.output_text.encode("utf-8")).hexdigest(),
            time_limit_ms_override=None,
            memory_limit_mb_override=None,
        )
        created_cases.append(case.model_dump(mode="json"))

    return {
        "testcase_set": testcase_set.model_dump(mode="json"),
        "testcases": created_cases,
        "script_line_count": len(lines),
        "generated_count": len(created_cases),
        "checks": {
            "validator": True,
            "main_solution": True,
            "checker": checker_asset is not None,
            "package_resource_count": len(package_resources),
            "brute_solution_count": len(brute_assets),
            "wrong_solution_count": len(wrong_assets),
        },
    }


def _single(role_assets: dict[str, list[ProblemAsset]], role: str) -> ProblemAsset:
    asset = _optional_single(role_assets, role)
    if not asset:
        fallback_name = {
            "validator": "validator.cpp",
            "checker": "checker.cpp",
            "main-solution": "main.cpp",
        }.get(role)
        if fallback_name:
            fallback_items: list[ProblemAsset] = []
            for group in role_assets.values():
                for item in group:
                    if Path(item.original_filename).name.lower() == fallback_name:
                        fallback_items.append(item)
            if fallback_items:
                executable_items = [item for item in fallback_items if Path(item.original_filename).suffix.lower() in _EXECUTABLE_SOURCE_SUFFIXES]
                asset = sorted(executable_items or fallback_items, key=lambda item: item.created_at)[-1]
    if not asset:
        raise PackageBuildError(f"{role} file is required.")
    return asset


def _optional_single(role_assets: dict[str, list[ProblemAsset]], role: str) -> ProblemAsset | None:
    items = role_assets.get(role, [])
    if not items:
        return None
    if role in {"main-solution", "brute-solution", "wrong-solution", "checker", "validator", "generator", "interactor"}:
        executable_items = [item for item in items if Path(item.original_filename).suffix.lower() in _EXECUTABLE_SOURCE_SUFFIXES]
        return sorted(executable_items, key=lambda item: item.created_at)[-1] if executable_items else None
    return sorted(items, key=lambda item: item.created_at)[-1]


def _latest_text_asset(assets: list[ProblemAsset]) -> str:
    if not assets:
        raise PackageBuildError("Test Script is required.")
    asset = sorted(assets, key=lambda item: item.created_at)[-1]
    return object_storage.read_text(asset.storage_key)


def _named_assets(assets: list[ProblemAsset]) -> dict[str, ProblemAsset]:
    result = {}
    for asset in assets:
        path = Path(asset.original_filename)
        result[asset.original_filename] = asset
        result[path.name] = asset
        result[path.stem] = asset
    return result


def _script_lines(script: str) -> list[str]:
    return [line.strip() for line in script.splitlines() if line.strip() and not line.strip().startswith("#")]


def _prepare_program(work_dir: Path, asset: ProblemAsset, resources: list[ProblemAsset]) -> PreparedProgram:
    work_dir.mkdir(parents=True, exist_ok=True)
    _copy_package_resources(work_dir, resources)
    source_name = Path(asset.original_filename).name
    suffix = Path(source_name).suffix.lower()
    source = object_storage.read_text(asset.storage_key)
    if suffix == ".py":
        source_path = work_dir / source_name
        source_path.write_text(source, encoding="utf-8")
        return PreparedProgram(source_name, ["python3.13" if shutil.which("python3.13") else "python3", str(source_path)], work_dir)
    if suffix == ".cpp":
        source_path = work_dir / source_name
        source_path.write_text(source, encoding="utf-8")
        binary = work_dir / "main"
        _compile(["g++", "-std=c++17", "-O2", source_path.name, "-o", binary.name], work_dir, source_name)
        return PreparedProgram(source_name, [str(binary)], work_dir)
    if suffix == ".c":
        source_path = work_dir / source_name
        source_path.write_text(source, encoding="utf-8")
        binary = work_dir / "main"
        _compile(["gcc", "-std=c99", "-O2", source_path.name, "-o", binary.name], work_dir, source_name)
        return PreparedProgram(source_name, [str(binary)], work_dir)
    if suffix == ".java":
        source_path = work_dir / "Main.java"
        source_path.write_text(source, encoding="utf-8")
        _compile(["javac", "--release", "8", source_path.name], work_dir, source_name)
        return PreparedProgram(source_name, ["java", "-cp", str(work_dir), "Main"], work_dir)
    raise PackageBuildError(f"Unsupported package source type: {source_name}")


def _copy_package_resources(work_dir: Path, resources: list[ProblemAsset]) -> None:
    for resource in resources:
        resource_name = Path(resource.original_filename).name
        if not resource_name:
            continue
        target = work_dir / resource_name
        if target.exists():
            continue
        target.write_text(object_storage.read_text(resource.storage_key), encoding="utf-8")


def _compile(command: list[str], cwd: Path, label: str) -> None:
    completed = subprocess.run(command, cwd=cwd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20, check=False)
    if completed.returncode != 0:
        raise PackageBuildError(f"{label} compile failed: {(completed.stderr or completed.stdout)[-4000:]}")


def _run_program(program: PreparedProgram, args: list[str], stdin: str, label: str, capture_stdout: bool = True) -> str:
    completed = subprocess.run(
        [*program.command, *args],
        cwd=program.work_dir,
        input=stdin,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=settings.package_build_timeout_seconds,
        check=False,
    )
    if completed.returncode != 0:
        raise PackageBuildError(f"{label} failed: {(completed.stderr or completed.stdout)[-4000:]}")
    return completed.stdout if capture_stdout else ""


def _run_checker(checker: PreparedProgram, work_root: Path, order: int, input_text: str, expected: str) -> None:
    case_dir = work_root / "checker-cases"
    case_dir.mkdir(exist_ok=True)
    input_path = case_dir / f"{order:03d}.in"
    expected_path = case_dir / f"{order:03d}.out"
    participant_path = case_dir / f"{order:03d}.participant.out"
    input_path.write_text(input_text, encoding="utf-8")
    expected_path.write_text(expected, encoding="utf-8")
    participant_path.write_text(expected, encoding="utf-8")
    _run_program(checker, [str(input_path), str(expected_path), str(participant_path)], "", f"checker test {order}", capture_stdout=False)


def _normalize(value: str) -> str:
    return "\n".join(line.rstrip() for line in value.replace("\r\n", "\n").strip().split("\n"))
