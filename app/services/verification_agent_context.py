"""Bounded public evidence checkpoints, without additional model calls."""

import copy
import json


def clip(text, size):
    raw = str(text).encode("utf-8")
    return raw[:size].decode("utf-8", errors="ignore"), len(raw) > size


def remember(state, call, result):
    """Retain tool observations; never summarize opaque/private model reasoning."""
    name = call["name"]
    if name in {"finish_report", "finish_task"}:
        return
    try:
        args = json.loads(call.get("arguments") or "{}")
        if not isinstance(args, dict):
            args = {}
    except (ValueError, TypeError):
        args = {}
    result = copy.deepcopy(result)
    if name in {"edit_code", "workspace_candidate"}:
        for execution in result.get("executions", []):
            for field in ("judge_message", "compile_message"):
                if execution.get(field):
                    execution[field], cut = clip(execution[field], 800)
                    if cut:
                        execution["excerpt_only"] = True
            if execution.get("probe"):
                execution["probe"] = {
                    "expected_output_source": "AI hypothesis",
                    "excerpt_only": True,
                }
    if name in {"run_code", "run_probe"}:
        key = "trial:" + result.get("submission_id", call["call_id"])
        for field in ("judge_message", "compile_message"):
            if result.get(field):
                result[field], cut = clip(result[field], 1600)
                if cut:
                    result["excerpt_only"] = True
        if result.get("probe"):
            for field in ("input", "expected_output"):
                result["probe"][field], cut = clip(result["probe"].get(field, ""), 800)
                if cut:
                    result["excerpt_only"] = True
    elif name in {"read_file", "workspace_read", "search_file"}:
        key = (
            name
            + ":"
            + str(args.get("file_id", args.get("path")))
            + ":"
            + str(args.get("offset", args.get("query", "")))
        )
        if "text" in result:
            result["text"], cut = clip(result["text"], 5000)
            if cut:
                result.update(complete=False, excerpt_only=True)
    elif name == "workspace_exec":
        key = "experiment:" + result.get("request_id", call["call_id"])
        if result.get("stdout"):
            result["stdout"], cut = clip(result["stdout"], 1600)
            if cut:
                result["excerpt_only"] = True
    else:
        key = name + ":" + str(result.get("artifact_id", result.get("file_id", "")))
    # File writes/patches can be huge; their saved result already names the artifact.
    entry = {"tool": name, "result": result}
    if len(json.dumps(entry, ensure_ascii=False).encode()) > 6500:
        entry = {
            "tool": name,
            "result": {
                "excerpt_only": True,
                "note": "Large result retained in saved tool records; use file/run IDs to read only what is needed.",
            },
        }
    notes = state.setdefault("observations", {})
    notes.pop(key, None)
    notes[key] = entry
    while (
        len(notes) > 24 or len(json.dumps(notes, ensure_ascii=False).encode()) > 18000
    ):
        notes.pop(next(iter(notes)))


def compact(state, *, final=False):
    if state.get("pending"):
        return False
    history = state["history"]
    # Supports already-running jobs from the previous engine version.
    calls = {
        item.get("call_id"): item
        for item in history
        if item.get("type") == "function_call"
    }
    for item in history:
        if item.get("type") == "function_call_output" and item.get("call_id") in calls:
            try:
                remember(state, calls[item["call_id"]], json.loads(item["output"]))
            except (ValueError, TypeError, KeyError):
                continue
    checkpoint = {
        "notice": "Public evidence checkpoint. Some excerpts are shortened. Saved files, complete execution records, and immutable judge criteria remain unchanged. Do not claim omitted text was reviewed in this context.",
        "plan": state.get("plan", []),
        "findings": state.get("findings", []),
        "observations": list(state.get("observations", {}).values()),
        "artifacts": [
            {k: v for k, v in item.items() if k != "source"} | {"artifact_id": aid}
            for aid, item in state.get("artifacts", {}).items()
        ],
        "files_read": state.get("files_read", [])[-20:],
        "workspace_paths": list(state.get("workspace", {})),
        "usage": state["usage"],
        "limits": state["limits"],
        "stop_reason": state.get("stop_reason"),
    }
    if final:
        checkpoint["next"] = (
            "No more experiments. Write a useful Korean report from the evidence: confirmed facts, root-cause hypotheses, failed fixes, remaining tests, and exact stopping reason. A probe expected output is an AI hypothesis, not a validated answer."
        )
    new = [
        history[0],
        {"role": "user", "content": json.dumps(checkpoint, ensure_ascii=False)},
    ]
    if not final and len(json.dumps(new).encode()) >= len(json.dumps(history).encode()):
        return False
    state["history"] = new
    state.pop("input_checkpoint", None)
    state["compactions"] = state.get("compactions", 0) + 1
    return True
