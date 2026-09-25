"""Trusted PID 1 inside gVisor. The command runs as a different, unprivileged UID.

Never run this entrypoint on the API host. The controller requires runtime=runsc.
Only bounded regular workspace files survive as a JSON snapshot.
"""

import base64
import json
import os
from pathlib import Path, PurePosixPath
import resource
import selectors
import signal
import subprocess
import time

MAX_FILE = 256 * 1024
MAX_TOTAL = 2 * 1024 * 1024
MAX_FILES = 48


def safe_path(name):
    path = PurePosixPath(name)
    if (
        not name
        or path.is_absolute()
        or any(p in (".", "..") for p in name.split("/"))
        or len(name) > 160
        or "\x00" in name
    ):
        raise ValueError("invalid path")
    return path


def child_limits():
    resource.setrlimit(resource.RLIMIT_CPU, (20, 20))
    resource.setrlimit(resource.RLIMIT_FSIZE, (8 * 1024 * 1024, 8 * 1024 * 1024))
    resource.setrlimit(resource.RLIMIT_NOFILE, (128, 128))
    resource.setrlimit(resource.RLIMIT_NPROC, (48, 48))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    os.setgroups([])
    os.setgid(10001)
    os.setuid(10001)


def main():
    job = json.loads(Path("/seed/job.json").read_text())
    root = Path("/workspace")
    os.chown(root, 10001, 10001)
    for name, encoded in job["files"].items():
        dest = root / safe_path(name)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(base64.b64decode(encoded, validate=True))
        os.chmod(dest, 0o700 if name in job.get("executables", []) else 0o600)
        os.chown(dest, 10001, 10001)
        for parent in dest.parents:
            if parent == root:
                break
            os.chown(parent, 10001, 10001)
    started = time.monotonic()
    proc = subprocess.Popen(
        ["/bin/sh", "-c", job["command"]],
        cwd=root,
        env={
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "HOME": "/workspace",
            "LANG": "C.UTF-8",
            "TMPDIR": "/tmp",
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        preexec_fn=child_limits,
    )
    output = bytearray()
    truncated = False
    timed_out = False
    with selectors.DefaultSelector() as selector:
        os.set_blocking(proc.stdout.fileno(), False)
        selector.register(proc.stdout, selectors.EVENT_READ)
        while proc.poll() is None:
            if time.monotonic() - started > job["timeout_seconds"]:
                timed_out = True
                break
            for key, _ in selector.select(0.1):
                chunk = os.read(key.fd, 16384)
                if not chunk:
                    selector.unregister(key.fd)
                    continue
                space = max(0, 12000 - len(output))
                output.extend(chunk[:space])
                truncated |= len(chunk) > space
        # Kill daemonized grandchildren too. PID 1 is excluded by kill(-1).
        try:
            os.kill(-1, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait(timeout=2)
        while True:
            try:
                chunk = os.read(proc.stdout.fileno(), 16384)
            except BlockingIOError:
                break
            if not chunk:
                break
            space = max(0, 12000 - len(output))
            output.extend(chunk[:space])
            truncated |= len(chunk) > space
    files, notes, executables = {}, [], []
    total = 0
    scanned = 0
    for directory, dirs, names in os.walk(root, followlinks=False):
        dirs[:] = sorted(d for d in dirs if not (Path(directory) / d).is_symlink())[:48]
        for name in sorted(names):
            scanned += 1
            if scanned > 256:
                break
            path = Path(directory) / name
            relative = path.relative_to(root).as_posix()
            if path.is_symlink() or not path.is_file():
                notes.append(relative[:160] + ": 특수 파일 제외")
                continue
            size = path.stat().st_size
            if len(files) >= MAX_FILES or size > MAX_FILE or total + size > MAX_TOTAL:
                notes.append(relative[:160] + ": 보관 한도 초과")
                continue
            try:
                safe_path(relative)
                raw = path.read_bytes()
            except (OSError, ValueError):
                continue
            files[relative] = base64.b64encode(raw).decode()
            total += len(raw)
            if path.stat().st_mode & 0o111:
                executables.append(relative)
        if scanned > 256:
            notes.append("파일 탐색 256개 한도에 도달했습니다.")
            break
    print(
        json.dumps(
            {
                "exit_code": proc.returncode,
                "timed_out": timed_out,
                "stdout": output.decode("utf-8", errors="replace"),
                "output_truncated": truncated,
                "wall_ms": round((time.monotonic() - started) * 1000),
                "files": files,
                "executables": executables,
                "notes": notes[:50],
            },
            ensure_ascii=False,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
