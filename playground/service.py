"""Private stateless controller; only fixed gVisor containers can execute code.

The Docker socket is present ONLY in this control service. Sandbox containers
have no socket, host mounts, application secrets, or network. Never fall back to runc.
"""

import base64
import hashlib
import hmac
import io
import json
import os
from pathlib import Path, PurePosixPath
import tarfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx

TOKEN = os.environ.get("PLAYGROUND_TOKEN", "")
IMAGE = os.environ.get("PLAYGROUND_IMAGE", "zoj-verification-playground:1")
CACHE = Path("/results")
LOCK = threading.Lock()
MAX_BODY = 3 * 1024 * 1024
LABEL = "org.zoj.verification-playground"


def validate_files(files):
    if not isinstance(files, dict) or len(files) > 48:
        raise ValueError("파일 수 한도 초과")
    total = 0
    for name, data in files.items():
        if (
            not isinstance(name, str)
            or not name
            or len(name) > 160
            or PurePosixPath(name).is_absolute()
            or any(p in ("", ".", "..") for p in name.split("/"))
            or "\x00" in name
        ):
            raise ValueError("허용되지 않은 파일 경로")
        raw = base64.b64decode(data, validate=True)
        if len(raw) > 256 * 1024:
            raise ValueError("파일 크기 한도 초과")
        total += len(raw)
    if total > 2 * 1024 * 1024:
        raise ValueError("작업 공간 크기 한도 초과")


def docker_client():
    return httpx.Client(
        transport=httpx.HTTPTransport(uds="/var/run/docker.sock"),
        base_url="http://docker/v1.45",
        timeout=35,
    )


def ready(client):
    info = client.get("/info")
    info.raise_for_status()
    if "runsc" not in info.json().get("Runtimes", {}):
        raise ValueError(
            "gVisor(runsc) 설치가 필요합니다. 기본 Docker 실행으로 대체하지 않습니다."
        )
    result = client.get("/images/" + IMAGE + "/json")
    if result.status_code != 200:
        raise ValueError("플레이그라운드 실행 이미지를 먼저 빌드하세요.")


def create_spec():
    return {
        "Image": IMAGE,
        "WorkingDir": "/workspace",
        "NetworkDisabled": True,
        "Labels": {LABEL: "true"},
        "Env": ["PYTHONDONTWRITEBYTECODE=1"],
        "HostConfig": {
            "Runtime": "runsc",
            "NetworkMode": "none",
            "Memory": 768 * 1024 * 1024,
            "MemorySwap": 768 * 1024 * 1024,
            "NanoCpus": 1_000_000_000,
            "PidsLimit": 64,
            "CapDrop": ["ALL"],
            "CapAdd": ["CHOWN", "DAC_OVERRIDE", "SETUID", "SETGID", "KILL"],
            "SecurityOpt": ["no-new-privileges:true"],
            "ShmSize": 8 * 1024 * 1024,
            "Tmpfs": {
                "/workspace": "rw,exec,nosuid,nodev,size=32m,mode=0755",
                "/tmp": "rw,nosuid,nodev,size=32m,mode=1777",
            },
            "LogConfig": {
                "Type": "json-file",
                "Config": {"max-size": "4m", "max-file": "1"},
            },
        },
    }


def bounded_body(response, maximum=4 * 1024 * 1024):
    content = bytearray()
    for chunk in response.iter_bytes():
        content.extend(chunk)
        if len(content) > maximum:
            raise ValueError("실행 출력 크기 한도 초과")
    return bytes(content)


def demux(raw):
    result = bytearray()
    while len(raw) >= 8:
        length = int.from_bytes(raw[4:8], "big")
        result.extend(raw[8 : 8 + length])
        raw = raw[8 + length :]
    return bytes(result)


def execute(job):
    ident = job.get("request_id")
    if (
        not isinstance(ident, str)
        or len(ident) != 64
        or any(c not in "0123456789abcdef" for c in ident)
    ):
        raise ValueError("잘못된 요청 ID")
    command = job.get("command")
    if not isinstance(command, str) or not 1 <= len(command.encode()) <= 8000:
        raise ValueError("명령 길이 한도 초과")
    seconds = job.get("timeout_seconds")
    if type(seconds) is not int or not 1 <= seconds <= 30:
        raise ValueError("실행 제한은 1~30초입니다.")
    validate_files(job.get("files"))
    job.setdefault("executables", [])
    if (
        not isinstance(job["executables"], list)
        or len(job["executables"]) > 48
        or any(name not in job["files"] for name in job["executables"])
    ):
        raise ValueError("잘못된 실행 파일 목록")
    request_hash = hashlib.sha256(json.dumps(job, sort_keys=True).encode()).hexdigest()
    CACHE.mkdir(exist_ok=True)
    cache = CACHE / (ident + ".json")
    if cache.exists():
        value = json.loads(cache.read_text())
        if value["request_hash"] != request_hash:
            raise ValueError("요청 ID가 다른 작업에 재사용되었습니다.")
        return value["result"]
    # A marker prevents an uncertain retry from silently executing code twice.
    marker = CACHE / (ident + ".started")
    if marker.exists():
        raise ValueError(
            "이 실행은 중단되었습니다. 실행 기록을 확인하고 새 명령을 요청하세요."
        )
    with docker_client() as client:
        ready(client)
        # Remove only our expired sandbox containers (e.g. a controller crash).
        old = client.get(
            "/containers/json",
            params={"all": "true", "filters": json.dumps({"label": [LABEL + "=true"]})},
        ).json()
        for item in old:
            if time.time() - item.get("Created", 0) > 120:
                client.delete(
                    "/containers/" + item["Id"], params={"force": "true", "v": "true"}
                )
        for path in CACHE.iterdir():
            if time.time() - path.stat().st_mtime > 86400 and path.suffix in {
                ".json",
                ".started",
            }:
                path.unlink(missing_ok=True)
        marker.write_text(request_hash)
        container_id = None
        try:
            created = client.post("/containers/create", json=create_spec())
            created.raise_for_status()
            container_id = created.json()["Id"]
            seed = json.dumps(
                {
                    k: job[k]
                    for k in ("files", "command", "timeout_seconds", "executables")
                }
            ).encode()
            archive = io.BytesIO()
            with tarfile.open(fileobj=archive, mode="w") as tar:
                info = tarfile.TarInfo("job.json")
                info.size, info.mode = len(seed), 0o444
                tar.addfile(info, io.BytesIO(seed))
            client.put(
                "/containers/" + container_id + "/archive",
                params={"path": "/seed"},
                content=archive.getvalue(),
                headers={"Content-Type": "application/x-tar"},
            ).raise_for_status()
            client.post("/containers/" + container_id + "/start").raise_for_status()
            deadline = time.monotonic() + seconds + 8
            state = {}
            while time.monotonic() < deadline:
                state = client.get("/containers/" + container_id + "/json").json()[
                    "State"
                ]
                if not state["Running"]:
                    break
                time.sleep(0.1)
            if state.get("Running"):
                client.post("/containers/" + container_id + "/kill")
                raise ValueError("격리 실행의 전체 시간 한도를 초과했습니다.")
            if state.get("OOMKilled"):
                raise ValueError("플레이그라운드 메모리 한도를 초과했습니다.")
            with client.stream(
                "GET",
                "/containers/" + container_id + "/logs",
                params={"stdout": "true", "stderr": "true"},
            ) as response:
                response.raise_for_status()
                raw = demux(bounded_body(response))
            try:
                result = json.loads(raw)
            except ValueError:
                raise ValueError(
                    "격리 실행이 정상 결과를 반환하지 않았습니다."
                ) from None
            validate_files(result.get("files"))
            result["runtime"] = "gvisor"
            result["network"] = "disabled"
            # Atomic checkpoint; retries after a network loss reuse exactly this result.
            temp = CACHE / (ident + ".tmp")
            temp.write_text(
                json.dumps({"request_hash": request_hash, "result": result})
            )
            temp.replace(cache)
            return result
        finally:
            if container_id:
                client.delete(
                    "/containers/" + container_id, params={"force": "true", "v": "true"}
                )


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass  # Never log code, filenames, headers or tokens.

    def reply(self, status, value):
        raw = json.dumps(value, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path != "/health":
            self.reply(404, {"error": "not found"})
            return
        try:
            with docker_client() as client:
                ready(client)
            self.reply(200, {"ready": True, "runtime": "gvisor"})
        except Exception:
            self.reply(
                503,
                {"ready": False, "error": "격리 런타임 또는 이미지 준비가 필요합니다."},
            )

    def do_POST(self):
        if (
            self.path != "/execute"
            or len(TOKEN) < 32
            or not hmac.compare_digest(
                self.headers.get("Authorization", ""), "Bearer " + TOKEN
            )
        ):
            self.reply(403, {"error": "forbidden"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= MAX_BODY:
                raise ValueError("요청 크기 한도 초과")
            self.connection.settimeout(10)
            job = json.loads(self.rfile.read(length))
        except (ValueError, TimeoutError):
            self.reply(422, {"error": "잘못된 실행 요청"})
            return
        if not LOCK.acquire(blocking=False):
            self.reply(429, {"error": "다른 검증 작업 실행 중"})
            return
        try:
            result = execute(job)
            self.reply(200, result)
        except ValueError as error:
            self.reply(422, {"error": str(error)})
        except Exception:
            self.reply(503, {"error": "플레이그라운드 실행 환경을 확인하세요."})
        finally:
            LOCK.release()


if __name__ == "__main__":
    if len(TOKEN) < 32:
        raise SystemExit("PLAYGROUND_TOKEN must contain at least 32 characters")
    ThreadingHTTPServer(("0.0.0.0", 8090), Handler).serve_forever()
