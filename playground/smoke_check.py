"""Run inside an isolated DinD test VM, with the runner image already built.
Never executes submitted code in this process: service.execute requires gVisor.
"""

import base64
import hashlib
import json
import sys
from uuid import uuid4

import service
import atexit
import shutil
import tempfile
from pathlib import Path

service.CACHE = Path(tempfile.mkdtemp(prefix="zoj-playground-smoke-"))
atexit.register(lambda: shutil.rmtree(service.CACHE, ignore_errors=True))


def run(command, files=None, seconds=10):
    return service.execute(
        {
            "request_id": hashlib.sha256(uuid4().bytes).hexdigest(),
            "command": command,
            "files": {
                k: base64.b64encode(v.encode()).decode()
                for k, v in (files or {}).items()
            },
            "timeout_seconds": seconds,
        }
    )


original = run(
    "python main.py < input.txt", {"main.py": "print(4)\n", "input.txt": "2 3\n"}
)
assert original["stdout"].strip() == "4" and original["exit_code"] == 0, {
    k: v for k, v in original.items() if k != "files"
}
cpp = run(
    "g++ -std=c++17 main.cpp -o main && ./main < input.txt",
    {
        "main.cpp": "#include <iostream>\nint main(){int a,b;std::cin>>a>>b;std::cout<<a+b;}\n",
        "input.txt": "2 3\n",
    },
    20,
)
assert cpp["stdout"].strip() == "5" and cpp["exit_code"] == 0, {
    k: v for k, v in cpp.items() if k != "files"
}
changed = run(
    "python -c \"from pathlib import Path; Path('b.txt').write_text('new'); Path('a.txt').unlink()\"",
    {"a.txt": "old"},
)
assert (
    "a.txt" not in changed["files"]
    and base64.b64decode(changed["files"]["b.txt"]) == b"new"
)
security = run(
    "python probe.py",
    {"probe.py": """import os,socket
assert not os.path.exists('/var/run/docker.sock')
assert not os.path.exists('/results')
assert not os.path.exists('/test')
assert not any(k in os.environ for k in ('OPENAI_API_KEY','PLAYGROUND_TOKEN','DATABASE_URL'))
try: open('/proc/1/environ','rb').read(); raise AssertionError('PID 1 readable')
except PermissionError: pass
try: open('/etc/evil','w'); raise AssertionError('root filesystem writable')
except PermissionError: pass
s=socket.socket();s.settimeout(.3)
try: s.connect(('1.1.1.1',443));raise AssertionError('network open')
except OSError: pass
print('isolation verified')
"""},
)
assert (
    security["exit_code"] == 0 and "isolation verified" in security["stdout"]
), security
flood = run("python -c \"print('x'*1000000)\"")
assert flood["output_truncated"] and len(flood["stdout"]) == 12000
loop = run("while :; do :; done", seconds=1)
assert loop["timed_out"], loop
links = run("ln -s /etc/passwd stolen; echo ok > kept")
assert "stolen" not in links["files"] and "kept" in links["files"]
background = run(
    "python -c \"import os,time; p=os.fork(); time.sleep(30) if p==0 else print('parent done')\"",
    seconds=1,
)
assert background["wall_ms"] < 5000, background
print(
    json.dumps(
        {
            "passed": [
                "python",
                "cpp",
                "create-delete-files",
                "no-host-socket-secrets-network",
                "different-uid",
                "rootfs-permissions",
                "output-bound",
                "timeout",
                "symlink-exclusion",
                "background-cleanup",
            ]
        },
        ensure_ascii=False,
    )
)

restored = service.execute(
    {
        "request_id": hashlib.sha256(uuid4().bytes).hexdigest(),
        "files": cpp["files"],
        "executables": cpp["executables"],
        "command": "./main < input.txt",
        "timeout_seconds": 5,
    }
)
assert restored["exit_code"] == 0 and restored["stdout"].strip() == "5", restored[
    "stdout"
]
# Check private controller auth and exact retry reuse over HTTP.
import threading, httpx

server = service.ThreadingHTTPServer(("127.0.0.1", 0), service.Handler)
service.TOKEN = "local-test-token-" + ("x" * 40)
threading.Thread(target=server.serve_forever, daemon=True).start()
url = "http://127.0.0.1:" + str(server.server_port)
assert httpx.post(url + "/execute", json={}).status_code == 403
payload = {
    "request_id": hashlib.sha256(uuid4().bytes).hexdigest(),
    "command": "date +%s%N",
    "files": {},
    "timeout_seconds": 5,
}
headers = {"Authorization": "Bearer " + service.TOKEN}
first = httpx.post(url + "/execute", headers=headers, json=payload, timeout=30)
second = httpx.post(url + "/execute", headers=headers, json=payload, timeout=30)
assert first.status_code == second.status_code == 200
assert first.json() == second.json()
assert (
    httpx.post(
        url + "/execute",
        headers=headers,
        json={**payload, "command": "echo other"},
        timeout=10,
    ).status_code
    == 422
)
server.shutdown()
print("Executable persistence, private HTTP auth and idempotent retries passed.")

java = run(
    "javac Main.java && java Main",
    {
        "Main.java": "public class Main { public static void main(String[] args) { System.out.println(5); } }"
    },
    20,
)
assert java["exit_code"] == 0 and java["stdout"].strip().endswith("5"), {
    k: v for k, v in java.items() if k != "files"
}
print("Java compile and execution passed.")
