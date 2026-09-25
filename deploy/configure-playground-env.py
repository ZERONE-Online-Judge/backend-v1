"""Provision a separate controller token. Never print or reuse the API/DB key."""

from pathlib import Path
import os
import secrets

root = Path(__file__).resolve().parent / "env"
backend = root / "backend.env"
text = backend.read_text()
prefix = "VERIFICATION_PLAYGROUND_TOKEN="
existing = next(
    (
        line[len(prefix) :].strip()
        for line in text.splitlines()
        if line.startswith(prefix)
    ),
    "",
)
token = existing or secrets.token_urlsafe(48)
lines = [
    line
    for line in text.splitlines()
    if not line.startswith(
        ("VERIFICATION_PLAYGROUND_TOKEN=", "VERIFICATION_PLAYGROUND_URL=")
    )
]
lines += ["VERIFICATION_PLAYGROUND_URL=http://playground-control:8090", prefix + token]
# Preserve ownership/mode of the existing production environment file.
backend.write_text("\n".join(lines) + "\n")
controller = root / "playground.env"
controller.write_text(
    "PLAYGROUND_TOKEN=" + token + "\nPLAYGROUND_IMAGE=zoj-verification-playground:1\n"
)
os.chmod(controller, 0o600)
# The repository owner must be able to read the controller env on later deployments.
os.chown(controller, backend.stat().st_uid, backend.stat().st_gid)
print("Playground endpoint and a separate private token configured.")
