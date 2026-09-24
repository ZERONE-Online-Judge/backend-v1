"""Reuse costly secret verification, but always check the current node record."""
import hashlib
import hmac
import secrets
import time
from collections import OrderedDict
from threading import Lock

from app.services.security import verify_password

_pepper = secrets.token_bytes(32)
_verified = OrderedDict()
_lock = Lock()
CACHE_SECONDS = 30
MAX_ENTRIES = 256


def valid_node_credential(node, secret: str) -> bool:
    # Disabling a node or rotating its persisted hash takes effect immediately;
    # neither authorization nor the database row is cached here.
    if not node or not node.schedulable:
        return False
    fingerprint = hmac.new(_pepper, secret.encode(), hashlib.sha256).digest()
    key = (node.judge_node_id, node.node_secret_hash, fingerprint)
    now = time.monotonic()
    with _lock:
        expires = _verified.get(key)
        if expires is not None and expires > now:
            _verified.move_to_end(key)
            return True
        _verified.pop(key, None)
    if not verify_password(secret, node.node_secret_hash):
        return False
    with _lock:
        _verified[key] = time.monotonic() + CACHE_SECONDS
        _verified.move_to_end(key)
        while len(_verified) > MAX_ENTRIES:
            _verified.popitem(last=False)
    return True
