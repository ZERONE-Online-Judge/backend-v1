"""Versioned password hashing; do not reuse the judge-node secret format."""

import hashlib
import hmac
import secrets

ROUNDS = 600_000


def validate_password(password):
    if not 15 <= len(password) <= 128 or not password.strip():
        raise ValueError(
            "비밀번호는 15~128자로 입력하세요. 공백과 한글을 사용할 수 있습니다."
        )
    if password.lower() in {
        "123456789012345",
        "1234567890123456",
        "passwordpassword",
        "qwertyuiopasdfgh",
        "aaaaaaaaaaaaaaa",
    }:
        raise ValueError("너무 흔한 비밀번호입니다. 다른 비밀번호를 사용하세요.")
    try:
        password.encode("utf-8")
    except UnicodeError:
        raise ValueError("비밀번호의 문자를 확인하세요.")
    return password


def hash_password(password):
    validate_password(password)
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256", password.encode(), bytes.fromhex(salt), ROUNDS
    ).hex()
    return f"pbkdf2-sha256-v1${ROUNDS}${salt}${digest}"


DUMMY = hash_password("unknown account dummy password")


def verify_password(password, encoded):
    candidate = encoded or DUMMY
    try:
        algorithm, rounds, salt, expected = candidate.split("$")
        if (
            algorithm != "pbkdf2-sha256-v1"
            or int(rounds) != ROUNDS
            or len(salt) != 32
            or len(expected) != 64
        ):
            return False
        actual = hashlib.pbkdf2_hmac(
            "sha256", password.encode(), bytes.fromhex(salt), ROUNDS
        ).hex()
        return bool(encoded) and hmac.compare_digest(actual, expected)
    except (ValueError, UnicodeError):
        return False
