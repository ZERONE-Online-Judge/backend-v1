"""Versioned password hashing; do not reuse the judge-node secret format."""

import hashlib
import hmac
import secrets
import string

ROUNDS = 600_000


def validate_password(password):
    if not 8 <= len(password) <= 128:
        raise ValueError("비밀번호는 8~128자로 입력하세요.")
    allowed = string.ascii_letters + string.digits + string.punctuation
    if any(c not in allowed for c in password):
        raise ValueError("영문·숫자·특수문자만 사용할 수 있습니다. 한글과 공백은 제외하세요.")
    if not any(c in string.ascii_letters for c in password):
        raise ValueError("영문을 1개 이상 포함하세요. 대소문자를 구분합니다.")
    if not any(c in string.digits for c in password):
        raise ValueError("숫자를 1개 이상 포함하세요.")
    if not any(c in string.punctuation for c in password):
        raise ValueError("특수문자를 1개 이상 포함하세요.")
    return password


def hash_password(password):
    validate_password(password)
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256", password.encode(), bytes.fromhex(salt), ROUNDS
    ).hex()
    return f"pbkdf2-sha256-v1${ROUNDS}${salt}${digest}"


DUMMY = hash_password("Unknown1!DummyPassword")


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
