from pathlib import Path


BACKEND_ROOT = Path(__file__).resolve().parents[2]
ENV_DIR = BACKEND_ROOT / "deploy" / "env"

FILES = {
    "backend.env.example": """APP_ENV=production
PUBLIC_BASE_URL=https://judge.example.com
CORS_ALLOW_ORIGINS=https://judge.example.com
DATABASE_URL=postgresql+psycopg://zerone:password@postgres:5432/zerone
REDIS_URL=redis://redis:6379/0
ENABLE_DEMO_SEED=false
ALLOW_EMPTY_OTP=false
BOOTSTRAP_SERVICE_MASTER_EMAIL=admin@example.com
BOOTSTRAP_SERVICE_MASTER_PASSWORD=change-me-long-random-password
BOOTSTRAP_SERVICE_MASTER_NAME=Service Master
AUTH_TOKEN_SECRET=change-me-64-byte-random-token-signing-secret
AUTH_TOKEN_ISSUER=zerone-online-judge
OBJECT_STORAGE_BACKEND=minio
OBJECT_STORAGE_ENDPOINT=http://minio:9000
OBJECT_STORAGE_BUCKET=zerone
OBJECT_STORAGE_ACCESS_KEY=zerone-minio
OBJECT_STORAGE_SECRET_KEY=change-me
OBJECT_STORAGE_SECURE=false
OBJECT_STORAGE_PRESIGN_TTL_SECONDS=900
LOCAL_OBJECT_STORAGE_ROOT=/var/lib/zerone-local-objects
STAFF_ACCESS_TOKEN_TTL_SECONDS=900
STAFF_REFRESH_TOKEN_TTL_SECONDS=1209600
PARTICIPANT_ACCESS_TOKEN_TTL_SECONDS=21600
OTP_TTL_SECONDS=300
OTP_REQUEST_COOLDOWN_SECONDS=10
INTERNAL_API_ALLOWED_CIDRS=10.0.0.0/8
MAX_SOURCE_CODE_BYTES=524288
SMTP_HOST=smtp.example.com
SMTP_PORT=587
SMTP_USERNAME=change-me
SMTP_PASSWORD=change-me
SMTP_FROM_EMAIL=no-reply@example.com
SMTP_USE_TLS=true
SMTP_TIMEOUT_SECONDS=20
MAIL_WORKER_POLL_INTERVAL_SECONDS=2
MAIL_WORKER_BATCH_SIZE=20
BUNDLE_WORKER_POLL_INTERVAL_SECONDS=1
BUNDLE_WORKER_BATCH_SIZE=8
BUNDLE_WORKER_MAX_ATTEMPTS=5
""",
    "db.env.example": """POSTGRES_DB=zerone
POSTGRES_USER=zerone
POSTGRES_PASSWORD=change-me
""",
    "minio.env.example": """MINIO_ROOT_USER=zerone-minio
MINIO_ROOT_PASSWORD=change-me
""",
}


def main() -> None:
    ENV_DIR.mkdir(parents=True, exist_ok=True)
    for name, content in FILES.items():
        (ENV_DIR / name).write_text(content, encoding="utf-8")
        print(f"generated {ENV_DIR / name}")


if __name__ == "__main__":
    main()
