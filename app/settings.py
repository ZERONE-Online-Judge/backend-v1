from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = "development"
    release_color: str = "local"
    release_version: str | None = None
    public_base_url: str = "http://localhost:5173"
    cors_allow_origins: str = "http://localhost:5173,http://127.0.0.1:5173"
    database_url: str = "sqlite:////private/tmp/zerone_online_judge_demo_v5.db"
    enable_demo_seed: bool = False
    allow_empty_otp: bool = False
    bootstrap_service_master_email: str | None = None
    bootstrap_service_master_password: str | None = None
    bootstrap_service_master_name: str = "Service Master"
    auth_token_secret: str = "dev-auth-token-secret-change-me"
    auth_token_issuer: str = "zerone-online-judge"
    staff_access_token_ttl_seconds: int = 900
    staff_refresh_token_ttl_seconds: int = 60 * 60 * 24 * 14
    participant_access_token_ttl_seconds: int = 60 * 60 * 6
    otp_ttl_seconds: int = 300
    otp_request_cooldown_seconds: int = 10
    object_storage_backend: str = "local"
    object_storage_endpoint: str = "localhost:9000"
    object_storage_bucket: str = "zerone"
    object_storage_access_key: str = "change-me"
    object_storage_secret_key: str = "change-me"
    object_storage_secure: bool = False
    object_storage_presign_ttl_seconds: int = 900
    local_object_storage_root: str = "/private/tmp/zerone_object_storage"
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_username: str | None = None
    smtp_password: str | None = None
    smtp_from_email: str | None = None
    smtp_use_tls: bool = True
    smtp_timeout_seconds: int = 20
    mail_worker_poll_interval_seconds: float = 2.0
    mail_worker_batch_size: int = 20
    bundle_worker_poll_interval_seconds: float = 1.0
    bundle_worker_batch_size: int = 8
    bundle_worker_max_attempts: int = 5
    package_build_timeout_seconds: float = 3.0
    judge_claim_poll_interval_seconds: float = 0.5
    judge_claim_max_wait_seconds: float = 25.0
    judge_claim_max_batch_size: int = 100
    judge_lease_timeout_seconds: int = 120
    judge_node_active_window_seconds: int = 30
    feature_submission_runtime_metrics: bool = True
    feature_public_scoreboard_penalty: bool = True
    feature_emergency_notice_auto: bool = True


settings = Settings()
