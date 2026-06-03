"""Single source of configuration for both the API process and the worker process.

The original split (pydantic settings in the API, bare ``os.getenv`` in the worker)
meant the two halves of the system could silently disagree about ``MAX_ATTEMPTS``
or the database URL. Everything now comes from this one model.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ProviderName = Literal["echo", "simulated", "openai"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- storage -------------------------------------------------------
    database_url: str = "postgresql+asyncpg://aila:aila@db:5432/aila"
    db_echo: bool = False
    auto_create_schema: bool = True

    # --- observability -------------------------------------------------
    log_level: str = "INFO"
    log_json: bool = True

    # --- API -----------------------------------------------------------
    page_size_default: int = Field(default=50, ge=1, le=500)
    page_size_max: int = Field(default=200, ge=1, le=1000)

    # --- delivery loop -------------------------------------------------
    claim_batch: int = Field(default=10, ge=1, le=1000)
    concurrency: int = Field(default=8, ge=1, le=512)
    poll_interval_seconds: float = Field(default=1.0, gt=0)
    max_attempts: int = Field(default=3, ge=1, le=50)
    lease_seconds: int = Field(default=300, ge=1)
    reap_interval_seconds: float = Field(default=5.0, gt=0)
    shutdown_grace_seconds: float = Field(default=20.0, ge=0)
    strict_conversation_order: bool = True

    # --- retry backoff -------------------------------------------------
    backoff_base_seconds: float = Field(default=2.0, gt=0)
    backoff_factor: float = Field(default=2.0, ge=1.0)
    backoff_max_seconds: float = Field(default=120.0, gt=0)
    backoff_jitter: float = Field(default=0.25, ge=0.0, le=1.0)

    # --- reply provider ------------------------------------------------
    reply_provider: ProviderName = "simulated"
    reply_min_delay_seconds: float = Field(default=5.0, ge=0)
    reply_max_delay_seconds: float = Field(default=240.0, ge=0)
    reply_fail_rate: float = Field(default=0.2, ge=0.0, le=1.0)
    reply_seed: int | None = None

    openai_api_key: str | None = None
    openai_base_url: str = "https://api.openai.com/v1"
    openai_model: str = "gpt-4o-mini"
    openai_timeout_seconds: float = Field(default=30.0, gt=0)

    @field_validator("log_level")
    @classmethod
    def _upper(cls, value: str) -> str:
        return value.upper()

    @model_validator(mode="after")
    def _check_ranges(self) -> Settings:
        if self.reply_max_delay_seconds < self.reply_min_delay_seconds:
            raise ValueError("reply_max_delay_seconds must be >= reply_min_delay_seconds")
        if self.page_size_max < self.page_size_default:
            raise ValueError("page_size_max must be >= page_size_default")
        if self.backoff_max_seconds < self.backoff_base_seconds:
            raise ValueError("backoff_max_seconds must be >= backoff_base_seconds")
        if self.reply_provider == "openai" and not self.openai_api_key:
            raise ValueError("reply_provider=openai requires openai_api_key")
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide settings, read once."""
    return Settings()
