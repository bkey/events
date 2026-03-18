from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings


class Settings(BaseSettings):  # type: ignore[misc]
    # MongoDB
    mongodb_url: str
    db_name: str = "events"
    events_collection: str = "events"
    dlq_collection: str = "dead_letter_events"

    # Elasticsearch
    elasticsearch_url: str
    events_index: str = "events"
    es_number_of_shards: int = 1
    es_number_of_replicas: int = 0
    es_refresh_interval: str = "1s"

    # Redis
    redis_url: str
    realtime_window_seconds: int = 300
    realtime_cache_ttl: int = 10
    realtime_cache_key: str = "stats:realtime"
    stats_cache_ttl: int = 60
    stats_cache_key_prefix: str = "stats:period"

    cache_hmac_secret: SecretStr = Field(alias="HMAC_SECRET")

    cors_allowed_origins: list[str] = []

    # App
    port: int = 8000
    max_batch_size: int = 500
    max_request_body_bytes: int = 5 * 1024 * 1024  # 5 MB
    v1_prefix: str = "/v1/events"


settings = Settings()


def get_settings() -> Settings:
    return settings
