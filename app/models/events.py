from datetime import datetime, timedelta, timezone
import json
from typing import Any

from pydantic import BaseModel, Field, HttpUrl, field_validator

USER_ID_MAX_LENGTH = 256
METADATA_MAX_BYTES = 10_240  # 10 KB serialized
METADATA_MAX_DEPTH = 3
METADATA_MAX_KEYS = 50
METADATA_MAX_STRING_LENGTH = 1_000


def _validate_metadata_value(v: Any, key: str, depth: int) -> None:
    """Recursively validate a single metadata value."""
    if isinstance(v, str):
        if len(v) > METADATA_MAX_STRING_LENGTH:
            raise ValueError(
                f"metadata string value for '{key}' exceeds maximum length of {METADATA_MAX_STRING_LENGTH}"
            )
    elif isinstance(v, dict):
        _validate_metadata(v, depth + 1)
    elif isinstance(v, list):
        for item in v:
            _validate_metadata_value(item, key, depth)


def _validate_metadata(value: dict[str, Any], depth: int = 0) -> None:
    """Recursively enforce metadata structure constraints."""
    if depth >= METADATA_MAX_DEPTH:
        raise ValueError(
            f"metadata exceeds maximum nesting depth of {METADATA_MAX_DEPTH}"
        )
    if len(value) > METADATA_MAX_KEYS:
        raise ValueError(f"metadata object has too many keys (max {METADATA_MAX_KEYS})")
    for k, v in value.items():
        if not isinstance(k, str):
            raise ValueError("metadata keys must be strings")
        _validate_metadata_value(v, k, depth)


class Event(BaseModel):
    event_id: str | None = Field(default=None, min_length=1, max_length=128)
    type: str
    timestamp: datetime
    user_id: str = Field(min_length=1, max_length=USER_ID_MAX_LENGTH)
    source_url: HttpUrl
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, v: dict[str, Any]) -> dict[str, Any]:
        """Reject metadata that is too large, too deep, or has oversized values."""
        serialized = json.dumps(v)
        if len(serialized.encode()) > METADATA_MAX_BYTES:
            raise ValueError(
                f"metadata exceeds maximum size of {METADATA_MAX_BYTES} bytes"
            )
        _validate_metadata(v)
        return v

    @field_validator("timestamp")
    @classmethod
    def timestamp_not_in_future(cls, v: datetime) -> datetime:
        """Reject timestamps that are in the future."""
        now = datetime.now(timezone.utc)
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        if v > now + timedelta(seconds=5):
            raise ValueError("timestamp must not be in the future")
        return v


class QueuedResponse(BaseModel):
    queued: int


class PaginatedEvents(BaseModel):
    """Paginated response for event listings."""

    total: int
    skip: int
    limit: int
    results: list[Event]
