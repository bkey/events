"""Property-based tests for Event metadata validation using Hypothesis."""

from datetime import datetime, timezone

from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import ValidationError
import pytest

from models.events import (
    METADATA_MAX_BYTES,
    METADATA_MAX_DEPTH,
    METADATA_MAX_KEYS,
    METADATA_MAX_STRING_LENGTH,
    Event,
)

VALID_BASE = {
    "type": "pageview",
    "timestamp": datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat(),
    "user_id": "user-1",
    "source_url": "https://example.com/",
}


def make_event(metadata: dict) -> Event:
    return Event(**VALID_BASE, metadata=metadata)


# --- metadata string values ---


@given(st.text(max_size=METADATA_MAX_STRING_LENGTH))
def test_metadata_string_at_or_below_limit_accepted(value: str) -> None:
    make_event({"key": value})


@given(
    st.text(
        min_size=METADATA_MAX_STRING_LENGTH + 1,
        max_size=METADATA_MAX_STRING_LENGTH + 100,
    )
)
def test_metadata_string_above_limit_rejected(value: str) -> None:
    with pytest.raises(ValidationError):
        make_event({"key": value})


# --- metadata key count ---


@given(st.integers(min_value=1, max_value=METADATA_MAX_KEYS))
def test_metadata_at_or_below_max_keys_accepted(n: int) -> None:
    make_event({str(i): "v" for i in range(n)})


@given(st.integers(min_value=METADATA_MAX_KEYS + 1, max_value=METADATA_MAX_KEYS + 10))
def test_metadata_above_max_keys_rejected(n: int) -> None:
    with pytest.raises(ValidationError):
        make_event({str(i): "v" for i in range(n)})


# --- nesting depth ---


def nested_dict(depth: int) -> dict:
    """Build a dict nested exactly `depth` levels deep."""
    d: dict = {"leaf": "value"}
    for _ in range(depth - 1):
        d = {"child": d}
    return d


@given(st.integers(min_value=1, max_value=METADATA_MAX_DEPTH))
def test_metadata_at_max_depth_accepted(depth: int) -> None:
    make_event(nested_dict(depth))


@given(st.integers(min_value=METADATA_MAX_DEPTH + 1, max_value=METADATA_MAX_DEPTH + 3))
def test_metadata_exceeding_max_depth_rejected(depth: int) -> None:
    with pytest.raises(ValidationError):
        make_event(nested_dict(depth))


# --- total size ---


@settings(max_examples=20)
@given(
    st.integers(min_value=METADATA_MAX_BYTES + 1, max_value=METADATA_MAX_BYTES + 500)
)
def test_metadata_exceeding_max_bytes_rejected(extra: int) -> None:
    # Build a value that pushes the serialized size over the limit
    padding = "x" * extra
    with pytest.raises(ValidationError):
        make_event({"key": padding})


# --- valid non-string scalar types ---


@given(
    st.one_of(
        st.integers(), st.floats(allow_nan=False, allow_infinity=False), st.booleans()
    )
)
def test_metadata_numeric_and_bool_values_accepted(value: int | float | bool) -> None:
    make_event({"key": value})


# --- lists of valid values ---


@given(st.lists(st.text(max_size=100), max_size=10))
def test_metadata_list_of_strings_accepted(values: list) -> None:
    make_event({"key": values})
