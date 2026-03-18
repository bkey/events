from datetime import datetime, timezone
import uuid


def make_doc(**kwargs) -> dict:
    """Return a fully-formed event document ready for process_events / insert_many."""
    eid = str(uuid.uuid4())
    return {
        "_id": str(uuid.uuid4()),
        "event_id": eid,
        "type": "pageview",
        "timestamp": datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat(),
        "user_id": "user-123",
        "source_url": "https://example.com/",
        "metadata": {},
        **kwargs,
    }


def make_payload(**kwargs) -> dict:
    """Return an event payload as the API accepts it (no _id)."""
    doc = make_doc(**kwargs)
    doc.pop("_id")
    return doc
