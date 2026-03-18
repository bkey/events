def enqueue_events(docs: list[dict[str, object]]) -> None:
    """Enqueue a batch of event documents for async processing.

    Raises OperationalError if the broker is unreachable.

    This is a placeholder. In the future, we would enqueue to SQS
    """
    from tasks.events import process_events  # lazy import to avoid circular dependency

    process_events.delay(docs)
