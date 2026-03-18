import logging
from typing import Any
from urllib.parse import urlparse, urlunparse

from elasticsearch import ApiError, AsyncElasticsearch, Elasticsearch, TransportError

from config.settings import settings
from db._utils import redact_url

logger = logging.getLogger(__name__)

# Explicit mapping keeps field types stable regardless of the first document
# ingested. metadata is dynamic so arbitrary keys are indexed automatically;
# string values are mapped as `keyword` (exact match, no tokenisation) which
# is appropriate for structured data such as device type, browser, campaign IDs.
_EVENTS_MAPPING: dict[str, Any] = {
    "settings": {
        "analysis": {
            "normalizer": {
                "lowercase_normalizer": {
                    "type": "custom",
                    "filter": ["lowercase"],
                }
            }
        }
    },
    "mappings": {
        "dynamic_templates": [
            {
                "metadata_strings": {
                    "path_match": "metadata.*",
                    "match_mapping_type": "string",
                    "mapping": {
                        "type": "keyword",
                        "normalizer": "lowercase_normalizer",
                    },
                }
            }
        ],
        "properties": {
            "event_id": {"type": "keyword"},
            "type": {"type": "keyword", "normalizer": "lowercase_normalizer"},
            "timestamp": {"type": "date"},
            "user_id": {"type": "keyword", "normalizer": "lowercase_normalizer"},
            "source_url": {"type": "text"},
            "metadata": {"type": "object", "dynamic": True},
        },
    },
}


async def ensure_index(client: AsyncElasticsearch) -> None:
    """Create the events index with its mapping if it does not already exist."""
    exists = await client.indices.exists(index=settings.events_index)
    if not exists:
        await client.indices.create(
            index=settings.events_index,
            mappings=_EVENTS_MAPPING["mappings"],
            settings={
                **_EVENTS_MAPPING["settings"],
                "number_of_shards": settings.es_number_of_shards,
                "number_of_replicas": settings.es_number_of_replicas,
                "refresh_interval": settings.es_refresh_interval,
            },
        )
        logger.info("Created Elasticsearch index '%s'", settings.events_index)
    else:
        logger.info("Elasticsearch index '%s' already exists", settings.events_index)


def _parse_es_url(url: str) -> tuple[str, dict[str, Any]]:
    """Split credentials out of the URL and return (clean_url, client_kwargs).

    elasticsearch-py 9.x does not support credentials embedded in the URL —
    they must be passed via ``basic_auth``.
    """
    parsed = urlparse(url)
    kwargs: dict[str, Any] = {}
    if parsed.username and parsed.password:
        kwargs["basic_auth"] = (parsed.username, parsed.password)
        hostname = parsed.hostname or ""
        netloc = f"{hostname}:{parsed.port}" if parsed.port else hostname
        url = urlunparse(parsed._replace(netloc=netloc))
    return url, kwargs


async def connect_elasticsearch() -> AsyncElasticsearch:
    """Connect to Elasticsearch and verify the connection."""
    url, kwargs = _parse_es_url(settings.elasticsearch_url)
    client = AsyncElasticsearch(url, **kwargs)
    try:
        await client.info()
    except (TransportError, ApiError) as e:
        await client.close()
        raise RuntimeError(
            f"Could not connect to Elasticsearch at {redact_url(settings.elasticsearch_url)}: {e}"
        ) from e

    return client


def connect_elasticsearch_sync() -> Elasticsearch:
    """Connect to Elasticsearch synchronously and verify the connection."""
    url, kwargs = _parse_es_url(settings.elasticsearch_url)
    client = Elasticsearch(url, **kwargs)
    try:
        client.info()
    except (TransportError, ApiError) as e:
        client.close()
        raise RuntimeError(
            f"Could not connect to Elasticsearch at {redact_url(settings.elasticsearch_url)}: {e}"
        ) from e
    return client
