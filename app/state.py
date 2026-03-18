from dataclasses import dataclass, field

from elasticsearch import AsyncElasticsearch
from pymongo import AsyncMongoClient
from redis.asyncio import Redis


@dataclass
class AppState:
    mongodb_client: AsyncMongoClient[dict[str, object]] | None = field(default=None)
    elasticsearch_client: AsyncElasticsearch | None = field(default=None)
    redis_client: Redis | None = field(default=None)
