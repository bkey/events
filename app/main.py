from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config.logging_config import configure_logging

configure_logging()

from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from config.limiter import limiter
from config.settings import settings
from db.elastic import connect_elasticsearch, ensure_index
from db.mongo import connect_db, ensure_indexes
from db.redis import connect_redis
from middleware.request_size import RequestSizeLimitMiddleware
from middleware.trace_id import TraceIDMiddleware
from routers.v1.events import router as events_router
from routers.v1.search import router as search_router
from routers.v1.stats import router as stats_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    app.state.mongodb_client = await connect_db()
    await ensure_indexes(app.state.mongodb_client)

    try:
        app.state.elasticsearch_client = await connect_elasticsearch()
        await ensure_index(app.state.elasticsearch_client)
    except RuntimeError:
        logger.warning("Elasticsearch unavailable at startup — search will return 503")
        app.state.elasticsearch_client = None

    try:
        app.state.redis_client = await connect_redis()
    except RuntimeError:
        logger.warning(
            "Redis unavailable at startup — realtime stats will not be cached"
        )
        app.state.redis_client = None

    yield

    await app.state.mongodb_client.close()
    if app.state.elasticsearch_client is not None:
        await app.state.elasticsearch_client.close()
    if app.state.redis_client is not None:
        await app.state.redis_client.aclose()


def create_app() -> FastAPI:
    app = FastAPI(version="1", lifespan=lifespan)
    app.add_middleware(TraceIDMiddleware)
    app.add_middleware(RequestSizeLimitMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allowed_origins,
        allow_credentials=False,
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type", "X-Trace-ID"],
    )
    app.include_router(events_router, prefix=settings.v1_prefix)
    app.include_router(search_router, prefix=settings.v1_prefix)
    app.include_router(stats_router, prefix=settings.v1_prefix)
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore[arg-type]

    return app


if __name__ == "__main__":
    import uvicorn

    logger.info(f"Starting application on port {settings.port}")
    uvicorn.run(
        app=create_app(),
        host="0.0.0.0",  # noqa: S104
        port=settings.port,
        reload=False,
        loop="uvloop",
    )
