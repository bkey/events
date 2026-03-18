from celery import Celery

from config.settings import settings

celery_app = Celery(
    "events",
    broker=settings.redis_url,
    include=["tasks.events"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_ignore_result=True,
)
