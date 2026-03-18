from datetime import datetime
from enum import Enum

from pydantic import AliasPath, BaseModel, ConfigDict, Field


class StatsPeriod(str, Enum):
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"


class RealtimeStatsBucket(BaseModel):
    """Event count by type over the realtime window."""

    type: str
    count: int


class EventStatsBucket(BaseModel):
    """A single bucket from a stats aggregation: one event type for one period."""

    model_config = ConfigDict(populate_by_name=True)

    type: str = Field(validation_alias=AliasPath("_id", "type"))
    period: datetime = Field(validation_alias=AliasPath("_id", "period"))
    count: int
