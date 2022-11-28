from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class ScheduleJobSchema(BaseModel):
    id: Optional[int] = Field(None)
    scheduler_job_id: Optional[str]
    creator: Optional[str]
    project: Optional[str]
    quartz_cron_expression: Optional[str]
    start_time: Optional[str]
    end_time: Optional[str]
    last_run_time: Optional[datetime] = None
    last_run_state: int = 0
    created: Optional[datetime] = None
    updated: Optional[datetime] = None
    deleted: Optional[datetime] = None