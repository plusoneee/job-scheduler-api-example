from enum import Enum
from typing import Generic, TypeVar, Optional
from pydantic import BaseModel, root_validator
from pydantic.generics import GenericModel
from datetime import datetime
from typing import Union, Optional
from pydantic import BaseModel
from datetime import datetime
from pydantic import BaseModel, validator

DataT = TypeVar('DataT')

# Status Message for API Response
class StatusMessage(str, Enum):
   SUCCESS = 'SUCCESS'
   FAIL = 'FAIL'

# Scheduler Job Interval format
class IntervalSetting(BaseModel):
   weeks: int = 0
   days: int = 1
   hours: int = 0
   minutes: int = 0
   seconds: int = 0
   
   @root_validator(pre=True)
   def at_least_one_hours(cls, values):
      if values['weeks'] + values['days'] + values['hours'] == 0:
         if values['seconds'] + (values['minutes']) * 60 < 1800:
            raise ValueError('Interval time at least 30 minutes.')
      return values


# Scheduler Job Activity DateTime
class ActivityDateTime(BaseModel):
   start: Optional[Union[datetime, str]] = None
   end: Optional[Union[datetime, str]] = None

    
# Scheduler Job Input Format
class ScheduleJobInput(BaseModel):
   project: str
   task: str
   creater: str = ''
   body: Optional[dict]
   exec_immediately: bool = False
   interval: IntervalSetting
   activity_datetime: ActivityDateTime


# Scheduler Job Output Format
class ScheduleJobOutput(BaseModel):
   job_id: str
   project: str
   task: str = ''
   creater: str = ''
   interval: IntervalSetting
   activity_datetime: ActivityDateTime

# API Response Format
class DataResponse(GenericModel, Generic[DataT]):
   status: int = 1
   messages: StatusMessage = StatusMessage.SUCCESS
   detail: Optional[DataT]
    
