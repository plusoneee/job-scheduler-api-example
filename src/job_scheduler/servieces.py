from enum import Enum
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.events import EVENT_JOB_REMOVED, EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from apscheduler.job import Job
from apscheduler.triggers import interval, cron, date
from datetime import datetime
from pydantic import BaseModel, validator
from typing import List, Optional, Callable, Union
from .custom_logger import logger
from .config import get_settings

SETTINGS = get_settings()


class TriggerType(str, Enum):
    INTERVAL = 'interval'
    CRON = 'cron'
    DATE = 'date'


class JobItem(BaseModel):
    job_id: str
    name: str
    _func: Callable
    _args: Optional[Union[tuple, list]]
    _kwargs: Optional[dict]
    _coalesce: Optional[bool] = True
    trigger: TriggerType
    next_run_time: Union[datetime, str]
    
    @validator("trigger", pre=True)
    def validate_trigger_type(cls, value):
        if isinstance(value, interval.IntervalTrigger):
            return TriggerType.INTERVAL
        elif isinstance(value, cron.CronTrigger):
            return TriggerType.CRON
        elif isinstance(value, date.DateTrigger):
            return TriggerType.DATE

    def dict_from_job(schedule_job: Job) -> dict:
        jobitem = JobItem(
            job_id = schedule_job.id,
            name = schedule_job.name,
            func = schedule_job.func,
            args = schedule_job.args,
            kwargs = schedule_job.kwargs,
            trigger = schedule_job.trigger,
            coalesce = schedule_job.coalesce,
            next_run_time = schedule_job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")
        )
        return jobitem.dict()
    
        
class JobsScheduler():
    db: str = SETTINGS.db_url
    table: str = SETTINGS.db_table
    thread_num: int = SETTINGS.exec_thread_num
    process_num: int = SETTINGS.exec_process_num
    max_instances: int = SETTINGS.exec_max_instances
    timezone: str =  SETTINGS.timezone
    
    @property
    def jobstore(self):
        return SQLAlchemyJobStore(url=JobsScheduler.db, tablename=JobsScheduler.table)

    @property
    def executors(self):
        return {
            'default': ThreadPoolExecutor(JobsScheduler.thread_num),
            'processpool': ProcessPoolExecutor(JobsScheduler.process_num)
        }

    @property
    def job_defaults(self):
        return {'coalesce': False, 'max_instances': JobsScheduler.max_instances}

    def __init__(self):
        self.scheduler = BackgroundScheduler(
                jobstores= {
                    'default': self.jobstore
                }, 
                executors=self.executors, 
                job_defaults=self.job_defaults, 
                timezone=JobsScheduler.timezone
            )
        self.start()
    
    def check_job_exist(func: Callable) -> Union[None, Callable]:
        logger.debug(f'Run Function: {func.__name__}')
        
        def wrapper(self, *args):
            job_id = args[0]
            if self.scheduler.get_job(job_id):
                return func(self, *args)
            logger.warn(f'Job {job_id} Not Exist')
            return None
        
        return wrapper

    def add_date_job(
        self,
        job: Callable,
        job_id: str,
        start_date: Union[datetime, str],
        args: Union[tuple, list] = None,
        kwargs: dict = None) -> dict:
        
        job = self.scheduler.add_job(
            job,
            id=job_id,
            trigger='date',
            run_date=start_date,
            args=args,
            kwargs=kwargs,
            replace_existing=True)
        
        return JobItem.dict_from_job(job)

    def add_interval_job(
        self,
        job: Callable, 
        job_id: str, 
        start_date: Union[datetime, str] = None, 
        end_date: Union[datetime, str] = None,
        weeks: int = 0,
        days: int = 0,
        hours: int = 0, 
        minutes: int = 0,
        seconds: int = 0,
        exec_immediately: bool = False,
        jitter: int = 200,
        args: tuple = None,
        kwargs: dict = None) -> dict:
        
        if exec_immediately:
            self.add_date_job(job, job_id=job_id, args=args, start_date=datetime.now())

        job = self.scheduler.add_job(
            job, 
            'interval', 
            id=job_id,
            weeks=weeks,
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            start_date=start_date,
            end_date=end_date,
            replace_existing=True,
            args=args,
            kwargs=kwargs,
            jitter=jitter)

        return JobItem.dict_from_job(job)

    def show_jobs(self) -> None:
        self.scheduler.print_jobs()

    def job_to_dictionary(self, job: Job) -> dict:
        return JobItem.dict_from_job(job)

    def get_job(self, job_id) -> dict:
        job = self.scheduler.get_job(job_id=job_id)
        if not job is None:
            return JobItem.job_to_response(job)

    def list_jobs_dict(self) -> List[dict]:
        jobs = self.jobstore.get_all_jobs()
        return [self.job_to_dictionary(job) for job in jobs]

    def list_jobs(self) -> List[Job]:
        return self.jobstore.get_all_jobs()

    @check_job_exist
    def pause_job(self, job_id) -> None:
        self.scheduler.pause_job(job_id=job_id)
    
    @check_job_exist
    def resume_job(self, job_id) -> None:
        self.scheduler.resume_job(job_id=job_id)
        
    @check_job_exist
    def remove_job(self, job_id) -> None:
        self.scheduler.remove_job(job_id=job_id)

    def remove_jobs(self, job_ids: List[str]) -> None:
        for job_id in job_ids:
            self.remove_job(job_id=job_id)
    
    def empty_jobs(self) -> None:
        self.scheduler.remove_all_jobs()
    
    def restart(self) -> None:
        if self.scheduler.state == 0:
            logger.info('Scheduler is not running, Restart it now.')
            self.scheduler.start(paused=True)
            for job in self.list_jobs():
                self.resume_job(job.id)

    def start(self) -> None:
        if self.scheduler.state == 0:
            logger.info('Scheduler is not running, Start it now.')
            self.scheduler.start()
    
    def shutdown(self) -> None:
        if not self.scheduler.state == 0:
            logger.warning('Scheduler prepare to shutdown. Pause all jobs before shutdown ...')
            
            for job in self.list_jobs():
                self.pause_job(job.id)

            logger.warning('Scheduler has been shutdown')
            self.scheduler.shutdown()
            
    def add_event_listener(self, callback, mask):
        self.scheduler.add_listener(callback, mask)
           
class JobEventCallback:
    
    @staticmethod
    def _remove(event):
        print(f'Remove Job: [{ event.job_id}]')

    @staticmethod
    def _execute(event):
        if event.exception is None:
            function_return_value = event.retval
            print(f'Execute Job:[{event.job_id}] successfully @{event.scheduled_run_time}')
            print(function_return_value)
        else:
            print(f'Exception: {event.exception}')
            
            
if __name__ == "__main__":
    
    import random
    
    def example_job():
        print('** Run example job**')

    # create scheduler instance
    scheduler = JobsScheduler()
    
    # add listener
    scheduler.add_event_listener(JobEventCallback._remove, EVENT_JOB_REMOVED)
    scheduler.add_event_listener(JobEventCallback._execute, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    
    # show (print) all jobs
    scheduler.show_jobs()
    
    project_job_id = ''.join(random.sample('qwertyuiopasdfghjkl;zxcvbnm', 20))

    # add a interval job
    scheduler.add_interval_job(job=example_job, job_id=project_job_id, seconds=10, exec_immediately=True)
    
    # list all jobs [dictionary]
    scheduler.list_jobs_dict()

    # remove a job by id
    scheduler.remove_job(project_job_id)
    
    # shutdown schedule, every job will be pause.
    scheduler.shutdown()

    # restart schedule if want to resume pause job.
    scheduler.restart()

    # show (print) all jobs
    scheduler.show_jobs()
