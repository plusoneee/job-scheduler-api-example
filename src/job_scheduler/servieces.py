from enum import Enum
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.job import Job
from apscheduler.triggers import interval, cron, date
from datetime import datetime
from pydantic import BaseModel, validator
from typing import List, Optional, Callable, Union
from .custom_logger import logger


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

    def job_to_response(schedule_job: Job) -> dict:
        job = JobItem(
            job_id = schedule_job.id,
            name = schedule_job.name,
            func = schedule_job.func,
            args = schedule_job.args,
            kwargs = schedule_job.kwargs,
            trigger = schedule_job.trigger,
            coalesce = schedule_job.coalesce,
            next_run_time = schedule_job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")
        )
        return job.dict()
    
        
class JobsScheduler():
    db: str = 'sqlite:///event_scheduler.db'
    thread_num: int = 10
    process_num: int = 10
    max_instances: int = 20
    timezone: str = "Asia/Taipei"

    @property
    def jobstore(self):
        return SQLAlchemyJobStore(url=JobsScheduler.db, tablename=self.project_name)

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
        self.project_name: str ='apscheduler_jobs' 
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
        
        return JobItem.job_to_response(job)

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

        return JobItem.job_to_response(job)

    def show_jobs(self) -> None:
        self.scheduler.print_jobs()

    def job_to_dictionary(self, job: Job) -> dict:
        return JobItem.job_to_response(job)

    def get_job(self, job_id) -> dict:
        job = self.scheduler.get_job(job_id=job_id)
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
        if not self.scheduler.running:
            logger.info('Scheduler is not running, Restart it now.')
            self.scheduler.start(paused=True)
            for job in self.list_jobs():
                self.resume_job(job.id)

    def start(self) -> None:
        if not self.scheduler.running:
            logger.info('Scheduler is not running, Start it now.')
            self.scheduler.start()
    
    def shutdown(self) -> None:
        if self.scheduler.running:
            logger.warning('Scheduler prepare to shutdown. Pause all jobs before shutdown ...')
            
            for job in self.list_jobs():
                self.pause_job(job.id)

            logger.warning('Scheduler has been shutdown')
            self.scheduler.shutdown()


if __name__ == "__main__":

    import random
    
    def example_job():
        print('** Run example job**')

    # create scheduler instance
    scheduler = JobsScheduler()

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