from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.job import Job
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

import time
from datetime import datetime
from pydantic import BaseModel
from typing import List, Optional, Callable, Union

class JobModel(BaseModel):
    id: str 
    name: str
    func: Callable
    args: Optional[Union[tuple, list]]
    kwargs: Optional[dict]
    coalesce: Optional[bool] = True
    trigger: str = 'interval'
    next_run_time: Union[datetime, str]

    def check_trigger(trigger):

        if isinstance(trigger, IntervalTrigger):
            return'interval'
        elif isinstance(trigger, CronTrigger):
            return 'cron'
        elif isinstance(trigger, DateTrigger):
            return 'date'

    def create(schedule_job: Job) -> dict:
        trigger = JobModel.check_trigger(schedule_job.trigger)
        job = JobModel(
            id = schedule_job.id,
            name = schedule_job.name,
            func = schedule_job.func,
            args = schedule_job.args,
            kwargs = schedule_job.kwargs,
            trigger = trigger,
            coalesce = schedule_job.coalesce,
            next_run_time = schedule_job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")
        )
        return job.dict()
        
class JobsScheduler():

    db: str = 'sqlite:///event_scheduler.db'
    thread_num: int = 10
    process_num: int = 10
    max_instances: int = 5

    def __init__(self):    
        self.project_name: str ='apscheduler_jobs'  # project name, one table for one project
        self.jobstore = SQLAlchemyJobStore(
                url=JobsScheduler.db,
                tablename=self.project_name
            )

        self.executors = {
            'default': ThreadPoolExecutor(JobsScheduler.thread_num),
            'processpool': ProcessPoolExecutor(JobsScheduler.process_num)
        }

        job_defaults = {
            'coalesce': False,
            'max_instances': JobsScheduler.max_instances
        }

        self.scheduler = BackgroundScheduler(
                jobstores= {
                    'default': self.jobstore
                }, 
                executors=self.executors, 
                job_defaults=job_defaults, 
                timezone="Asia/Taipei"
            )

        self.start()

    def add_interval_job(
        self,
        job: Callable, 
        id: str, 
        start_date: Union[datetime, str] = None, 
        end_date: Union[datetime, str] = None,
        weeks: int = 0,
        days: int = 0,
        hours: int = 0, 
        minutes: int = 0,
        seconds: int = 0
        ):

        print('+ Add Interval Job')
        job = self.scheduler.add_job(
            job, 
            'interval', 
            id=id, 
            weeks=weeks,
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            start_date=start_date,
            end_date=end_date,
            replace_existing=True
            )
    
        if job.pending: job.modify(start_date=datetime.now())
        
    def show_jobs(self):
        self.scheduler.print_jobs()

    def list_jobs(self) -> List[dict]:
        jobs = self.jobstore.get_all_jobs()
        job_list = [JobModel.create(job) for job in jobs]
        return job_list

    def get_job(self, id):
        job = self.scheduler.get_job(id)
        return JobModel.create(job)

    def remove_job(self, id):
        print(f'- Remove Job [{id}]')
        job = self.scheduler.get_job(id)
        if job:
            job.remove()
        
    def start(self):
        if not self.scheduler.running:
            print('*** Scheduler is not running, start it now ***')
            self.scheduler.start()
    
    def shutdown(self):
        if self.scheduler.running:
            print('*** Scheduler shutdown now ***')
            self.scheduler.shutdown()

    def update_job(self):
        pass

def myjob():
    print('run job [1]')
    time.sleep(5)

if __name__ == "__main__":

    scheduler = JobsScheduler()
    # show all jobs
    scheduler.show_jobs()
    
    # add a interval job
    scheduler.add_interval_job(job=myjob, id='01', seconds=5)
    print(scheduler.get_job(id='01'))

    # remove a job by id
    scheduler.remove_job(id='01')
    scheduler.show_jobs()

    # shutdown schedule
    scheduler.shutdown()
