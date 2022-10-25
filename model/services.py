from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.job import Job

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
    max_instances: int = 20

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

    def check_job(func):
        def wrapper(self, *args):
            job_id = args[0]
            if self.scheduler.get_job(job_id):
                return func(self, *args)
            
            print(f'[Warning]: {func.__name__}: Job {job_id} Not Exist')
            return None
        return wrapper

    def add_date_job(
        self,
        job: Callable,
        id: str,
        start_date: Union[datetime, str],
        args: Union[tuple, list] = None,
        kwargs: dict = None
        ):

        print('+ Add Date Job')
        self.scheduler.add_job(
            job,
            id=id,
            trigger='date',
            run_date=start_date,
            args=args,
            kwargs=kwargs
        )

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
        seconds: int = 0,
        exec_immediately: bool = False,
        args: tuple = None,
        kwargs: dict = None
        ):

        if exec_immediately:
            print('+ Run Job Immdiately')
            now = datetime.now()
            self.add_date_job(job, id=id, start_date=now)

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
            replace_existing=True,
            args=args,
            kwargs=kwargs
            )
        
    def show_jobs(self):
        self.scheduler.print_jobs()

    def list_jobs_dict(self) -> List[dict]:
        jobs = self.jobstore.get_all_jobs()
        return [self.job_to_dictionary(job) for job in jobs]

    def list_jobs(self) -> List[Job]:
        return self.jobstore.get_all_jobs()

    def job_to_dictionary(self, job: Job) -> dict:
        return JobModel.create(job)

    @check_job
    def remove_job(self, id):
        print(f'- Remove Job [{id}]')
        self.scheduler.remove_job(id)

    @check_job
    def pause_job(self, id):
        print(f'- Pause Job [{id}]')
        self.scheduler.pause_job(job_id=id)
    
    @check_job
    def resume_job(self, id):
        print(f'- Resume Job [{id}]')
        self.scheduler.resume_job(job_id=id)

    @check_job
    def update_job(self):
        print(f'- Resume Job [{id}]')
        pass
    
    def restart(self):
        if not self.scheduler.running:
            print('*** Scheduler is not running, Restart it now ***')
            self.scheduler.start(paused=True)

            for job in self.list_jobs():
                self.resume_job(job.id)

    def start(self):
        if not self.scheduler.running:
            print('*** Scheduler is not running, Start it now ***')
            self.scheduler.start()
    
    def shutdown(self):
        if self.scheduler.running:
            print('*** Scheduler prepare to shutdown ***')
            # pause all jobs
            print('Pause all jobs before shutdown')
            for job in self.list_jobs():
                self.pause_job(job.id)

            print('*** Scheduler Shutdown Now ***')
            self.scheduler.shutdown()
        
def q_job():
    print('** Run job [1] **')
    time.sleep(10)

if __name__ == "__main__":

    import random

    # create scheduler instance
    scheduler = JobsScheduler()

    # show (print) all jobs
    scheduler.show_jobs()
    
    project_job_id = ''.join(random.sample('qwertyuiopasdfghjkl;zxcvbnm', 20))

    # add a interval job
    scheduler.add_interval_job(job=q_job, id=project_job_id, seconds=10, exec_immediately=True)
    
    # remove a job by id
    scheduler.remove_job(id=project_job_id)
    
    # shutdown schedule, every job will be pause.
    scheduler.shutdown()

    # restart schedule if want to resume pause job.
    scheduler.restart()

    # show (print) all jobs
    scheduler.show_jobs()
