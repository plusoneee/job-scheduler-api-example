from datetime import datetime
from typing import List, Callable, Union

from apscheduler import events
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.job import Job
from apscheduler.triggers.cron import CronTrigger

from Application.config import settings

class JobListenerTool:

    def __init__(self, scheduler):
        self.ap_scheduler = scheduler
        self.add_event_listener()

    def add_event_listener(self):
        self.add_executed_listener(self.ap_scheduler)
        self.add_remove_listener(self.ap_scheduler)
        self.add_error_listener(self.ap_scheduler)
        self.add_empty_listener(self.ap_scheduler)

    def _removed(self, event):     
        pass

    def _executed(self, event):
        pass
    
    @staticmethod
    def _empty(event):
        pass

    @staticmethod
    def _error(event):
        pass

    def add_remove_listener(self, scheduler):
        scheduler.add_listener(
            self._removed,
            events.EVENT_JOB_REMOVED
        )

    def add_executed_listener(self, scheduler):
        scheduler.add_listener(
            self._executed, 
            events.EVENT_JOB_EXECUTED
        )

    def add_error_listener(self, scheduler):
        scheduler.add_listener(
            self._error, 
            events.EVENT_JOB_ERROR
        )

    def add_submitted_listener(self, scheduler):
        scheduler.add_listener(
            self._submitted, 
            events.EVENT_JOB_SUBMITTED
        )

    def add_empty_listener(self, scheduler):
        scheduler.add_listener(
            self._empty, 
            events.EVENT_ALL_JOBS_REMOVED
        )


class JobsScheduler():
    
    SETTINGS = settings
    
    db_conn: str = f"postgresql+psycopg2://{SETTINGS.db_user}:@{SETTINGS.db_url}:{SETTINGS.db_port}/{SETTINGS.db_name}"
    table: str = 'schedule_run_job'
    timezone: str = SETTINGS.timezone
    thread_num: int = 5
    process_num: int = 5
    max_instances: int = 10

    @property
    def jobstore(self):
        return SQLAlchemyJobStore(
            url=JobsScheduler.db_conn, 
            tablename=JobsScheduler.table,
            engine_options={
                "pool_pre_ping":True
                }
            )

    @property
    def executors(self):
        return {
            'default': ThreadPoolExecutor(JobsScheduler.thread_num),
            'processpool': ProcessPoolExecutor(JobsScheduler.process_num)
        }

    @property
    def job_defaults(self):
        return {
            'coalesce': False, 
            'max_instances': JobsScheduler.max_instances
            }

    @property
    def scheduler(self):
        return self._scheduler

    def __init__(self):
        self._scheduler = BackgroundScheduler(
                jobstores= {
                    'default': self.jobstore
                }, 
                executors=self.executors, 
                job_defaults=self.job_defaults, 
                timezone=JobsScheduler.timezone
            )
        
        self.start()

    def check_job_exist(func: Callable) -> Union[None, Callable]:
        
        def wrapper(self, *args):
            job_id = args[0]
            if self.scheduler.get_job(job_id):
                return func(self, *args)
            return None
        
        return wrapper
    
    def add_date_job(
        self,
        job: Callable,
        job_id: str,
        start_date: Union[datetime, str],
        args: Union[tuple, list] = None,
        kwargs: dict = None
    ) -> dict:
        
        job = self.scheduler.add_job(
            job,
            id=job_id,
            trigger='date',
            run_date=start_date,
            args=args,
            kwargs=kwargs,
            replace_existing=True)
        
        return 

    def add_cron_job(
        self,
        job: Callable,
        job_id: str,
        quartz_cron_expression: str,
        args: list = None,
        replace_existing=True
    ) -> dict:

        job = self.scheduler.add_job(
            job, 
            CronTrigger.from_crontab(quartz_cron_expression),
            id=job_id,
            args=args,
            coalesce=True,
            replace_existing=True
            )

        return


    def add_interval_job(
        self,
        job: Callable, 
        job_id: str, 
        start_date: Union[datetime, str] = None, 
        end_date: Union[datetime, str] = None,
        days: int = 0,
        hours: int = 0, 
        minutes: int = 0,
        exec_immediately: bool = False,
        args: tuple = None,
        kwargs: dict = None) -> dict:
        
        if exec_immediately:
            self.add_date_job(
                job, 
                job_id=job_id, 
                args=args, 
                start_date=datetime.now()
                )

        job = self.scheduler.add_job(
            job, 
            'interval', 
            id=job_id,
            days=days,
            hours=hours,
            minutes=minutes,
            start_date=start_date,
            end_date=end_date,
            replace_existing=True,
            args=args,
            kwargs=kwargs)

        return 

    def show_jobs(self) -> None:
        self.scheduler.print_jobs()

    def get_job(self, job_id) -> dict:
        job = self.scheduler.get_job(job_id=job_id)
        print(job)
        if not job is None:
            return

    def list_jobs_dict(self) -> List[dict]:
        jobs = self.jobstore.get_all_jobs()
        return jobs
        
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
            self.scheduler.start(paused=True)
            for job in self.list_jobs():
                self.resume_job(job.id)

    def start(self) -> None:
        if self.scheduler.state == 0:
            self.scheduler.start()
    
    def shutdown(self) -> None:
        if not self.scheduler.state == 0:
            
            for job in self.list_jobs():
                self.pause_job(job.id)

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
    scheduler.add_interval_job(job=example_job, job_id=project_job_id, exec_immediately=True)
    
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