from fastapi import APIRouter, status, HTTPException
from .schemas import DataResponse, ScheduleJobOutput, ScheduleJobInput
from .servieces import JobsScheduler
from .tasks.example_task import ANY_PROJECT_TASK_DICTIONARY
import random

router = APIRouter()

@router.on_event("startup")
async def startup():
    global scheduler 
    scheduler = JobsScheduler()

def create_serial_number():
    return  ''.join(random.sample('ABCDEFGHIJKLMNOPQRSTUVWXYZ', 20))

@router.post(
    "/jobs",
    response_model=DataResponse,
    status_code=status.HTTP_201_CREATED,
    description="Add New Jobs",
    tags=["Schedule Job"]
)
async def add_job(req: ScheduleJobInput):

    project = req.project
    task = req.task
    interval = req.interval
    creater = req.creater
    act_time = req.activity_datetime
    
    task_function = ANY_PROJECT_TASK_DICTIONARY.get(task)
    job_id = f"{project}-{task}-".lower() + create_serial_number()
    
    act_time.end = None if act_time.end == '' else act_time.end
    
    if not task_function:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Project task not exist. Please check the `task` of the job.'
            ) 

    job = scheduler.add_interval_job(
        task_function,
        args=[req.body],
        job_id=job_id,
        exec_immediately=True,
        start_date=act_time.start,
        end_date=act_time.end,
        weeks=interval.weeks,
        days=interval.days,
        hours=interval.hours,
        minutes=interval.minutes,
        seconds=interval.seconds
        )

    output = ScheduleJobOutput(
        job_id=job.get('job_id'),
        project=project,
        task=task,
        creater=creater,
        interval=interval,
        activity_datetime=act_time
        )
    
    return DataResponse(detail=output)
   

@router.get(
    "/jobs",
    response_model=DataResponse,
    status_code=status.HTTP_200_OK,
    description="Query Jobs",
    tags=["Schedule Job"]
)
async def query_jobs():
    return DataResponse(detail=scheduler.list_jobs_dict())


@router.delete(
    "/jobs/empty",
    response_model=DataResponse,
    status_code=status.HTTP_200_OK,
    description="Empty Jobs",
    tags=["Schedule Job"]
)
async def empty_job():
    scheduler.empty_jobs()
    return DataResponse(detail=f'Remove all Jobs')


@router.delete(
    "/jobs/{job_id}",
    response_model=DataResponse,
    status_code=status.HTTP_200_OK,
    description="Delete A Job by ID",
    tags=["Schedule Job"]
)
async def delete_job(job_id: str):
    scheduler.remove_job(job_id)
    return DataResponse(detail=f'Removed Job [{job_id}]')


@router.get(
    "/jobs/{job_id}",
    response_model=DataResponse,
    status_code=status.HTTP_200_OK,
    description="Get A Job by ID",
    tags=["Schedule Job"]
)
async def get_job(job_id: str):
    job = scheduler.get_job(job_id)
    return DataResponse(detail=job)


@router.post(
    "/doc-example",
    response_model=DataResponse,  # default response pydantic model 
    status_code=status.HTTP_201_CREATED,  # default status code
    description="Description of the well documented endpoint",
    tags=["Documentation Example"],
    summary="Summary of the Endpoint"
)
async def well_documented_example():
    pass
