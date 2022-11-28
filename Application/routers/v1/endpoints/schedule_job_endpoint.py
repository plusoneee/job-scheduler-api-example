from fastapi import APIRouter, status, HTTPException, Depends
from Application.routers.v1.schemas.schedule_job import ScheduleJobSchema
from Application.routers.v1.repository.schedule_job_repository import ShceduleJobRepository
from Application.routers.v1.services.run_scheduler import JobsScheduler
from typing import List

router = APIRouter()
  
@router.post(
    "/jobs",
    response_model=ScheduleJobSchema,
    status_code=status.HTTP_201_CREATED,
    description="Add New Jobs",
    tags=["v1"]
)
async def add_job(req: ScheduleJobSchema):
    repo = ShceduleJobRepository()
    args = ScheduleJobSchema.parse_obj(req)
    job = repo.add(args)
    return job

@router.get(
    "/jobs/{job_id}",
    response_model=ScheduleJobSchema,
    status_code=status.HTTP_201_CREATED,
    description="Get Job by ID",
    tags=["v1"]
)
async def get_job(job_id:int):
    return ShceduleJobRepository().get(job_id)

@router.delete(
    "/jobs/{job_id}",
    response_model=ScheduleJobSchema,
    status_code=status.HTTP_201_CREATED,
    description="Delete Job by ID",
    tags=["v1"]
)
async def delete_job(job_id:int):
    return ShceduleJobRepository().delete(job_id)

@router.get(
    "/jobs",
    response_model=List[ScheduleJobSchema],
    status_code=status.HTTP_201_CREATED,
    description="Query Jobs",
    tags=["v1"]
)
async def query_job(args: ScheduleJobSchema = Depends(ScheduleJobSchema)):
    return ShceduleJobRepository().query(args)


@router.put(
    "/jobs/{job_id}",
    response_model=ScheduleJobSchema,
    status_code=status.HTTP_201_CREATED,
    description="Update a Job",
    tags=["v1"]
)
async def add_job(req: ScheduleJobSchema, job_id: int):
    repo = ShceduleJobRepository()
    args = ScheduleJobSchema.parse_obj(req)
    job = repo.update(id=job_id, args=args)
    return job