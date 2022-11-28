
from Application.routers.v1.repository.base_repository import BaseRepository
from Application.routers.v1.schemas.schedule_job import ScheduleJobSchema

from Application.routers.v1.models.schedule_job import ScheduleJobModel
from Application.routers.v1.models.database import db

from typing import List, Optional
from sqlalchemy import select
from datetime import datetime

class ShceduleJobRepository(BaseRepository):

    def get(
        self,
        id: int
    ) -> Optional[ScheduleJobSchema]:
        item = self.base_get(ScheduleJobModel, id=id)
        if item:
            return ScheduleJobSchema(**item)

    def add(
        self,
        args: ScheduleJobSchema,
    ) -> dict:
        job_model = ScheduleJobModel(
            id=args.id,
            scheduler_job_id = args.scheduler_job_id,
            creator = args.creator,
            project = args.project,
            quartz_cron_expression = args.quartz_cron_expression,
            start_time = args.start_time,
            end_time=args.end_time,
            last_run_time = args.last_run_time,
            last_run_state = args.last_run_state      
        )
        
        job_dict = self.base_add(model=job_model)
        return job_dict
    
    def delete(
        self,
        id: int
    ) -> Optional[ScheduleJobSchema]:
        item = self.base_delete(ScheduleJobModel, id=id)
        if item: return ScheduleJobSchema(**item)

    def update(
        self,
        id: int,
        args: ScheduleJobSchema
    ) -> Optional[ScheduleJobSchema]:
        with db.session() as session:
            
            stmt = select(ScheduleJobModel).where(
                ScheduleJobModel.id == id,
            ).with_for_update(of=ScheduleJobModel)
            
            job = session.execute(stmt).scalars().first()
            
            if job is None:
                return None
            if args.scheduler_job_id:
                job.scheduler_job_id = args.scheduler_job_id
            
            if args.project:
                job.project = args.project
                
            if args.creator:
                job.creator = args.creator

            if args.last_run_state:
                job.last_run_state = args.last_run_state
            
            job.updated = datetime.now()
            
            session.commit()
            session.refresh(job)
            return ScheduleJobSchema(**job.as_dict())

    def query(
        self,
        args: ScheduleJobSchema,
    ) -> List[ScheduleJobSchema]:
        
        filters = set()
        
        if args.creator:
            filters.add(ScheduleJobModel.creator == args.creator)
        
        if args.project:
            filters.add(ScheduleJobModel.project == args.project)
            
        result = []
        items = self.base_query(ScheduleJobModel, filters)
        
        for item in items:
            result.append(ScheduleJobSchema(**item))
            
        return result
    

