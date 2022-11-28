import copy
import attr
from Application.routers.v1.models.database import mapper_registry
from sqlalchemy import Column, Table
from sqlalchemy.sql import func
from sqlalchemy.sql.sqltypes import BigInteger, String, TIMESTAMP, Integer

@mapper_registry.mapped
@attr.s
class ScheduleJobModel:
    
    __table__ = Table(
        "schedule_job",
        mapper_registry.metadata,
        Column('id', BigInteger, primary_key=True, autoincrement=True),
        Column('scheduler_job_id', String, nullable=False),
        Column('creator', String, nullable=False),
        Column('project', String, nullable=True),
        Column('quartz_cron_expression', String, nullable=True),
        Column('start_time', String, nullable=True),
        Column('end_time', String, nullable=True),
        Column('last_run_time', TIMESTAMP, nullable=False),
        Column('last_run_state', Integer, nullable=True),
        Column('created', TIMESTAMP, server_default=func.current_timestamp()),
        Column('updated', TIMESTAMP, nullable=True),
        Column('deleted', TIMESTAMP, nullable=True)
    )
    
    id = attr.ib()
    scheduler_job_id = attr.ib()
    creator = attr.ib()
    project = attr.ib()
    quartz_cron_expression = attr.ib()
    start_time = attr.ib()
    end_time = attr.ib()
    last_run_time = attr.ib()
    last_run_state = attr.ib()
    created = attr.ib(default=None)
    updated = attr.ib(default=None)
    deleted = attr.ib(default=None)
    
    def as_dict(
        self
    ) -> dict:
        data = dict()

        temp = copy.deepcopy(self.__dict__)
        del temp['_sa_instance_state']
        data = {**data, **temp}
        return data