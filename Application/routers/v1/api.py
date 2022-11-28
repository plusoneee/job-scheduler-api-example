from fastapi import APIRouter
from Application.routers.v1.endpoints import schedule_job_endpoint

api_router_v1 = APIRouter()

api_router_v1.include_router(
    schedule_job_endpoint.router, 
    prefix="", 
    tags=["v1"]
)