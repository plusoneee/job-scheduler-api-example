import uvicorn
from fastapi import FastAPI
import os

from Application.routers.v1.api import api_router_v1

tags_meterdata = [{
        "name": "Job Schedule",
        "description": "A simple application for provide CRUD operation for task scheduling."
    }]

app = FastAPI(
    title="Jobs Scheduler API Documentation",
    description="A simple application for provide CRUD operation for task scheduling.",
    version="v1",
    openapi_tags=tags_meterdata,
    redoc_url=None,
    contact={
        "name":"Joy Liao"
    },
    swagger_ui_parameters={
        "syntaxHighlight.theme": "nord",
        "docExpansion": "full",
    }
)

app.include_router(api_router_v1, prefix='/api/v1')

if __name__ == "__main__":
    uvicorn.run("Application.main:app", host="localhost", port=8045, timeout_keep_alive=0)