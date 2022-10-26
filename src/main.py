import uvicorn
from fastapi import FastAPI

# import endpoints
from src.job_scheduler.router import router as user_router

tags_meterdata = [{
        "name": "FastAPI Best Practices",
        "description": "Simple Application for Practices."
    }]

app = FastAPI(
    title="FastAPI Best Practices",
    description="FastAPI Best Practices",
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

# add router to fastapi app
app.include_router(user_router, prefix='/schedule')

if __name__ == "__main__":
    uvicorn.run("main:app", host="localhost", port=8044, timeout_keep_alive=0)