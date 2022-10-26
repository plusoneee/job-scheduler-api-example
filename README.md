# Job Scheduler API Exmaple

A Web API for create a **Schedule Job** CRUD operation.

## Getting Started
### Requirments
* Install Python3 & pip first. Then run the following command to install python dependencies:
```
pip install fastapi
pip install "uvicorn[standard]"
pip install APScheduler
pip install SQLAlchemy
```

## Run Application

* Run the API server with [uvicorn](https://www.uvicorn.org/): 
```
uvicorn src.main:app --reload --port <YOUR_PORT>
```
Once the application starts, you will see the terminal display:
```
INFO:  Uvicorn running on http://127.0.0.1:<YOUR_PORT> (Press CTRL+C to quit)
```
* Vist the documentation via `http://127.0.0.1:<YOUR_PORT>/docs`.
