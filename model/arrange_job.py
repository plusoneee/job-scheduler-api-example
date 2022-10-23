import sqlite3
import time
from typing import List
from pydantic import BaseModel


class ArrangeJobItem(BaseModel):
    
    job_id: str = None
    next_run_time: str = None
    job_state: str = None
    user_id: str = None
    
       
class ArrangeJobTable:
    
    
    def __init__(self, 
                 db: str ='jobs.sqlite', 
                 table: str ='apscheduler_jobs'):
        
        self.db = db
        self.table = table
        self.jobs: List[ArrangeJobItem] = []

    @staticmethod
    def insert(arrange_job: ArrangeJobItem):
        pass
    
    @staticmethod
    def get(job_id: str):
        pass
    
    @staticmethod
    def update(arrange_job: ArrangeJobItem):
        pass
    
    @staticmethod
    def remove(job_id: str):
        pass
    
    def append_job(self, job: ArrangeJobItem):
        self.jobs.append(job)
        
    
    def jobs_to_dictionary(self):
        
        rows = dict()
        
        for job in self.jobs:
            rows[job.job_id] = job.dict()
            
        return rows    
    
class SQLiteCursorUtility:
    
    def __init__(self, 
                 db: str = 'jobs.sqlite',
                 table: str = 'apscheduler_jobs'
                ):
        self.db = db
        self.table = table
        

    def get_all_colunmn_name(
            self,
            db: str = 'jobs.sqlite',
            table: str = 'apscheduler_jobs'
            ):
        
        cur = sqlite3.connect(db).cursor()
        cur.execute(f'select * from {table}')
        
        return [row[0] for row in cur.description]
        
    def add_db_columns_to_apscheduler(
            self,
            db: str = 'jobs.sqlite',
            table: str = 'apscheduler_jobs',
            columns: List = []
        ):
        
        cur = sqlite3.connect(db).cursor()

        query = 'BEGIN TRANSACTION; '    
        for col in columns:
            query += f'ALTER TABLE {table} ADD {col} TEXT DEFAULT ''; '
            
        query += 'COMMIT;'
        cur.execute(query)
        
        cur.close()
        
        columns = self.get_all_colunmn_name(db=db, table=table)
        return columns
    
    
if __name__ == '__main__':
    
    table = ArrangeJobTable()
    job = ArrangeJobItem(job_id='1')
    table.append_job(job)
    
    print(table.jobs_to_dictionary())