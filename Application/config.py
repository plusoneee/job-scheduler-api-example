from pydantic import BaseSettings
from dotenv import load_dotenv
import os

load_dotenv(verbose=True)


class Settings(BaseSettings):
    db_name: str = os.getenv('DB_NAME')
    db_url: str = os.getenv('DB_URL')
    db_port: int = os.getenv('DB_PORT')
    db_user: str = os.getenv('DB_USER')
    db_password: str = os.getenv('DB_PASSWORD')
    db_table: str = 'scheduler_jobs'
    timezone: str = 'Asia/Taipei'
    class config:
        env_file = ".env"
   
settings = Settings()