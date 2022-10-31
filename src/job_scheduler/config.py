import os
import yaml
from pydantic import BaseSettings

def load_yaml(conf_file):

    yaml_settings = dict()
    yaml_path = 'src'+ os.sep + conf_file
    
    with open(yaml_path) as f:
        yaml_settings.update(yaml.load(f, Loader=yaml.FullLoader))
        
    return yaml_settings
    
class Settings(BaseSettings):
    
    yaml_settings = load_yaml("config.yaml")
    
    db = yaml_settings.get('db', None)
    executor =  yaml_settings.get('executor', None)
    
    if db is None:
        raise KeyError("Make sure 'db' key was define in config.yaml.")
    if executor is None:
        raise KeyError("Make sure 'executor' key was define in config.yaml.")
    
    db_url: str = db.get('url')
    db_user: str = db.get('user')
    db_password: str = db.get('password')
    db_table: str = db.get('table')
    db_name: str = db.get('name')
    timezone: str = db.get('timezone')
    
    exec_process_num: int = executor.get('process_num')
    exec_thread_num: int = executor.get('thread_num')
    exec_max_instances: int = executor.get('max_instances')

def get_settings() -> Settings:
    return Settings()
