import os
import yaml
from pydantic import BaseSettings

def load_yaml(conf_file):
    # define base dir 
    # load & parse yaml
    base_dir = os.path.join( os.path.dirname( __file__ ), '..')
    yaml_settings = dict()
    with open(os.path.join(base_dir, conf_file)) as f:
        yaml_settings.update(yaml.load(f, Loader=yaml.FullLoader))
        
    return yaml_settings
    
class Settings(BaseSettings):
    
    yaml_settings = load_yaml("config.yaml")
    
    # database
    db_url: str = yaml_settings['db']['url']
    db_table: str = yaml_settings['db']['table']
    timezone: str = yaml_settings['db']['timezone']
    # excutor 
    exec_process_num: int = yaml_settings['executor']['process_num']
    exec_thread_num: int = yaml_settings['executor']['thread_num']
    exec_max_instances: int = yaml_settings['executor']['max_instances']


def get_settings() -> Settings:
    return Settings()
