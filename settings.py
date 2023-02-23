import multiprocessing
import os
from pydantic import BaseSettings, validator


class GlobalConfig(BaseSettings):
    cnt_zip: int = 50
    cnt_xml: int = 100
    cnt_parallel_threads: int = 1 #multiprocessing.cpu_count()
    output_dir: str = 'output'
    
    @validator("output_dir")
    def validate_no_sql_injection(cls, value):
        if not os.path.exists(value):
            os.mkdir(value)
        return value
    
    class Config:
        env_file = ".env"


config = GlobalConfig()
