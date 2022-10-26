import time
from datetime import datetime

class AnyProject():

    @staticmethod
    def func_one(*args, **kwargs):
        print(f'[{datetime.now()}] Run test 01 function ... ')
        time.sleep(1)
        
    @staticmethod
    def func_two(*args, **kwargs):
        print(f'[{datetime.now()}] Run test 02 function ... ')
        time.sleep(1)
    
    @staticmethod
    def func_three(*args, **kwargs):
        print(f'[{datetime.now()}] Run test 03 function ... ')
        time.sleep(1)
        
ANY_PROJECT_TASK_DICTIONARY = {
    'func-one': AnyProject().func_one,
    'func-two': AnyProject().func_two,
    'func-three': AnyProject().func_three
}