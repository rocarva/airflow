import sys

sys.path.append("/opt/airflow")

from airflow.models import BaseOperator, DAG, TaskInstance

from hook.twitter_hook import TwitterHook
from datetime import datetime, timedelta
import time
from os.path import join
from pathlib import Path

class TwitterOperator(BaseOperator):
    
    template_fields = ["search_query", "file_path", "day", "no_of_tweets","language"]
    
    def __init__(self,file_path,search_query,no_of_tweets,day,language ,**kwargs) :
        self.search_query = search_query
        self.no_of_tweets = no_of_tweets
        self.day = day 
        self.language = language
        self.file_path = file_path
        
        super().__init__(**kwargs)
    
    def create_parent_folder(self):
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True) 
        
        
    
    def execute(self,context):
        search_query = self.search_query
        no_of_tweets = self.no_of_tweets
        day = self.day
        language = self.language
        json_twitter = (TwitterHook(search_query, no_of_tweets, day, language).run())
        self.create_parent_folder()
        with open(self.file_path,"w") as outfile:
            outfile.write(json_twitter)
        

if __name__ == "__main__":
    day = time.strftime("%Y-%m-%d")
    language = "pt-br"
    search_query = "data science"
    no_of_tweets = 150
    
    with DAG(
        'TwitterTest',
        #default_args=default_args,
        description='Conexão e extração dados do Twitter',
        #schedule_interval=timedelta(days=1),
        start_date=datetime.now(),
        #tags=['example'],
    ) as dag:        
        task_operator = TwitterOperator(file_path=join("datalake/twitter_datascience",
                                                       f"extract_date={datetime.now().date()}",
                                                       f"datascience_{datetime.now().date().strftime('%Y%m%d')}.json")
                                        ,search_query=search_query, no_of_tweets=no_of_tweets, day=day,language=language, task_id = "test_run")
        task_intance = TaskInstance(task=task_operator)
        task_operator.execute(task_operator.task_id) # type: ignore
        
        

