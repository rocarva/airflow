

import sys

sys.path.append("/opt/airflow")
from airflow.operators.python_operator import PythonOperator
from airflow.models import BaseOperator, DAG, TaskInstance
from airflow import DAG , macros
from airflow.utils.dates import days_ago
from operators.twitter_operator import TwitterOperator
from datetime import datetime, timedelta
import time
from os.path import join

from operators.twitter_ingestao import * 

day = time.strftime("%Y-%m-%d")
language = "pt-br"
search_query = "Boticario Maquiagem"
no_of_tweets = 150

    
with DAG(
    dag_id='TwitterDAG',    
    description='Conexão e extração dados do Twitter',    
    start_date=days_ago(2),
    schedule_interval="@daily"
    
    
) as dag:        
    task_operator = TwitterOperator(file_path=join("datalake/twitter_boticario",
                                                    f"extract_date={datetime.now().date()}",
                                                    f"datascience_{datetime.now().date().strftime('%Y%m%d')}.json")
                                    ,search_query=search_query, no_of_tweets=no_of_tweets, day=day,language=language, task_id = "twitter_boticario")
    task_intance = TaskInstance(task=task_operator)
    task_operator.execute(task_operator.task_id)
    tarefa1 = PythonOperator(
        task_id = 'extrai_dados_arquivos', 
        python_callable = ingeta_twitter
    )  
task_operator >> tarefa1