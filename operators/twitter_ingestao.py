
import os
import sys
import  sqlalchemy 
from sqlalchemy import create_engine
import psycopg2
user = "postgres"
password = "changeme"
host = "host.docker.internal"

import json
from datetime import datetime, timedelta
import time
from airflow.providers.http.hooks.http import HttpHook
import sys
sys.path.append("airflow/")
import tweepy
import pandas as pd 
from airflow.models import BaseOperator, DAG, TaskInstance
import glob
from glob import glob

caminho =glob(os.path.join("datalake","twitter_boticario", f"extract_date={datetime.now().date()}", f"datascience_{datetime.now().date().strftime('%Y%m%d')}.json"))

print(caminho[0])

def string_conexao():
    conn_string = f"postgresql://{user}:{password}@{host}:5432/loja"
    return conn_string  

def connecta_db():
    db = create_engine(string_conexao(), echo = True , future=False)
    conn = db.connect()
    return conn

def ler_json():    
    gb = pd.read_json(caminho[0])
    gb.drop(columns=[1,2,3,4,5], inplace= True)
    gb.rename(columns={0:"nome",6:"texto"}, inplace=True)
    gb['data_extracao'] = datetime.today().strftime('%Y-%m-%d')
    return gb

# print(ler_json())   

def ingeta_twitter():    
    ler_json().to_sql('raspagem_tweets', con=connecta_db(), if_exists='replace',
            index=False, dtype={
                
                'nome':sqlalchemy.types.VARCHAR(length=100),
                'texto':sqlalchemy.types.VARCHAR(length=280),
                'data_extracao':sqlalchemy.types.DATE()    
            })
    conn = psycopg2.connect(string_conexao())
    conn.autocommit = True
    conn.cursor()

