from airflow import DAG
import pendulum
from airflow.operators.bash_operator import  BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.macros import ds_add

import sys
sys.path.append("/opt/airflow")

import pandas as pd 
import glob
from glob import glob
# import psycopg2
import  sqlalchemy 
import shutil
import os
# from pandasql import psycopg2
from datetime import datetime
from sqlalchemy import create_engine
from operators.ingestao_operator import *



with DAG(
    'ProcessamentoArquivos',
    start_date=pendulum.datetime(2023, 2, 1, tz="UTC"),    
    schedule_interval='0 0 * * 1', # executar toda segunda feira  
) as dag:
    tarefa1 = PythonOperator(
        task_id = 'extrai_dados_arquivos', 
        python_callable = ingestao_postgres
    )
    tarefa2 = PythonOperator(
        task_id = 'move_arquivos_processados', 
        python_callable = move_arquivos
    )
    tarefa3 = PythonOperator(
        task_id = 'extrai_venda_ano_mes_e_ingestao', 
        python_callable = ingestao_ano_mes
    )
    tarefa4 = PythonOperator(
        task_id = 'extrai_venda_marca_linha', 
        python_callable = ingestao_marca_linha
    )
    tarefa5 = PythonOperator(
        task_id = 'extrai_venda_marca_ano_mes', 
        python_callable = ingestao_venda_marca_ano_mes
    )
    tarefa6 = PythonOperator(
        task_id = 'extrai_venda_linha_ano_mes', 
        python_callable = ingestao_venda_linha_ano_mes
    )


    
tarefa1 >> tarefa2 >> tarefa3 >> tarefa4 >> tarefa5 >> tarefa6

