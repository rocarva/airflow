import sys
sys.path.append("/opt/airflow")

import pandas as pd 
import glob
from glob import glob
import psycopg2
import  sqlalchemy 
import shutil
import os
from pandasql import sqldf
from datetime import datetime
from sqlalchemy import create_engine

user = "postgres"
password = "changeme"

# user = str(os.getenv("USER"))
# password = str(os.getenv("PASSWORD"))

host = "host.docker.internal"
# ler todas as arquivos na pasta origem 

arquivos = sorted(glob(r'datalake/source/*.xlsx'))
## mover os arquivos apos o processo 
destination = 'datalake/destination/'

def string_conexao():
   conn_string = f"postgresql://{user}:{password}@{host}:5432/loja"
   return conn_string 

def connecta_db():
    db = create_engine(string_conexao(), echo = True , future=False)
    conn = db.connect()
    return conn


def ler_base():

    # concatenar os 3 arquivos 
    base = pd.concat((pd.read_excel(cont) for cont in arquivos), ignore_index=True)
    # alterando o formato da coluna datavenda
    base['DATA_VENDA'] = pd.to_datetime(base['DATA_VENDA'], format='%Y%m%d')
    return base 


def ingestao_postgres():    
    ler_base().to_sql('vendas', con=connecta_db(), if_exists='replace',
            index=False, dtype={
                'ID_MARCA': sqlalchemy.types.INTEGER(),
                'MARCA':sqlalchemy.types.VARCHAR(length=100),
                'ID_LINHA':sqlalchemy.types.INTEGER(),
                'LINHA':sqlalchemy.types.VARCHAR(length=100),
                'DATA_VENDA':sqlalchemy.types.DATE(),
                'QTD_VENDA': sqlalchemy.types.INTEGER() 
            })
    conn = psycopg2.connect(string_conexao())
    conn.autocommit = True
    conn.cursor()
    conn.close()

# ingestao_postgres()    


def move_arquivos():    
    # iterate on all files to move them to destination folder
    for file_path in arquivos:
        dst_path = os.path.join(destination, os.path.basename(file_path))
        shutil.move(file_path, dst_path)
        print(f"Moved {file_path} -> {dst_path}")

# move_arquivos()

# venda por ano mes
def ler_ano_mes():        
    vendas_por_mes = pd.read_sql_query('select sum("QTD_VENDA") as vendas,  extract(year  from "DATA_VENDA") as ano ,extract(month  from "DATA_VENDA") as mes from vendas group by extract(year  from "DATA_VENDA"), extract(month  from "DATA_VENDA")',con = string_conexao())
    vendas_por_mes['data_extracao'] = datetime.today().strftime('%Y-%m-%d')    
    return vendas_por_mes

def ingestao_ano_mes():
    
    ler_ano_mes().to_sql('vendas_por_mes', con=connecta_db(), if_exists='replace',
            index=False, dtype={
                'vendas': sqlalchemy.types.INTEGER(),
                'ano': sqlalchemy.types.INTEGER(),
                'mes': sqlalchemy.types.INTEGER(),
                'data_extracao':sqlalchemy.types.DATE()
            })
    connecta_db().close()

# vebda marca e linha

def ler_venda_marca_linha():    
    vendas_marca_linha = pd.read_sql_query('select  "MARCA" ,"LINHA", sum("QTD_VENDA") as vendas from vendas group by  "MARCA", "LINHA"',con = string_conexao())
    vendas_marca_linha['data_extracao'] = datetime.today().strftime('%Y-%m-%d')
    return vendas_marca_linha    

def ingestao_marca_linha():    
    ler_venda_marca_linha().to_sql('vendas_marca_linha', con=connecta_db(), if_exists='replace',
          index=False, dtype={
              'marca': sqlalchemy.types.VARCHAR(length=100),
              'linha': sqlalchemy.types.VARCHAR(length=100),
              'vendas': sqlalchemy.types.INTEGER(),
              'data_extracao':sqlalchemy.types.DATE()
          })
    connecta_db().close()
    
# vendas marca ano mes 
def ler_venda_marca_ano_mes():    
    vendas_marca_ano_mes = pd.read_sql_query('select  "MARCA" , sum("QTD_VENDA") as vendas,extract(month  from "DATA_VENDA") as mes ,  extract(year  from "DATA_VENDA") as ano  from vendas group by  "MARCA",  extract(month  from "DATA_VENDA"),extract(year  from "DATA_VENDA")',con = connecta_db())
    vendas_marca_ano_mes['data_extracao'] = datetime.today().strftime('%Y-%m-%d')
    return vendas_marca_ano_mes

def ingestao_venda_marca_ano_mes():    
    ler_venda_linha_ano_mes().to_sql('vendas_marca_ano_mes', con=connecta_db(), if_exists='replace',
            index=False, dtype={
                'marca': sqlalchemy.types.VARCHAR(length=100),
                'vendas': sqlalchemy.types.INTEGER(),
                'mes': sqlalchemy.types.INTEGER(),
                'ano': sqlalchemy.types.INTEGER(),           
                'data_extracao':sqlalchemy.types.DATE()
            })
    connecta_db().close()
        

# vendas_linha_ano_mes

def ler_venda_linha_ano_mes():    
    vendas_linha_ano_mes = pd.read_sql_query('select  "LINHA" , sum("QTD_VENDA") as vendas,extract(month  from "DATA_VENDA") as mes ,  extract(year  from "DATA_VENDA") as ano from vendas group by  "LINHA",  extract(month  from "DATA_VENDA"),extract(year  from "DATA_VENDA") ',con = connecta_db())
    vendas_linha_ano_mes['data_extracao'] = datetime.today().strftime('%Y-%m-%d')
    return vendas_linha_ano_mes

def ingestao_venda_linha_ano_mes():    
    ler_venda_linha_ano_mes().to_sql('vendas_linha_ano_mes', con=connecta_db(), if_exists='replace',
            index=False, dtype={
                'marca': sqlalchemy.types.VARCHAR(length=100),
                'vendas': sqlalchemy.types.INTEGER(),
                'mes': sqlalchemy.types.INTEGER(),
                'ano': sqlalchemy.types.INTEGER(),           
                'data_extracao':sqlalchemy.types.DATE()
            })
    connecta_db().close()
    