# Etapa 1
# Suporte de bibliotecas
import sys
import boto3
import pandas as pd
import numpy as np
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import mean, stddev, stddev_pop, col

#Funcoes 
def quarter_start(year: int, q: int) -> datetime:
    month = [1, 4, 7, 10]
    return datetime(year, month[q - 1], 1)

def get_tile_url(service_type: str, year: int, q: int) -> str:
    dt = quarter_start(year, q)

    base_url = "s3://720017990039speedtestglobalperformanceraw/raw/parquet/performance"
    url = f"{base_url}/type={service_type}/year={dt:%Y}/quarter={q}/{dt:%Y-%m-%d}_performance_{service_type}_tiles.parquet"
    return url

# Conexão ao arquivo parquet via spark
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

ano = [2022, 2023]
quad = [1, 2, 3, 4]
type_service = ['fixed', 'mobile']

for a in ano:
    for q in quad:
        if a == 2023 and q > 1:
            pass
        else:
            for t in type_service:
                tile_url = get_tile_url(t, a, q)
    
                dynamicFrame = glueContext.create_dynamic_frame.from_options(
                    connection_type = 's3', 
                    connection_options = {'paths': [tile_url]}, 
                    format = 'parquet'
                )
                
                # Exibição do arquivo
                dynamicFrame.printSchema()
                dynamicFrame.show(5)
                
                # Converter o DynamicFrame em um DataFrame do Apache Spark
                data_frame = dynamicFrame.toDF()
                
                # Algumas estatisticas do dynamicFrame
                print('Seleção de variáveis numéricas: ')
                data_frame.select('avg_d_kbps', 'avg_u_kbps', 'avg_lat_ms', 'tests', 'devices').describe().show()
                print('Seleção de variáveis descritivas: ')
                data_frame.select('quadkey', 'tile').describe().show()
                
                # DUPLICADAS
                ## Remover as linhas duplicadas do DataFrame
                data_frame_no_duplicates = data_frame.dropDuplicates()
                ## Comparar o número de linhas do DataFrame resultante com o número de linhas do DynamicFrame original
                if data_frame_no_duplicates.count() != data_frame.count():
                    print(f'O DynamicFrame tem linhas duplicadas. Contagem: {data_frame.count()-data_frame_no_duplicates.count()}')
                else:
                    print(f'O DynamicFrame não tem linhas duplicadas. Contagem: {data_frame.count()}')
                
                # Atualizando data_frame
                data_frame_no_dup = data_frame_no_duplicates
                
                # BRANCO 
                ## Verificação de linhas em branco
                num_rows_original = data_frame_no_dup.count()
                data_frame_no_dup_no_blank = data_frame_no_dup.dropna()
                num_rows_no_blank = data_frame_no_dup_no_blank.count()
                
                if num_rows_no_blank != num_rows_original:
                    print(f'O DataFrame tem linhas em branco. Contagem: {num_rows_original-num_rows_no_blank}')
                else:
                    print(f'O DataFrame não tem linhas em branco. Contagem: {num_rows_original}')
                
                
                # Detecção de outliers
                columns = ["avg_d_kbps","avg_u_kbps","avg_lat_ms","tests","devices"]
                
                for i in columns:
                    # Atribuindo
                    intermed_dataframe = data_frame_no_dup_no_blank
                    # Desvio padrão da coluna p/ detectar os valores discrepantes
                    stddev = intermed_dataframe.select(stddev_pop(i)).collect()[0][0]
                    # Defina um limite para identificar valores discrepantes (por exemplo, 3 desvios padrão)
                    threshold = 3 * stddev
                    # Filtrando as linhas com valores que excedem o limite estabelecido
                    intermed_dataframe = data_frame_no_dup_no_blank.filter(col(i) <= threshold)
                
                trusted_dataframe = intermed_dataframe
                
                print('Contagem do dataframe sem duplicacao ou branco:', data_frame_no_dup_no_blank.count())
                print('DataFrame sem outliers nas variáveis inteiras:', trusted_dataframe.count())
                
                
                tile_url = tile_url.replace('raw', 'trusted')
                # Escrevendo o dataframe para dentro do s3
                trusted_dataframe.write.format("parquet").mode("overwrite").save(tile_url)
                print(tile_url)
                
                del dynamicFrame, trusted_dataframe, tile_url, data_frame
