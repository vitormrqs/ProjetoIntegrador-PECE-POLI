
#%additional_python_modules numpy, pandas
#%extra_py_files s3://aws-glue-assets-720017990039-us-east-1/requirements/site-packages.zip
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
from pyspark.sql.functions import mean, stddev, stddev_pop, col, lit, monotonically_increasing_id, expr, avg
from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import regexp_extract
from pyspark.sql.types import FloatType

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
#Funcoes 
def quarter_start(year: int, q: int) -> datetime:
    month = [1, 4, 7, 10]
    return datetime(year, month[q - 1], 1)

def get_tile_url(service_type: str, year: int, q: int) -> str:
    dt = quarter_start(year, q)

    base_url = "s3://720017990039speedtestglobalperformancetrusted/trusted/parquet/performance"
    url = f"{base_url}/type={service_type}/year={dt:%Y}/quarter={q}/{dt:%Y-%m-%d}_performance_{service_type}_tiles.parquet"
    return url
ano = [2022, 2023]
quad = [1, 2, 3, 4]
type_service = ['fixed']
for a in ano:
    for q in quad:
        if a == 2023 and q > 1:
            pass
        else:
            for t in type_service:
                tile_url = get_tile_url(t, a, q)
                
                #Mobile
                tile_url = tile_url.replace('fixed', 'mobile')
                dynamicFrame_mobile = glueContext.create_dynamic_frame.from_options(
                                    connection_type = 's3', 
                                    connection_options = {'paths': [tile_url]}, 
                                    format = 'parquet'
                                )

                dynamicFrame_mobile = dynamicFrame_mobile.withColumn('id_Tipo', lit(2)) # mobile == 2

                #Fixed
                tile_url = tile_url.replace('mobile', 'fixed')
                dynamicFrame_fixed = glueContext.create_dynamic_frame.from_options(
                                    connection_type = 's3', 
                                    connection_options = {'paths': [tile_url]}, 
                                    format = 'parquet'
                                )
                dynamicFrame_fixed = dynamicFrame_fixed.withColumn('id_Tipo', lit(1)) # fixed == 1

                # Append the DataFrames together
                df_combined = dynamicFrame_mobile.union(dynamicFrame_fixed)
                
                # Add id_Periodo
                if a == 2022 and q == 1:
                    df_combined_q_y = df_combined.withColumn('id_Periodo', lit(1))
                elif a == 2022 and q == 2:
                    df_combined_q_y = df_combined.withColumn('id_Periodo', lit(2))
                elif a == 2022 and q == 3:
                    df_combined_q_y = df_combined.withColumn('id_Periodo', lit(3))
                elif a == 2022 and q == 4:
                    df_combined_q_y = df_combined.withColumn('id_Periodo', lit(4))
                elif a == 2023 and q == 1:
                    df_combined_q_y = df_combined.withColumn('id_Periodo', lit(5))               

                #Add Dataframe
                if a == 2022 and q == 1:
                    df_delivery = df_combined_q_y
                else:
                    df_delivery = df_delivery.union(df_combined_q_y)
# Drop unnecessary columns
df_delivery = df_delivery.drop_fields(['avg_lat_down_ms','avg_lat_up_ms'])

# Define a mapping dictionary to rename columns
mapping = [
    ("quadkey", "quadrante"),
    ("tile", "tile"),
    ("avg_d_kbps", "media_download"),
    ("avg_u_kbps", "media_upload"),
    ("avg_lat_ms", "media_latencia"),
    ("tests", "testes"),
    ("devices", "dispositivos_testes"),
    ("id_Tipo", "id_Tipo"),
    ("id_Periodo", "id_Periodo")
]

# Apply column renaming using the mapping
df_delivery = df_delivery.apply_mapping(mapping)
# Define the regular expressions
longitude_regex = r'(-?\d+\.\d+)\s'
latitude_regex = r'[-?\d+\.\d+]\s(\d+\.\d+)'

# Apply the regular expressions and extract longitude and latitude
df_delivery = df_delivery.withColumn('longitude', regexp_extract('tile', longitude_regex, 1).cast(DecimalType(13,10)))
df_delivery = df_delivery.withColumn('latitude', regexp_extract('tile', latitude_regex, 1).cast(DecimalType(13,10)))
# Convert DynamicFrame to DataFrame
df = df_delivery.toDF()

# Specify the column to check for null values
column_name = "longitude"

# Count the null values in the specified column
null_count = df.filter(df[column_name].isNull() | (df[column_name] == "")).count()

# Check if the column has null values
has_null_values = null_count > 0

print("Null count:", null_count)
#Lat+Long total
rows_count = df_delivery.count()
rows_count
df_delivery_v2 = df_delivery.toDF()

#Obtendo apenas hemisfério norte e longitudes negativas
latitude_lower_limit = 0
longitude_upper_limit = 0

# Filter the DataFrame based on the conditions
df_delivery_v2 = df_delivery_v2.filter(
    (df_delivery["latitude"] <= 5) 
    & (df_delivery["latitude"] >= -33) 
    & (df_delivery["longitude"] >= -73)
    & (df_delivery["longitude"] <= -34)
)

df_delivery_v2 = DynamicFrame.fromDF(df_delivery_v2, glueContext, "dynamic_frame")

df_delivery_v2.show(5)
#Contagem lat >0
rows_count = df_delivery_v2.count()
rows_count
#Contagem lat>0 + long<0
rows_count = df_delivery_v2.count()
rows_count
#Contagem lat + long BRASIL
rows_count = df_delivery_v2.count()
rows_count
df = df_delivery_v2.toDF()

# Perform select and distinct operations
distinct_df = df.select('quadrante', 'latitude', 'longitude', 'tile') \
               .distinct() \
               .withColumn('id_Localizacao', monotonically_increasing_id() + 1)

# Convert DataFrame back to DynamicFrame
dim_Localizacao = DynamicFrame.fromDF(distinct_df, glueContext, "dynamic_frame")

# Print the schema of the new DynamicFrame
dim_Localizacao.show(5)
#Nao distintos dim_Localizacao
row_count = dim_Localizacao.count()
row_count
#Distintos dim_Localizacao
distinct_tile_count = dim_Localizacao.toDF()
distinct_tile_count = distinct_tile_count.select('tile').distinct().count()
distinct_tile_count
# Criação da table dim_Periodo
dim_Periodo = spark.createDataFrame([(1, '2022', 1), (2, '2022', 2), (3, '2022', 3), (4, '2022', 4), (5, '2023', 1)], ['id_Periodo', 'ano', 'quadrimestre'])
# Criação da table dim_Tipo
dim_Tipo = spark.createDataFrame([(1, 'fixed'), (2, 'mobile')], ['id_Tipo', 'tipo'])
# Convert DynamicFrames to DataFrames
df1 = dim_Localizacao.toDF()
df2 = df_delivery_v2.toDF()

# Perform the join operation on 'df_delivery' with 'dim_Localizacao' using the 'quadrante' and 'tile' columns
joined_df = df2.join(df1, ['quadrante', 'tile'], 'left')

# Select only the required fields ('media_download', 'media_upload', 'media_latencia', 'testes', 'dispositivos_testes', 'id_Tipo', 'id_Periodo', 'id_Localizacao')
selected_df = joined_df.select('media_download', 'media_upload', 'media_latencia', 'testes', 'dispositivos_testes', 'id_Tipo', 'id_Periodo', 'id_Localizacao')

# Drop the 'quadrante' and 'tile' columns
df_delivery_rev02 = selected_df.drop('quadrante', 'tile', 'longitude','latitude')
df_delivery_rev02.show(5)
df_delivery_rev02.count()
df_delivery_rev02.write.format("parquet").mode("overwrite").save("s3://720017990039speedtestglobalperformancedelivery/fato/")
dim_Tipo.write.format("parquet").mode("overwrite").save("s3://720017990039speedtestglobalperformancedelivery/dim/dim_Tipo/")
dim_Periodo.write.format("parquet").mode("overwrite").save('s3://720017990039speedtestglobalperformancedelivery/dim/dim_Periodo/')
#Deletando o tile
df = dim_Localizacao.toDF()
dim_Localizacao_dataframe = df.select(['quadrante', "latitude", "longitude", 'id_Localizacao'])
dim_Localizacao_dataframe.write.format("parquet").mode("overwrite").save('s3://720017990039speedtestglobalperformancedelivery/dim/dim_Localizacao/')
dim_Localizacao_dataframe.count()
# Convert DynamicFrame to DataFrame
df = df_delivery_rev02.toDF()

# Get the maximum value from a column
max_value = df.agg({"id_Localizacao": "max"}).collect()[0][0]

# Print the maximum value
print(max_value)
df_delivery_frame = df_delivery.toDF()

# Count the values in the 'device' column
device_counts = df_delivery_frame.groupBy(['id_Tipo','id_Periodo']).count()

# Show the counts
device_counts.show()
df_teste = df_delivery.toDF()
df_teste = df_teste.select('quadrimestre').distinct()
df_teste.show()
job.commit()