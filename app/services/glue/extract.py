import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp, lit, col, date_format

# Inicializar contexto Spark e Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Obter parâmetros do job
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'INPUT_PATH'
])

job_name = args['JOB_NAME']
input_path = args['INPUT_PATH']
job.init(job_name, args)

print(f"Job Name: {job_name}")
print(f"Input Path: {input_path}")

# Configurar bucket e prefixos
bucket_name = "bovespa-etl-360494"
raw_path = f"s3://{bucket_name}/{input_path}"
interim_path = f"s3://{bucket_name}/interim/"

# Extrair a data do nome do arquivo (supondo um padrão como part-YYYY-MM-DD.parquet)
try:
    # Extrair a data do nome do arquivo
    file_name = input_path.split('/')[-1]
    date_part = file_name.split('-', 1)[1].split('.')[0]  # Pega "2025-02-25" de "part-2025-02-25.parquet"
    year, month, day = date_part.split('-')
    partition_date = f"{year}-{month}-{day}"
    print(f"Extracted date: {partition_date}")
except Exception as e:
    print(f"Error extracting date from filename: {e}")
    # Fallback para data atual se a extração falhar
    from datetime import datetime
    partition_date = datetime.now().strftime("%Y-%m-%d")
    print(f"Using current date as fallback: {partition_date}")

# Ler os dados do parquet
try:
    print(f"Reading parquet file from: {raw_path}")
    df = spark.read.parquet(raw_path)
    print(f"Dataframe schema: {df.schema}")
    print(f"Number of records: {df.count()}")
    
    # Adicionar metadados e informações de processamento
    df = df.withColumn("processing_date", current_timestamp())
    df = df.withColumn("source_file", lit(input_path))
    df = df.withColumn("partition_date", lit(partition_date))
    
    # Salvar os dados no diretório interim
    output_path = f"{interim_path}partition_date={partition_date}"
    print(f"Writing data to interim area: {output_path}")
    
    # Salvar os dados sem particionar (já incluímos a coluna de partição)
    df.write.mode("append").parquet(output_path)
    
    print("Extract job completed successfully!")
    
except Exception as e:
    print(f"Error processing the file: {e}")
    raise e

job.commit()
