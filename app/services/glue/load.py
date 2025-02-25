import sys
import boto3
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp

# Inicializar contexto Spark e Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Obter parâmetros do job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# Configurar bucket e caminhos
bucket_name = "bovespa-etl-360494"
final_path = f"s3://{bucket_name}/final/"
database_name = "default"
table_name = "bovespa_data"

try:
    print(f"Reading transformed data from: {final_path}")
    
    # Ler os dados do diretório final
    df = spark.read.parquet(final_path)
    
    print(f"Schema of final data: {df.schema}")
    print(f"Number of records: {df.count()}")
    print("Sample data:")
    df.show(5, truncate=False)
    
    # Adicionar timestamp de carregamento
    df = df.withColumn("load_timestamp", current_timestamp())
    
    # Criar um DataFrame dinâmico do Glue
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "df")
    
    print(f"Creating or updating table {table_name} in Glue Catalog")
    
    # Garantir que a tabela existe no catálogo
    sink = glueContext.getSink(
        connection_type="s3",
        path=final_path,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["partition_date", "stock_code"]
    )
    sink.setFormat("parquet")
    sink.setCatalogInfo(catalogDatabase=database_name, catalogTableName=table_name)
    sink.writeFrame(dynamic_frame)
    
    print("Load job completed successfully! Data is now available in Athena.")
    
except Exception as e:
    print(f"Error during load process: {e}")
    import traceback
    traceback.print_exc()
    raise e

job.commit()
