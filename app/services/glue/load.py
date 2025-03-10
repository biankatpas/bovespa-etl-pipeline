import sys
import traceback
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


def initialize_glue_context():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    return sc, glueContext, spark

def register_table(spark, database_name, table_name, data_path):
    try:
        print(f"Registering table {table_name} in Glue Catalog")
        
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
        USING PARQUET
        LOCATION '{data_path}'
        """)
        
        # Run MSCK REPAIR to update partitions
        spark.sql(f"MSCK REPAIR TABLE {database_name}.{table_name}")
        
        print(f"Successfully registered table {database_name}.{table_name}")
        print(f"Data is now available for querying in Athena")
        return True
    except Exception as e:
        print(f"Error registering table: {e}")
        traceback.print_exc()
        return False

def main():
    sc, glueContext, spark = initialize_glue_context()
    job = Job(glueContext)
    
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job.init(args['JOB_NAME'], args)
    
    bucket_name = "bovespa-etl-360494"
    final_path = f"s3://{bucket_name}/final/"
    database_name = "default"
    table_name = "bovespa_data"
    
    try:
        register_table(spark, database_name, table_name, final_path)
        print("Load job completed successfully!")
        
    except Exception as e:
        print(f"Error during load process: {e}")
        traceback.print_exc()
        raise e
    
    job.commit()

if __name__ == "__main__":
    main()
