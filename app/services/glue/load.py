import sys
import traceback
import boto3
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp


def initialize_glue_context():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    return sc, glueContext, spark

def read_final_data(spark, final_path):
    print(f"Reading transformed data from: {final_path}")
    df = spark.read.parquet(final_path)
    
    print(f"Schema of final data: {df.schema}")
    print(f"Number of records: {df.count()}")
    df.show(5, truncate=False)
    
    return df

def enrich_data(df):
    return df.withColumn("load_timestamp", current_timestamp())

def register_in_catalog(df, database_name, table_name, final_path):
    try:
        print(f"Creating or updating table {table_name} in Glue Catalog")
        
        df.write \
          .format("parquet") \
          .mode("append") \
          .partitionBy("dt", "stock_code") \
          .option("path", final_path) \
          .saveAsTable(f"{database_name}.{table_name}")
        
        print(f"Successfully created/updated table {database_name}.{table_name}")
        return True
    except Exception as e:
        print(f"Error writing to catalog: {e}")
        traceback.print_exc()
        return False

def save_to_s3_only(df, final_path):
    try:
        print("Saving to S3 only as fallback")
        df.write \
          .format("parquet") \
          .mode("append") \
          .partitionBy("dt", "stock_code") \
          .save(final_path)
        
        print("Data successfully saved to S3. You may need to run AWS Glue Crawler to catalog it.")
        return True
    except Exception as e:
        print(f"S3 saving also failed: {e}")
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
        df = read_final_data(spark, final_path)
        df = enrich_data(df)
        
        if not register_in_catalog(df, database_name, table_name, final_path):
            save_to_s3_only(df, final_path)
        
        print("Load job completed successfully! Data is now available in Athena.")
        
    except Exception as e:
        print(f"Error during load process: {e}")
        traceback.print_exc()
        raise e
    
    job.commit()

if __name__ == "__main__":
    main()
