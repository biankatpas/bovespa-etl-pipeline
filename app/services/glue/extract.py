import sys
import boto3
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp, lit, col, date_format


def initialize_glue_context():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    return sc, glueContext, spark

def get_job_parameters():
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'INPUT_PATH'
    ])
    return args

def extract_date_from_filename(input_path):
    try:
        file_name = input_path.split('/')[-1]
        date_part = file_name.split('-', 1)[1].split('.')[0]
        year, month, day = date_part.split('-')
        partition_date = f"{year}-{month}-{day}"
        print(f"Extracted date: {partition_date}")
        return partition_date
    except Exception as e:
        print(f"Error extracting date from filename: {e}")
        partition_date = datetime.now().strftime("%Y-%m-%d")
        print(f"Using current date as fallback: {partition_date}")
        return partition_date

def read_and_process_data(spark, raw_path, input_path, partition_date):
    print(f"Reading parquet file from: {raw_path}")
    df = spark.read.parquet(raw_path)
    print(f"Dataframe schema: {df.schema}")
    print(f"Number of records: {df.count()}")
    
    df = df.withColumn("processing_date", current_timestamp())
    df = df.withColumn("source_file", lit(input_path))
    
    return df

def save_to_interim(df, interim_path, partition_date):
    output_path = f"{interim_path}dt={partition_date}"
    print(f"Writing data to interim area: {output_path}")
    df.write.mode("append").parquet(output_path)
    print("Extract job completed successfully!")

def main():
    sc, glueContext, spark = initialize_glue_context()
    job = Job(glueContext)
    
    args = get_job_parameters()
    job_name = args['JOB_NAME']
    input_path = args['INPUT_PATH']
    
    job.init(job_name, args)
    
    print(f"Job Name: {job_name}")
    print(f"Input Path: {input_path}")
    
    bucket_name = "bovespa-etl-360494"
    raw_path = f"s3://{bucket_name}/{input_path}"
    interim_path = f"s3://{bucket_name}/interim/"
    
    partition_date = extract_date_from_filename(input_path)
    
    try:
        df = read_and_process_data(spark, raw_path, input_path, partition_date)
        save_to_interim(df, interim_path, partition_date)
    except Exception as e:
        print(f"Error processing the file: {e}")
        raise e
    
    job.commit()

if __name__ == "__main__":
    main()
