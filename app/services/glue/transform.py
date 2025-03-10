import sys
import traceback
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_date, sum, count, avg, round, when, length, lit, year, concat, datediff, isnan, regexp_replace
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType


def initialize_glue_context():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    return sc, glueContext, spark

def read_interim_data(spark, interim_path):
    print(f"Reading data from interim area: {interim_path}")
    
    df = spark.read.option("basePath", interim_path).parquet(interim_path)
    
    print(f"Original schema: {df.schema}")
    print(f"Number of records: {df.count()}")
    df.show(5, truncate=False)
    return df

def transform_data(df):
    total_rows = df.count()
    
    null_counts = {}
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        null_percentage = (null_count / total_rows) * 100
        null_counts[column] = (null_count, null_percentage)
        print(f"Column '{column}': {null_count} nulls ({null_percentage:.2f}%)")
    
    columns_to_drop = [column for column, (null_count, percentage) in null_counts.items() 
                      if percentage > 95]  # Drop if more than 95% null
        
    if columns_to_drop:
        df = df.drop(*columns_to_drop)
    
    df = df.withColumnRenamed("asset", "stock_name") \
           .withColumnRenamed("cod", "stock_code")
    
    df = df.withColumn("part_numeric", 
                      when(col("part").isNotNull(), 
                          regexp_replace(col("part"), ",", ".").cast(DoubleType()))
                      .otherwise(0.0))
    
    try:
        df = df.withColumn("date_formatted", to_date(col("date"), "yyyy-MM-dd"))
        
        start_of_year = concat(year(col("date_formatted")), lit("-01-01"))
        df = df.withColumn("days_since_beginning_of_year", 
                         datediff(col("date_formatted"), 
                                to_date(start_of_year, "yyyy-MM-dd")))
    except Exception as e:
        print(f"Error processing dates: {e}")
        df = df.withColumn("days_since_beginning_of_year", lit(30))
    
    window_spec = Window.partitionBy("stock_code", "dt")
    
    df = df.withColumn("record_count", count("*").over(window_spec)) \
           .withColumn("average_part", avg("part_numeric").over(window_spec)) \
           .withColumn("type_count", sum(when(col("type").isNotNull(), 1).otherwise(0)).over(window_spec)) \
           .withColumn("avg_name_length", avg(length(col("stock_name"))).over(window_spec))
    
    df = df.withColumn("record_density", 
                      when(col("average_part") > 0,
                          round(col("record_count") / col("average_part"), 4))
                      .otherwise(0.0))
        
    return df

def save_to_final(glueContext, df, final_path):
    count = df.count()
    print(f"Number of records in transformed dataframe: {count}")
    
    if count == 0:
        print("No data to write after transformation")
        return
    
    try:
        print("Converting DataFrame to Glue DynamicFrame")
        dynamic_frame = DynamicFrame.fromDF(df, glueContext, "df_transformed")
        
        print("Writing data to S3")
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options={
                "path": final_path,
                "partitionKeys": ["dt", "stock_code"]
            },
            format="parquet",
            transformation_ctx="write_final_data"
        )
        
        print("Creating table in Glue Catalog")
        sink = glueContext.getSink(
            connection_type="s3",
            path=final_path,
            enableUpdateCatalog=True,
            updateBehavior="UPDATE_IN_DATABASE",
            partitionKeys=["dt", "stock_code"]
        )
        sink.setFormat("parquet")
        sink.setCatalogInfo(catalogDatabase="default", catalogTableName="bovespa_transformed")
        sink.writeFrame(dynamic_frame)
        print("Table created successfully in Glue Catalog")
        
    except Exception as e:
        print(f"Error writing data: {e}")
        traceback.print_exc()
        
        try:
            print("Trying alternative approach using DataFrame API")
            df.write \
                .option("path", final_path) \
                .mode("overwrite") \
                .partitionBy("dt", "stock_code") \
                .format("parquet") \
                .saveAsTable("default.bovespa_transformed")
            print("Alternative approach successful")
        except Exception as alt_e:
            print(f"DataFrame API approach also failed: {alt_e}")
            traceback.print_exc()

def main():
    sc, glueContext, spark = initialize_glue_context()
    job = Job(glueContext)
    
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job.init(args['JOB_NAME'], args)
    
    bucket_name = "bovespa-etl-360494"
    interim_path = f"s3://{bucket_name}/interim/"
    final_path = f"s3://{bucket_name}/final/"
    
    try:
    
        df = read_interim_data(spark, interim_path)
        
        df_transformed = transform_data(df)
        
        save_to_final(glueContext, df_transformed, final_path)
        
        print("Transform job completed successfully!")
        
    except Exception as e:
        print(f"Error during transformation: {e}")
        traceback.print_exc()
        raise e
    
    job.commit()

if __name__ == "__main__":
    main()
