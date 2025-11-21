from spark_jobs.utils.spark_utils import create_spark
import pyspark.sql.functions as F

def main():
    spark = create_spark("bronze_to_silver")
    # Read raw CSV from MinIO
    df = spark.read.option("header", True).csv("s3a://ecommerce-bronze/bronze/orders.csv")
    # Basic cleaning
    df2 = df.dropDuplicates().withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
    # Example cast & null handling
    df2 = df2.withColumn("quantity", F.coalesce(F.col("quantity").cast("int"), F.lit(0)))
    # write to silver location
    df2.write.mode("overwrite").parquet("s3a://ecommerce-bronze/silver/orders/")
    print("Bronze->Silver complete")
    spark.stop()

if __name__ == "__main__":
    main()
