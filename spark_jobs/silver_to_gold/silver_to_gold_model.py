from spark_jobs.utils.spark_utils import create_spark
import pyspark.sql.functions as F

def main():
    spark = create_spark("silver_to_gold")
    orders = spark.read.parquet("s3a://ecommerce-bronze/silver/orders/")
    products = spark.read.option("header", True).csv("s3a://ecommerce-bronze/bronze/products.csv")
    customers = spark.read.option("header", True).csv("s3a://ecommerce-bronze/bronze/customers.csv")

    # dim_product
    dim_product = products.selectExpr("product_id", "product_name", "category").dropDuplicates()
    dim_product.write.mode("overwrite").parquet("s3a://ecommerce-bronze/gold/dim_product")

    # dim_customer (lightweight example)
    dim_customer = customers.selectExpr("customer_id", "first_name", "last_name", "email").dropDuplicates()
    dim_customer.write.mode("overwrite").parquet("s3a://ecommerce-bronze/gold/dim_customer")

    # fact_orders
    fact_orders = orders.join(products, on="product_id", how="left") \
                        .join(customers, on="customer_id", how="left") \
                        .select("order_id", "order_date", "customer_id", "product_id", "quantity", "price")
    fact_orders = fact_orders.withColumn("revenue", F.col("quantity") * F.col("price"))
    fact_orders.write.mode("overwrite").parquet("s3a://ecommerce-bronze/gold/fact_orders")

    print("Silver->Gold complete")
    spark.stop()

if __name__ == "__main__":
    main()
