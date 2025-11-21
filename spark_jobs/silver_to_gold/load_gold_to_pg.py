import os
import pandas as pd
import psycopg2
from pyspark.sql import SparkSession

def spark_session():
    return SparkSession.builder.appName("load_gold_to_pg") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()

def pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_WAREHOUSE_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_WAREHOUSE_PORT", 5433)),
        database=os.getenv("POSTGRES_WAREHOUSE_DB", "dw"),
        user=os.getenv("POSTGRES_WAREHOUSE_USER", "dw_user"),
        password=os.getenv("POSTGRES_WAREHOUSE_PASS", "dw_pass")
    )

def to_postgres(df_pandas, table):
    conn = pg_conn()
    cur = conn.cursor()
    # create simple table if not exists (schema infer)
    cols = ','.join([f"{c} text" for c in df_pandas.columns])
    cur.execute(f"CREATE TABLE IF NOT EXISTS {table} ({cols});")
    conn.commit()
    # upsert - for demo use truncate+insert (replace in prod with upsert or COPY)
    cur.execute(f"TRUNCATE TABLE {table};")
    conn.commit()
    # insert rows
    cols_fmt = ','.join(df_pandas.columns)
    for i, row in df_pandas.iterrows():
        vals = tuple(str(x) if x is not None else None for x in row)
        placeholders = ','.join(['%s']*len(row))
        cur.execute(f"INSERT INTO {table} ({cols_fmt}) VALUES ({placeholders});", vals)
    conn.commit()
    cur.close()
    conn.close()

def main():
    spark = spark_session()
    # read parquet into pandas via spark to avoid large memory copy
    fact = spark.read.parquet("s3a://ecommerce-bronze/gold/fact_orders").toPandas()
    dim_c = spark.read.parquet("s3a://ecommerce-bronze/gold/dim_customer").toPandas()
    dim_p = spark.read.parquet("s3a://ecommerce-bronze/gold/dim_product").toPandas()

    to_postgres(dim_c, "dim_customer")
    to_postgres(dim_p, "dim_product")
    to_postgres(fact, "fact_orders")
    print("Loaded gold tables to Postgres")
    spark.stop()

if __name__ == "__main__":
    main()
