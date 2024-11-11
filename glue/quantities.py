import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list
from datetime import datetime


spark = SparkSession.builder.appName("quantities").getOrCreate()
df_claims = spark.read.json("s3://data-challenge/claims/")
df_claims.createOrReplaceTempView("claims_view")

query = """
    WITH quantities AS (
        SELECT
            ndc,
            quantity,
            count(*) AS frequency
        FROM
            claims_view
        GROUP BY
            ndc,
            quantity
    ),
    ranked_quantities AS (
        SELECT
            ndc,
            quantity,
            ROW_NUMBER() OVER (PARTITION BY ndc ORDER BY frequency DESC) AS row
        FROM
            quantities
    )
    
    SELECT
        ndc,
        quantity AS most_prescribed_quantity
    FROM
        ranked_quantities
    WHERE
        row <= 5

"""

df = spark.sql(query)

df_result = df.groupBy("ndc") \
    .agg(collect_list("most_prescribed_quantity").alias("most_prescribed_quantity"))

today = datetime.now().strftime('year=%Y/month=%m/day=%d')
df_result.coalesce(1) \
    .write.mode("overwrite").json(f"s3://data-challenge/output/4-quantities/{today}")

