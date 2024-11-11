import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, collect_list
from datetime import datetime


spark = SparkSession.builder.appName("recommendations").getOrCreate()
df_claims = spark.read.json("s3://data-challenge/claims/")
df_claims.createOrReplaceTempView("claims_view")

df_reverts = spark.read.json("s3://data-challenge/reverts/")
df_reverts.createOrReplaceTempView("reverts_view")

df_pharmacies = spark.read.csv("s3://data-challenge/pharmacies/", header=True, inferSchema=True)
df_pharmacies.createOrReplaceTempView("pharmacies_view")

query = """
    SELECT
        ndc,
        chain AS name,
        avg(price / quantity) AS avg_price
    FROM
        claims_view AS claims
    INNER JOIN
        pharmacies_view AS pharmacies
    ON
        claims.npi = pharmacies.npi
    WHERE
        claims.id NOT IN (
            SELECT claim_id 
            FROM 
                reverts_view
        )
    GROUP BY
        ndc,
        chain
"""

df = spark.sql(query)

df_result = df.groupBy("ndc") \
    .agg(collect_list(struct("name", "avg_price")).alias("chain"))

today = datetime.now().strftime('year=%Y/month=%m/day=%d')
df_result.coalesce(1) \
    .write.mode("overwrite").json(f"s3://data-challenge/output/3-recommendations/{today}")

