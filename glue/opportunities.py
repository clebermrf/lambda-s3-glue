import boto3
from pyspark.sql import SparkSession
from datetime import datetime


spark = SparkSession.builder.appName("opportunities").getOrCreate()
df_claims = spark.read.json("s3://data-challenge/claims/")
df_claims.createOrReplaceTempView("claims_view")

df_reverts = spark.read.json("s3://data-challenge/reverts/")
df_reverts.createOrReplaceTempView("reverts_view")

df_pharmacies = spark.read.csv("s3://data-challenge/pharmacies/", header=True, inferSchema=True)
df_pharmacies.createOrReplaceTempView("pharmacies_view")

query = """
    WITH claims AS (
        SELECT
            npi,
            ndc,
            count(*) AS quantity,
            sum(price) AS total_price
        FROM
            claims_view
        WHERE
            id NOT IN (
                SELECT claim_id 
                FROM 
                    reverts_view
            )
        GROUP BY
            npi,
            ndc
    ),
    reverts AS (
        SELECT
            npi,
            ndc,
            count(*) AS quantity,
            sum(price) AS total_price
        FROM
            claims_view
        WHERE
            id IN (
                SELECT claim_id 
                FROM 
                    reverts_view
            )
        GROUP BY
            npi,
            ndc
    )
    
    SELECT
        claims.npi AS npi,
        claims.ndc AS ndc,
        COALESCE(claims.quantity, 0) + COALESCE(reverts.quantity, 0) AS fills,
        COALESCE(reverts.quantity, 0) AS reverted,
        COALESCE(claims.total_price, 0) / COALESCE(claims.quantity, 0) AS avg_price,
        COALESCE(claims.total_price, 0) AS total_price
    FROM
        claims
    LEFT JOIN
        reverts
    ON
        claims.npi = reverts.npi
        AND claims.ndc = reverts.ndc
"""

df_result = spark.sql(query)

today = datetime.now().strftime('year=%Y/month=%m/day=%d')
df_result.coalesce(1) \
    .write.mode("overwrite").json(f"s3://data-challenge/output/2-opportunities/{today}")

