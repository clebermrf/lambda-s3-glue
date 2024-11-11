import json
import urllib.parse
import boto3
from typing import List, Optional
from pydantic import TypeAdapter, BaseModel
from datetime import datetime


print('Loading function')
s3 = boto3.client('s3')

class Claim(BaseModel):
    id: str
    ndc: str
    npi: str
    quantity: float
    price: float
    timestamp: datetime

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    s3_event = json.loads(event["Records"][0]["body"])
    bucket = s3_event["Records"][0]["s3"]["bucket"]["name"]
    key = s3_event["Records"][0]["s3"]["object"]["key"]

    try:

        # Parse objects
        response = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(response["Body"].read().decode("utf-8"))

        if isinstance(data, list):
            claims = [Claim.model_validate(item) for item in data]
        elif isinstance(data, dict):
            claims = [Claim.model_validate(data)]
        else:
            raise ValueError(f"Expected a list or dict, but got {type(data).__name__}")

        # Upload objects to S3
        for claim in claims:
            print(claim.id)
            s3.put_object(
                Bucket="data-challenge",
                Key=f"claims/{claim.id}.json",
                Body=claim.json()
            )

        return {
            "statusCode": 200,
            "body": [claim.json() for claim in claims]
        }
    
    except Exception as e:
        print(e)
        raise e
