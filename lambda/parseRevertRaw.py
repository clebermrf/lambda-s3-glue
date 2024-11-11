import json
import urllib.parse
import boto3
from typing import List, Optional
from pydantic import TypeAdapter, BaseModel
from datetime import datetime


print('Loading function')
s3 = boto3.client('s3')

class Revert(BaseModel):
    id: str
    claim_id: str
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
            reverts = [Revert.model_validate(item) for item in data]
        elif isinstance(data, dict):
            reverts = [Revert.model_validate(data)]
        else:
            raise ValueError(f"Expected a list or dict, but got {type(data).__name__}")

        # Upload objects to S3
        for revert in reverts:
            print(revert.id)
            s3.put_object(
                Bucket="data-challenge",
                Key=f"reverts/{revert.id}.json",
                Body=revert.json()
            )

        return {
            "statusCode": 200,
            "body": [revert.json() for revert in reverts]
        }
    
    except Exception as e:
        print(e)
        raise e
