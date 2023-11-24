import json
import boto3
import os
import requests

S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
S3_REGION = os.environ['S3_REGION']
LINE_TOKEN = os.environ['LINE_TOKEN']


dynamodb = boto3.resource('dynamodb')
SmartShelfState_table = dynamodb.Table('SmartShelfState')
s3 = boto3.client('s3')


def send_line(product: str, stock: int, time: str, s3_image: str):
    print(product, stock, time, s3_image)
    response = requests.post("https://notify-api.line.me/api/notify", 
            headers={"Authorization": f"Bearer {LINE_TOKEN}"},
            data={
                "message": f"{product} {stock} {time} {s3_image}",
                "imageThumbnail":s3_image,
                "imageFullsize": s3_image,
            },
        )
    return response


def lambda_handler(event, context):
    last_stock_count = SmartShelfState_table.get_item(Key={"Key": "lastStockCount"})
    last_stock_count = int(last_stock_count["Item"]["Value"])
    # {'Item': {'Value': {'N': '0'}, 'Key': {'S': 'lastStockCount'}}
    is_dirty = False
    
    for record in event["Records"]:
        body = json.loads(record["body"])
        
        if (body["StockCount"] != last_stock_count):
            print("StockCount changed", last_stock_count, body["StockCount"])
            
            last_stock_count = body["StockCount"]
            is_dirty = True
            
            s3_URL = body["S3Uri"].replace(f"s3://{S3_BUCKET_NAME}", f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com")
            send_line(body["ProductType"], body["StockCount"], body["TimeStamp"], s3_URL)

            s3.copy_object(Bucket=S3_BUCKET_NAME, CopySource=body["S3Uri"].replace("s3://", ""), Key="latest.jpg")
    
    # Update table            
    if is_dirty:
        SmartShelfState_table.put_item(Item={"Key": "lastStockCount", "Value": last_stock_count})

    return {
        "statusCode": 200,
        "data": {
            "LastStockCount": last_stock_count,
        }
    }
