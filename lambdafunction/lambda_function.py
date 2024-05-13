import json

def lambda_handler(event, context):
    # Search Service
    return {
        'statusCode': 200,
        'body': json.dumps('Search Service')
    }
