import json,boto3,os,logging

# import requests
logger = logging.getLogger(__name__)

dynamodb = boto3.resource('dynamodb')
sqs = boto3.resource('sqs')

TABLE_NAME = os.environ['TASK_TABLE_NAME']
table = dynamodb.Table(TABLE_NAME)

def _get_user_id(event):
    try:
        return event['requestContext']['authorizer']['claims']['sub']
    except Exception as e:
        logger.exception(f'Failed to get user id: {e}')
        return event.get('requestContext', {}).get('authorizer', {}).get('jwt', {}).get('claims', {}).get('sub')


def lambda_handler(event, context):
    user_id = _get_user_id(event)
    if not user_id:
        return {
            "statusCode": 401,
            "body": json.dumps({
                "message": "Unauthorized"
            })
        }

    task_id = event.get('pathParameters', {}).get('taskId')
    if not task_id:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "message": "Invalid request: Task ID is required"
            })
        }

    key = {"PK": f"TASK#{task_id}", "SK": f"USER#{user_id}"}
    response = table.get_item(Key=key)
    task = response.get('Item')
    if not task:
        return {
            "statusCode": 404,
            "body": json.dumps({
                "message": "Task not found"
            })
        }

    return {
        "statusCode": 200,
        "body": json.dumps(task)
    }
