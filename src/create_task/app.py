import json, os, time, uuid, boto3, logging


logger = logging.getLogger(__name__)

dynamodb = boto3.resource('dynamodb')
sqs = boto3.resource('sqs')

TABLE_NAME = os.environ['TASK_TABLE_NAME']
QUEUE_URL = os.environ['EXPIRY_QUEUE_URL']
table = dynamodb.Table(TABLE_NAME)

def _get_user_id(event):
    try:
        return event['requestContext']['authorizer']['claims']['sub']
    except Exception as e:
        logger.exception(f'Failed to get user id: {e}')
        return event.get('requestContext', {}).get('authorizer', {}).get('jwt', {}).get('claims', {}).get('sub')

def lambda_handler(event, context):
    body = event.get('body')
    if isinstance(body, str):
        body = json.loads(body)
    user_id = _get_user_id(event)
    if not user_id:
        return {
            "statusCode": 401,
            "body": json.dumps({
                "message": "Unauthorized"
            })
        }

    description = body.get('description')
    now = int(time.time())
    task_id = str(uuid.uuid4())
    task_deadline = now + 5*60

    task = {
        "PK": f"TASK#{task_id}",
        "SK": f"USER#{user_id}",
        "TaskId": task_id,
        "UserId": user_id,
        "Description": description,
        "DateCreated": now,
        "Status": "Pending",
        "Deadline": task_deadline,
        "GSI1PK": f"TASK#{task_id}",
        "GSI1SK": f"STATUS#Pending#DEADLINE#{task_deadline}"
    }

    table.put_item(Item=task)
    return {
        "statusCode": 201,
        "body": json.dumps(
            {
                "message": "Task created successfully",
                "task": task
            }
        )
    }
