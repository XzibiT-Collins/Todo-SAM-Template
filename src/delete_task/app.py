import os, json, boto3,logging
from cors_helper import build_response

logger = logging.getLogger(__name__)

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ["TASK_TABLE_NAME"]
table = dynamodb.Table(TABLE_NAME)

def _get_user_id(event):
    try:
        return event["requestContext"]["authorizer"]["claims"]["sub"]
    except Exception as e:
        logger.exception(f"Failed to get user id: {e}")
        return event.get("requestContext", {}).get("authorizer", {}).get("jwt", {}).get("claims", {}).get("sub")

def lambda_handler(event, context):
    user_id = _get_user_id(event)
    if not user_id:
        return build_response(401, {"message": "Unauthorized"})

    task_id = event.get("pathParameters", {}).get("task_id")
    if not task_id:
        return {"statusCode":400,"body":json.dumps({"message":"Invalid request: Task ID is required"})}

    pk = f"USER#{user_id}"
    sk = f"TASK#{task_id}"

    try:
        table.delete_item(Key={"PK": pk, "SK": sk})
    except Exception as e:
        return build_response(500, {"message": "Delete failed", "error": str(e)})

    # DynamoDB Streams will pick up deletion event -> StreamProcessor handles any additional logic
    return build_response(200, {"message": "Task deleted"})
