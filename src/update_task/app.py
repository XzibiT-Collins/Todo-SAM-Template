import os, json, boto3, logging
from datetime import datetime

from boto3.dynamodb.conditions import Attr
from cors_helper import build_response

logger = logging.getLogger(__name__)

dynamodb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")

TABLE_NAME = os.environ["TASK_TABLE_NAME"]
QUEUE_URL = os.environ["EXPIRY_QUEUE_URL"]
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
        return build_response(400, {"message": "Invalid request: Task ID is required"})

    body = event.get("body")
    if isinstance(body, str):
        body = json.loads(body) if body else {}
    body = body or {}

    # allowed updates: Description, Status, Deadline
    updates = {}
    if "description" in body: updates["Description"] = body["description"]
    if "status" in body: updates["Status"] = body["status"]
    if "deadline" in body: updates["Deadline"] = datetime.fromtimestamp(body["deadline"])

    if not updates:
        return build_response(400, {"message":"Nothing to update"})

    pk = f"USER#{user_id}"
    sk = f"TASK#{task_id}"

    # Build UpdateExpression
    expr = []
    attr_vals = {}
    attr_names = {}
    i = 0
    for k,v in updates.items():
        i+=1
        placeholder = f":v{i}"
        name_placeholder = f"#n{i}"
        expr.append(f"{name_placeholder} = {placeholder}")
        attr_vals[placeholder] = v
        attr_names[name_placeholder] = k

    update_expression = "SET " + ", ".join(expr)

    # Ensure we only update item if it still belongs to user (optional)
    try:
        table.update_item(
            Key={"PK": pk, "SK": sk},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=attr_names,
            ExpressionAttributeValues=attr_vals,
            ConditionExpression=Attr("PK").exists()
        )
    except Exception as e:
        return build_response(400, {"message":"Update failed","error":str(e)})

    # # If deadline changed and new deadline in future -> send new scheduled message (note SQS DelaySeconds max 900)
    # if "Deadline" in updates:
    #     deadline = int(updates["Deadline"])
    #     now = int(time.time())
    #     delay = deadline - now
    #     if delay < 0:
    #         delay = 0
    #     if delay > 900:
    #         delay = 900
    #
    #     sqs.send_message(
    #         QueueUrl=QUEUE_URL,
    #         MessageBody=json.dumps({"taskId": task_id, "userId": user_id, "deadline": deadline}),
    #         MessageGroupId=task_id,
    #         MessageDeduplicationId=str(uuid.uuid4())
    #     )
    return build_response(200, {"message":"updated"})
