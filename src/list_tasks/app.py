import os, boto3, logging
from cors_helper import build_response

logger = logging.getLogger(__name__)

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ["TASK_TABLE_NAME"]
STATUS_INDEX = os.environ.get("STATUS_INDEX","StatusIndex")
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


    # optional query param ?status=Pending
    qs = event.get("queryStringParameters") or {}
    status = qs.get("status")

    pk = f"USER#{user_id}"
    if status:
        # query GSI by GSI1PK and begins_with GSI1SK with status
        resp = table.query(
            IndexName=STATUS_INDEX,
            KeyConditionExpression="GSI1PK = :pk AND begins_with(GSI1SK, :s)",
            ExpressionAttributeValues={":pk": pk, ":s": f"STATUS#{status}"}
        )
        items = resp.get("Items", [])
    else:
        resp = table.query(KeyConditionExpression="PK = :pk", ExpressionAttributeValues={":pk": pk})
        items = resp.get("Items", [])

    return build_response(200, {"items": items})
