import os, json, boto3, logging
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

TABLE_NAME = os.environ["TASK_TABLE_NAME"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    # event is SQS event -> iterate Records
    for rec in event.get("Records", []):
        logger.info(event)
        try:
            body = json.loads(rec["body"])
        except Exception as e:
            logger.exception(f"Failed to parse body: {e}")
            continue

        task_id = body.get("taskId")
        user_id = body.get("userId")

        deadline = datetime.fromtimestamp(body.get("deadline", 0))
        if not task_id or not user_id:
            continue

        pk = f"USER#{user_id}"
        sk = f"TASK#{task_id}"

        resp = table.get_item(Key={"PK": pk, "SK": sk})
        item = resp.get("Item")
        if not item:
            # item deleted
            continue

        status = item.get("Status")
        if status != "Pending":
            # completed/expired already -> skip
            continue

        now = datetime.now()
        if now < deadline:
            # arrived early (possible if clipped delay) -> skip (or requeue); skip for simplicity
            continue

        # mark expired only if still pending (conditional)
        try:
            table.update_item(
                Key={"PK": pk, "SK": sk},
                UpdateExpression="SET #s = :expired",
                ConditionExpression="#s = :pending",
                ExpressionAttributeNames={"#s": "Status"},
                ExpressionAttributeValues={":expired": "Expired", ":pending": "Pending"}
            )
        except ClientError as e:
            logger.exception(f"Failed to mark task expired: {e}")
            # conditional failed (likely changed) -> skip
            continue

        # publish SNS email
        desc = item.get("Description", "")
        message = f"Your task has expired: {desc} (taskId={task_id})"
        try:
            sns.publish(TopicArn=SNS_TOPIC_ARN, Subject="Task expired", Message=message)
        except Exception as e:
            logger.exception(f"Failed to publish SNS message: {e}")
            pass

    return {"statusCode":200}
