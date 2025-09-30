import os, json, boto3

sqs = boto3.client("sqs")
QUEUE_URL = os.environ["EXPIRY_QUEUE_URL"]

def lambda_handler(event, context):
    for rec in event.get("Records", []):
        eventName = rec.get("eventName")
        # On MODIFY or REMOVE
        if eventName == "MODIFY":
            new = rec.get("dynamodb", {}).get("NewImage", {})
            old = rec.get("dynamodb", {}).get("OldImage", {})
            # If status moved to Completed or Deleted -> send a cancellation audit message to SQS (for observability).
            new_status = new.get("Status", {}).get("S") if isinstance(new.get("Status"), dict) else new.get("Status")
            if new_status == "Completed":
                taskId = new.get("TaskId", {}).get("S") if isinstance(new.get("TaskId"), dict) else new.get("TaskId")
                userId = new.get("UserId", {}).get("S") if isinstance(new.get("UserId"), dict) else new.get("UserId")
                # send "cancellation" message - note: this does not remove delayed messages from SQS.
                try:
                    sqs.send_message(
                        QueueUrl=QUEUE_URL,
                        MessageBody=json.dumps({"action":"cancel","taskId":taskId,"userId":userId}),
                        MessageGroupId=taskId,
                        MessageDeduplicationId=str(taskId) + "-cancel"
                    )
                except Exception:
                    pass
        elif eventName == "REMOVE":
            old = rec.get("dynamodb", {}).get("OldImage", {})
            taskId = old.get("TaskId", {}).get("S") if isinstance(old.get("TaskId"), dict) else old.get("TaskId")
            userId = old.get("UserId", {}).get("S") if isinstance(old.get("UserId"), dict) else old.get("UserId")
            try:
                sqs.send_message(
                    QueueUrl=QUEUE_URL,
                    MessageBody=json.dumps({"action":"deleted","taskId":taskId,"userId":userId}),
                    MessageGroupId=taskId,
                    MessageDeduplicationId=str(taskId) + "-del"
                )
            except Exception:
                pass

    return {"statusCode":200}
