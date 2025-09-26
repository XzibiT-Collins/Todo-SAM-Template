import os,boto3,logging

SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
sns = boto3.client('sns')

logger = logging.getLogger(__name__)

# import requests

def lambda_handler(event, context):
    logger.info("Post confirmation lambda triggered")
    # Cognito PostConfirmation trigger: event['request']['userAttributes']
    user_attrs = event.get("request", {}).get("userAttributes", {})
    email = user_attrs.get("email")
    if email:
        try:
            sns.subscribe(TopicArn=SNS_TOPIC_ARN, Protocol='email', Endpoint=email)
            logger.info("subscribed email: %s" % email)
        except Exception as e:
            logger.exception(f"User subscription failed: {str(e)}")
    return event
