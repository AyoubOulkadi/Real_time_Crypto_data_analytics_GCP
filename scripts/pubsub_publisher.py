import json
import logging
from google.cloud import pubsub_v1

# Set up logging
logger = logging.getLogger(__name__)

def publish_to_pubsub(project_id, topic_name, data):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    
    for data_point in data:
        message_data = json.dumps(data_point)
        message_bytes = message_data.encode("utf-8")
        publisher.publish(topic_path, data=message_bytes)

    logger.info(f"Historical data collected and published to Pub/Sub topic: {topic_name}")
