import random
import json
import datetime
import time

from faker import Faker
from google.cloud import pubsub

PROJECT_ID = '<PROJECT_ID>'
TOPIC = 'tweet_topic'

username = []
faker = Faker()
publisher = pubsub.PublisherClient()
topic_path = publisher.topic
