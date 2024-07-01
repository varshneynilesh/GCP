import random
import json
import datetime
import time

from faker import Faker
from google.cloud import pubsub

PROJECT_ID = '<PROJECT_ID>'
TOPIC = 'tweets_topic'

usernames = []
faker = Faker()
publisher = pubsub.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC)


def publish(publisher, topic, message):
    data = message.encode('utf-8')
    return publisher.publish(topic_path, data=data)


def generate_tweet():
    data = {}
    data['created_at'] = datetime.datetime.now().strftime('%d/%b/%Y:%H:%M:%S')
    data['tweet_id'] = faker.uuid4()
    data['text'] = faker.sentence()
    data['user'] = random.choice(usernames)
    return json.dumps(data)

if __name__ == '__main__':
    for i in range(200):
        newprofile = faker.simple_profile()
        usernames.append(newprofile['username'])
    print("Hit CTRL-C to stop Tweeting!")
    while True:
        publish(publisher, topic_path, generate_tweet())
        time.sleep(0.5)
