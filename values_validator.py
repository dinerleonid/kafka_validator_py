import sys
import json
from confluent_kafka import Consumer, KafkaException, KafkaError, Producer

CLOUDKARAFKA_BROKERS = "rocket-01.srvs.cloudkafka.com:9094," \
                       "rocket-02.srvs.cloudkafka.com:9094," \
                       "rocket-03.srvs.cloudkafka.com:9094"
CLOUDKARAFKA_USERNAME = "ea60d71a"
CLOUDKARAFKA_PASSWORD = "rdhn3_BHHlfOa5ITgqfzRCJXZxIfjP9i"
CLOUDKARAFKA_SOURCE_TOPIC = "ea60d71a-source"
CLOUDKARAFKA_DEST_TOPICS = ["ea60d71a-dest"]

if __name__ == '__main__':
    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    producer_conf = {
        'bootstrap.servers': CLOUDKARAFKA_BROKERS,
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'SCRAM-SHA-256',
        'sasl.username': CLOUDKARAFKA_USERNAME,
        'sasl.password': CLOUDKARAFKA_PASSWORD
    }
    consumer_conf = {
        'bootstrap.servers': CLOUDKARAFKA_BROKERS,
        'group.id': "%s-consumer" % CLOUDKARAFKA_USERNAME,
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'SCRAM-SHA-256',
        'sasl.username': CLOUDKARAFKA_USERNAME,
        'sasl.password': CLOUDKARAFKA_PASSWORD
    }

test_cases = [
    {
        "title": "USD currency, price is low after conversion",
        "input_message": {
            "player_id": 123,
            "country": "US",
            "bet_amount": 50,
            "currency": "USD",
            "exchange_rate": 1
        },
        "expected_output_message": {
            "player_id": 123,
            "country": "US",
            "bet_amount_usd": 50,
            "bet_type": "Low"
        }
    },
    {
        "title": "Non usd currency, price is high after conversion",
        "input_message": {
            "player_id": 234,
            "country": "US",
            "bet_amount": 90,
            "currency": "EUR",
            "exchange_rate": 1.3
        },
        "expected_output_message": {
            "player_id": 234,
            "country": "US",
            "bet_amount_usd": 117,
            "bet_type": "High"
        }
    },
    {
        "title": "USD currency, country IL",
        "input_message": {
            "player_id": 345,
            "country": "IL",
            "bet_amount": 90,
            "currency": "USD",
            "exchange_rate": 1
        },
        "expected_output_message": {
            "player_id": 345,
            "country": "IL",
            "bet_amount_usd": 90,
            "bet_type": "Low"
        }
    }
]


def send_to_stream_service(input_message):
    p = Producer(producer_conf)
    p.produce(CLOUDKARAFKA_SOURCE_TOPIC, json.dumps(input_message))
    p.flush()


def wait_and_receive_output_message_from_stream_service():
    c = Consumer(consumer_conf)
    c.subscribe(CLOUDKARAFKA_DEST_TOPICS)
    timeout = 30.0  # seconds
    msg = c.poll(timeout)
    c.close()
    return msg


def assert_test_case_expectations(title, actual_msg, expected_msg):
    if actual_msg is not None or expected_msg is not None:
        if actual_msg is not None:
            actual = json.loads(actual_msg.value().decode('utf-8'))
            if actual['country'] != "US":
                print("FAIL: Outside US, message filtered")
                return
            expected = expected_msg
            if actual != expected:
                print("'{0}' FAIL: Expected message {1}, but got {2}".format(title, expected, actual))
                return
            print("'{0}' SUCCESS".format(title))
            if expected_msg is None:
                print(
                    "'{0}' FAIL: Expected output message {1}, but did not get any message".format(title, expected_msg))
                return


for case in test_cases:
    send_to_stream_service(case['input_message'])
    output_message = wait_and_receive_output_message_from_stream_service()

    # ASSERT: compare input message to output message
    assert_test_case_expectations(case['title'], output_message, case['expected_output_message'])
