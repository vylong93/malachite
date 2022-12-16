#!/usr/bin/env python3

import logging
import argparse
import random
import time
import json


from paho.mqtt import client as mqtt_client


logging.basicConfig(format='%(asctime)s %(levelname)s %(funcName)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)


def connect_mqtt(client_id, broker, port):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logging.info('Connected to MQTT Broker!')
        else:
            logging.info('Failed to connect, return code {}'.format(rc))
    
    # Set Connecting Client ID
    client = mqtt_client.Client(client_id)
    # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client, topic, duration=1):
    logging.info('Start Publishing messages...')
    while True:
        time.sleep(duration)
        data = generate_sample_data()
        msg = json.dumps(data)

        result = client.publish(topic, msg) # result: [0, 1]
        logging.info('result:{}'.format(result))

        status = result[0]
        if status == 0:
            logging.info('Send[{}]:toTopic:[{}]'.format(msg, topic))
        else:
            logging.info('FailedToSend[{}]:toTopic:[{}]'.format(msg, topic))


def generate_sample_data():
    temp = round(random.uniform(22.5, 37.5), 2)
    pressure = round(random.uniform(700.5, 999.5), 2)
    humidity = round(random.uniform(40.5, 79.5), 2)
    gas = round(random.uniform(200.5, 255.5), 2)
    return {"t":temp, "p":pressure, "h":humidity, "g":gas}


# Subcriber: mosquitto_sub -d -t testTopic
def main():
    parser = argparse.ArgumentParser(description='MQTT Python Sample Publisher')
    parser.add_argument('-b', '--broker', action='store', required=False, help='Broker to publish')
    parser.add_argument('-p', '--port', action='store', required=False, help='Port to publish')
    parser.add_argument('-t', '--topic', action='store', required=False, help='Topic to publish')
    parser.add_argument('-d', '--duration', action='store', required=False, help='Publish duration in seconds')

    args = parser.parse_args()

    broker = args.broker if args.broker else 'localhost'
    port = int(args.port) if args.port else 1883
    topic = args.topic if args.topic else 'testTopic'
    duration = int(args.duration) if args.duration else 5

    client_id = f'python-mqtt-{random.randint(0, 1000)}'
    client = connect_mqtt(client_id, broker, port)
    client.loop_start()
    publish(client, topic, duration)


if __name__ == "__main__":
    main()
