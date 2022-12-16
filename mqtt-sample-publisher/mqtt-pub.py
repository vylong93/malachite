#!/usr/bin/env python3

import logging
import random
import time


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


def publish(client, topic):
    logging.info('Start Publishing messages...')
    while True:
        time.sleep(1)
        data = random.randint(0, 10000)
        msg = 'm:{}'.format(data)

        result = client.publish(topic, msg) # result: [0, 1]
        logging.info('result:{}'.format(result))

        status = result[0]
        if status == 0:
            logging.info('Send[{}]:toTopic:[{}]'.format(msg, topic))
        else:
            logging.info('FailedToSend[{}]:toTopic:[{}]'.format(msg, topic))


# Subcriber: mosquitto_sub -d -t testLdangTopic
logging.info('MQTT Python Sample Publisher')

broker = 'localhost'
port = 1883
topic = 'testLdangTopic'
client_id = f'python-mqtt-{random.randint(0, 1000)}'
username = None
password = None

client = connect_mqtt(client_id, broker, port)
client.loop_start()
publish(client, topic)
