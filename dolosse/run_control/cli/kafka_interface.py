"""
file: midas_kafka_control_producer
brief: command line interface that waits for input to produce control commands to kafka topic
author: Caswell Pieters
date: 21 February 2020
"""

from confluent_kafka import Producer
import json
import yaml
import sys
from datetime import datetime

run_number = 0


# per message callback
def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                         (msg.topic(), msg.partition(), msg.offset()))


# read yaml file
with open('kafka_interface.yaml') as yf:
    config = yaml.safe_load(yf)
BootstrapServers = config['kafka']['bootstrap_servers']
control_topic = config['kafka']['topics']['control']
manage_topic = config['kafka']['topics']['manage']

# configure producer
kafka_conf = {'bootstrap.servers': BootstrapServers}

# create producer
producer = Producer(**kafka_conf)

try:
    while True:
        command = input('input control command: ')
        if command == 'start':
            run_number = input('run number please: ')

        date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')

        json_string = json.dumps({"run": {"action": command, "run_number": run_number, "date" : date}})
        print(json_string)
        try:
            producer.produce(control_topic, json_string, callback=delivery_callback)
        except BufferError:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(producer))
        producer.poll(0)

except KeyboardInterrupt:
    # Wait until all messages have been delivered
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(producer))
    producer.flush()
    print('that is all folks')
