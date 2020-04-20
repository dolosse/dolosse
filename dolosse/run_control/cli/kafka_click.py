"""
file: midas_kafka_control_producer
brief: command line interface using click for producing control commands to kafka
author: Caswell Pieters
date: 21 February 2020
"""

from confluent_kafka import Producer
import json
import yaml
import sys
import click
from datetime import datetime


class Message:
    def __init__(self, topic, json_msg):
        self.json = json_msg
        self.topic = topic

    def execute(self):
        # produce to control topic
        try:
            producer.produce(self.topic, self.json, callback=delivery_callback)
        except BufferError:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(producer))

        producer.flush()


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


# get input
@click.command()
@click.option('--command', help='start/stop/pause/resume')
@click.option('--run_number', default=0, help='The run number to start with.')
@click.option('--equipment', type=click.File('rb'), help='JSON file containing equipment information')
@click.option('--metadata', type=click.File('rb'), help='JSON file containing metadata information')
def produce_json(command, run_number, equipment, metadata):
    if command:
        # add time
        date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')

        # compose control message
        control_json = json.dumps({"category": "control", "run": {"action": command,
                                                                  "run_number": run_number, "date": date}})

        control_msg = Message(control_topic, control_json)
        control_msg.execute()

        manage_msg = Message(manage_topic, control_json)
        manage_msg.execute()

    if equipment:
        # read equipment json data
        equipment_json = json.dumps(json.loads(equipment.read()))

        manage_msg = Message(manage_topic, equipment_json)
        manage_msg.execute()

    if metadata:
        # read metadata json data
        metadata_json = json.dumps(json.loads(metadata.read()))

        manage_msg = Message(manage_topic, metadata_json)
        manage_msg.execute()


if __name__ == '__main__':
    produce_json()
