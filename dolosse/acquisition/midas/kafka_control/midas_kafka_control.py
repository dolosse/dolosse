"""
file: midas_kafka_control
brief: implementing midas run control through kafka topics, by connecting to the midas ODB
author: Caswell Pieters
date: 18 February 2020
"""

import json
import sys
import threading
import yaml

from confluent_kafka import Consumer, Producer, KafkaException
import midas.client


class ControlMessage:
    def __init__(self, json_msg):
        self.action = json_msg['run']['action']
        self.value = int(json_msg['run']['run_number'])

    def execute(self):
        if self.action == 'Start':
            client.start_run(self.value)
        elif self.action == 'Stop':
            client.stop_run()
        elif self.action == 'Pause':
            client.pause_run()
        elif self.action == 'Resume':
            client.resume_run()


def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                         (msg.topic(), msg.partition(), msg.offset()))


def key_capture_thread():
    global keep_going
    input()
    keep_going = False


def midas_info(equipment):
    global producer, topic_feedback, topic_errors

    daq_states = ['Stopped', 'Paused', 'Running']
    run_state = client.odb_get('/Runinfo/State', True, False)
    run_number = client.odb_get('/Runinfo/Run number', True, False)
    events = client.odb_get('/Equipment/' + equipment + '/Statistics/Events sent', True, False)
    evt_rate = client.odb_get('/Equipment/' + equipment + '/Statistics/Events per sec.', True, False)
    kb_rate = client.odb_get('/Equipment/' + equipment + '/Statistics/kBytes per sec.', True, False)
    error = 'test error'
    feedback = 'test feedback'

    # compose feedback message
    feedback_json = json.dumps({"category": "feedback", "daq": {"run_state": daq_states[run_state - 1],
                                                                "evt_rate": evt_rate,
                                                                "kB_rate": kb_rate,
                                                                "events": events}, "os": feedback})

    # compose error message
    error_json = json.dumps({"category": "errors", "msg": error})

    try:
        # Produce feedback json message
        producer.produce(topic_feedback, feedback_json, callback=delivery_callback)

    except BufferError:
        sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                         len(producer))

    try:
        # Produce error json message
        producer.produce(topic_errors, error_json, callback=delivery_callback)

    except BufferError:
        sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                         len(producer))


if __name__ == "__main__":
    keep_going = True

    # read yaml file
    with open('midas_kafka_control.yaml') as yf:
        config = yaml.safe_load(yf)
    bootstrap_servers = config['kafka']['bootstrap_servers']
    topics = [config['kafka']['topics']['control']]
    topic_errors = config['kafka']['topics']['errors']
    topic_feedback = config['kafka']['topics']['feedback']
    group_id = config['kafka']['group_id']
    expt_name = config['kafka']['expt_name']
    host_name = config['kafka']['expt_host']
    midas_equipment = config['kafka']['midas_equipment']

    # consumer configuration
    kafka_conf = {'bootstrap.servers': bootstrap_servers, 'group.id': group_id, 'session.timeout.ms': 6000,
                  'auto.offset.reset': 'latest'}

    # producer configuration
    kafka_conf_prod = {'bootstrap.servers': bootstrap_servers}

    # create consumer
    consumer = Consumer(kafka_conf)
    consumer.subscribe(topics)

    # create producer
    producer = Producer(**kafka_conf_prod)

    client = midas.client.MidasClient("kafka_control", host_name=host_name, expt_name=expt_name)

    threading.Thread(target=key_capture_thread, args=(), name='key_capture_thread',
                     daemon=True).start()

    while keep_going:
        midas_info(midas_equipment)
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            # Proper message
            sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                             (msg.topic(), msg.partition(), msg.offset(),
                              str(msg.key())))
            json_data = json.loads(msg.value())
            control_msg = ControlMessage(json_data)
            control_msg.execute()

    # Close down consumer to commit final offsets.
    consumer.close()
    # Wait for messages to be delivered
    producer.flush()
    # disconnect from midas experiment
    client.disconnect()
