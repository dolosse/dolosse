"""
file: midas_kafka_control
brief: implementing midas run control through kafka topics, by connecting to the midas ODB
author: Caswell Pieters
date: 18 February 2020
"""
import midas.client
from confluent_kafka import Consumer, Producer, KafkaException
import json
import yaml
import sys
from time import sleep


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


def control_init():
    global topics, topic_errors, topic_feedback, group_id, expt_name, host_name, consumer, producer
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


def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                         (msg.topic(), msg.partition(), msg.offset()))


def midas_info(equipment, run_state, run_number, events, evt_rate, kb_rate, error, feedback):
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
    topics = []
    topic_errors = topic_feedback = group_id = expt_name = host_name = ' '
    consumer = producer = None
    daq_run_state = 1
    daq_run_number = daq_events = daq_evt_rate = daq_kb_rate = 0
    daq_feedback = daq_error = 'None'
    midas_equipment = 'Trigger'

    control_init()

    client = midas.client.MidasClient("kafka_control", host_name=host_name, expt_name=expt_name)

    try:
        while True:
            midas_info(midas_equipment, daq_run_state, daq_run_number, daq_events,
                       daq_evt_rate, daq_kb_rate, daq_error, daq_feedback)
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
        sleep(1)

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
        # Wait for messages to be delivered
        producer.flush()
        # disconnect from midas experiment
        client.disconnect()
