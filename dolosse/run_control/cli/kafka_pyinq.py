"""
file: midas_kafka_control_producer
brief: command line interface using PyInquirer for producing control commands to kafka
author: Caswell Pieters
date: 24 March 2020
"""

from __future__ import print_function, unicode_literals
from json import loads, dumps
import os
from pathlib import Path
from regex import match
import sys
from threading import Thread
from yaml import safe_load

from colorama import Fore, Style
from confluent_kafka import Producer, Consumer, KafkaException
from PyInquirer import style_from_dict, Token, prompt, Validator, ValidationError


class RunNumberValidator(Validator):
    def validate(self, document):
        ok = match('^(0|[1-9][0-9]{0,9})$', document.text)
        if not ok:
            raise ValidationError(
                message='Please enter a positive or zero run number',
                cursor_position=len(document.text))  # Move cursor to end


class PathValidator(Validator):
    def validate(self, document):
        path_ok = match('^(/[^/ ]*)+/?$', document.text)
        file_ok = Path(document.text).exists()
        if not path_ok:
            raise ValidationError(
                message='Please enter a valid absolute pathname',
                cursor_position=len(document.text))  # Move cursor to end
        if not file_ok:
            raise ValidationError(
                message='File does not exist',
                cursor_position=len(document.text))  # Move cursor to end


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


# clear terminal
def clear():
    os.system('cls' if os.name == 'nt' else 'clear')


# per message callback
def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                         (msg.topic(), msg.partition(), msg.offset()))


# interactive command function
def command_interface():
    global run, daq_state, daq_error, daq_feedback, daq_evt_rate, daq_events, daq_kB_rate

    # PyInquirer style settings
    style = style_from_dict({
        Token.Separator: '#cc5454',
        Token.QuestionMark: '#673ab7 bold',
        Token.Selected: '#cc5454',  # default
        Token.Pointer: '#673ab7 bold',
        Token.Instruction: '',  # default
        Token.Answer: '#f44336 bold',
        Token.Question: '',
    })

    run_question = [
        {
            'type': 'rawlist',
            'name': 'command',
            'message': 'Run control',
            'choices': ['Start', 'Stop', 'Pause', 'Resume',
                        'Refresh', 'Exit']
        }
    ]

    start_questions = [
        {
            'type': 'input',
            'name': 'run_number',
            'message': 'Run number?',
            'validate': RunNumberValidator
        },
        {
            'type': 'input',
            'name': 'equipment',
            'message': 'Equipment json file path?',
            'validate': PathValidator
        },
        {
            'type': 'input',
            'name': 'metadata',
            'message': 'Metadata json file path?',
            'validate': PathValidator
        },
    ]

    # start interactive control
    run_number = 0

    while run:
        clear()
        print(Fore.GREEN + "DAQ state: " + daq_state + Fore.RED + " | Last error: " +
              daq_error + Fore.BLUE + " | Last report: " + daq_feedback + Fore.CYAN +
              " | Evt. rate: " + str(round(float(daq_evt_rate), 2)) + Fore.GREEN + " | kB rate: " +
              str(round(float(daq_kB_rate), 2)) + Fore.RED + " | Events: " + daq_events)

        print(Style.RESET_ALL)

        answers = prompt(run_question, style=style)

        if answers['command'] == 'Start':
            start_answers = prompt(start_questions, style=style)
            equipment_filename = Path(start_answers['equipment'])
            metadata_filename = Path(start_answers['metadata'])
            run_number = start_answers['run_number']

            with open(equipment_filename) as ef:
                equipment_json = dumps(loads(ef.read()))
            with open(metadata_filename) as mf:
                metadata_json = dumps(loads(mf.read()))

            # compose control message
            control_json = dumps({"category": "control", "run": {"action": answers['command'],
                                                                 "run_number": run_number}})
            control_msg = Message(topic_control, control_json)
            control_msg.execute()

            manage_msg = Message(topic_manage, control_json)
            manage_msg.execute()

            manage_msg.json = equipment_json
            manage_msg.execute()

            manage_msg.json = metadata_json
            manage_msg.execute()

        elif answers['command'] == 'Exit':
            # Wait until all messages have been delivered
            sys.stderr.write('%% Waiting for %d deliveries\n' % len(producer))
            producer.flush()
            print('That\'s all folks!')
            run = False

        elif answers['command'] == 'Refresh':
            clear()

        else:
            control_json = dumps({"category": "control", "run": {"action": answers['command'],
                                                                 "run_number": run_number}})
            control_msg = Message(topic_control, control_json)
            control_msg.execute()


def status_readout():
    global run, daq_state, daq_error, daq_feedback, daq_evt_rate, daq_events, daq_kB_rate
    while run:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            # Proper message
            sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                             (msg.topic(), msg.partition(), msg.offset(), str(msg.key())))
            json_data = loads(msg.value())
            if json_data['category'] == 'feedback':
                daq_state = json_data['daq']['run_state']
                daq_feedback = json_data['os']
                daq_evt_rate = str(json_data['daq']['evt_rate'])
                daq_kB_rate = str(json_data['daq']['kB_rate'])
                daq_events = str(json_data['daq']['events'])
            elif json_data['category'] == 'errors':
                daq_error = json_data['msg']

    consumer.close()


if __name__ == '__main__':
    # start in a state to begin execution, and set DAQ state to "normal"
    run = True
    daq_state = 'Stopped'
    daq_error = daq_feedback = 'None'
    daq_evt_rate = daq_kB_rate = daq_events = "0"

    # read yaml file
    with open('kafka_interface.yaml') as yf:
        config = safe_load(yf)
    bootstrap_servers = config['kafka']['bootstrap_servers']
    topic_control = config['kafka']['topics']['control']
    topics = [config['kafka']['topics']['feedback'], config['kafka']['topics']['errors']]
    group_id = config['kafka']['group_id_interface']
    topic_manage = config['kafka']['topics']['manage']

    # configure producer
    kafka_conf = {'bootstrap.servers': bootstrap_servers}

    # create producer
    producer = Producer(**kafka_conf)

    # consumer configuration
    kafka_cons_conf = {'bootstrap.servers': bootstrap_servers, 'group.id': group_id,
                       'session.timeout.ms': 6000,
                       'auto.offset.reset': 'latest'}

    # create consumer
    consumer = Consumer(kafka_cons_conf)
    try:
        consumer.subscribe(topics)
    except KafkaException:
        print('Kafka Error in subscribing to consumer topics')
        run = False
    except RuntimeError:
        print('Could not subscribe to consumer topics - Consumer closed')
        run = False

    t2 = Thread(target=command_interface)
    t1 = Thread(target=status_readout)

    t1.start()
    t2.start()
