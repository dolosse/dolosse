"""
file: kafka_run_control_interface.py
brief: command line interface using PyInquirer for producing control commands to kafka
author: Caswell Pieters
date: 24 March 2020
"""

from argparse import ArgumentParser
from json import loads, dumps
import logging
import os
from pathlib import Path
from regex import match
from threading import Thread, Event
from yaml import safe_load

from colorama import Fore, Style
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError, Message
from PyInquirer import style_from_dict, Token, prompt, Validator, ValidationError


class RunNumberValidator(Validator):
    """A run number validation class implementing the Validator abstract base class."""

    def validate(self, document):
        """
        Validates that the run number is greater than or equal to zero or raises a ValidationError.
        :param document : The input document from the prompt.
        :type document : Document
        :raises ValidationError : Not a valid run number
        :return :
        """

        ok = match('^(0|[1-9][0-9]{0,9})$', document.text)
        if not ok:
            raise ValidationError(
                message='Please enter a positive or zero run number',
                cursor_position=len(document.text))  # Move cursor to end


class PathValidator(Validator):
    """A path validation class implementing the Validator abstract base class."""

    def validate(self, document):
        """
        Validates the syntax of the absolute file path  or raises a ValidationError.
        :param document : The input document from the prompt.
        :type document : Document
        :raises ValidationError : Not a valid absolute path
        :return :
        """

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


class KafkaMessage:
    """This is a class for producing messages."""

    def __init__(self, topic, json_msg, producer):
        """
        This constructor for the KafkaMessage class.
        :param topic : The Kafka control topic to produce to.
        :type topic : str
        :param json_msg : The json control message to produce.
        :type json_msg : str
        :param producer : The Kafka producer
        :type producer : Producer
        :return :
        """

        self.json = json_msg
        self.topic = topic
        self.producer = producer

    def execute(self):
        """Produce the message to the topic."""

        try:
            self.producer.produce(self.topic, self.json, callback=delivery_callback)
        except BufferError:
            logging.warning('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                            len(self.producer))

        self.producer.flush()


def clear():
    """Clears the terminal."""

    os.system('cls' if os.name == 'nt' else 'clear')


# per message callback
def delivery_callback(err, msg):
    """
    Reports the failure or success of a message delivery.
    :param err : The error that occurred on None or success.
    :type err : KafkaError
    :param msg  : The message that was produced or failed.
    :type msg : Message
    :return :
    """

    if err:
        logging.error('%% Message failed delivery: %s\n' % err)
    else:
        logging.info('%% Message delivered to %s [%d] @ %d\n' %
                     (msg.topic(), msg.partition(), msg.offset()))


# interactive command function
def command_interface(exit_event, status, t_manage, t_control, producer):
    """
    The thread that accepts input from the command line and prints DAQ status.
    :param exit_event : The thread event that signals stopping the thread.
    :type exit_event : Event
    :param status  : The DAQ status dictionary.
    :type status : dict
    :param t_manage  : The Kafka management topic.
    :type t_manage : str
    :param t_control  : The Kafka control topic.
    :type t_control : str
    :param producer  : The Kafka producer
    :type producer : Producer
    :return:
    """

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

    while not exit_event.isSet():
        clear()
        print(Fore.GREEN + "DAQ state: " + status['daq_state'] + Fore.RED + " | Last error: " +
              status['daq_error'] + Fore.BLUE + " | Last report: " + status['daq_feedback'] + Fore.CYAN +
              " | Evt. rate: " + str(round(float(status['daq_evt_rate']), 2)) + Fore.GREEN + " | kB rate: " +
              str(round(float(status['daq_kB_rate']), 2)) + Fore.RED + " | Events: " + status['daq_events'])

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

            # compose control KafkaMessage
            control_json = dumps({"category": "control", "run": {"action": answers['command'],
                                                                 "run_number": run_number}})
            control_msg = KafkaMessage(t_control, control_json, producer)
            control_msg.execute()

            manage_msg = KafkaMessage(t_manage, control_json, producer)
            manage_msg.execute()

            manage_msg.json = equipment_json
            manage_msg.execute()

            manage_msg.json = metadata_json
            manage_msg.execute()

        elif answers['command'] == 'Exit':
            # Wait until all messages have been delivered
            logging.info('%% Waiting for %d deliveries\n' % len(producer))
            producer.flush()
            print('That\'s all folks!')
            exit_event.set()

        elif answers['command'] == 'Refresh':
            clear()

        else:
            control_json = dumps({"category": "control", "run": {"action": answers['command'],
                                                                 "run_number": run_number}})
            control_msg = KafkaMessage(t_control, control_json, producer)
            control_msg.execute()


def status_readout(exit_event, status, consumer):
    """
    The thread that gathers status information from feedback topics.
    :param exit_event  : The thread event that signals stopping the thread.
    :type exit_event : Event
    :param status  : The DAQ status dictionary.
    :type status : dict
    :param consumer  : The Kafka consumer
    :type consumer : Consumer
    :raises KafkaException : Consumer error
    :return :
    """

    while not exit_event.isSet():
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            logging.error("Kafka Exception occurred", exc_info=True)
            raise KafkaException(msg.error())
        else:
            # Proper message
            logging.info('%% %s [%d] at offset %d with key %s:\n' %
                         (msg.topic(), msg.partition(), msg.offset(), str(msg.key())))
            json_data = loads(msg.value())
            if json_data['category'] == 'feedback':
                status['daq_state'] = json_data['daq']['run_state']
                status['daq_feedback'] = json_data['os']
                status['daq_evt_rate'] = str(json_data['daq']['evt_rate'])
                status['daq_kB_rate'] = str(json_data['daq']['kB_rate'])
                status['daq_events'] = str(json_data['daq']['events'])
            elif json_data['category'] == 'errors':
                status['daq_error'] = json_data['msg']

    consumer.close()


def get_yaml_file():
    """
    Gets the yaml file from the first command line input parameter.
    :return : The absolute path to the yaml config file
    :rtype  : str
    """

    # parse command line argument for config yaml file
    ap = ArgumentParser()
    ap.add_argument("--yaml-config", required=True, help='The absolute path to the yaml config file')
    args = ap.parse_args()
    if not Path(args.yaml_config).exists():
        print('Yaml config file does not exist')
    else:
        return args.yaml_config


def main():
    """
    The main function of the cli interface.

    Creates the logger for Kafka message reports, gets the yaml config, creates the Kafka producer and consumer,
    and starts the command input and status readout threads.
    :raises KafkaException : Consumer could not subscribe to topics
    :raises RuntimeError : Consumer could not subscribe to topics
    :return :
    """

    # create logger
    logging.basicConfig(level=logging.DEBUG, filename='kafka_interface.log', filemode='w',
                        format='%(name)s - %(levelname)s - %(message)s')

    # get the yaml config
    yaml_file = get_yaml_file()

    # read yaml file
    with open(yaml_file) as yf:
        yaml_conf = safe_load(yf)

    # status parameters
    daq_status = {
        'daq_state': 'Stopped',
        'daq_error': ' ',
        'daq_feedback': ' ',
        'daq_evt_rate': '0',
        'daq_kB_rate': '0',
        'daq_events': '0'
    }

    # configure producer
    kafka_conf = {'bootstrap.servers': yaml_conf['kafka']['bootstrap_servers']}

    # create interface producer
    int_producer = Producer(**kafka_conf)

    # consumer configuration
    kafka_cons_conf = {'bootstrap.servers': yaml_conf['kafka']['bootstrap_servers'],
                       'group.id': yaml_conf['kafka']['group_id_interface'], 'auto.offset.reset': 'latest'}

    # create interface  consumer
    int_consumer = Consumer(kafka_cons_conf)

    # exit signaling event
    e = Event()

    try:
        int_consumer.subscribe([yaml_conf['kafka']['topics']['feedback'], yaml_conf['kafka']['topics']['errors']])
    except KafkaException:
        print('Kafka Error in subscribing to consumer topics')
        logging.error("Kafka Exception occurred", exc_info=True)
        e.set()
    except RuntimeError:
        print('Could not subscribe to consumer topics - Consumer closed')
        e.set()

    t2 = Thread(target=command_interface, args=(e, daq_status, yaml_conf['kafka']['topics']['manage'],
                                                yaml_conf['kafka']['topics']['control'], int_producer,))
    t1 = Thread(target=status_readout, args=(e, daq_status, int_consumer,))

    t1.start()
    t2.start()
    t1.join()
    t2.join()


if __name__ == '__main__':
    main()
