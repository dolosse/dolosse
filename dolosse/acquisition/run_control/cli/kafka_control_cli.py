"""
file: kafka_control_cli.py
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


def send_command(topic, json_msg, producer):
    """
        This functions sends the command received as a json message to a Kafka topic.
        :param topic : The Kafka control topic to produce to.
        :type topic : str
        :param json_msg : The json control message to produce.
        :type json_msg : str
        :param producer : The Kafka producer
        :type producer : Producer
        :return :
    """

    try:
        producer.produce(topic, json_msg, callback=delivery_callback)
    except BufferError:
        logging.warning('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                        len(producer))
    except KafkaException:
        logging.error("Kafka Exception occurred", exc_info=True)

    producer.flush()


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

            # compose control message
            control_json = dumps({"category": "control", "run": {"action": answers['command'],
                                                                 "run_number": run_number}})

            # send all json messages to topics
            send_command(t_control, control_json, producer)

            send_command(t_manage, control_json, producer)

            send_command(t_manage, equipment_json, producer)

            send_command(t_manage, metadata_json, producer)

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
            send_command(t_control, control_json, producer)


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
            build_status_msg(json_data, status)

    consumer.close()


def build_status_msg(json_msg, status):
    """
    Unpacks the json data from the topic msg into a dictionary.
    :param json_msg  : Json format message as read by the consumer.
    :type json_msg : dict
    :param status  : The DAQ status dictionary
    :type status : dict
    :return :
    """

    if json_msg['category'] == 'feedback':
        status['daq_state'] = json_msg['daq']['run_state']
        status['daq_feedback'] = json_msg['os']
        status['daq_evt_rate'] = str(json_msg['daq']['evt_rate'])
        status['daq_kB_rate'] = str(json_msg['daq']['kB_rate'])
        status['daq_events'] = str(json_msg['daq']['events'])
    elif json_msg['category'] == 'errors':
        status['daq_error'] = json_msg['msg']


def verify_yaml_file(arg_config):
    """
    Checks whether the path to the yaml file points to an existing file and exits if not.
    :param arg_config : The command line argument path to the yaml config file
    :type arg_config : str
    :return : The absolute path to the yaml config file
    :rtype  : str
    """

    if not Path(arg_config).exists():
        print('Yaml config file does not exist')
        exit(1)
    else:
        return arg_config


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
    return verify_yaml_file(args.yaml_config)


def initialize(yaml_conf):
    """
    Initializing objects.

    Creates the logger for Kafka message reports, and creates the Kafka producer and consumer.
    :raises KafkaException : Consumer could not subscribe to topics
    :raises RuntimeError : Consumer could not subscribe to topics
    :return : A tuple of the producer and consumer
    :rtype : Producer, Consumer
    """

    # create logger
    logging.basicConfig(level=logging.DEBUG, filename='kafka_interface.log', filemode='w',
                        format='%(name)s - %(levelname)s - %(message)s')

    # configure producer
    kafka_conf = {'bootstrap.servers': yaml_conf['kafka']['bootstrap_servers']}

    # create interface producer
    producer = Producer(**kafka_conf)

    # consumer configuration
    kafka_cons_conf = {'bootstrap.servers': yaml_conf['kafka']['bootstrap_servers'],
                       'group.id': yaml_conf['kafka']['group_id_interface'], 'auto.offset.reset': 'latest'}

    # create interface  consumer
    consumer = Consumer(kafka_cons_conf)

    try:
        consumer.subscribe([yaml_conf['kafka']['topics']['feedback'], yaml_conf['kafka']['topics']['errors']])
    except KafkaException:
        print('Kafka Error in subscribing to consumer topics')
        logging.error("Kafka Exception occurred", exc_info=True)
        exit(1)
    except RuntimeError:
        print('Could not subscribe to consumer topics - Consumer closed')
        exit(1)

    return producer, consumer


def main():
    """
    The main function of the cli interface.

    Get the yaml config, starts the command input and status readout threads.
    :return :
    """

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

    # initialize
    init_values = initialize(yaml_conf)
    int_producer = init_values[0]
    int_consumer = init_values[1]

    # exit signaling event
    e = Event()

    t2 = Thread(target=command_interface, args=(e, daq_status, yaml_conf['kafka']['topics']['manage'],
                                                yaml_conf['kafka']['topics']['control'], int_producer,))
    t1 = Thread(target=status_readout, args=(e, daq_status, int_consumer,))

    t1.start()
    t2.start()
    t1.join()
    t2.join()


if __name__ == '__main__':
    main()
