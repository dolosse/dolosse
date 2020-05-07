"""
file: kafka_control_cli_test.py
brief: Tests for the kafka control command line interface
author: Caswell Pieters
date: 07 May 2020
"""

import pytest
import json
import cli.kafka_control_cli as cli
from confluent_kafka import Producer, KafkaException
from PyInquirer import ValidationError
from prompt_toolkit.document import Document


def test_build_json():
    """Tests building the dictionary from the received json message"""

    status = {}
    json_msg = {"category": "feedback",
                "daq":
                    {"run_state": "Stopped", "evt_rate": 0.0, "kB_rate": 0.0, "events": 760.0},
                "os": "test feedback"}
    cli.build_status_msg(json_msg, status)
    assert status['daq_state'] == json_msg['daq']['run_state'], 'test_failed'


@pytest.mark.skip
def test_send_command():
    """Tests for kafka exceptions on a control message send to a kafka topic that fails"""

    # configure producer
    kafka_conf = {'bootstrap.servers': '196.24.232.101'}
    json_msg = json.dumps({"category": "control", "run": {"action": "Stop", "run_number": 5}})
    topic = 'rbs_midas_control'

    # create interface producer
    producer = Producer(**kafka_conf)

    with pytest.raises(KafkaException):
        cli.send_command(topic, json_msg, producer)


def test_number_validate():
    """Tests the validation of run number input as greater than or equal to zero"""

    number_validate = cli.RunNumberValidator()

    with pytest.raises(ValidationError):
        number_validate.validate(Document("-23"))


def test_path_validate():
    """Tests the validation of file path syntax and whether file exists"""

    path_validate = cli.PathValidator()

    with pytest.raises(ValidationError):
        path_validate.validate(Document("/asdflk/lfkjasdf0"))


def test_yaml_verify():
    """Tests the validation of the yaml config file as existing"""

    path = '/home/caswell/dolosse_source/kafka_interface_cli/kafka_interface.yaml'
    return_path = cli.verify_yaml_file(path)
    assert path == return_path, 'test failed'
