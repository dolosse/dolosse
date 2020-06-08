"""
file: test_kafka_control_cli.py
brief: Tests for the kafka control command line interface
author: Caswell Pieters
date: 07 May 2020
"""

import pytest
import json
import dolosse.acquisition.run_control.cli.kafka_control_cli as cli
from confluent_kafka import Producer, KafkaException


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
