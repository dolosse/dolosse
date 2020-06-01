"""
file: test_kafka_control_cli.py
brief: Tests for the kafka control command line interface
author: Caswell Pieters
date: 07 May 2020
"""

import pytest
import dolosse.acquisition.run_control.cli.kafka_control_cli as cli
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
