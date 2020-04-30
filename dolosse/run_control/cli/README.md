# Kafka interface to MIDAS (Python)

A Python interface script for experiment run control that sends json format messages to a Kafka topic and reads out
from a topic to display DAQ status information.


## Contents

Contains a python script for control and DAQ status reporting through PyInquirer

Examples of the json formats for the message categories are included.


## Usage 


```bash
python kafka_control_cli.py --yaml-config [absolute path to yaml config]  # PyInquirer-based interactive CLI that accepts all inputs and shows DAQ status
```

The interface has a manual refresh option for DAQ status information. 


>The associated .yaml file must include configuration information for Kafka brokers

## Requirements

Confluent kafka, PyInquirer, yaml