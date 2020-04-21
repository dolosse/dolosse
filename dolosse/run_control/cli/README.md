# Kafka interface to MIDAS (Python)

A Python interface script for [MIDAS](https://midas.triumf.ca/MidasWiki/index.php/Main_Page) experiments that sends json format messages to a Kafka topic for control 


## Contents

Contains a python script for control and DAQ status reporting through PyInquirer

Examples of the json formats for the message categories are included.


## Usage 


```bash
python kafka_pyinq.py  # PyInquirer-based interactive CLI that accepts all inputs and shows DAQ status
```

The interface has a manual refresh option for DAQ status information. 


>The associated .yaml file must include configuration information for Kafka brokers

**Note:** The script produces status messages to stderr , so pipe those to a log file
## Requirements

Confluent kafka, PyInquirer, yaml