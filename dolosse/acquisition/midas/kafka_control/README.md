# MIDAS control through Kafka (Python)

A script that consumes control messages from the kafka topic, executes those commands in MIDAS, as well as gathering status information. 

## Contents
This script uses the MIDAS Python wrappers. An example yaml configuration file is included. 

## Usage

```bash
python midas_kafka_control.py  
```
>The associated .yaml file must include configuration information for Kafka brokers, MIDAS experiment name, host name, etc

**Note:** The script produces status messages to stderr , so pipe those to a log file

## Requirements
Confluent kafka
