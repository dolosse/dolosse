# Midas framework kafka control in C

A C program within the [MIDAS](https://midas.triumf.ca/MidasWiki/index.php/Main_Page) framework that consumes json format control messages from a Kafka topic and produces DAQ status information to topics. 

## Contents

The program is based on the example experiment that ships with legacy MIDAS v2.1

## Dependancies

librdkafka, json-c, libyaml

## Installation

The associated .yaml file must include configuration information for Kafka brokers, MIDAS experiment name, host name, etc.


Within the experiment directory do the following:

```bash
make midlibkafka  
make kafka_control
```

```bash
./kafka_control
```

Status messages to stderr , so pipe those to a log file