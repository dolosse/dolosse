# Kafka interface to MIDAS (Python)

Python interface scripts for [MIDAS](https://midas.triumf.ca/MidasWiki/index.php/Main_Page) experiments that sends json format messages to a Kafka topic for control 


## Contents

Contains python script for basic command line control, command control through Click, and a more detailed interface with DAQ status info through PyInquirer

Examples of the json formats for the message categories are included.


## Usage 

```bash
python kafka_interface.py  # Basic interface in an infinite loop that accepts only start/stop/run number
```

```bash
python kafka_click.py  # Click CLI that accepts more inputs
```

**--command** <start | stop | pause | resume>

**--run_number** *<Zero or positive integer>*

**--equipment** *<Path to JSON file containing equipment information>*

**--metadata** *<Path to JSON file containing metadata information>*

```bash
python kafka_pyinq.py  # PyInquirer-based interactive CLI that accepts all inputs and shows DAQ status
```

The interface has a manual refresh option for DAQ status information. 

<any key+Enter> quits the program.

>The associated .yaml file must include configuration information for Kafka brokers

**Note:** All scripts produce status messages to stderr , so pipe those to a log file
## Requirements

Confluent kafka, PyInquirer