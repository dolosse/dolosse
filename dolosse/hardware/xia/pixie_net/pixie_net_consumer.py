"""
file: pixie_net-consumer.py
brief: A Kafka consumer that processes Pixie16 data. Creates a thread for each partition plus extra.
author: S. V. Paulauskas
date: March 30, 2019
"""
from confluent_kafka import Consumer, KafkaError
from statistics import mean
import time


class ServiceExit(Exception):
    """
    Custom exception which is used to trigger the clean exit
    of all running threads and the main program.
    """
    pass


def service_shutdown(signum, frame):
    print('Caught shutdown signal, SIGNUM=%d' % signum)
    raise ServiceExit


consumer = Consumer({'bootstrap.servers': '192.168.1.25:9092',
                     'group.id': 'pixie_net_consumer',
                     'default.topic.config': {
                         'auto.offset.reset': 'latest'},
                     'auto.commit.interval.ms': 500})
consumer.subscribe(['pixie_net'])

timeouts = records = 0
messages__in_interval = errors_in_interval = idle_time_in_interval = 0
stats_interval_start_time = time.time()
message_processing_times = []

TIMEOUT = 0.01
STATS_INTERVAL = 2
try:
    while True:
        msg = consumer.poll(timeout=TIMEOUT)
        if msg is None:
            idle_time_in_interval += TIMEOUT
        elif not msg.error():
            message_processing_start_time = time.time()
            print("Begin processing data buffer")
            print(msg.value())

            message_processing_times.append(time.time() - message_processing_start_time)
        elif msg.error().code() != KafkaError._PARTITION_EOF:
            print(msg.error())

        elapsed_time = time.time() - stats_interval_start_time
        if elapsed_time >= STATS_INTERVAL:
            msg = {'idle_time': idle_time_in_interval}
            if message_processing_times:
                msg['min_message_processsing_time'] = min(message_processing_times)
                msg['ave_message_processing_time'] = mean(message_processing_times)
                msg['max_message_processing_time'] = max(message_processing_times)
            print(msg)
            stats_interval_start_time = time.time()

            idle_time_in_interval = 0
            message_processing_times.clear()
except ServiceExit:
    consumer.close()
    print("Received shutdown call!")
print("See you space cowboy!")
