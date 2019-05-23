"""
file: kafka_consumer.py
brief: A Kafka consumer that processes Pixie16 data. Creates a thread for each partition plus extra.
author: S. V. Paulauskas
date: January 27, 2019
"""
from io import BytesIO
import logging
import threading
import time
from statistics import mean

from confluent_kafka import Consumer, KafkaError

from dolosse.hardware.xia.pixie16.list_mode_data_decoder import ListModeDataDecoder
from dolosse.hardware.xia.pixie16.list_mode_data_mask import ListModeDataMask


class KafkaConsumer(threading.Thread):
    logger = logging.getLogger(__name__)

    def __init__(self, cfg, name):
        threading.Thread.__init__(self)

        self.shutdown_flag = threading.Event()
        self.handled = False
        self.cfg = cfg
        self.total_processed_messages = 0
        self.total_idle_time = 0
        self.total_errors = 0
        self.total_runtime = 0

        try:
            self.consumer = Consumer({'bootstrap.servers': cfg['consumer']['bootstrap_servers'],
                                      'group.id': cfg['consumer']['group'],
                                      'default.topic.config': {
                                          'auto.offset.reset': cfg['consumer'][
                                              'auto_offset_reset']},
                                      'auto.commit.interval.ms': cfg['consumer'][
                                          'auto_commit_interval_ms']})
        except Exception as ex:
            pass

    def run(self):
        logging.info("Started")
        self.consumer.subscribe([self.cfg['consumer']['topic']])

        timeouts = records = 0
        messages__in_interval = errors_in_interval = idle_time_in_interval = 0
        stats_interval_start_time = time.time()
        message_processing_times = []

        data_mask = ListModeDataMask(250, 30474)

        while not self.shutdown_flag.is_set() or self.handled:
            msg = self.consumer.poll(timeout=self.cfg['consumer']['poll_timeout_s'])
            if msg is None:
                idle_time_in_interval += self.cfg['consumer']['poll_timeout_s']
            elif not msg.error():
                message_processing_start_time = time.time()
                logging.info("Begin processing data buffer")

                ListModeDataDecoder(BytesIO(msg.value()), data_mask).run()

                message_processing_times.append(time.time() - message_processing_start_time)
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())

            elapsed_time = time.time() - stats_interval_start_time
            if elapsed_time >= self.cfg['consumer']['stats_interval_s']:
                msg = {
                    'idle_time': idle_time_in_interval
                }
                if message_processing_times:
                    msg['min_message_processsing_time'] = min(message_processing_times)
                    msg['ave_message_processing_time'] = mean(message_processing_times)
                    msg['max_message_processing_time'] = max(message_processing_times)
                logging.info("'Thread status update.', " + str(msg).strip('{}'))
                stats_interval_start_time = time.time()

                idle_time_in_interval = 0
                message_processing_times.clear()
            elif self.shutdown_flag.is_set():
                logging.info("Thread received the shutdown signal, wrapping up...")
            elif self.handled:
                logging.warning(
                    "Thread became inactive for some reason, check logs for more information!")
        self.consumer.close()
        logging.info("Thread Finished!")
