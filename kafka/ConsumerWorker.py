from confluent_kafka import Consumer, KafkaError
import logging
import threading
import time

class ConsumerWorker(threading.Thread):
    logger = logging.getLogger(__name__)

    def __init__(self, cfg, name):
        threading.Thread.__init__(self)

        self.shutdown_flag = threading.Event()
        self.handled = False
        self.cfg = cfg
        try:
            self.consumer = Consumer({'bootstrap.servers': cfg['consumer']['bootstrap_servers'],
                                  'group.id': cfg['consumer']['group'],
                                  'default.topic.config': {'auto.offset.reset': cfg['consumer']['auto_offset_reset']},
                                  'auto.commit.interval.ms': cfg['consumer']['auto_commit_interval_ms']})
        except Exception as ex:
            pass

    def run(self):
        logging.info("Started")
        self.consumer.subscribe([self.cfg['consumer']['topic']])

        timeouts = records = 0
        stats_interval_start_time = time.time()
        while not self.shutdown_flag.is_set() or self.handled:
            msg = self.consumer.poll(timeout=self.cfg['consumer']['poll_timeout_s'])
            if msg is None:
                timeouts += 1
            elif not msg.error():
                records += 1
                print("Got {records} records after {timeouts} timeouts".format(records=records, timeouts=timeouts))
                timeouts = 0
                records = 0
                print(msg.value().decode())
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())

            elapsed_time = time.time() - stats_interval_start_time
            if elapsed_time >= self.cfg['consumer']['stats_interval_s']:
                logging.info("Thread status update.")
                stats_interval_start_time = time.time()
            elif self.shutdown_flag.is_set():
                logging.info("Thread received the shutdown signal, wrapping up...")
            elif self.handled:
                logging.warning("Thread became inactive for some reason, check logs for more information!")
        self.consumer.close()
        logging.info("Thread end at" + str(time.time()))
