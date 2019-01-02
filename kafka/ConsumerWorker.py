from confluent_kafka import Consumer, KafkaError
import threading
import json
import time


class ConsumerWorker(threading.Thread):
    def __init__(self, cfg):
        threading.Thread.__init__(self)

        self.shutdown_flag = threading.Event()
        self.cfg = cfg
        self.consumer = Consumer({'group.id': cfg['consumer_group'],
                                  'default.topic.config': {'auto.offset.reset': 'earliest'},
                                  'auto.commit.interval.ms': 500})

    def run(self):
        print("Thread start at", time.time())
        self.consumer.subscribe([self.cfg['topic']])

        timeouts = 0
        records = 0
        while not self.shutdown_flag.is_set():
            msg = self.consumer.poll(timeout=0.200)
            print(self.getName(), msg)
            if msg is None:
                timeouts += 1
                continue
            if not msg.error():
                records += 1
                print("Got {records} records after {timeouts} timeouts".format(records=records, timeouts=timeouts))
                timeouts = 0
                records = 0
                msg_value = json.loads(msg.value())
                print(json.dumps(msg_value))
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
        self.consumer.close()
        print("Thread end at", time.time())
