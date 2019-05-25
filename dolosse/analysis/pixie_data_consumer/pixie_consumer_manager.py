"""
file: consumer.py
brief: An example kafka consumer that consumes Pixie16 data from a topic
author: S. V. Paulauskas
date: January 01, 2019
"""
import logging
import signal
import time
from logging.config import dictConfig

import yaml

from dolosse.analysis.pixie_data_consumer.kafka_consumer import KafkaConsumer


def service_shutdown(signum, frame):
    logging.info('Caught shutdown signal, SIGNUM=%d' % signum)
    raise SystemExit


signal.signal(signal.SIGABRT, service_shutdown)
signal.signal(signal.SIGINT, service_shutdown)
signal.signal(signal.SIGTERM, service_shutdown)


def shutdown_threads(threads):
    for thread in threads:
        thread.shutdown_flag.set()
        thread.join()


if __name__ == '__main__':
    logging.info('Main thread start')

    try:
        with open('consumer.yaml') as f:
            cfg = yaml.safe_load(f)
        logging.config.dictConfig(cfg['logging'])

        threads = []
        start = time.time()
        for i in range(0, cfg['consumer']['number_of_threads']):
            t = KafkaConsumer(cfg=cfg, name="ConsumerThread-" + str(i))
            t.setDaemon(True)
            t.start()
            threads.append(t)
        while True:
            if len(threads) > 0:
                threads = [t for t in threads if not t.isAlive()]
                # TODO : Get results from thread
            else:
                logging.critical("All threads have died.")
                break
            time.sleep(0.5)
    except (KeyboardInterrupt, SystemExit, InterruptedError):
        shutdown_threads(threads)
    except yaml.scanner.ScannerError as err:
        logging.exception('consumer.py ran into an issue loading the YAML config.')
        exit(1)
    except ValueError as err:
        logging.exception('consumer.py caught a value error while loading the config')
        exit(1)
    logging.info("See you space cowboy!")
