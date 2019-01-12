import logging
from logging.config import dictConfig
import signal
import time
import yaml

from ConsumerWorker import ConsumerWorker


class ServiceExit(Exception):
    """
    Custom exception which is used to trigger the clean exit
    of all running threads and the main program.
    """
    pass


def service_shutdown(signum, frame):
    logging.info('Caught shutdown signal, SIGNUM=%d' % signum)
    raise ServiceExit


def shutdown_threads(threads):
    for thread in threads:
        thread.shutdown_flag.set()
        thread.join()


if __name__ == '__main__':
    logging.info('Main thread start')
    try:
        with open('consumer.yaml') as f:
            cfg = yaml.safe_load(f)
    except yaml.scanner.ScannerError as err:
        logging.exception('consumer.py ran into an issue loading the YAML config.')
        exit(1)
    except ValueError as err:
        logging.exception('consumer.py caught a value error while loading the config')
        exit(1)

    try:
        logging.config.dictConfig(cfg['logging'])
    except Exception as ex:
        logging.exception('Caught exception while setting up logger')
        exit(1)

    # Register the signal handlers
    signal.signal(signal.SIGABRT, service_shutdown)
    signal.signal(signal.SIGINT, service_shutdown)
    signal.signal(signal.SIGTERM, service_shutdown)

    threads = []
    start = time.time()
    try:
        for i in range(0, cfg['consumer']['number_of_threads']):
            t = ConsumerWorker(cfg=cfg, name="ConsumerThread-" + str(i))
            t.setDaemon(True)
            t.start()
            threads.append(t)
        while True:
            try:
                if len(threads) > 0:
                    for t in threads:
                        if not t.isAlive():
                            # get results from thread
                            t.handled = True
                    threads = [t for t in threads if not t.handled]
                else:
                    logging.critical("All threads have died.")
                    break
                time.sleep(0.5)
            except InterruptedError:
                pass
    except ServiceExit:
        shutdown_threads(threads)
    logging.info("See you space cowboy!")
