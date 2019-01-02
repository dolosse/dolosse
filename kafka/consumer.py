import logging
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
    print('Caught signal %d' % signum)
    raise ServiceExit


if __name__ == '__main__':
    print('Starting main program')
    with open('consumer.yaml') as f:
        cfg = yaml.safe_load(f)

    # Register the signal handlers
    signal.signal(signal.SIGABRT, service_shutdown)
    signal.signal(signal.SIGINT, service_shutdown)
    signal.signal(signal.SIGTERM, service_shutdown)

    threads = []
    num_threads = cfg['numThreads']
    start = time.time()
    try:
        for i in range(0, num_threads):
            t = ConsumerWorker(cfg=cfg)
            t.setDaemon(True)
            t.start()
            threads.append(t)
        while True:
            try:
                time.sleep(0.5)
            except InterruptedError:
                pass
    except ServiceExit:
        for thread in threads:
            thread.shutdown_flag.set()
            thread.join()
    print("Done!")
