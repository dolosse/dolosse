"""
file: producer.py
brief: Simple producer that reads an existing data file into a Kafka Topic
author: S. V. Paulauskas
date: January 27, 2019
"""
import io
import os
import keyring
import psycopg2
import struct
import time
import yaml

from confluent_kafka import Producer
from psycopg2 import pool

import constants.data as data
from pixie16.list_mode_data_mask import ListModeDataMask
from pixie16.list_mode_data_decoder import ListModeDataDecoder
from data_formats.pld import header

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


if __name__ == '__main__':
    cfg = dict()
    with open('producer.yaml') as f:
        cfg = yaml.safe_load(f)

    p = Producer({'bootstrap.servers': cfg['producer']['bootstrap_servers']})

    FILES = [
        # 'D:/data/svp/kafka-tests/kafka-data-test-0.pld',
         'D:/data/svp/kafka-tests/bagel-single-spill-1.pld',
        #    'D:/data/utk/pixieworkshop/pulser_003.ldf',
        # 'D:/data/ithemba/bagel/runs/runBaGeL_337.pld',
        #    'D:/data/anl/vandle2015/a135feb_12.ldf'
    ]

    for file in FILES:
        print("Working on file: ", file)
        filename, file_extension = os.path.splitext(file)

        num_data_blocks = num_end_of_file = num_buffer_padding = num_head_blocks = num_unknown \
            = total_words = num_dir_blocks = 0

        with open(file, 'rb') as f:
            read_start_time = time.time()
            while True:
                chunk = f.read(data.WORD)
                if chunk:
                    if chunk == data.DATA_BLOCK:
                        num_data_blocks += 1

                        # First word in a PLD data buffer is the total size of the buffer
                        total_data_buffer_size = (struct.unpack('I',
                                                                f.read(data.WORD))[0]) * data.WORD

                        # Reads the entire data buffer in one shot.
                        threads = []
                        with io.BytesIO(f.read(total_data_buffer_size)) as buffer:
                            while True:
                                first_word = buffer.read(data.WORD)
                                if first_word == b'':
                                    break
                                module_words = (struct.unpack('I', first_word)[0] - 2) * data.WORD
                                module_number = struct.unpack('I', buffer.read(data.WORD))[0]

                                # Trigger any available delivery report callbacks from previous
                                # produce() calls
                                p.poll(0)

                                # Asynchronously produce a message, the delivery report callback
                                # will be triggered from poll() above, or flush() below, when the
                                # message has been successfully delivered or failed permanently.
                                p.produce(cfg['producer']['topic'],
                                          io.BytesIO(buffer.read(module_words)).read(),
                                          partition=module_number,
                                          callback=delivery_report)

                                # Wait for any outstanding messages to be delivered and delivery
                                # report callbacks to be triggered.
                                p.flush()
                    elif chunk == data.DIR_BLOCK:
                        num_dir_blocks += 1
                    elif chunk == data.END_OF_FILE:
                        num_end_of_file += 1
                    elif chunk == data.BUFFER_PADDING:
                        num_buffer_padding += 1
                    elif chunk == data.HEAD_BLOCK:
                        num_head_blocks += 1
                        print(header.PldHeader().read_header(f))
                    else:
                        print(struct.unpack('I', chunk)[0])
                        num_unknown += 1
                else:
                    break
            print("Basic statistics for the file:",
                  "\n\tNumber of Dirs         : ", num_dir_blocks,
                  "\n\tNumber of Head         : ", num_head_blocks,
                  "\n\tNumber of Data Blocks  : ", num_data_blocks,
                  "\n\tNumber of Buffer Pads  : ", num_buffer_padding,
                  "\n\tNumber of End of Files : ", num_end_of_file,
                  "\n\tNumber of Unknowns     : ", num_unknown,
                  "\n\tTotal Words in File    : ", os.stat(file).st_size / 4,
                  "\n\tTime To Read (s)       : ", time.time() - read_start_time)
        f.close()
