"""
file: binary_data_reader.py
brief: Reads binary data from files or other sources
author: S. V. Paulauskas
date: January 22, 2019
"""
import io
import os
import keyring
import psycopg2
import struct
import time
import uuid
import yaml

from psycopg2 import pool

import constants.data as data
from pixie16.list_mode_data_mask import ListModeDataMask
from pixie16.list_mode_data_decoder import ListModeDataDecoder
from data_formats.pld import header
from data_formats.ldf import header as ldf_header
from data_formats.ldf import constants as ldf_constants

if __name__ == "__main__":
    FILES = [
        # 'D:/data/svp/kafka-tests/kafka-data-test-0.pld',
        # 'D:/data/svp/kafka-tests/bagel-single-spill-1.pld',
        #    'D:/data/utk/pixieworkshop/pulser_003.ldf',
        # 'D:/data/ithemba/bagel/runs/runBaGeL_337.pld',
            'D:/data/anl/vandle2015/a135feb_12.ldf',
        #'D:/data/utk/pixieworkshop/pulser_003.ldf'
    ]

    with open('consumer.yaml') as f:
        cfg = yaml.safe_load(f)

    db_connection_pool = \
        psycopg2.pool.ThreadedConnectionPool(1, 200, user=cfg['database']['username'],
                                             password=keyring.get_password(cfg['database']['host'],
                                                                           cfg['database'][
                                                                               'username']),
                                             host=cfg['database']['host'],
                                             port=cfg['database']['port'],
                                             database=cfg['database']['name'])
    for file in FILES:
        print("Working on file: ", file)
        filename, file_extension = os.path.splitext(file)

        num_data_blocks = num_end_of_file = num_buffer_padding = num_head_blocks = num_unknown \
            = total_words = num_dir_blocks = 0

        with open(file, 'rb') as f:
            read_start_time = time.time()
            data_mask = ListModeDataMask(250, 30474)

            while True:
                chunk = f.read(data.WORD)
                if chunk:
                    if chunk == data.DATA_BLOCK:
                        num_data_blocks += 1

                        # First word in a PLD data buffer is the total size of the buffer
                        total_data_buffer_size = (struct.unpack('I', f.read(data.WORD))[0]) * \
                                                 data.WORD

                        threads = []
                        # Reads the entire data buffer in one shot.
                        with io.BytesIO(f.read(total_data_buffer_size)) as buffer:
                            while True:
                                first_word = buffer.read(data.WORD)
                                if first_word == b'':
                                    break

                                if file_extension == '.pld':
                                    module_words = (struct.unpack('I', first_word)[
                                                        0] - 2) * data.WORD
                                    module_number = struct.unpack('I', buffer.read(data.WORD))[0]

                                    io.BytesIO(buffer.read(module_words))
                                    thread_uuid = uuid.uuid4()
                                    t = ListModeDataDecoder(io.BytesIO(buffer.read(module_words)),
                                                           data_mask,
                                                           db_connection_pool.getconn(),
                                                           'test')
                                    t.setDaemon(True)
                                    t.start()
                                    threads.append(t)

                                    while threads:
                                        for thread in threads:
                                            if not thread.isAlive():
                                                db_connection_pool.putconn(conn=thread.db_connection)
                                        threads = [t for t in threads if t.isAlive()]
                                if file_extension == '.ldf':
                                    number_of_words = (
                                                      struct.unpack('I', first_word)[0]) * data.WORD

                    elif chunk == data.DIR_BLOCK:
                        num_dir_blocks += 1
                        if file_extension == '.ldf':
                            io.BytesIO(f.read(
                                ldf_constants.LDF_DATA_BUFFER_SIZE * data.WORD))
                    elif chunk == data.END_OF_FILE:
                        num_end_of_file += 1
                        if file_extension == '.ldf':
                            f.read(ldf_constants.LDF_DATA_BUFFER_SIZE_WITHOUT_HEADER)
                    elif chunk == data.BUFFER_PADDING:
                        num_buffer_padding += 1
                    elif chunk == data.HEAD_BLOCK:
                        num_head_blocks += 1
                        if file_extension == '.pld':
                            # TODO : We should read this before we send so that if there's an issue we don't get out of whack
                            print(header.PldHeader().read_header(f))
                        if file_extension == '.ldf':
                            print(ldf_header.LdfHeader().
                                read_header(io.BytesIO(f.read(
                                ldf_constants.LDF_DATA_BUFFER_SIZE_WITHOUT_HEADER * data.WORD))))
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
