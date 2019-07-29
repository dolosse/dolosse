"""
file: binary_data_reader.py
brief: Reads binary data from files or other sources
author: S. V. Paulauskas
date: January 22, 2019
"""
from io import BytesIO
import os
from struct import unpack
import time
import yaml

import pandas as pd

import dolosse.constants.data as data
from dolosse.hardware.xia.pixie16.list_mode_data_mask import ListModeDataMask
from dolosse.hardware.xia.pixie16.list_mode_data_decoder import decode_listmode_data
from dolosse.data_formats.pld import header
from dolosse.data_formats.ldf import header as ldf_header
from dolosse.data_formats.ldf import constants as ldf_constants
from dolosse.common.thread_pool import ThreadPool


def process_data_buffer(file_extension, buffer):
    while True:
        first_word = buffer.read(data.WORD)
        if first_word == b'':
            break

        if file_extension == '.pld':
            module_words = (unpack('I', first_word)[0] - 2) * data.WORD
            module_number = unpack('I', buffer.read(data.WORD))[0]

            pd.DataFrame(decode_listmode_data(BytesIO(buffer.read(module_words)), data_mask))\
                .to_parquet(fname='data/test', partition_cols=['crate'])
            #   .to_parquet(fname='data/test', partition_cols=['crate', 'slot', 'channel'])
            if file_extension == '.ldf':
                number_of_words = (unpack('I', first_word)[0]) * data.WORD
                print(number_of_words)


if __name__ == "__main__":
    FILES = [
        # 'D:/data/svp/kafka-tests/kafka-data-test-0.pld',
        # 'D:/data/svp/kafka-tests/bagel-single-spill-1.pld',
        # 'D:/data/utk/pixieworkshop/pulser_003.ldf',
        'D:/data/ithemba/bagel/runs/runBaGeL_337.pld',
        # 'D:/data/anl/vandle2015/a135feb_12.ldf',
        # 'D:/data/utk/pixieworkshop/pulser_003.ldf',
        # 'C:/Users/stanp/Documents/projects/dolosse/data/timing_001.pld'
    ]

    with open('config.yaml') as f:
        cfg = yaml.safe_load(f)

    pool = ThreadPool(25)

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
                        total_data_buffer_size = (unpack('I', f.read(data.WORD))[0]) * data.WORD
                        pool.add_task(process_data_buffer, file_extension,
                                      BytesIO(f.read(total_data_buffer_size)))
                    elif chunk == data.DIR_BLOCK:
                        num_dir_blocks += 1
                        if file_extension == '.ldf':
                            BytesIO(f.read(
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
                                read_header(BytesIO(f.read(
                                ldf_constants.LDF_DATA_BUFFER_SIZE_WITHOUT_HEADER * data.WORD))))
                    else:
                        print(unpack('I', chunk)[0])
                        num_unknown += 1
                else:
                    break
            pool.wait_completion()
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
