"""
file: binary_data_reader.py
brief: Reads binary data from files or other sources
author: S. V. Paulauskas
date: January 22, 2019
"""
from io import BytesIO
from multiprocessing import Pool
import os
from struct import unpack
import time

from pandas import DataFrame
import yaml

import dolosse.constants.data as data
from dolosse.hardware.xia.pixie16.list_mode_data_mask import ListModeDataMask
from dolosse.hardware.xia.pixie16.list_mode_data_decoder import decode_listmode_data
from dolosse.data_formats.pld import header


def process_data_buffer(args):
    """
    Processes a data buffer
    :param args: An arry where the first element is the file extension and the second element is
                 the data buffer
    :return: A list of decoded dictionary objects.
    """
    buffer = args[0]
    data_mask = args[1]
    while True:
        first_word = buffer.read(data.WORD)
        if first_word == b'':
            break

        module_words = (unpack('I', first_word)[0] - 2) * data.WORD
        module_number = unpack('I', buffer.read(data.WORD))[0]
        DataFrame(decode_listmode_data(BytesIO(buffer.read(module_words)), data_mask)) \
            .to_parquet(fname='data/test', partition_cols=['crate', 'slot', 'channel'])


if __name__ == "__main__":
    FILES = [
        # 'D:/data/svp/kafka-tests/kafka-data-test-0.pld',
        # 'D:/data/svp/kafka-tests/bagel-single-spill-1.pld',
        # 'D:/data/ithemba/bagel/runs/runBaGeL_337.pld',
        'X:/data/utk/polycfd/lowgain/lowgain_005.pld'
    ]

    with open('config.yaml') as f:
        cfg = yaml.safe_load(f)

    pool = Pool()

    for file in FILES:
        print("Working on file: ", file)
        filename, file_extension = os.path.splitext(file)

        if file_extension not in ['.pld']:
            raise NotImplementedError("Unrecognized file type: ", file_extension)
        file_size = os.path.getsize(file)
        data_buffer_list = results = []

        num_data_blocks = num_end_of_file = num_buffer_padding = num_head_blocks = num_unknown \
            = total_words = num_dir_blocks = 0

        with open(file, 'rb') as f:
            read_start_time = time.time()
            data_mask = ListModeDataMask(250, 30474)

            print("Started reading data buffers into memory...")
            while True:
                chunk = f.read(data.WORD)
                if chunk:
                    if chunk == data.DATA_BLOCK:
                        num_data_blocks += 1
                        # First word in a data buffer is the total size of the buffer
                        total_data_buffer_size = (unpack('I', f.read(data.WORD))[0] - 1) * data.WORD
                        data_buffer_list.append(
                            [BytesIO(f.read(total_data_buffer_size)), data_mask])
                    elif chunk == data.DIR_BLOCK:
                        num_dir_blocks += 1
                    elif chunk == data.END_OF_FILE:
                        num_end_of_file += 1
                    elif chunk == data.BUFFER_PADDING:
                        num_buffer_padding += 1
                    elif chunk == data.HEAD_BLOCK:
                        num_head_blocks += 1
                        if file_extension == '.pld':
                            # TODO : We should read this before we send so that if there's an issue
                            #        we don't get out of whack
                            print(header.PldHeader().read_header(f))
                    else:
                        num_unknown += 1
                else:
                    break

            unpacking_time = time.time()
            print("Finished reading buffers into memory in %s s." % (unpacking_time
                                                                     - read_start_time))
            print("Sending %s DATA buffers for decoding." % len(data_buffer_list))

            results = pool.map(process_data_buffer, data_buffer_list)
            decoding_time = time.time()
            print("Decoding completed in %s s. \nNow writing to JSONL file." % (
                    decoding_time - unpacking_time))

            num_triggers = 0
            data_list = []
            while results:
                data_list.extend(results.pop())
            if data_list:
                DataFrame(data_list).to_parquet(fname='data/test1',
                                                partition_cols=['crate', 'slot', 'channel'])

            print("Basic statistics for the file:",
                  "\n\tNumber of Dirs         : ", num_dir_blocks,
                  "\n\tNumber of Head         : ", num_head_blocks,
                  "\n\tNumber of Data Blocks  : ", num_data_blocks,
                  "\n\tNumber of Buffer Pads  : ", num_buffer_padding,
                  "\n\tNumber of End of Files : ", num_end_of_file,
                  "\n\tNumber of Unknowns     : ", num_unknown,
                  "\n\tTotal Words in File    : ", os.stat(file).st_size / 4,
                  "\n\tNumber of Triggers     : ", num_triggers,
                  "\n\tTime To Read (s)       : ", time.time() - read_start_time)

        f.close()
