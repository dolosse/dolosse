"""
file: binary_data_reader.py
brief: Reads data from a PLD file.
author: S. V. Paulauskas
date: January 22, 2019
"""
from io import BytesIO
from json import dumps
from multiprocessing import Pool
from os.path import isdir, splitext, basename
from os import mkdir, stat
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
    :param args: An arry where the first element is the data buffer and the second element is
                 the binary mask that we'll apply to the data.
    :return: A list of decoded dictionary objects.
    """
    remaining_bytes = args[0]
    buffer = args[1]
    mask = args[2]

    events = list()

    while remaining_bytes:
        module_bytes = unpack('I', buffer.read(data.WORD))[0] * data.WORD
        vsn = unpack('I', buffer.read(data.WORD))[0]
        data_bytes = module_bytes - 2 * data.WORD
        events = events + decode_listmode_data(BytesIO(buffer.read(data_bytes)), mask)
        remaining_bytes = remaining_bytes - module_bytes

    return events


def construct_log_message(name, message, extras=None):
    """
    TODO: Update this to actually use logging class...
    :param name: name of the file that we're processing
    :param message: the message that we'd like to include.
    :param extras: Extra information that we'd like to have in the message
    :return:
    """
    info = {
        'time': time.time(),
        'file': name,
        'message': message,
        'script': __file__,
    }
    if extras:
        info.update(extras)
    return dumps(info) + "\n"


def binary_data_reader():
    with open('config-dev.yaml') as f:
        cfg = yaml.safe_load(f)

    if not isdir(cfg['output']['root']):
        mkdir(cfg['output']['root'])

    log = open(cfg['output']['root'] + "/run.log", 'a')
    meta = open(cfg['output']['root'] + "/meta.log", 'a')

    for file in cfg['input']['file_list']:
        file_name, file_extension = splitext(basename(file))
        if file_extension not in ['.pld']:
            raise NotImplementedError("Unrecognized file type: ", file_extension)
        data_buffer_list = []
        log.write(construct_log_message(file_name, "Starting to process %s" % file))

        num_data_blocks = num_end_of_file = num_buffer_padding = num_head_blocks = num_unknown \
            = num_dir_blocks = num_dead_blocks = 0

        with open(file, 'rb') as f:
            read_start_time = time.time()
            data_mask = ListModeDataMask(cfg['hardware']['pixie']['frequency'],
                                         cfg['hardware']['pixie']['firmware'])

            log.write(
                construct_log_message(file_name, "Started reading data buffers into memory."))
            log.flush()
            while True:
                chunk = f.read(data.WORD)
                if chunk:
                    if chunk == data.DATA_BLOCK:
                        num_data_blocks += 1
                        buffer_bytes = unpack('I', f.read(data.WORD))[0] * data.WORD
                        data_buffer_list.append(
                            [buffer_bytes, BytesIO(f.read(buffer_bytes)), data_mask])
                    elif chunk == data.DIR_BLOCK:
                        num_dir_blocks += 1
                    elif chunk == data.END_OF_FILE:
                        num_end_of_file += 1
                    elif chunk == data.BUFFER_PADDING:
                        num_buffer_padding += 1
                    elif chunk == data.DEADTIME_BLOCK:
                        num_dead_blocks += 1
                    elif chunk == data.HEAD_BLOCK:
                        head_dict = {}
                        num_head_blocks += 1
                        if file_extension == '.pld':
                            head_dict = header.PldHeader().read_header(f)
                        head_dict.update(file=file_name)
                        meta.write(dumps(head_dict) + "\n")
                        meta.flush()
                    else:
                        num_unknown += 1
                else:
                    break

            unpacking_time = time.time()
            log.write(
                construct_log_message(file_name, "Finished reading buffers into memory in %s s." %
                                      (unpacking_time - read_start_time)))
            log.write(construct_log_message(file_name, "Sending %s DATA buffers for decoding." %
                                            len(data_buffer_list)))
            results = Pool().map(process_data_buffer, data_buffer_list)
            decoding_time = time.time()
            log.write(construct_log_message(file_name, "Decoding completed in %s s." %
                                            (decoding_time - unpacking_time)))

            data_list = []
            log.write(construct_log_message(file_name, "Aggregating triggers into single list."))
            log.flush()
            while results:
                data_list.extend(results.pop())

            log.write(construct_log_message(file_name, "Now writing to parquet."))
            log.flush()
            if data_list:
                DataFrame(data_list).to_parquet(path=f"{cfg['output']['root']}/{file_name}.parquet")

            log.write(construct_log_message(file_name, "Finished working on the file", {
                'number_of_dirs': num_dir_blocks,
                'number_of_heads': num_head_blocks,
                'number_of_data': num_data_blocks,
                'number_of_dead': num_dead_blocks,
                'number_of_pads': num_buffer_padding,
                'number_of_eof': num_end_of_file,
                'number_of_unknowns': num_unknown,
                'total_words': stat(file).st_size / 4,
                'time_to_read': time.time() - read_start_time
            }))
        f.close()
    log.close()
    meta.close()


if __name__ == '__main__':
    try:
        binary_data_reader()
    except KeyboardInterrupt:
        print("Exiting the program now.")
