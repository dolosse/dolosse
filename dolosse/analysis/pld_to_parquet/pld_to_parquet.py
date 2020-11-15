"""
file: pld_to_parquet.py
brief: Reads data from a PLD file.
author: S. V. Paulauskas
date: January 22, 2019
"""
from argparse import ArgumentParser
from io import BytesIO
from logging import config, getLogger
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


def pld_to_parquet():
    parser = ArgumentParser(description='Converts PLD files into Apache Parquet format.')
    parser.add_argument('cfg', type=str, default='config.yaml', help='The YAML configuration file')
    args = parser.parse_args()

    with open(args.cfg) as f:
        cfg = yaml.safe_load(f)

    config.dictConfig(cfg['logging'])
    logger = getLogger()

    if not isdir(cfg['output_directory']):
        mkdir(cfg['output_directory'])

    for file in cfg['input_files']:
        file_name, file_extension = splitext(basename(file))
        if file_extension not in ['.pld']:
            logger.error("Unrecognized file type: %s" % file_extension)
            continue
        data_buffer_list = []
        logger.info("Starting to process %s" % file)

        num_data_blocks = num_end_of_file = num_buffer_padding = num_head_blocks = num_unknown \
            = num_dir_blocks = num_dead_blocks = 0

        with open(file, 'rb') as f:
            read_start_time = time.time()
            data_mask = ListModeDataMask(cfg['hardware']['pixie']['frequency'],
                                         cfg['hardware']['pixie']['firmware'])
            logger.info("Started reading data buffers into memory.")
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
                        num_head_blocks += 1
                        logger.info("HEAD - %s" % header.PldHeader().read_header(f))
                    else:
                        num_unknown += 1
                else:
                    break

            unpacking_time = time.time()
            logger.info("Took %s s to read buffers." % round(unpacking_time - read_start_time, 3))

            logger.info("Sending %s DATA buffers for decoding." % len(data_buffer_list))
            results = Pool().map(process_data_buffer, data_buffer_list)

            logger.info("Aggregating triggers into single list.")
            data_list = []
            while results:
                data_list.extend(results.pop())

            decoding_time = time.time()
            logger.info("Decoding completed in %s s." % round(decoding_time - unpacking_time, 3))

            parquet_name = f"{cfg['output_directory']}/{file_name}.parquet"
            logger.info("Now writing to %s." % parquet_name)
            if not cfg["dry_run"] and data_list:
                DataFrame(data_list).to_parquet(path=parquet_name)
            else:
                logger.warning("Dry run enabled, will not write to parquet.")

            logger.info("Finished processing %s." % file)
            logger.info("SUMMARY - %s" % {
                'number_of_dirs': num_dir_blocks,
                'number_of_heads': num_head_blocks,
                'number_of_data': num_data_blocks,
                'number_of_dead': num_dead_blocks,
                'number_of_pads': num_buffer_padding,
                'number_of_eof': num_end_of_file,
                'number_of_unknowns': num_unknown,
                'total_words': stat(file).st_size / data.WORD,
                'processing_time_in_seconds': round(time.time() - read_start_time, 3)
            })
            f.close()


if __name__ == '__main__':
    try:
        pld_to_parquet()
    except KeyboardInterrupt:
        print("Exiting the program now.")
