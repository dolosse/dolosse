"""
Reads binary data from files or other sources
"""
from functools import partial
import io
import json
import os
import struct
import time
import threading

from pixie16.list_mode_data_mask import ListModeDataMask

DIR_BLOCK = b'DIR '
HEAD_BLOCK = b'HEAD'
DATA_BLOCK = b'DATA'
END_OF_FILE = b'EOF '
END_OF_BUFFER = b'EOB '
BUFFER_PADDING = b'\xff\xff\xff\xff'
# There are 4 bytes in 1 32-bit word, this will be the basis for our reads.
WORD = 4


def read_pld_header(stream):
    """ Reads the header from a UTK PLD file. """
    return {
        'run_number': struct.unpack('I', stream.read(WORD))[0],
        'max_spill_size': struct.unpack('I', stream.read(WORD))[0],
        'run_time': struct.unpack('I', stream.read(WORD))[0],
        'format': stream.read(WORD * 4).decode(),
        'facility': stream.read(WORD * 4).decode(),
        'start_date': stream.read(WORD * 6).decode(),
        'end_date': stream.read(WORD * 6).decode(),
        'title': stream.read(struct.unpack('I', stream.read(WORD))[0]).decode()
    }


def decode_data(stream, mask, outfile):
    """ Decodes data from Pixie16 binary data stream """
    decoded_data = []
    for chunk in iter(partial(stream.read, WORD), b''):
        word0 = struct.unpack('I', chunk)[0]
        word1 = struct.unpack('I', stream.read(WORD))[0]
        word2 = struct.unpack('I', stream.read(WORD))[0]
        word3 = struct.unpack('I', stream.read(WORD))[0]

        data = {
            'crate': (word0 & mask.crate()[0]) >> mask.crate()[1],
            'slot': (word0 & mask.slot()[0]) >> mask.slot()[1],
            'channel': (word0 & mask.channel()[0]) >> mask.channel()[1],
            'header_length': (word0 & mask.header_length()[0]) >> mask.header_length()[1],
            'event_length': (word0 & mask.event_length()[0]) >> mask.event_length()[1],
            'finish_code': (word0 & mask.finish_code()[0]) >> mask.finish_code()[1],
            'event_time_low': word1,
            'event_time_high': (word2 & mask.event_time_high()[0]) >> mask.event_time_high()[1],
            'cfd_fractional_time': (word2 & mask.cfd_fractional_time()[0]) >>
                                   mask.cfd_fractional_time()[1],
            'cfd_trigger_source_bit': (word2 & mask.cfd_trigger_source()[0]) >>
                                      mask.cfd_trigger_source()[1],
            'cfd_forced_trigger_bit': (word2 & mask.cfd_forced_trigger()[0]) >>
                                      mask.cfd_forced_trigger()[1],
            'energy': (word3 & mask.energy()[0]) >> mask.energy()[1],
            'trace_length': (word3 & mask.trace_length()[0]) >> mask.trace_length()[1],
            'trace_out_of_range': (word3 & mask.trace_out_of_range()[0]) >>
                                  mask.trace_out_of_range()[1]
        }
        decoded_data.append(data)
        decoded_data.append("\n")
    outfile.write(json.dumps(decoded_data))
    outfile.write("\n")
    # return data


FILES = [
    # 'D:/data/svp/kafka-tests/kafka-data-test-0.pld',
    # 'D:/data/svp/kafka-tests/bagel-single-spill-1.pld',
    #    'D:/data/utk/pixieworkshop/pulser_003.ldf',
    'D:/data/ithemba/bagel/runs/runBaGeL_337.pld',
    #    'D:/data/anl/vandle2015/a135feb_12.ldf'
]

for file in FILES:
    print("Working on file: ", file)
    filename, file_extension = os.path.splitext(file)

    num_data_blocks = num_end_of_file = num_buffer_padding = num_head_blocks = num_unknown \
        = total_words = num_dir_blocks = 0

    output_file = open("test.dat", "w")

    with open(file, 'rb') as f:
        read_start_time = time.time()
        data_mask = ListModeDataMask(250, 30474)

        while True:
            chunk = f.read(WORD)
            if chunk:
                if chunk == DATA_BLOCK:
                    num_data_blocks += 1

                    # First word in a PLD data buffer is the total size of the buffer
                    total_data_buffer_size = (struct.unpack('I', f.read(WORD))[0]) * WORD

                    # Reads the entire data buffer in one shot.
                    with io.BytesIO(f.read(total_data_buffer_size)) as buffer:
                        threads = []
                        while True:
                            first_word = buffer.read(WORD)
                            if first_word == b'':
                                break
                            module_words = (struct.unpack('I', first_word)[0] - 2) * WORD
                            module_number = struct.unpack('I', buffer.read(WORD))[0]

                            thread = threading.thread(
                                target=decode_data(io.BytesIO(buffer.read(module_words)), data_mask,
                                                   output_file))
                            thread.start()
                            threads.append(thread)

                            # output_file.write(json.dumps(data, indent=2) + ",\n")
                elif chunk == DIR_BLOCK:
                    num_dir_blocks += 1
                elif chunk == END_OF_FILE:
                    num_end_of_file += 1
                elif chunk == BUFFER_PADDING:
                    num_buffer_padding += 1
                elif chunk == HEAD_BLOCK:
                    num_head_blocks += 1
                    print(read_pld_header(f))
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
