import struct
import io
import time

dir_block = b'DIR '
head_block = b'HEAD'
data_block = b'DATA'
end_of_file = b'EOF '
buffer_padding = b'\xff\xff\xff\xff'

files = ['D:/data/svp/kafka-tests/kafka-data-test-0.pld',
         'D:/data/svp/kafka-tests/bagel-single-spill.pld',
         'D:/data/utk/pixieworkshop/pulser_003.ldf',
         'D:/data/ithemba/bagel/runs/runBaGeL_337.pld',
         'D:/data/anl/vandle2015/a135feb_12.ldf'
         ]

for file in files:
    print("Working on file: ", file)
    num_data_blocks = num_end_of_file = num_buffer_padding = num_head_blocks = num_other_stuff = total_words = num_dir_blocks = 0
    with io.open(file, 'rb') as f:
        read_start_time = time.time()
        while True:
            chunk = f.read(4)
            total_words += 1
            if chunk:
                if chunk == data_block:
                    num_data_blocks += 1
                elif chunk == dir_block:
                    num_dir_blocks += 1
                elif chunk == end_of_file:
                    num_end_of_file += 1
                elif chunk == buffer_padding:
                    num_buffer_padding += 1
                elif chunk == head_block:
                    num_head_blocks += 1
                else:
                    num_other_stuff += 1
                    # print(chunk, struct.unpack('i', chunk)[0])
            else:
                break
        print("Basic statistics for the file:",
              "\n\tNumber of Dirs         : ", num_dir_blocks,
              "\n\tNumber of Head         : ", num_head_blocks,
              "\n\tNumber of Data Blocks  : ", num_data_blocks,
              "\n\tNumber of Buffer Pads  : ", num_buffer_padding,
              "\n\tNumber of End of Files : ", num_end_of_file,
              "\n\tTotal Words in File    : ", total_words,
              "\n\tTime To Read (s)       : ", time.time() - read_start_time, )

    f.close()
