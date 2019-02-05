"""
file: data.py
brief: A namespace containing constants related to data management
author: S. V. Paulauskas
date: January 27, 2019
"""

# There are 4 bytes in 1 32-bit word, this will be the basis for our reads.
WORD = 4

# Here we define some of the keywords that define different types of blocks in the binary data.
DIR_BLOCK = b'DIR '
HEAD_BLOCK = b'HEAD'
DATA_BLOCK = b'DATA'
PAC_BLOCK = b'PAC '
SCALAR_BLOCK = b'SCAL'
DEADTIME_BLOCK = b'DEAD'
END_OF_FILE = b'EOF '
END_OF_BUFFER = b'EOB '
BUFFER_PADDING = b'\xff\xff\xff\xff'