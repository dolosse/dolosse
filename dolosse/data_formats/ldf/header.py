"""
file: header.py
brief: Defines how to handle an LDF Header buffer
author: S. V. Paulauskas
date: February 04, 2019
"""
import struct

import dolosse.constants.data as data

class LdfHeader:
    """
    Defines a structure that reads the header information from a ORNL HEAD buffer
    """

    def __init__(self):
        self.run_number = self.max_spill_size = self.run_time = 0
        self.format = self.facility = self.start_date = self.end_date = self.title = ''

    def read_header(self, stream):
        """
        Reads the header out of the provided stream
        :param stream: the stream that contains the header to read
        :return: a dictionary containing the header information
        """
        return {
            'buffer_size': struct.unpack('I', stream.read(data.WORD))[0],
            'facility': stream.read(data.WORD * 2).decode(),
            'format': stream.read(data.WORD * 2).decode(),
            'type': stream.read(data.WORD * 4).decode(),
            'date': stream.read(data.WORD * 4).decode(),
            'title': stream.read(data.WORD * 20).decode(),
            'run_number': struct.unpack('I', stream.read(data.WORD))[0],
        }
