"""
file: header.py
brief: Defines how to handle PLD headers
author: S. V. Paulauskas
date: January 26, 2019
"""
import struct

import constants.data as data


class PldHeader:
    def __init__(self):
        self.run_number = self.max_spill_size = self.run_time = 0
        self.format = self.facility = self.start_date = self.end_date = self.title = ''

    def read_header(self, stream):
        """ Reads the header from a UTK PLD file. """
        return {
            'run_number': struct.unpack('I', stream.read(data.WORD))[0],
            'max_spill_size': struct.unpack('I', stream.read(data.WORD))[0],
            'run_time': struct.unpack('I', stream.read(data.WORD))[0],
            'format': stream.read(data.WORD * 4).decode(),
            'facility': stream.read(data.WORD * 4).decode(),
            'start_date': stream.read(data.WORD * 6).decode(),
            'end_date': stream.read(data.WORD * 6).decode(),
            'title': stream.read(struct.unpack('I', stream.read(data.WORD))[0]).decode()
        }
