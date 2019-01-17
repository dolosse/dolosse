"""
    file: list_mode_data_decoder.py
    brief: Decodes binary data produced by an XIA Pixie16 module
    author: S. V. Paulauskas
    date: January 17, 2019
"""
from functools import partial
import json
import struct
import threading


class ListModeDataDecoder(threading.Thread):
    """
    Class that loops through a data stream to locate and process Pixie16
    list mode data.
    """

    def __init__(self, stream, mask, outfile):
        threading.Thread.__init__(self)
        self.stream = stream
        self.mask = mask
        self.outfile = outfile

    def run(self):
        """ Decodes data from Pixie16 binary data stream """
        word = 4
        decoded_data = []
        for chunk in iter(partial(self.stream.read, word), b''):
            word0 = struct.unpack('I', chunk)[0]
            word1 = struct.unpack('I', self.stream.read(word))[0]
            word2 = struct.unpack('I', self.stream.read(word))[0]
            word3 = struct.unpack('I', self.stream.read(word))[0]

            data = {
                'crate': (word0 & self.mask.crate()[0]) >> self.mask.crate()[1],
                'slot': (word0 & self.mask.slot()[0]) >> self.mask.slot()[1],
                'channel': (word0 & self.mask.channel()[0]) >> self.mask.channel()[1],
                'header_length': (word0 & self.mask.header_length()[0]) >>
                                 self.mask.header_length()[1],
                'event_length': (word0 & self.mask.event_length()[0]) >>
                                self.mask.event_length()[1],
                'finish_code': (word0 & self.mask.finish_code()[0]) >> self.mask.finish_code()[1],
                'event_time_low': word1,
                'event_time_high': (word2 & self.mask.event_time_high()[0]) >>
                                   self.mask.event_time_high()[1],
                'cfd_fractional_time': (word2 & self.mask.cfd_fractional_time()[0]) >>
                                       self.mask.cfd_fractional_time()[1],
                'cfd_trigger_source_bit': (word2 & self.mask.cfd_trigger_source()[0]) >>
                                          self.mask.cfd_trigger_source()[1],
                'cfd_forced_trigger_bit': (word2 & self.mask.cfd_forced_trigger()[0]) >>
                                          self.mask.cfd_forced_trigger()[1],
                'energy': (word3 & self.mask.energy()[0]) >> self.mask.energy()[1],
                'trace_length': (word3 & self.mask.trace_length()[0])
                                >> self.mask.trace_length()[1],
                'trace_out_of_range': (word3 & self.mask.trace_out_of_range()[0])
                                      >> self.mask.trace_out_of_range()[1]
            }
            decoded_data.append(data)
            decoded_data.append("\n")
        #self.outfile.write(json.dumps(decoded_data))
        #self.outfile.write("\n")
