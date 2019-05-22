"""
file: list_mode_data_decoder.py
brief: Decodes binary data produced by an XIA Pixie16 module
author: S. V. Paulauskas
date: January 17, 2019
"""
from functools import partial
import struct
import threading

import dolosse.constants.data as data


def decode_word_zero(word, mask):
    """

    :param word: The word that we're going to decode
    :return: A dictionary containing the decoded information.
    """
    return {
        'channel': (word & mask.channel()[0]) >> mask.channel()[1],
        'slot': (word & mask.slot()[0]) >> mask.slot()[1],
        'crate': (word & mask.crate()[0]) >> mask.crate()[1],
        'header_length': (word & mask.header_length()[0]) >>
                         mask.header_length()[1],
        'event_length': (word & mask.event_length()[0]) >>
                        mask.event_length()[1],
        'finish_code': (word & mask.finish_code()[0]) >> mask.finish_code()[1]
    }


def decode_word_one(word, mask):
    """
    All Pixie16 firmwares use Word 1 as the low 32-bits of the time stamp
    :param word: The word that we're not going to do anything with.
    :return: The input word
    """
    return word


def decode_word_two(word, mask):
    print(word)


def decode_word_three(word, mask):
    print(word)


def decode_external_timestamps():
    print("decoding timestamps")


def decode_qdc():
    print("decoding qdc")


def decode_energy_sums():
    print("decoding energysums")


def decode_trace():
    print("decoding trace")


class ListModeDataDecoder(threading.Thread):
    """
    Class that loops through a data stream to locate and process Pixie16
    list mode data.
    """

    def __init__(self, stream, mask):
        """
        Constructor
        :param stream: The stream that we'll read data from
        :param mask: the data mask that we'll use to decode data
        """
        threading.Thread.__init__(self)
        self.stream = stream
        self.mask = mask
        self.finished = False

    def run(self):
        """ Decodes data from Pixie16 binary data stream """
        # TODO : Will need to have it create a list of the dictionaries and return that list.
        for chunk in iter(partial(self.stream.read, data.WORD), b''):
            dict0 = decode_word_zero(struct.unpack('I', chunk)[0], self.mask)
            dict1 = decode_word_one(struct.unpack('I', self.stream.read(data.WORD))[0], self.mask)
            dict2 = decode_word_two(struct.unpack('I', self.stream.read(data.WORD))[0], self.mask)
            dict3 = decode_word_three(struct.unpack('I', self.stream.read(data.WORD))[0], self.mask)

            decoded_data = {
                'channel': (word0 & self.mask.channel()[0]) >> self.mask.channel()[1],
                'slot': (word0 & self.mask.slot()[0]) >> self.mask.slot()[1],
                'crate': (word0 & self.mask.crate()[0]) >> self.mask.crate()[1],
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
        self.finished = True
