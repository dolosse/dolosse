"""
file: list_mode_data_decoder.py
brief: Decodes binary data produced by an XIA Pixie16 module
author: S. V. Paulauskas
date: January 17, 2019
"""
from functools import partial
from struct import unpack

from dolosse.constants.data import WORD


def decode_word_zero(word, mask):
    """
    Decodes the
       * Channel
       * Slot
       * Crate
       * Header Length (Base 4 words + options)
       * Event Length (Header + Trace Length / 2)
       * Finish Code
    :param word: The word that we're going to decode
    :param mask: The mask that we'll be using.
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


def decode_word_two(word, mask):
    """
    Decodes the second word in the standard 4 word header.
    :param word: The word that we're going to decode.
    :param mask: The mask we'll use to decode that word.
    :return: A dictionary containing the decoded information
    """
    return {
        'event_time_high': (word & mask.event_time_high()[0]) >> mask.event_time_high()[1],
        'cfd_fractional_time': (word & mask.cfd_fractional_time()[0]) >>
                               mask.cfd_fractional_time()[1],
        'cfd_trigger_source_bit': (word & mask.cfd_trigger_source()[0]) >>
                                  mask.cfd_trigger_source()[1],
        'cfd_forced_trigger_bit': (word & mask.cfd_forced_trigger()[0]) >>
                                  mask.cfd_forced_trigger()[1]
    }


def decode_word_three(word, mask):
    """
    The final word of the standard header.
    :param word: The word that we're going to decode.
    :param mask: The mask we'll use to decode that word.
    :return: A dictionary containing the decoded information
    """
    return {
        'energy': (word & mask.energy()[0]) >> mask.energy()[1],
        'trace_length': (word & mask.trace_length()[0]) >> mask.trace_length()[1],
        'trace_out_of_range': (word & mask.trace_out_of_range()[0]) >> mask.trace_out_of_range()[1]
    }


def decode_energy_sums(buf):
    """
    The first three words are the leading edge, flattop, and falling edge of the trapezoidal filter.
    The last word is the baseline encoded in IEEE 754 format. We need to decode this set of data
    in a different way than the other header words.
    https://en.wikipedia.org/wiki/IEEE_754#IEEE_754-2008
    https://stackoverflow.com/questions/39593087/double-conversion-to-decimal-value-ieee-754-in-python
    :param buf: The buffer containing the encoded information
    :return: An array with the elements we decoded.
    """
    return [unpack('I', buf.read(WORD))[0],
            unpack('I', buf.read(WORD))[0],
            unpack('I', buf.read(WORD))[0],
            unpack(b'<f', buf.read(WORD))[0]]


def decode_listmode_data(stream, mask):
    """
    Decodes data from Pixie16 binary data stream. We'll have an unknown number of events in the
    data buffer. Therefore, we'll need to loop over the buffer and decode the events as we go. We
    store the decoded events as a dictionary, and put those dictionaries into an array.
    :param stream: The data stream that we'll be decoding
    :param mask: The binary data mask that we'll need to decode the data.
    """
    # TODO : Update the loop here to use decode buffer for the first 4 words of the header.
    # TODO : Will need to add in decoding of optional header information
    decoded_data_list = []
    for chunk in iter(partial(stream.read, WORD), b''):
        decoded_data = decode_word_zero(unpack('I', chunk)[0], mask)
        decoded_data.update({
            'event_time_low': unpack('I', stream.read(WORD))[0],
        })
        decoded_data.update(
            decode_word_two(unpack('I', stream.read(WORD))[0], mask))
        decoded_data.update(
            decode_word_three(unpack('I', stream.read(WORD))[0], mask))
        decoded_data_list.append(decoded_data)
    return decoded_data_list
