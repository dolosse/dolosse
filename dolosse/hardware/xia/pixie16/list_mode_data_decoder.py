"""
file: list_mode_data_decoder.py
brief: Decodes binary data produced by an XIA Pixie16 module
author: S. V. Paulauskas
date: January 17, 2019
"""
from enum import Enum
from functools import partial
from io import BytesIO
from struct import unpack

from dolosse.constants.data import WORD, HALF_WORD


class HeaderCodes(Enum):
    """
    Defines the various header values that we expect. If we get something that's not one of these
    then we have a problem.
    """
    STATS_BLOCK = 1
    HEADER = 4
    HEADER_W_ETS = 6
    HEADER_W_ESUM = 8
    HEADER_W_ESUM_ETS = 10
    HEADER_W_QDC = 12
    HEADER_W_QDC_ETS = 14
    HEADER_W_ESUM_QDC = 16
    HEADER_W_ESUM_QDC_ETS = 18


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
        'header_length': (word & mask.header_length()[0]) >> mask.header_length()[1],
        'event_length': (word & mask.event_length()[0]) >> mask.event_length()[1],
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


def decode_external_timestamp(buf, mask):
    """
    External time stamps come in from the front panel, we will need to be able to decode them so that
    we can do cross system time correlations. This time works exactly like the channel event time
    in the header.
    :param buf: The buffer we'll be decoding
    :param mask: The mask we need to decode the data.
    :return:
    """
    return [unpack('I', buf.read(WORD))[0],
            (unpack('I', buf.read(WORD))[0] & mask.event_time_high()[0])
            >> mask.event_time_high()[1]]


def decode_qdc(buf):
    """
    The Pixie-16 modules collect QDC Sums of each signal they process. They're stored in 8 words.
    :param buf: Buffer containing the QDC data
    :return: An array containing the QDC data
    """
    qdc = []
    for chunk in iter(partial(buf.read, WORD), b''):
        qdc.append(unpack('I', chunk)[0])
    return qdc


def decode_trace(buf):
    """
    Pixie-16 stores traces with two samples per word. This is about the only place we need to use
    a half word to decode the data structure.
    :param buf: The buffer containing the trace.
    :return:
    """
    trc = []
    for chunk in iter(partial(buf.read, HALF_WORD), b''):
        trc.append(unpack('H', chunk)[0])
    return trc


def process_optional_header_data(buf, header_length, mask):
    """
    Process additional header data if they exist. We need to execute the buffer reads in order due
    to how the data structure works. The order is always ESUMS -> QDC -> EXTERNAL_TIMESTAMP (ETS).
    If one of the three is missing everything else shifts up.
    :param buf: The buffer that we'll be reading from
    :param header_length: The header length so that we know what the buffer contains.
    :param mask: The mask that we need to know how many words each component has.
    :return: A dictionary containing the decoded data.
    """
    data = {}
    if HeaderCodes(header_length) in [code for code in HeaderCodes if "ESUM" in code.name]:
        data['esums'] = \
            decode_energy_sums(BytesIO(buf.read(WORD * mask.number_of_energy_sum_words())))
    if HeaderCodes(header_length) in [code for code in HeaderCodes if "QDC" in code.name]:
        data['qdc'] = decode_qdc(BytesIO(buf.read(WORD * mask.number_of_qdc_words())))
    if HeaderCodes(header_length) in [code for code in HeaderCodes if "ETS" in code.name]:
        data['external_timestamp'] = decode_external_timestamp(
            BytesIO(buf.read(WORD * mask.number_of_external_timestamp_words())), mask)
    return data


def decode_listmode_data(stream, mask):
    """
    Decodes data from Pixie16 binary data stream. We'll have an unknown number of events in the
    data buffer. Therefore, we'll need to loop over the buffer and decode the events as we go. We
    store the decoded events as a dictionary, and put those dictionaries into an array.
    :param stream: The data stream that we'll be decoding
    :param mask: The binary data mask that we'll need to decode the data.
    """
    decoded_data_list = []
    for chunk in iter(partial(stream.read, WORD), b''):
        decoded_data = decode_word_zero(unpack('I', chunk)[0], mask)
        decoded_data.update({
            'event_time_low': unpack('I', stream.read(WORD))[0]})
        decoded_data.update(
            decode_word_two(unpack('I', stream.read(WORD))[0], mask))
        decoded_data.update(
            decode_word_three(unpack('I', stream.read(WORD))[0], mask))

        try:
            decoded_data.update(
                process_optional_header_data(stream, decoded_data['header_length'], mask))
        except ValueError:
            raise BufferError('Unexpected Header Length: %s\n\tCRATE:SLOT:CHAN = %s:%s:%s'
                              % (decoded_data['header_length'], decoded_data['crate'],
                                 decoded_data['slot'], decoded_data['channel']))

        if decoded_data['trace_length'] != 0:
            if decoded_data['event_length'] - decoded_data['header_length'] \
                    == decoded_data['trace_length'] * 0.5:
                decoded_data.update({'trace': decode_trace(BytesIO(stream.read(
                    WORD * int(decoded_data['trace_length'] * 0.5))))})
            else:
                raise ValueError(
                    'Event Length (%s) does not match with Header(%s) + Trace (%s) Length !' % (
                        decoded_data['event_length'], decoded_data['header_length'],
                        decoded_data['trace_length']))

        decoded_data_list.append(decoded_data)
    return decoded_data_list
