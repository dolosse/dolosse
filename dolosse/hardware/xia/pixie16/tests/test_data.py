"""
file: test_data.py
brief: File containing test data for the various Pixie16 test fixtures.
author: S. V. Paulauskas
date: November 02, 2019
"""
from struct import pack


def pack_data(data, type):
    """
    Packs an iterable into a bytes like object
    :param data: The data that we'll pack into the bytes object.
    :param type: The data type that we'll pack into, E.g. unsigned int, long int, double.
    :return: The bytes like object that we've packed from the list.
    """
    payload = b''
    for element in data:
        payload += pack(type, element)
    return payload


class Pixie16TestData:
    def __init__(self):
        pass

    @staticmethod
    def qdc(as_bytes=False):
        data = [123, 456, 789, 987, 654, 321, 147, 258]
        if as_bytes:
            return pack_data(data, 'I')
        return data

    @staticmethod
    def trace(as_bytes=False):
        data = [437, 436, 434, 434, 437, 437, 438, 435, 434, 438, 439, 437, 438,
                434, 435, 439, 438, 434, 434, 435, 437, 440, 439, 435, 437, 439,
                438, 435, 436, 436, 437, 439, 435, 433, 434, 436, 439, 441, 436,
                437, 439, 438, 438, 435, 434, 434, 438, 438, 434, 434, 437, 440,
                439, 438, 434, 436, 439, 439, 437, 436, 434, 436, 438, 437, 436,
                437, 440, 440, 439, 436, 435, 437, 501, 1122, 2358, 3509, 3816,
                3467, 2921, 2376, 1914, 1538, 1252, 1043, 877, 750, 667, 619,
                591, 563, 526, 458, 395, 403, 452, 478, 492, 498, 494, 477, 460,
                459, 462, 461, 460, 456, 452, 452, 455, 453, 446, 441, 440, 444,
                456, 459, 451, 450, 447, 445, 449, 456, 456, 455]
        if as_bytes:
            return pack_data(data, "H")
        return data

    @staticmethod
    def external_timestamp(as_bytes=False):
        data = [987654321, 1596]
        if as_bytes:
            return pack_data(data, "I")
        return data

    @staticmethod
    def esums(as_bytes=False, decoded=False):
        data = [12, 13, 14, 1164725159]
        if as_bytes:
            return pack_data(data, "I")
        if decoded:
            data[3] = 3780.728271484375
        return data

    @staticmethod
    def header(freq=250, firmware=30474, as_bytes=False, decoded=False):
        if freq == 250 and firmware == 30474:
            data = [540717, 123456789, 26001, 2345]
            if decoded:
                data = {
                    'channel': 13,
                    'slot': 2,
                    'crate': 0,
                    'header_length': 4,
                    'event_length': 4,
                    'finish_code': 0,
                    'event_time_low': 123456789,
                    'event_time_high': 26001,
                    'cfd_fractional_time': 0,
                    'cfd_trigger_source_bit': 0,
                    'cfd_forced_trigger_bit': 0,
                    'energy': 2345,
                    'trace_length': 0,
                    'trace_out_of_range': 0
                }
        if as_bytes:
            return pack_data(data, 'I')
        return data

    @staticmethod
    def header_with_trace(freq=250, firmware=30474, as_bytes=False, decoded=False):
        if freq == 250 and firmware == 30474:
            data = [8667181, 123456789, 26001, 8128809]
            if decoded:
                data = {
                    'channel': 13,
                    'slot': 2,
                    'crate': 0,
                    'header_length': 4,
                    'event_length': 66,
                    'finish_code': 0,
                    'event_time_low': 123456789,
                    'event_time_high': 26001,
                    'cfd_fractional_time': 0,
                    'cfd_trigger_source_bit': 0,
                    'cfd_forced_trigger_bit': 0,
                    'energy': 2345,
                    'trace_length': 124,
                    'trace_out_of_range': 0
                }
        if as_bytes:
            return pack_data(data, 'I')
        return data
