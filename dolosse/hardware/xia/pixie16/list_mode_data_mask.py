"""
file: list_mode_data_mask.py
brief: Defines data masks for all frequencies, bit resolutions, and revisions
author: S. V. Paulauskas
date: January 16, 2019
"""


class ListModeDataMask:
    """ Defines data masks used to decode data from XIA's Pixie-16 product line. """

    def __init__(self, frequency=250, firmware=30474):
        if frequency not in [100, 250, 500]:
            raise ValueError(self.__class__.__name__ + ": Please specify a valid frequency "
                                                       "[100, 250, 500].")
        if firmware < 17562:
            raise ValueError(self.__class__.__name__ + ": Please specify a valid firmware revision"
                                                       " greater than 17562.")
        self.frequency = frequency
        self.firmware = firmware

    @staticmethod
    def basic_header_words():
        """ The number of words that make up a basic header with no additional information """
        return 4

    @staticmethod
    def number_of_energy_sum_words():
        """ The number of words that make up the energy sums when they're in the header """
        return 4

    @staticmethod
    def number_of_external_timestamp_words():
        """ The number of words that make up an external timestamp when it's in the header """
        return 2

    @staticmethod
    def number_of_qdc_words():
        """ The number of words that make up the QDC when it's in the header """
        return 8

    @staticmethod
    def channel():
        """ Provides the mask needed to decode the Channel from Word 0 """
        return 0x0000000F, 0

    @staticmethod
    def slot():
        """ Provides the mask needed to decode the Slot from Word 0 """
        return 0x000000F0, 4

    @staticmethod
    def crate():
        """ Provides the mask needed to decode the Crate from Word 0 """
        return 0x00000F00, 8

    @staticmethod
    def header_length():
        """ Provides the mask needed to decode the header_length from Word 0 """
        return 0x0001F000, 12

    @staticmethod
    def finish_code():
        """ Provides the mask needed to decode the Finish Code from Word 0 """
        return 0x80000000, 31

    @staticmethod
    def event_time_high():
        """ Provides the mask needed to decode the Event Time High from Word 2 """
        return 0x0000FFFF, 0

    @staticmethod
    def trace_element():
        """ Pixie16 stores trace information in packed in two elements per word. This provides the
            mask needed to decode the trace element stored in the upper bits of the pair """
        return 0x0000FFFF, 16

    def event_length(self):
        """ Provides the mask needed to decode the Event Length in Word 0 """
        if self.firmware < 29432:
            return 0x3FFE0000, 17
        return 0x7FFE0000, 17

    def cfd_fractional_time(self):
        """
        The CFD Fractional Time always starts on Bit 16 of Header Word 2.
        :return: Tuple of mask and bit for shifting.
        """
        if self.frequency == 100:
            if self.firmware <= 29432:
                return 0xFFFF0000, 16
            return 0x7FFF0000, 16

        if self.frequency == 250:
            if self.firmware == 20466:
                return 0xFFFF0000, 16
            if 27361 <= self.firmware <= 29432:
                return 0x7FFF0000, 16

        if self.frequency == 500:
            if self.firmware >= 29432:
                return 0x1FFF0000, 16

        return 0x3FFF0000, 16  # We'll default to 250, > 29432

    def cfd_size(self):
        """ Provides the CFD Size, which we use to reconstruct the trigger's arrival time """
        if self.frequency == 500:
            return 8192
        if self.frequency == 100:
            if 17562 <= self.firmware < 30474:
                return 65536
            if self.firmware >= 30474:
                return 32768
        if self.frequency == 250:
            if self.firmware == 20466:
                return 65536
            if 27361 <= self.firmware < 30980:
                return 32768
        return 16384

    def cfd_trigger_source(self):
        """ Provides the mask needed to decode the CFD Trigger Source Bit in Word 2 """
        if self.frequency == 250:
            if 27361 <= self.firmware < 30474:
                return 0x80000000, 31
            if self.firmware >= 30474:
                return 0x40000000, 30
        if self.frequency == 500:
            if self.firmware >= 29432:
                return 0xE0000000, 29
        return 0x0, 0

    def cfd_forced_trigger(self):
        """ Provides the mask needed to decode the CDF Forced Trigger Bit in Word 2 """
        if self.frequency in [100, 250]:
            if self.firmware >= 30474:
                return 0x80000000, 31
        return 0x0, 0

    def energy(self):
        """ Provides the mask needed to decode the Energy from Word 3 """
        if self.firmware in [29432, 30474, 30980, 30981]:
            return 0x00007FFF, 0
        return 0x0000FFFF, 0

    def trace_length(self):
        """ Provides the mask needed to decode the Trace Length from Word 3"""
        if 17562 <= self.firmware < 34688:
            return 0xFFFF0000, 16
        return 0x7FFF0000, 16

    def trace_out_of_range(self):
        """ Provides the mask needed to decode the Trace Out of Range Bit """
        if 17562 <= self.firmware < 29432:
            return 0x40000000, 30
        if 29432 <= self.firmware < 34688:
            return 0x00008000, 15
        return 0x80000000, 31

    def generate_value_error_message(self, name):
        """ Generates the string used for ValueErrors """
        return "%s::%s - Could not determine the appropriate mask for firmware (%s) and " \
               "frequency (%s)" % (self.__class__.__name__, name, self.firmware, self.frequency)
