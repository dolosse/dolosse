class list_mode_data_mask:
    def __init__(self, frequency, firmware):
        if frequency not in [100, 250, 500]:
            raise ValueError(self.__name__ + ": Please specify a valid frequency [100, 250, 500].")
        if firmware < 17562:
            raise ValueError(self.__name__ + ": Please specify a valid firmware revision greater than 17562.")
        self.frequency = frequency
        self.firmware = firmware

    @staticmethod
    def basic_header_words():
        return 4

    @staticmethod
    def number_of_energy_sum_words():
        return 4

    @staticmethod
    def number_of_external_timestamp_words():
        return 2

    @staticmethod
    def number_of_qdc_words():
        return 8

    @staticmethod
    def channel():
        return [0x0000000F, 0]

    @staticmethod
    def slot_id():
        return [0x000000F0, 4]

    @staticmethod
    def crate():
        return [0x00000F00, 8]

    @staticmethod
    def header_length():
        return [0x0001F000, 12]

    @staticmethod
    def finish_code():
        return [0x80000000, 31]

    @staticmethod
    def event_time_high():
        return [0x0000FFFF, 0]

    @staticmethod
    def trace_element():
        return [0x0000FFFF, 16]

    def event_length(self):
        if 17562 <= self.firmware < 29432:
            return [0x3FFE0000, 17]
        if self.firmware >= 29432:
            return [0x7FFE0000, 17]
        raise ValueError(self.generate_value_error_message('event_length'))

    def cfd_fractional_time(self):
        """
        The CFD Fractional Time always starts on Bit 16 of Header Word 2.
        :return: Tuple of mask and bit for shifting.
        """
        if self.frequency == 100:
            if self.firmware <= 29432:
                return [0xFFFF0000, 16]
            else:
                return [0x7FFF0000, 16]

        if self.frequency == 250:
            if self.firmware == 20466:
                return [0xFFFF0000, 16]
            elif 27361 <= self.firmware <= 29432:
                return [0x7FFF0000, 16]
            else:
                return [0x3FFF0000, 16]

        if self.frequency == 500:
            if self.firmware >= 29432:
                return [0x1FFF0000, 16]

        raise ValueError(self.generate_value_error_message('cfd_fractional_time'))

    def cfd_size(self):
        if self.frequency == 500:
            return 8192.
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
            if self.firmware >= 30980:
                return 16384
        raise ValueError(self.generate_value_error_message('cfd_size'))


    def cfd_trigger_source(self):
        if self.frequency == 250:
            if 27361 <= self.firmware < 30474:
                return [0x80000000, 31]
            elif self.firmware >= 30474:
                return [0x40000000, 30]
            else:
                return [0, 0]
        if self.frequency == 500:
            if self.firmware >= 29432:
                return [0xE0000000, 29]
        raise ValueError(self.generate_value_error_message('cfd_trigger_source'))

    def cfd_forced_trigger(self):
        if self.frequency == 100 or self.frequency == 250:
            if self.firmware >= 30474:
                return [0x80000000, 31]
        return ValueError(self.generate_value_error_message('cfd_forced_trigger'))

    def energy(self):
        if self.firmware in [29432, 30474, 30980, 30981]:
            return [0x00007FFF, 0]
        elif self.firmware in [17562, 20466, 27361, 34688]:
            return [0x0000FFFF, 0]
        else:
            raise ValueError(self.generate_value_error_message('energy'))

    def trace_length(self):
        if 17562 <= self.firmware < 34688:
            return [0xFFFF0000, 16]
        if self.firmware >= 34688:
            return [0xFFFF0000, 16]
        return ValueError(self.generate_value_error_message('trace_length'))

    def trace_out_of_range(self):
        if 17562 <= self.firmware < 29432:
            return [0x40000000, 30]
        if 29432 <= self.firmware < 34688:
            return [0x00008000, 15]
        if self.firmware >= 34688:
            return [0x80000000, 31]
        raise ValueError(self.generate_value_error_message('trace_out_of_range'))

    def generate_value_error_message(self, name):
        return "%s::%s - Could not determine the appropriate mask for firmware (%s) and frequency (%s)" % self.__name__,\
               name, self.firmware, self.frequency
