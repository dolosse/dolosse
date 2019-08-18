"""
file: list_mode_data_decoder_tests.py
brief: Unittests for the list_mode_data_decoder class.
author: S. V. Paulauskas
date: March 5, 2019
"""
from io import BytesIO
import unittest

import dolosse.hardware.xia.pixie16.list_mode_data_decoder as decoder
import dolosse.hardware.xia.pixie16.list_mode_data_mask as lmdm


# TODO : We'll need to add tests to cover the major firmware revisions.
class ListModeDataDecoderTestCase(unittest.TestCase):
    """
    Test fixture for the List Mode Data Decoder functions
    """
    def setUp(self):
        """
        Configures the data mask for all the tests to use
        """
        self.mask = lmdm.ListModeDataMask(250, 30474)

    def test_decode_word_zero(self):
        """
        Tests that we can decode the first word of the header properly
        """
        self.assertEqual({
            'channel': 13,
            'slot': 2,
            'crate': 0,
            'header_length': 4,
            'event_length': 4,
            'finish_code': 0
        }, decoder.decode_word_zero(540717, self.mask))

    def test_decode_word_two(self):
        """
        Checks that we can decode the third word of the header properly
        """
        self.assertEqual({
            'event_time_high': 26001,
            'cfd_fractional_time': 0,
            'cfd_trigger_source_bit': 0,
            'cfd_forced_trigger_bit': 0
        }, decoder.decode_word_two(26001, self.mask))

    def test_decode_word_three(self):
        """
        Test that we can decode the final word of the header properly
        """
        self.assertEqual({
            'energy': 2345,
            'trace_length': 0,
            'trace_out_of_range': 0
        }, decoder.decode_word_three(2345, self.mask))

    def test_decode_energy_sums(self):
        """
        Test that we can decode the energy sums. These can be tricky
        b/c the baseline is encoded in IEEE 754 format.
        """
        result = decoder.decode_energy_sums(
            BytesIO(b'\x0C\x00\x00\x00\x0D\x00\x00\x00\x0E\x00\x00\x00\xA7\x4B\x6C\x45'))
        self.assertEqual([12, 13, 14], result[:3])
        self.assertAlmostEqual(3780.72827148438, result[3])

    def test_decode_listmode_data(self):
        """
        Test that we can decode a full 4 word header properly.
        """
        self.assertEqual(
            decoder.decode_listmode_data(
                BytesIO(b'\x2D\x40\x08\x00\x15\xCD\x5B\x07\x91\x65\x00\x00\x29\x09\x00\x00'),
                self.mask),
            [{
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
            }])


if __name__ == '__main__':
    unittest.main()
