"""
file: list_mode_data_decoder_tests.py
brief: Unittests for the list_mode_data_decoder class.
author: S. V. Paulauskas
date: March 5, 2019
"""
from io import BytesIO
import unittest

from dolosse.hardware.xia.pixie16.tests.test_data import Pixie16TestData as td

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
        self.frequency = 250
        self.firmware = 30474
        self.mask = lmdm.ListModeDataMask(self.frequency, self.firmware)

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
        }, decoder.decode_word_zero(td.header(self.frequency, self.firmware)[0], self.mask))

    def test_decode_word_two(self):
        """
        Checks that we can decode the third word of the header properly
        """
        self.assertEqual({
            'event_time_high': 26001,
            'cfd_fractional_time': 0,
            'cfd_trigger_source_bit': 0,
            'cfd_forced_trigger_bit': 0
        }, decoder.decode_word_two(td.header(self.frequency, self.firmware)[2], self.mask))

    def test_decode_word_three(self):
        """
        Test that we can decode the final word of the header properly
        """
        self.assertEqual({
            'energy': 2345,
            'trace_length': 0,
            'trace_out_of_range': 0
        }, decoder.decode_word_three(td.header(self.frequency, self.firmware)[3], self.mask))

    def test_decode_energy_sums(self):
        """
        Test that we can decode the energy sums. These can be tricky
        b/c the baseline is encoded in IEEE 754 format.
        """
        result = decoder.decode_energy_sums(BytesIO(td.esums(True)))
        self.assertEqual(td.esums()[:3], result[:3])
        self.assertAlmostEqual(3780.72827148438, result[3])


    def test_decode_external_timestamp(self):
        """
        Tests that we can decode external timestamps appropriately.
        """
        self.assertEqual(td.external_timestamp(), decoder.decode_external_timestamp(
            BytesIO(td.external_timestamp(True)), self.mask))

    def test_decode_qdc(self):
        """
        Tests that we can decode the QDC header into an array.
        """
        self.assertEqual(td.qdc(), decoder.decode_qdc(BytesIO(td.qdc(True))))

    def test_decode_listmode_data(self):
        """
        Test that we can decode a full 4 word header properly.
        """
        self.assertEqual(
            decoder.decode_listmode_data(
                BytesIO(td.header(self.frequency, self.firmware, True)),
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
