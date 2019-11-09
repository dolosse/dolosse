"""
file: list_mode_data_decoder_tests.py
brief: Unittests for the list_mode_data_decoder class.
author: S. V. Paulauskas
date: March 5, 2019
"""
from io import BytesIO
import unittest

from dolosse.hardware.xia.pixie16.tests.test_data import Pixie16TestData as td, pack_data

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
        self.assertEqual(td.esums(decoded=True),
                         decoder.decode_energy_sums(BytesIO(td.esums(True))))

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

    def test_decode_trace(self):
        """
        Tests that we can decode a trace from the data stream.
        """
        self.assertEqual(td.trace(), decoder.decode_trace(BytesIO(td.trace(True))))

    def test_process_optional_header_data_bad_header_length(self):
        """Tests that we raise a Value Error when decoding a bad header length."""
        with self.assertRaises(ValueError):
            decoder.process_optional_header_data(BytesIO(td.external_timestamp(True)), 3, self.mask)

    def test_process_optional_header_data(self):
        """ Test that we can process all the various header data correctly. """
        self.assertDictEqual({'external_timestamp': td.external_timestamp()},
                             decoder.process_optional_header_data(
                                 BytesIO(td.external_timestamp(True)),
                                 decoder.HeaderCodes.HEADER_W_ETS, self.mask))
        self.assertDictEqual({'esums': td.esums(False, True)},
                             decoder.process_optional_header_data(BytesIO(td.esums(True)),
                                                                  decoder.HeaderCodes.HEADER_W_ESUM,
                                                                  self.mask))
        self.assertDictEqual(
            {'external_timestamp': td.external_timestamp(), 'esums': td.esums(False, True)},
            decoder.process_optional_header_data(
                BytesIO(td.esums(True) + td.external_timestamp(True)),
                decoder.HeaderCodes.HEADER_W_ESUM_ETS, self.mask))
        self.assertDictEqual({'qdc': td.qdc()},
                             decoder.process_optional_header_data(BytesIO(td.qdc(True)),
                                                                  decoder.HeaderCodes.HEADER_W_QDC,
                                                                  self.mask))
        self.assertDictEqual({'external_timestamp': td.external_timestamp(), 'qdc': td.qdc()},
                             decoder.process_optional_header_data(
                                 BytesIO(td.qdc(True) + td.external_timestamp(True)),
                                 decoder.HeaderCodes.HEADER_W_QDC_ETS, self.mask))
        self.assertDictEqual({'esums': td.esums(False, True), 'qdc': td.qdc()},
                             decoder.process_optional_header_data(
                                 BytesIO(td.esums(True) + td.qdc(True)),
                                 decoder.HeaderCodes.HEADER_W_ESUM_QDC, self.mask))
        self.assertDictEqual({'external_timestamp': td.external_timestamp(), 'qdc': td.qdc(),
                              'esums': td.esums(False, True)}, decoder.process_optional_header_data(
            BytesIO(td.esums(True) + td.qdc(True) + td.external_timestamp(True)),
            decoder.HeaderCodes.HEADER_W_ESUM_QDC_ETS, self.mask))

    def test_decode_listmode_data(self):
        """
        Test that we can decode a full 4 word header properly.
        """
        self.assertEqual([td.header(decoded=True)],
                         decoder.decode_listmode_data(BytesIO(td.header(as_bytes=True)), self.mask))
        self.assertEqual([{**td.header_with_trace(decoded=True), **{'trace': td.trace()}}],
                         decoder.decode_listmode_data(
                             BytesIO(td.header_with_trace(as_bytes=True) + td.trace(True)),
                             self.mask))

    def test_decode_listmode_data_bad_header_length(self):
        header = td.header()
        header[0] = 561197
        with self.assertRaises(BufferError):
            decoder.decode_listmode_data(BytesIO(pack_data(header, "I")), self.mask)

    def test_decode_listmode_data_bad_event_length(self):
        header = td.header()
        header[0] = 671789
        header[3] = 657705
        with self.assertRaises(ValueError):
            decoder.decode_listmode_data(BytesIO(pack_data(header, "I")), self.mask)


if __name__ == '__main__':
    unittest.main()
