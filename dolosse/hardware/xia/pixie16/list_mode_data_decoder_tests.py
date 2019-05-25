"""
file: list_mode_data_decoder_tests.py
brief: Unittests for the list_mode_data_decoder class.
author: S. V. Paulauskas
date: March 5, 2019
"""
import unittest
import hardware.xia.pixie16.list_mode_data_decoder as lmdd
import hardware.xia.pixie16.list_mode_data_mask as lmdm


class ListModeDataDecoderTestCase(unittest.TestCase):
    def setUp(self):
        self.decoder = lmdd.ListModeDataDecoder(None, lmdm.ListModeDataMask(250, 30474))

    def test_decode_word_zero(self):
        self.assertEqual({
            'channel': 13,
            'slot': 2,
            'crate': 0,
            'header_length': 4,
            'event_length': 4,
            'finish_code': 0
        }, self.decoder.decode_word_zero(540717))


if __name__ == '__main__':
    unittest.main()
