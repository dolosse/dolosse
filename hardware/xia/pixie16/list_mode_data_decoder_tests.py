"""
file: list_mode_data_decoder_tests.py
brief: Unittests for the list_mode_data_decoder class.
author: S. V. Paulauskas
date: March 5, 2019
"""
import unittest
import hardware.xia.pixie16.list_mode_data_decoder as lmdd


class ListModeDataDecoderTestCase(unittest.TestCase):
    # def setUp(self):
    #    self.widget = lmdd.ListModeDataDecoder('The widget')

    def test_always_passes(self):
        self.assertEqual(10 % 2, 0)


if __name__ == '__main__':
    unittest.main()
