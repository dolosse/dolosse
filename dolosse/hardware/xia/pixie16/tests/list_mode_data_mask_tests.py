"""
file: list_mode_data_mask_tests.py
brief: Unittests for the list_mode_data_mask class.
author: S. V. Paulauskas
date: April 25, 2019
"""
import unittest
from dolosse.hardware.xia.pixie16.list_mode_data_mask import ListModeDataMask

firmwares = [17562, 20466, 27361, 29432, 30474, 30980, 30981, 34688,
             34703, 35921]

frequencies = [100, 250, 500]


class ListModeDataMaskTestCase(unittest.TestCase):
    """ Class definition for the unittests """

    def test_construction(self):
        """ Tests that the constructor throws errors when we provide bad data"""
        for f in firmwares:
            with self.assertRaises(ValueError):
                ListModeDataMask(1234, f)
        for f in frequencies:
            with self.assertRaises(ValueError):
                ListModeDataMask(f, 10)

    def test_trace_length(self):
        """ Tests that we get the correct trace length for all firmwares"""
        for firmware in firmwares:
            for freq in frequencies:
                if firmware < 34688:
                    self.assertEqual(ListModeDataMask(freq, firmware).trace_length(),
                                     (0xFFFF0000, 16))


if __name__ == '__main__':
    unittest.main()
