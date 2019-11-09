"""
file: list_mode_data_mask_tests.py
brief: Unittests for the list_mode_data_mask class.
author: S. V. Paulauskas
date: April 25, 2019
"""
from itertools import product
import unittest
from dolosse.hardware.xia.pixie16.list_mode_data_mask import ListModeDataMask as lmdmask


class lmdmaskTestCase(unittest.TestCase):
    """ Class definition for the unittests """

    def setUp(self):
        """
        Configures the set of firmwares and frequencies that we'll be testing.
        :return:
        """
        self.firmwares = [17562, 20466, 27361, 29432, 30474, 30980, 30981, 34688, 34703, 35921]
        self.frequencies = [100, 250, 500]
        self.combos = product(self.firmwares, self.frequencies)

    def test_event_length(self):
        """Tests that we get the right event length depending on our frequency and firmware."""
        for firm, freq in self.combos:
            if firm < 29432:
                self.assertEqual((0x3FFE0000, 17), lmdmask(freq, firm).event_length())
            else:
                self.assertEqual((0x7FFE0000, 17), lmdmask(freq, firm).event_length())

    def test_construction_bad_firmware(self):
        """ Tests that the constructor throws errors when we provide Firmware less than 17562"""
        for f in self.firmwares:
            with self.assertRaises(ValueError):
                lmdmask(1234, f)

    def test_construction_bad_frequency(self):
        """Ensure constructor raises ValueError with frequencies not in [100, 250, 500]"""
        for f in self.frequencies:
            with self.assertRaises(ValueError):
                lmdmask(f, 10)

    def test_basic_header_words(self):
        """Tests that basic_header_words is always equal to 4."""
        for firm, freq in self.combos:
            self.assertEqual(4, lmdmask(freq, firm).basic_header_words())

    def test_trace_element(self):
        """Tests that the trace element parsing is always the same."""
        for firm, freq in self.combos:
            self.assertEqual((0x0000FFFF, 16), lmdmask(freq, firm).trace_element())

    def test_cdf_fractional_time(self):
        """Tests that we get the right combinations for CDF fractional times."""
        for firm, freq in self.combos:
            if freq == 100:
                if firm <= 29432:
                    self.assertEqual((0xFFFF0000, 16), lmdmask(freq, firm).cfd_fractional_time())
                else:
                    self.assertEqual((0x7FFF0000, 16), lmdmask(freq, firm).cfd_fractional_time())
            elif freq == 250:
                if firm == 20466:
                    self.assertEqual((0xFFFF0000, 16),
                                     lmdmask(freq, firm).cfd_fractional_time())
                elif 27361 <= firm <= 29432:
                    self.assertEqual((0x7FFF0000, 16), lmdmask(freq, firm).cfd_fractional_time())
                else:
                    self.assertEqual((0x3FFF0000, 16), lmdmask(freq, firm).cfd_fractional_time())
            elif freq == 500:
                if firm >= 29432:
                    self.assertEqual((0x1FFF0000, 16), lmdmask(freq, firm).cfd_fractional_time())
                else:
                    self.assertEqual((0x3FFF0000, 16), lmdmask(freq, firm).cfd_fractional_time())

    def test_cdf_size(self):
        """ Tests the CFD Size, which we use to reconstruct the trigger's arrival time """
        for firm, freq in self.combos:
            if freq == 100:
                if 17562 <= firm < 30474:
                    self.assertEqual(65536, lmdmask(freq, firm).cfd_size())
                if firm >= 30474:
                    self.assertEqual(32768, lmdmask(freq, firm).cfd_size())
            elif freq == 250:
                if firm == 20466:
                    self.assertEqual(65536, lmdmask(freq, firm).cfd_size())
                elif 27361 <= firm < 30980:
                    self.assertEqual(32768, lmdmask(freq, firm).cfd_size())
                else:
                    self.assertEqual(16384, lmdmask(freq, firm).cfd_size())
            elif freq == 500:
                self.assertEqual(8192, lmdmask(freq, firm).cfd_size())

    def test_cdf_trigger_source(self):
        """ Provides the mask needed to decode the CFD Trigger Source Bit in Word 2 """
        for firm, freq in self.combos:
            if freq == 250:
                if 27361 <= firm < 30474:
                    self.assertEqual((0x80000000, 31), lmdmask(freq, firm).cfd_trigger_source())
                if firm >= 30474:
                    self.assertEqual((0x40000000, 30), lmdmask(freq, firm).cfd_trigger_source())
            elif freq == 500:
                if firm >= 29432:
                    self.assertEqual((0xE0000000, 29), lmdmask(freq, firm).cfd_trigger_source())
            else:
                self.assertEqual((0x0, 0), lmdmask(freq, firm).cfd_trigger_source())

    def test_trace_length(self):
        """ Tests that we get the correct trace length for all firmwares"""
        for firmware, freq in self.combos:
            if firmware < 34688:
                self.assertEqual((0xFFFF0000, 16), lmdmask(freq, firmware).trace_length())
            else:
                self.assertEqual((0x7FFF0000, 16), lmdmask(freq, firmware).trace_length())

    def test_cfd_forced_trigger(self):
        """ Tests that we decode the CDF Forced Trigger Bit in Word 2 """
        for firm, freq in self.combos:
            if freq in [100, 250]:
                if firm >= 30474:
                    self.assertEqual((0x80000000, 31), lmdmask(freq, firm).cfd_forced_trigger())
            else:
                self.assertEqual((0x0, 0), lmdmask(freq, firm).cfd_forced_trigger())

    def test_energy(self):
        """ Tests that we decode the Energy from Word 3 """
        for firm, freq in self.combos:
            if firm in [29432, 30474, 30980, 30981]:
                self.assertEqual((0x00007FFF, 0), lmdmask(freq, firm).energy())
            else:
                self.assertEqual((0x0000FFFF, 0), lmdmask(freq, firm).energy())

    def test_trace_out_of_range(self):
        """ Tests that we decode the Trace Out of Range Bit """
        for firm, freq in self.combos:
            if 17562 <= firm < 29432:
                self.assertEqual((0x40000000, 30), lmdmask(freq, firm).trace_out_of_range())
            elif 29432 <= firm < 34688:
                self.assertEqual((0x00008000, 15), lmdmask(freq, firm).trace_out_of_range())
            else:
                self.assertEqual((0x80000000, 31), lmdmask(freq, firm).trace_out_of_range())

    def test_generate_value_error_message(self):
        """ Tests that we generate the string used for ValueErrors """
        for firm, freq in self.combos:
            expected = "%s::%s - Could not determine the appropriate mask for firmware (%s) and " \
                       "frequency (%s)" % ('ListModeDataMask', 'test', firm, freq)
            self.assertEqual(expected, lmdmask(freq, firm).generate_value_error_message('test'))


if __name__ == '__main__':
    unittest.main()
