"""
file: list_mode_data_decoder_tests.py
brief: Unittests for the list_mode_data_decoder class.
author: S. V. Paulauskas
date: March 5, 2019
"""
from io import BytesIO
import unittest

from dolosse.common import decode_buffer


# TODO : We'll need to add tests to cover the major firmware revisions.
class DecodeBufferTestCase(unittest.TestCase):
    """
    Text fixture for decode_buffer
    """

    def test_decode_buffer_default(self):
        """
        Ensures that we can decode properly with the default arguments
        :return: Nothing
        """
        self.assertEqual(
            [123, 456, 789, 987, 654, 321, 135, 791],
            decode_buffer(
                BytesIO(b'\x7B\x00\x00\x00\xC8\x01\x00\x00\x15\x03\x00\x00\xDB\x03'
                        b'\x00\x00\x8E\x02\x00\x00\x41\x01\x00\x00\x87\x00\x00\x00'
                        b'\x17\x03\x00\x00')))

    def test_decode_buffer_mod(self):
        """
        Ensures that we can decode properly with non-default arguments
        :return: Nothing
        """
        self.assertEqual(
            [437, 436, 434, 434, 437, 437, 438, 435, 434, 438, 439, 437, 438, 434, 435,
             439, 438, 434, 434, 435, 437, 440, 439, 435, 437, 439, 438, 435, 436, 436,
             437, 439, 435, 433, 434, 436, 439, 441, 436, 437, 439, 438, 438, 435, 434,
             434, 438, 438, 434, 434, 437, 440, 439, 438, 434, 436, 439, 439, 437, 436,
             434, 436, 438, 437, 436, 437, 440, 440, 439, 436, 435, 437, 501, 1122,
             2358, 3509, 3816, 3467, 2921, 2376, 1914, 1538, 1252, 1043, 877, 750, 667,
             619, 591, 563, 526, 458, 395, 403, 452, 478, 492, 498, 494, 477, 460, 459,
             462, 461, 460, 456, 452, 452, 455, 453, 446, 441, 440, 444, 456, 459, 451,
             450, 447, 445, 449, 456, 456, 455],
            decode_buffer(
                BytesIO(b'\xB5\x01\xB4\x01\xB2\x01\xB2\x01\xB5\x01\xB5\x01\xB6\x01\xB3\x01'
                        b'\xB2\x01\xB6\x01\xB7\x01\xB5\x01\xB6\x01\xB2\x01\xB3\x01\xB7\x01'
                        b'\xB6\x01\xB2\x01\xB2\x01\xB3\x01\xB5\x01\xB8\x01\xB7\x01\xB3\x01'
                        b'\xB5\x01\xB7\x01\xB6\x01\xB3\x01\xB4\x01\xB4\x01\xB5\x01\xB7\x01'
                        b'\xB3\x01\xB1\x01\xB2\x01\xB4\x01\xB7\x01\xB9\x01\xB4\x01\xB5\x01'
                        b'\xB7\x01\xB6\x01\xB6\x01\xB3\x01\xB2\x01\xB2\x01\xB6\x01\xB6\x01'
                        b'\xB2\x01\xB2\x01\xB5\x01\xB8\x01\xB7\x01\xB6\x01\xB2\x01\xB4\x01'
                        b'\xB7\x01\xB7\x01\xB5\x01\xB4\x01\xB2\x01\xB4\x01\xB6\x01\xB5\x01'
                        b'\xB4\x01\xB5\x01\xB8\x01\xB8\x01\xB7\x01\xB4\x01\xB3\x01\xB5\x01'
                        b'\xF5\x01\x62\x04\x36\x09\xB5\x0D\xE8\x0E\x8B\x0D\x69\x0B\x48\x09'
                        b'\x7A\x07\x02\x06\xE4\x04\x13\x04\x6D\x03\xEE\x02\x9B\x02\x6B\x02'
                        b'\x4F\x02\x33\x02\x0E\x02\xCA\x01\x8B\x01\x93\x01\xC4\x01\xDE\x01'
                        b'\xEC\x01\xF2\x01\xEE\x01\xDD\x01\xCC\x01\xCB\x01\xCE\x01\xCD\x01'
                        b'\xCC\x01\xC8\x01\xC4\x01\xC4\x01\xC7\x01\xC5\x01\xBE\x01\xB9\x01'
                        b'\xB8\x01\xBC\x01\xC8\x01\xCB\x01\xC3\x01\xC2\x01\xBF\x01\xBD\x01'
                        b'\xC1\x01\xC8\x01\xC8\x01\xC7\x01'), 'H', 2))


if __name__ == '__main__':
    unittest.main()
