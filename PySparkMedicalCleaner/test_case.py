''''This script contains unit tests for the simpleone module.'''

import unittest
import simpleone


class Testcase(unittest.TestCase):

    # Test case for the cap_text function in simpleone module.
    def test_cap_text(self):
        self.assertEqual(simpleone.cap_text("hello world"), "Hello World")
        self.assertEqual(simpleone.cap_text(
            "python programming"), "Python Programming")
        self.assertEqual(simpleone.cap_text("test case"), "Test Case")
    '''Test case for empty string input.'''

    def test_cap_text_empty(self):
        self.assertEqual(simpleone.cap_text(""), "")
    '''Test case for multiple words input.'''

    def test_cap_text_multiple_words(self):
        self.assertEqual(simpleone.cap_text(
            "this test"), "This Test")
        self.assertEqual(simpleone.cap_text(
            "another here"), "Another Here")


if __name__ == '__main__':
    unittest.main()
