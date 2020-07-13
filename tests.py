import unittest
from helper import Sparkfn


class TestStringMethods(unittest.TestCase):
    'Testing UDF spark function'

    def test_pattern(self):
        urlList = ['https://domain1.com/display-product/1   ','  https://domain1.com/search-products','https://domain1.com/buy-product']
        for url in urlList:
            self.assertIsNotNone(Sparkfn.getPattern(url))

    def test_is_numeric(self):
        self.assertTrue(Sparkfn.is_numeric('1.12'))
        self.assertTrue(Sparkfn.is_numeric('1123'))
        self.assertFalse(Sparkfn.is_numeric('asc!'))
        self.assertFalse(Sparkfn.is_numeric('123.A'))

    def test_totimestamp(self):
        st_ts ='2020-10-10 10:10:10'
        self.assertTrue(isinstance(Sparkfn.to_timestamp(st_ts),int))

if __name__ == '__main__':
    unittest.main()
