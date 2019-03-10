import unittest
from pg8000 import Interval


# Type conversion tests
class Tests(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testIntervalConstructor(self):
        i = Interval(days=1)
        self.assertEqual(i.months, 0)
        self.assertEqual(i.days, 1)
        self.assertEqual(i.microseconds, 0)

    def intervalRangeTest(self, parameter, in_range, out_of_range):
        for v in out_of_range:
            try:
                Interval(**{parameter: v})
                self.fail("expected OverflowError")
            except OverflowError:
                pass
        for v in in_range:
            Interval(**{parameter: v})

    def testIntervalDaysRange(self):
        out_of_range_days = (-2147483648, +2147483648,)
        in_range_days = (-2147483647, +2147483647,)
        self.intervalRangeTest("days", in_range_days, out_of_range_days)

    def testIntervalMonthsRange(self):
        out_of_range_months = (-2147483648, +2147483648,)
        in_range_months = (-2147483647, +2147483647,)
        self.intervalRangeTest("months", in_range_months, out_of_range_months)

    def testIntervalMicrosecondsRange(self):
        out_of_range_microseconds = (
            -9223372036854775808, +9223372036854775808,)
        in_range_microseconds = (
            -9223372036854775807, +9223372036854775807,)
        self.intervalRangeTest(
            "microseconds", in_range_microseconds, out_of_range_microseconds)


if __name__ == "__main__":
    unittest.main()
