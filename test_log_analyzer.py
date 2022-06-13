import collections
import datetime
import unittest

import log_analyzer
from log_analyzer import LOG_RECORD_RE


class TestSuite(unittest.TestCase):
    def test_incorrect_gzip(self):
        incorrect_gzip = 'temp.txt'
        self.assertEqual(log_analyzer.is_gzip_file(incorrect_gzip), False)

    def test_incorrect_config(self):
        incorrect_path = '/var/log'
        with self.assertRaises(Exception):
            self.assertEqual(log_analyzer.load_config(incorrect_path, None), True)

    def test_get_report_path(self):
        DateNamedFileInfo = collections.namedtuple('DateNamedFileInfo', ['file_path', 'file_date'])
        DateNamedFileInfo.file_path, DateNamedFileInfo.file_date = '/var/log', datetime.datetime.strptime('2022024', "%Y%m%d").date()
        expected = './reports/report-2022.02.04.html'
        report_path = log_analyzer.get_report_path('./reports', DateNamedFileInfo)
        self.assertEqual(report_path, expected)

    def test_process_line(self):
        line = """1.199.4.96 -  - [29/Jun/2017:04:09:45 +0300] "GET /api/v2/banner/20770003/statistic/?date_from=2017-06-29&date_to=2017-06-29 HTTP/1.1" 200 109 "-" "Lynx/2.8.8dev.9 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/2.10.5" "-" "1498698585-3800516057-4708-9762513" "2ae7917fdbbe67b1c8" 0.060"""
        self.assertEqual(log_analyzer.process_line(line, LOG_RECORD_RE), ('/api/v2/banner/20770003/statistic/?date_from=2017-06-29&date_to=2017-06-29', 0.06))

    def test_get_statistics(self):
        line = {'/api/v2/banner/20770003': [0.1, 0.01, 0.03, 0.5]}
        expected = [{'url': '/api/v2/banner/20770003', 'count': 4, 'count_perc': 100.0, 'time_avg': 0.16,
                     'time_max': 0.5, 'time_med': 0.065, 'time_perc': 100.0, 'time_sum': 0.64}]
        self.assertEqual(log_analyzer.get_statistics(line, 1), expected)

    def test_process_incorrect_line(self):
        line = 'abcde'
        self.assertEqual(log_analyzer.process_line(line, LOG_RECORD_RE), None)


if __name__ == '__main__':
    unittest.main()
