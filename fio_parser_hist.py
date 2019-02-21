import json
import numpy as np
from collections import OrderedDict
from pprint import pprint

from fio_parser_utils import (
    parse_latency_time, parse_processing_time, parse_transmission_time,
    save_json, read_json, separate_rw_result
)


def parse_hist(json_path="fio_tests/fio_tests_node_hist.json"):
    test_result = read_json(json_path)
    temp = OrderedDict([('mean', []), ('std', [])])

    result = OrderedDict([
        ('read', OrderedDict([
            ('latency_time', OrderedDict([('mean', []), ('std', [])])),
            ('processing_time', OrderedDict([('mean', []), ('std', [])])),
            ('transmission_time', OrderedDict([('mean', []), ('std', [])])),
        ])),
        ('write', OrderedDict([
            ('latency_time', OrderedDict([('mean', []), ('std', [])])),
            ('processing_time', OrderedDict([('mean', []), ('std', [])])),
            ('transmission_time', OrderedDict([('mean', []), ('std', [])])),
        ])),
    ])

    param2parse = {
        'latency_time': parse_latency_time,
        'processing_time': parse_processing_time,
        'transmission_time': parse_transmission_time,
    }

    rw_modes = ['read', 'write']
    for test in test_result.values():
        rw_results = separate_rw_result(test['4K']['sdd']['rw']['result'])
        for rw_mode, rw_result in zip(rw_modes, rw_results):
            for param, parse_func in param2parse.items():
                mean, std = parse_func(rw_result)
                result[rw_mode][param]['mean'].append(mean)
                result[rw_mode][param]['std'].append(std)
    return result


def main():
    result = parse_hist("fio_tests/hist/fio_tests_node_hist_1000.json")
    save_json(result, "hists/hist_1000.json")

if __name__ == '__main__':
    main()
