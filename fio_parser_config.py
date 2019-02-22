import argparse
import sys
from collections import OrderedDict

from fio_parser_utils import (
    TIME_MULTS, READ_TEST_NAMES, WRITE_TEST_NAMES, READ_WRITE_TEST_NAMES,
    parse_transmission_time, parse_latency_time, parse_processing_time,
    parse_rate_time, parse_seek_time, parse_overheads_time, aggregate_rw,
    mean_std_lists_to_Ordered_dict, separate_rw_result, save_json, read_json
)


def parse_fio_tests(json_path="fio_tests_node_1.json", output_type="normal"):
    """Parse fio tests.

    TODO:
        add "json" to output_type.
    """
    test_result = read_json(json_path)
    packet_config = OrderedDict()

    for size_str, disks in test_result.items():
        block_size = int(size_str[:-1]) * 1000

        # common parameters
        trans_mean = []
        trans_std_dev = []
        latency_mean = []
        latency_std_dev = []
        read_proc_mean = []
        read_proc_std_dev = []
        write_proc_mean = []
        write_proc_std_dev = []

        for disk, tests in disks.items():
            for test_name, data in tests.items():
                result = data["result"]
                if test_name in READ_TEST_NAMES + READ_WRITE_TEST_NAMES:
                    if test_name in READ_WRITE_TEST_NAMES:
                        read_result = result[:result.find("write")]
                    else:
                        read_result = result
                    trans_mean_, trans_std_dev_ = parse_transmission_time(
                        read_result)
                    latency_mean_, latency_std_dev_ = parse_latency_time(
                        read_result)
                    read_proc_mean_, read_proc_std_dev_ = parse_processing_time(
                        read_result)
                    trans_mean.append(trans_mean_)
                    trans_std_dev.append(trans_std_dev_)
                    latency_mean.append(latency_mean_)
                    latency_std_dev.append(latency_std_dev_)
                    read_proc_mean.append(read_proc_mean_)
                    read_proc_std_dev.append(read_proc_std_dev_)
                if test_name in WRITE_TEST_NAMES + READ_WRITE_TEST_NAMES:
                    if test_name in READ_WRITE_TEST_NAMES:
                        write_result = result[result.find("write"):]
                    else:
                        write_result = result
                    trans_mean_, trans_std_dev_ = parse_transmission_time(
                        write_result)
                    latency_mean_, latency_std_dev_ = parse_latency_time(
                        write_result)
                    write_proc_mean_, write_proc_std_dev_ = parse_processing_time(
                        write_result)
                    trans_mean.append(trans_mean_)
                    trans_std_dev.append(trans_std_dev_)
                    latency_mean.append(latency_mean_)
                    latency_std_dev.append(latency_std_dev_)
                    write_proc_mean.append(write_proc_mean_)
                    write_proc_std_dev.append(write_proc_std_dev_)

        n_disks = len(disks)
        n_read = len(READ_TEST_NAMES + READ_WRITE_TEST_NAMES)
        n_write = len(WRITE_TEST_NAMES + READ_WRITE_TEST_NAMES)
        assert len(trans_mean) == n_disks * (n_read + n_write)
        assert len(trans_std_dev) == n_disks * (n_read + n_write)
        assert len(latency_mean) == n_disks * (n_read + n_write)
        assert len(latency_std_dev) == n_disks * (n_read + n_write)
        assert len(read_proc_mean) == n_disks * n_read
        assert len(read_proc_std_dev) == n_disks * n_read
        assert len(write_proc_mean) == n_disks * n_write
        assert len(write_proc_std_dev) == n_disks * n_write

        transmission_time = mean_std_lists_to_Ordered_dict(
            trans_mean, trans_std_dev)
        latency_time = mean_std_lists_to_Ordered_dict(
            latency_mean, latency_std_dev)
        read_processing_time = mean_std_lists_to_Ordered_dict(
            read_proc_mean, read_proc_std_dev)
        write_processing_time = mean_std_lists_to_Ordered_dict(
            write_proc_mean, write_proc_std_dev)

        # special parameters
        spec_params = {}
        for test_name in READ_TEST_NAMES + WRITE_TEST_NAMES:
            rate_mean = []
            rate_std_dev = []
            seek_mean = []
            seek_std_dev = []
            overhead_mean = []
            overhead_std_dev = []
            for disk, tests in disks.items():
                result = tests[test_name]["result"]
                rate_mean_, rate_std_dev_ = parse_rate_time(result, block_size)
                seek_mean_, seek_std_dev_ = parse_seek_time(result)
                overhead_mean_, overhead_std_dev_ = parse_overheads_time(
                    result)
                rate_mean.append(rate_mean_)
                rate_std_dev.append(rate_std_dev_)
                seek_mean.append(seek_mean_)
                seek_std_dev.append(seek_std_dev_)
                overhead_mean.append(overhead_mean_)
                overhead_std_dev.append(overhead_std_dev_)
            spec_params[test_name] = OrderedDict([
                ("rate_time", mean_std_lists_to_Ordered_dict(
                    rate_mean, rate_std_dev)),
                ("seek_time", mean_std_lists_to_Ordered_dict(
                    seek_mean, seek_std_dev)),
                ("overheads_time", mean_std_lists_to_Ordered_dict(
                    overhead_mean, overhead_std_dev)),
            ])

        for test_name in READ_WRITE_TEST_NAMES:
            rate_mean = []
            rate_std_dev = []
            seek_mean = []
            seek_std_dev = []
            overhead_mean = []
            overhead_std_dev = []
            for disk, tests in disks.items():
                result = tests[test_name]["result"]
                read_result = result[:result.find("write")]
                write_result = result[result.find("write"):]
                rate_mean_, rate_std_dev_ = aggregate_rw(
                    parse_rate_time(read_result, block_size),
                    parse_rate_time(write_result, block_size),
                )
                seek_mean_, seek_std_dev_ = aggregate_rw(
                    parse_seek_time(read_result),
                    parse_seek_time(write_result),
                )
                overhead_mean_, overhead_std_dev_ = aggregate_rw(
                    parse_overheads_time(read_result),
                    parse_overheads_time(write_result),
                )
                rate_mean.append(rate_mean_)
                rate_std_dev.append(rate_std_dev_)
                seek_mean.append(seek_mean_)
                seek_std_dev.append(seek_std_dev_)
                overhead_mean.append(overhead_mean_)
                overhead_std_dev.append(overhead_std_dev_)
            spec_params[test_name] = OrderedDict([
                ("rate_time", mean_std_lists_to_Ordered_dict(
                    rate_mean, rate_std_dev)),
                ("seek_time", mean_std_lists_to_Ordered_dict(
                    seek_mean, seek_std_dev)),
                ("overheads_time", mean_std_lists_to_Ordered_dict(
                    overhead_mean, overhead_std_dev)),
            ])

        packet_config[size_str] = OrderedDict()
        packet_config[size_str]["size"] = block_size
        packet_config[size_str]["transmission_time"] = transmission_time
        packet_config[size_str]["latency_time"] = latency_time
        packet_config[size_str][
            "read_processing_time"] = read_processing_time
        packet_config[size_str][
            "write_processing_time"] = write_processing_time
        packet_config[size_str]["seq_read"] = spec_params["read"]
        packet_config[size_str]["seq_write"] = spec_params["write"]
        packet_config[size_str]["rand_read"] = spec_params["randread"]
        packet_config[size_str]["rand_write"] = spec_params["randwrite"]
        packet_config[size_str]["seq_read_write"] = spec_params["rw"]
        packet_config[size_str]["rand_read_write"] = spec_params["randrw"]

    assert len(packet_config) == len(test_result)

    return packet_config


def main(args):
    parser = argparse.ArgumentParser()

    parser.add_argument("-test", "--test_path", type=str,
                        default="fio_tests/fio_tests_0.json", required=False)
    parser.add_argument("-config", "--save_config_path", type=str,
                        default="packet_configs/packet_config_0.json", required=False)

    args = parser.parse_args(args)

    test_path = args.test_path
    save_config_path = args.save_config_path

    result = parse_fio_tests(test_path)
    save_json(result, save_config_path)

if __name__ == "__main__":
    main(sys.argv[1:])
