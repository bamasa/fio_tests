import json
import numpy as np
from collections import OrderedDict

TIME_MULTS = {
    "sec": 1,
    "msec": 1e-3,
    "usec": 1e-6,
    "nsec": 1e-9,
    "KB/s": 1000,
    "KiB/s": 1000,
}

READ_TEST_NAMES = ["read", "randread"]
WRITE_TEST_NAMES = ["write", "randwrite"]
READ_WRITE_TEST_NAMES = ["rw", "randrw"]


def parse_avg_std(result, parameter, without=[]):
    for line in result.splitlines():
        line = line.replace(" ", "")
        if parameter in line and "avg" in line:
            right_line = True
            for w_parameter in without:
                if w_parameter in line:
                    right_line = False
            if right_line:
                time_format = line[line.find("(") + 1:line.rfind(")")]
                time_mult = TIME_MULTS[
                    time_format] if time_format in TIME_MULTS else 1
                mean = line[line.find("avg=") + 4:]
                mean = float(mean[:mean.find(",")])
                std_dev = line[line.find("stdev=") + 6:]
                std_dev = float(std_dev[:std_dev.find(",")])
                mean = mean * time_mult
                std_dev = std_dev * time_mult
                return mean, std_dev
    raise Exception("'{}' not found".format(parameter))


def parse_transmission_time(result):
    """Parse transmission time using fio submission latency."""
    trans_mean, trans_std_dev = parse_avg_std(result, "slat(")
    return trans_mean, trans_std_dev


def parse_latency_time(result):
    """Parse latency time using fio latency."""
    lat_mean, lat_std_dev = parse_avg_std(result, "lat(", ["slat", "clat"])
    return lat_mean, lat_std_dev


def parse_processing_time(result):
    """Parse latency time using fio completion latency."""

    # completion latency
    proc_mean, proc_std_dev = parse_avg_std(result, "clat(")
    return proc_mean, proc_std_dev


def parse_rate_time(result, block_size):
    """Parse rate time using fio bandwidth."""
    bw_mean, bw_std_dev = parse_avg_std(result, "bw(")
    rate_mean = block_size / bw_mean
    # TODO: math approach for std
    # rate_std_dev = np.std(
    #     block_size / np.random.normal(bw_mean, bw_std_dev, 10 ** 5))
    rate_std_dev = 0
    return rate_mean, rate_std_dev


def parse_seek_time(result):
    """Parse seek time using fio IOPS."""
    # seek time
    # = average rotational delay + avg seek time (here)
    # = 1 / IOPS
    # (https://serverfault.com/questions/920433/what-is-the-relation-between-block-size-and-io)
    # TODO: math approach for std
    iops_mean, iops_std_dev = parse_avg_std(result, "iops:")
    seek_mean = 1 / iops_mean
    return seek_mean, 0


def parse_overheads_time(result):
    """Overheads time.

    Note:
        According to wiki (https://en.wikipedia.org/wiki/Hard_disk_drive_performance_characteristics):
            The command processing time or command overhead is the time it takes for the drive electronics
            to set up the necessary communication between the various components in the device so it
            can read or write the data. This is of the order of 3 μs, very much less than other overhead times,
            so it is usually ignored when benchmarking hardware.
    """
    return 3e-6, 0


def aggregate_rw(tuple_1, tuple_2):
    """Aggregate results obtained by readwrite tests."""
    return np.mean([tuple_1[0], tuple_2[0]]), np.linalg.norm([tuple_1[1], tuple_2[1]])


def mean_std_lists_to_Ordered_dict(mean_list, std_list):
    return OrderedDict([
        ("mean", np.mean(mean_list)),
        ("std_dev", np.linalg.norm(std_list)),
    ])


def parse_fio_tests(json_path="new.json", output_type="normal"):
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


def save_json(results, save_path):
    with open(save_path, 'w+') as fp:
        json.dump(results, fp, indent=2)


def read_json(json_path):
    with open(json_path, 'r') as fp:
        result = json.load(fp)
    return result


def main():
    result = parse_fio_tests()
    save_json(result, "packet_config_v1.json")

if __name__ == "__main__":
    main()
