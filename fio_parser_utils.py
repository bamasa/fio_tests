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


def separate_rw_result(result):
    sep = result.find("write")
    read_result = result[:sep]
    write_result = result[sep:]
    return read_result, write_result


def save_json(results, save_path):
    with open(save_path, 'w+') as fp:
        json.dump(results, fp, indent=2)


def read_json(json_path):
    with open(json_path, 'r') as fp:
        result = json.load(fp)
    return result
