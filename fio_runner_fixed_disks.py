# fio tests for the gotatlin node.

from collections import OrderedDict
from fio_run_utils import run_test, save_json

N_DISK_SAMPLE = 1
RUNTIME = 10
BLOCK_SIZES = [4]  # kB ~ 1000


RW_LIST = ['rw']

DISKS = ["sdd"]


def print_start(n_cum, n_times):
    print("#start")
    test_time = N_DISK_SAMPLE * \
        len(RW_LIST) * RUNTIME * len(BLOCK_SIZES) * n_cum * n_times
    print("#test time: {} min".format(test_time / 60))


def print_end():
    print("\n#done")


def main():
    n_cum = 10
    n_times = 10
    print_start(n_cum, n_times)
    for i in range(n_cum):
        cum_result = OrderedDict()
        print("----i:", i)
        for j in range(n_times):
            print("----j:", i)
            result = run_test(block_sizes=BLOCK_SIZES, disks=DISKS, n_disks_sample=None,
                              runtime=RUNTIME, timeout=RUNTIME * 3, iodepth=1,
                              config_path='test.ini', rw_list=RW_LIST, output_format='normal', random_offset=True)
            cum_result[i] = result
            # save_json(result, "fiotests/fio_tests_{}.json".format(i))
        save_json(cum_result, "fiotests/cum_result_{}.json".format(i))
    print_end()

if __name__ == '__main__':
    main()
