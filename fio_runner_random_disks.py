# fio tests for the gotatlin node.

from collections import OrderedDict
from fio_run_utils import run_test, save_json

N_DISK_SAMPLE = 5
RUNTIME = 30
BLOCK_SIZES = [2**x for x in range(2, 12)]  # kB ~ 1000


RW_LIST = [
    'read',
    'write',
    'randread',
    'randwrite',
    'rw',
    'randrw',
]


def print_start():
    print("#start")
    test_time = N_DISK_SAMPLE * len(RW_LIST) * RUNTIME * len(BLOCK_SIZES)
    print("#test time: {} min".format(test_time / 60))


def print_end():
    print("\n#done")


def main():
    print_start()
    for i in range(100):
        result = run_test(block_sizes=BLOCK_SIZES, n_disks_sample=N_DISK_SAMPLE,
                          runtime=RUNTIME, timeout=RUNTIME * 3, iodepth=1,
                          config_path='test.ini', rw_list=RW_LIST, output_format='normal', random_offset=False)
        save_json(result, "fiotests/fio_tests_{}.json".format(i))
    print_end()

if __name__ == '__main__':
    main()
