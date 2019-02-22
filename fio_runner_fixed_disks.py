# fio tests for the gotatlin node.

from collections import OrderedDict
from fio_run_utils import run_test, save_json

N_DISK_SAMPLE = None
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

DISKS = ["sdf", "sdd", "sdp", "sdw", "sdah"]


def print_start(n_tests):
    print("#start")
    test_time = len(DISKS) * len(RW_LIST) * RUNTIME * len(BLOCK_SIZES)
    print("#time of one test: {} min".format(test_time / 60))
    print("#test time: {} h".format(test_time * n_tests / 3600))


def print_end():
    print("\n#done")


def main():
    n_tests = 100
    print_start(n_tests)
    for i in range(n_tests):
        print("###", i)
        result = run_test(block_sizes=BLOCK_SIZES, disks=DISKS, n_disks_sample=None,
                          runtime=RUNTIME, timeout=RUNTIME * 3, iodepth=1,
                          config_path='test.ini', rw_list=RW_LIST, output_format='normal', random_offset=True)
        save_json(result, "fiotests/fio_tests_{}.json".format(i))
    print_end()

if __name__ == '__main__':
    main()
