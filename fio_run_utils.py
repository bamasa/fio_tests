import os
import subprocess
import time
import signal
import json
import random
from collections import OrderedDict
from pprint import pprint

N_DISK_SAMPLE = 5
RUNTIME = 30
BLOCK_SIZES = [2**x for x in range(2, 12)]  # kB ~ 1000


RW_LIST = [
    'read',
    'write',
    # 'trim',
    'randread',
    'randwrite',
    # 'randtrim',
    'rw',
    'randrw',
    # 'trimwrite',
]

# iostat
DISKS = [
    # Device:   tps    kB_read/s   kB_wrtn/s  kB_read   kB_wrtn
    "sdf",  # 12.42    297.69       218.40  781783684  573541336
    "sde",  # 15.01    569.32       206.90 1495117436  543339972
    "sdc",  # 13.99    458.75       190.11 1204736820  499265048
    "sdb",  # 14.64    512.87       198.81 1346866720  522117272
    # "sda",  # 14.46    451.30       195.86 1185190556  514364860
    "sdd",  # 12.46    566.33       199.57 1487273112  524092256
    "sdg",  # 17.66    484.14       197.64 1271415368  519037272
    "sdh",  # 16.19    484.61       211.38 1272663828  555112732
    "sdi",  # 16.67    449.89       200.90 1181469188  527604220
    # "sdj",  # 17.78    655.75       218.54 1722090164  573905892
    "sdk",  # 15.72    383.27       206.38 1006519872  541978888
    "sdl",  # 18.10    648.21       222.26 1702299192  583682880
    "sdm",  # 17.98    660.55       212.29 1734709064  557502412
    "sdn",  # 15.80    340.05       197.75  893030108  519320784
    "sdp",  # 20.65    712.20       244.07 1870354736  640963648
    "sdq",  # 15.96    372.70       190.39  978760052  499992156
    "sdr",  # 12.02    300.43       212.32  788982912  557590064
    "sds",  # 14.89    474.10       236.51 1245045772  621111148
    "sdt",  # 16.03    408.40       190.88 1072520544  501280056
    "sdu",  # 19.57    659.00       235.87 1730625180  619423184
    "sdv",  # 14.41    456.47       219.62 1198750552  576754636
    "sdw",  # 18.90    607.26       245.82 1594750044  645552644
    "sdx",  # 14.50    302.97       190.37  795653792  499946260
    "sdy",  # 16.01    413.48       216.13 1085859396  567593784
    # "sdz",  # 38.65  9.31      2132.28   24461909 5599693020
    "sdaa",  # 18.74       550.61      250.34 1445976432  657423160
    "sdab",  # 15.61       351.95      205.22  924285904  538939484
    "sdac",  # 17.72       525.83      230.12 1380912544  604320112
    "sdad",  # 17.39       544.60      410.12 1430188028 1077033356
    "sdae",  # 18.92       570.35      225.59 1497828424  592424812
    "sdaf",  # 14.47       522.64      218.39 1372541572  573529060
    "sdag",  # 14.86       395.94      224.76 1039785848  590254544
    "sdah",  # 15.54       335.04      193.92  879854184  509259616
    "sdai",  # 15.56       361.28      204.98  948774236  538318436
    # "sdaj",  # 18.15       440.18      227.48 1155982788  597405988
    # "sdak", #   0.00    0.00   0.00    2880    0
    # "sdal", #   0.00    0.00   0.00    2880    0
]


def run_cmd(*popenargs, input=None, check=False, **kwargs):
    if input is not None:
        if 'stdin' in kwargs:
            raise ValueError('stdin and input arguments may not both be used.')
        kwargs['stdin'] = subprocess.PIPE
    kwargs['stdout'] = subprocess.PIPE
    process = subprocess.Popen(*popenargs, **kwargs)
    try:
        stdout, stderr = process.communicate(input)
    except:
        process.kill()
        process.wait()
        raise
    retcode = process.poll()
    if check and retcode:
        raise subprocess.CalledProcessError(
            retcode, process.args, output=stdout, stderr=stderr)
    return retcode, stdout, stderr


def fio_config(rw, block_size=4, disk_name='sda', iodepth=1, random_offset=False):
    """Create fio config.

    Args:
        rw (str): name of read/write test.
        blocksize (int): size (kB).
        disk_name (str): disk name.
        iodepth (int): queue depth.
        random_offset (bool): random fio offset (avoid caching).

    Returns:
        config (str): fio config.

    """
    test_name = rw + "_test"
    config = """[{}]
blocksize={}k
filename=/dev/{}
rw={}
direct=1
buffered=0
ioengine=libaio
iodepth={}""".format(test_name, block_size, disk_name, rw, iodepth)
    if random_offset:
        config += '\noffset={}%'.format(random.randint(1, 99))
    return config

# test = """[readtest]
# blocksize=4k
# filename=~/dev/sda
# rw=randread
# direct=1
# buffered=0
# ioengine=libaio
# iodepth=16"""

# assert test == create_fio_config('read', 4, 'sda', 16)


def save_fio_config(config, filepath='test.ini'):
    with open(filepath, "w+") as f:
        f.write(config)


def run_test(block_sizes=BLOCK_SIZES, disks=DISKS, n_disks_sample=N_DISK_SAMPLE,
             runtime=RUNTIME, timeout=RUNTIME * 3, iodepth=1, config_path='test.ini',
             rw_list=RW_LIST, output_format='normal', random_offset=False):
    """Run fio tests.

    Args:
        block_size (int): size (kB).
        n_disks_sample (int or None): test all disks without sampling if None,
            otherwise number sampling of tested disks.
        disks (list of str): list of available disks.
        runtime(int): test runtime (sec).
        timeout (int): timeout (sec).
        iodepth (int): queue depth.
        config_path (str): temp path to config.
        rw_list (list of str): list of read/write tests.
        output_format ('normal' or 'json'): fio output format.
        random_offset (bool): random fio offset (avoid caching).

    Returns:
        result (OrderedDict): results of fio tests.

    Note:
        It is a question how to define iodepth,
        I read these links:
            https://wiki.mikejung.biz/Benchmarking#iodepth
            https://unix.stackexchange.com/questions/459045/what-exactly-is-iodepth-in-fio
            https://tobert.github.io/post/2014-04-17-fio-output-explained.html
        So I compared iops with iodepth=1 and iodepth=16 -- there was no difference,
        So we took 1.
    """
    result = OrderedDict()

    try:
        for block_size in block_sizes:
            print("\nsize: {}K".format(block_size))
            size = str(block_size) + "K"
            result[size] = OrderedDict()
            if n_disks_sample is not None:
                disks_sample = random.choice(disks, n_disks_sample)
            else:
                disks_sample = disks
            for disk in disks:
                print("\n\tdisk: " + disk)
                result[size][disk] = OrderedDict()
                for rw in rw_list:
                    print("\t\ttest_name:", rw)
                    config = fio_config(
                        rw, block_size, disk, iodepth, random_offset)
                    save_fio_config(config, config_path)
                    cmd = "sudo fio {} --runtime={} --output-format={}".format(
                        config_path, runtime, output_format)
                    output = run_cmd(cmd.split())[1].decode('utf-8')
                    result[size][disk][rw] = OrderedDict()
                    result[size][disk][rw]["config"] = config
                    result[size][disk][rw]["result"] = output
    except:
        print("\t\terror")

    return result


def save_json(dict, save_path):
    with open(save_path, 'w') as fp:
        json.dump(dict, fp, indent=2)
