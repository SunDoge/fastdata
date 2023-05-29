import os

import typed_args as ta
from pathlib import Path


@ta.argument_parser()
class Args(ta.TypedArgs):
    root: Path = ta.add_argument(type=Path)
    """
    tfrecord folder
    """

    masks: str = ta.add_argument('-m', '--masks', default='*.tfrecord')
    """
    """


def main():
    # pattern = "/mnt/cephfs/home/chenyaofo/datasets/imagenet-tfrec/train/*.tfrecord"
    # pattern = "/mnt/ssd/chenyf/val/*.tfrecord"
    # pattern = "target/imagenet-tfrec/train/*.tfrecord"
    args = Args.parse_args()

    for filename in args.root.glob(args.masks):
        print(filename)
        fd = os.open(filename, os.O_RDONLY)
        os.posix_fadvise(fd, 0, os.fstat(fd).st_size, os.POSIX_FADV_DONTNEED)


if __name__ == '__main__':
    main()
