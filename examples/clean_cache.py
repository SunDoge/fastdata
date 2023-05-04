import os
import glob


# pattern = "/mnt/cephfs/home/chenyaofo/datasets/imagenet-tfrec/val/*.tfrecord"
pattern = "/mnt/ssd/chenyf/val/*.tfrecord"

for filename in glob.glob(pattern):
    print(filename)
    fd = os.open(filename, os.O_RDONLY)
    os.posix_fadvise(fd, 0, os.fstat(fd).st_size, os.POSIX_FADV_DONTNEED)
