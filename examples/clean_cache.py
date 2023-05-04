import os
import glob
import fcntl

for filename in glob.glob("/mnt/cephfs/home/chenyaofo/datasets/imagenet-tfrec/val/*.tfrecord"):
    print(filename)
    fd = os.open(filename, os.O_RDONLY)
    os.posix_fadvise(fd, 0, os.fstat(fd).st_size, os.POSIX_FADV_DONTNEED)
