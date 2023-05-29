import mylib
from tqdm import tqdm
import torch
from torch.utils.dlpack import from_dlpack
import torchdata.datapipes.iter as dpiter
from torch.utils.data import DataLoader

print(mylib.add(1, 2))

# ds = mylib.one_tfrecord("/mnt/ssd/chenyf/val/*.tfrecord", 8)
# dp = dpiter.FileLister('/mnt/ssd/chenyf/val/', masks='*.tfrecord')
dp = dpiter.FileLister('imagenet-tfrec/train', masks='*.tfrecord')
paths = list(dp)
ds = mylib.async_tfrecord(paths, 32, 32, 1024 * 1024)


def to_tensor(dic):
    res = {}
    for key, value in dic.items():
        if type(value).__name__ == 'PyCapsule':
            res[key] = from_dlpack(value)
        else:
            res[key] = value
    return res


dp = dpiter.Mapper(ds, fn=to_tensor)

loader = DataLoader(dp, batch_size=128)


with tqdm() as pbar:
    for batch in loader:
        pbar.update(batch['label'].size(0))

# n = 100000
# for data in tqdm(mylib.pure_data(n), total=n):
#     from_dlpack(data['image'])
