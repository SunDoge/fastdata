import mylib
from tqdm import tqdm
import torch
from torch.utils.dlpack import from_dlpack

print(mylib.add(1, 2))

ds = mylib.one_tfrecord("/mnt/ssd/chenyf/val/*.tfrecord", 32)

for data in tqdm(ds):
    # print(from_dlpack(data['image']).shape)
    # print(data['label'])
    img = from_dlpack(data['image'])
    pass

# n = 100000
# for data in tqdm(mylib.pure_data(n), total=n):
#     from_dlpack(data['image'])
