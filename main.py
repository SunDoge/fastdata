import mylib
from tqdm import tqdm
from torch.utils.dlpack import from_dlpack
import torch


print(mylib.add(1,1))





class ToPytorchTensor:

    def __init__(
        self, it,
        to_pytorch: list # (key, convert_fn)
    ) -> None:
        self.it = it
        self.to_pytorch = to_pytorch

    def __iter__(self):
        for example in self.it:
            x = {}
            for key, convert_fn in self.to_pytorch:
                x[key] = convert_fn(example[key])
            yield x
    
        


it = mylib.make_record_dataset('/mnt/cephfs/home/chenyaofo/datasets/imagenet-tfrec/train/imagenet-1k-train-000003.tfrecord', 8)
it = ToPytorchTensor(it, to_pytorch=[
    ('image', from_dlpack),
    ('label', torch.tensor)
])
with tqdm() as pbar:
    for data in it:
        pbar.write(str(data['label']))
        pbar.update(1)


