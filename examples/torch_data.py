import torch
import torchvision.transforms as transforms
from PIL import Image
from io import BytesIO
from torchdata.datapipes.iter import FileLister, FileOpener, TFRecordLoader, Mapper, Shuffler, Batcher, Collator, ShardingFilter
from torchdata.dataloader2 import adapter, DataLoader2, PrototypeMultiProcessingReadingService
# from codebase.torchutils.serialization import jsonunpack
from tqdm import tqdm

from torch.utils.data import DataLoader


def get_train_transforms():
    return transforms.Compose([
        transforms.Lambda(lambda x: Image.open(BytesIO(x)).convert("RGB")),
        # transforms.Resize((224, 224)),
        # transforms.ToTensor(),
        # transforms.Normalize(mean=[0.485, 0.456, 0.406],
        #                      std=[0.229, 0.224, 0.225]),
    ])


def mapping_fn(content):
    return (content['label'],)


batch_size = 128


def get_data_loader():
    image_transforms = get_train_transforms()

    # dp = FileLister("/mnt/cephfs/home/chenyaofo/datasets/imagenet-tfrec/val", masks="*.tfrecord", non_deterministic=False)
    dp = FileLister("/mnt/ssd/chenyf/val", masks="*.tfrecord", non_deterministic=False)
    dp = ShardingFilter(dp)
    dp = FileOpener(dp, mode="rb")
    dp = TFRecordLoader(dp, spec={
        # "metadata": (tuple(), None),
        "image": (tuple(), None),
        "label": (tuple(), torch.int32),
    })
    # dp = Shuffler(dp, buffer_size=100)
    dp = Mapper(dp, fn=lambda content: (image_transforms(content["image"]), content["label"]))
    # dp = Mapper(dp, fn=mapping_fn)
    dp = Batcher(dp, batch_size=batch_size)
    dp = Collator(dp, collate_fn=lambda x: x)

    loader = DataLoader(dp, num_workers=8, collate_fn=lambda x: x)

    return loader


loader = get_data_loader()

print(loader)
with tqdm() as pbar:
    for item in loader:
        # import ipdb; ipdb.set_trace()
        # print(torch.var_mean(item[0]))
        # break
        pbar.update(batch_size)
