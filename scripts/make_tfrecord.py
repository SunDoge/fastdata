import tfrecord
import glob
from torchvision.datasets import ImageFolder
from pathlib import Path
import os


def make_records(
    root_dir: Path,
    record_dir: Path,
):
    pass


def main():
    root = 'imagenette2-160/val'
    ds = ImageFolder(
        root,
        loader=lambda x: x
    )

    writer = tfrecord.TFRecordWriter('imagenette2-160-val.tfrecord')
    for i in range(len(ds)):
        path, class_idx = ds[i]
        relpath = os.path.relpath(path, root)
        with open(path, 'rb') as f:
            image_bytes = f.read()

        writer.write(dict(
            fname=(relpath.encode(), "byte"),
            image=(image_bytes, "byte"),
            label=(class_idx, "int")
        ))

    writer.close()


if __name__ == '__main__':
    main()
