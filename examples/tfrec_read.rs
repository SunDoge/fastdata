use std::{io::Cursor, time::Instant};

use fastdata::{
    error::Result,
    ops::image::vips::{convert_to_rgb, CenterCrop, SmallestMaxSize},
    readers::tfrecord::TfRecordReader,
};
use libvips::VipsImage;
use prost::Message;
use rayon::prelude::{ParallelBridge, ParallelIterator};

#[derive(Debug, Clone, Copy)]
struct Aug {
    pub resize: SmallestMaxSize,
    pub crop: CenterCrop,
}

impl Aug {
    pub fn apply(&self, img: &VipsImage) -> Result<VipsImage> {
        let img = convert_to_rgb(img)?;
        // dbg!(img.get_bands());
        let img = self.resize.apply(&img)?;
        let img = self.crop.apply(&img)?;
        Ok(img)
    }
}

fn main() {
    let mut reader = TfRecordReader::open(
        "/mnt/cephfs/home/chenyaofo/datasets/imagenet-tfrec/val/imagenet-1k-val-000100.tfrecord",
    )
    .expect("fail to open");
    // reader.set_check_integrity(true);

    let vips_app = libvips::VipsApp::new("aug", false).unwrap();
    vips_app.concurrency_set(1);

    let aug = Aug {
        resize: SmallestMaxSize {
            max_size: 256,
            kernel: libvips::ops::Kernel::Lanczos2,
        },
        crop: CenterCrop {
            width: 224,
            height: 224,
        },
    };

    let start_time = Instant::now();
    let num_records = reader
        .iter()
        .unwrap()
        .par_bridge()
        .map(|buf| {
            let example =
                fastdata::tensorflow::Example::decode(&mut Cursor::new(buf.unwrap())).unwrap();
            let image_bytes = example.get_bytes_list("image")[0];
            let label = example.get_int64_list("label")[0];

            let img = VipsImage::new_from_buffer(image_bytes, "").unwrap();
            let img = aug.apply(&img).unwrap();
            let image_buffer = img.image_write_to_memory();
            (image_buffer, label)
        })
        .count();

    let rate = num_records as f64 / start_time.elapsed().as_secs_f64();
    println!("rate: {rate} record/s");
}
