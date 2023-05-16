use std::{io::Cursor, time::Instant};

// use crossbeam_channel::bounded;
use kanal::bounded;

use fastdata::ops::image::opencv::{BgrToRgb, CenterCrop, SmallestMaxSize};
use fastdata::ops::shuffler::Shuffle;
use fastdata::{error::Result, readers::tfrecord::TfRecordReader};

use opencv::prelude::*;
use prost::Message;
use rayon::prelude::*;
use rayon::prelude::{ParallelBridge, ParallelIterator};

#[derive(Debug, Clone, Default)]
struct Aug {
    convert_color: BgrToRgb,
    resize: SmallestMaxSize,
    crop: CenterCrop,
}

impl Aug {
    pub fn apply(&mut self, img: &Mat) -> Mat {
        let img = self.convert_color.apply(img);
        let img = self.resize.apply(&img);
        let img = self.crop.apply(&img);
        img
    }
}

fn main() {
    // rayon::ThreadPoolBuilder::new()
    //     .num_threads(32)
    //     .build_global()
    //     .unwrap();

    // let (sender, receiver) = bounded(1024 * 1024 * 10);
    // let pattern = "/mnt/cephfs/home/chenyaofo/datasets/imagenet-tfrec/val/*.tfrecord";
    let pattern = "/mnt/ssd/chenyf/val/*.tfrecord";
    let tfrecords: Vec<_> = glob::glob(pattern).unwrap().collect();

    opencv::core::set_num_threads(0).unwrap();
    println!(
        "use optimization {}",
        opencv::core::use_optimized().unwrap()
    );

    let num_threads = 32;
    let capacity = 1024 * 1024;

    let read_threads = 8;

    let (sender1, receiver1) = bounded(capacity);
    let (sender2, receiver2) = bounded(capacity);
    let (sender3, receiver3) = bounded(capacity);

    rayon::spawn(move || {
        tfrecords
            .into_iter()
            .for_each(|path| sender1.send(path.unwrap()).unwrap());
    });

    for read_idx in 0..read_threads {
        let r1 = receiver1.clone();
        let s2 = sender2.clone();
        rayon::spawn(move || {
            // let mut aug = Aug::default();
            r1.for_each(|path| {
                println!("idx: {} tfrecord: {}", read_idx, path.display());
                let reader = TfRecordReader::open(path).unwrap();
                reader.for_each(|buf| {
                    // let example =
                    //     fastdata::tensorflow::Example::decode(&mut Cursor::new(buf.unwrap()))
                    //         .unwrap();
                    // let image_bytes = example.get_bytes_list("image")[0];
                    // let label = example.get_int64_list("label")[0];

                    // let img_buf = Mat::from_slice(image_bytes).unwrap();
                    // let img =
                    //     opencv::imgcodecs::imdecode(&img_buf, opencv::imgcodecs::IMREAD_COLOR)
                    //         .unwrap();

                    // let img = aug.apply(&img);

                    // s2.send(()).unwrap();
                    s2.send(buf.unwrap()).unwrap();
                });
            });
            // println!("drop idx {}", read_idx);
        });
    }

    drop(sender2);

    for thread_idx in 0..num_threads {
        let r2 = receiver2.clone();
        let s3 = sender3.clone();

        rayon::spawn(move || {
            let mut aug = Aug::default();
            r2.for_each(|buf| {
                let example = fastdata::tensorflow::Example::decode(&mut Cursor::new(buf)).unwrap();
                let image_bytes = example.get_bytes_list("image")[0];
                let label = example.get_int64_list("label")[0];

                let img_buf = Mat::from_slice(image_bytes).unwrap();
                let img =
                    opencv::imgcodecs::imdecode(&img_buf, opencv::imgcodecs::IMREAD_COLOR).unwrap();

                let img = aug.apply(&img);

                s3.send(()).unwrap();
            });
            // println!("drop thread: {}", thread_idx);
        });
    }

    drop(sender3);

    let start_time = Instant::now();
    let num_records = receiver3.count();

    let rate = num_records as f64 / start_time.elapsed().as_secs_f64();
    println!("rate: {rate} record/s records {num_records}");
}
