use std::{io::Cursor, time::Instant};

use fastdata::{error::Result, readers::tfrecord::TfRecordReader};
use libvips::VipsImage;
use opencv::prelude::*;
use prost::Message;
use rayon::prelude::{ParallelBridge, ParallelIterator};

fn main() {
    opencv::core::set_num_threads(0).unwrap();

    rayon::ThreadPoolBuilder::new()
        .num_threads(32)
        .build_global()
        .unwrap();

    let tfrecords =
        glob::glob("/mnt/cephfs/home/chenyaofo/datasets/imagenet-tfrec/val/*.tfrecord").unwrap();

    let start_time = Instant::now();
    let num_records = tfrecords
        .take(10)
        .flat_map(|path| {
            let path = path.unwrap();
            println!("tfrecord: {}", path.display());
            let reader = TfRecordReader::open(&path).expect("fail to open");
            reader
        })
        .par_bridge()
        .map(|buf| {
            let example =
                fastdata::tensorflow::Example::decode(&mut Cursor::new(buf.unwrap())).unwrap();
            let image_bytes = example.get_bytes_list("image")[0];
            let label = example.get_int64_list("label")[0];
            let mat_buf = Mat::from_slice(image_bytes).unwrap();
            let img =
                opencv::imgcodecs::imdecode(&mat_buf, opencv::imgcodecs::IMREAD_COLOR).unwrap();
            let mut resized = Mat::default();
            opencv::imgproc::resize(
                &img,
                &mut resized,
                (256, 256).into(),
                0.0,
                0.0,
                opencv::imgproc::INTER_LINEAR,
            )
            .unwrap();
            label
        })
        .count();

    let rate = num_records as f64 / start_time.elapsed().as_secs_f64();
    println!("rate: {rate} record/s");
}
