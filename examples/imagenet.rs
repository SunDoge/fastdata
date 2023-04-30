use std::{io::Cursor, time::Instant};

use fastdata::ops::shuffler::Shuffle;
use fastdata::{error::Result, readers::tfrecord::TfRecordReader};
use opencv::prelude::*;
use prost::Message;
use rayon::prelude::{ParallelBridge, ParallelIterator};

fn main() {
    // let vips_app = libvips::VipsApp::new("aug", false).unwrap();
    // dbg!(
    //     vips_app.concurency_get(),
    //     vips_app.cache_get_max(),
    //     vips_app.cache_get_max_files(),
    //     vips_app.cache_get_max_mem(),
    // );

    // vips_app.concurrency_set(2);
    // vips_app.cache_set_max(10000000);
    // vips_app.cache_set_max_files(0);
    // vips_app.cache_set_max_mem(1024 * 1024 * 1024 * 1024);
    // vips_app.cache_set_max_mem(0);
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
            

            // let img = VipsImage::new_from_buffer(image_bytes, "").unwrap();
            // let img = aug.apply(&img).unwrap();
            // let image_buffer = img.image_write_to_memory();
            label
        })
        .count();

    let rate = num_records as f64 / start_time.elapsed().as_secs_f64();
    println!("rate: {rate} record/s");
}
