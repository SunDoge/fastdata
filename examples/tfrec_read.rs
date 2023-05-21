use std::{
    io::{BufReader, Cursor},
    time::Instant,
};

use bytes::Buf;
use fastdata::{error::Result, readers::tfrecord::TfrecordReader};
use prost::Message;
use rayon::prelude::{ParallelBridge, ParallelIterator};

fn main() {
    let file = std::fs::File::open(
        // "/mnt/cephfs/home/chenyaofo/datasets/imagenet-tfrec/val/imagenet-1k-val-000100.tfrecord",
        "ints.tfrecord",
    )
    .unwrap();
    let buf_reader = BufReader::new(file);
    let mut reader = TfrecordReader::from(buf_reader);
    reader.set_check_integrity(true);

    let start_time = Instant::now();
    let num_records = reader
        .par_bridge()
        .map(|buf| {
            let example = fastdata::tensorflow::Example::decode(&*buf.unwrap()).unwrap();
            let data = example.get_bytes_list("data").unwrap()[0];
            // dbg!(data.len())
            // let image_bytes = example.get_bytes_list("image")[0];
            // let label = example.get_int64_list("label")[0];
            // let mat_buf = Mat::from_slice(image_bytes).unwrap();
            // let img =
            //     opencv::imgcodecs::imdecode(&mat_buf, opencv::imgcodecs::IMREAD_COLOR).unwrap();

            // assert_eq!(image_buffer.len(), 224 * 224 * 3);
            // (image_buffer, label)
        })
        .count();

    let rate = num_records as f64 / start_time.elapsed().as_secs_f64();
    println!("rate: {rate} record/s");
}
