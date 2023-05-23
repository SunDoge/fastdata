use std::{
    collections::VecDeque, fs::File, io::Read, os::fd::AsRawFd, path::PathBuf, time::Instant,
};

use fastdata::readers::io_uring_tfrecord::{io_uring_loop, IoUringTfrecordReader};
use io_uring::{opcode, types, IoUring};
use kanal::bounded;
use rayon::prelude::*;
use slab::Slab;

const QUEUE_DEPTH: usize = 32;
const BUFFER_SIZE: usize = 16 * 1024;

fn main() {
    // let tfrecords = glob::glob("/home/denghuang/datasets/imagenet-tfrec/val/*.tfrecord").unwrap();
    let tfrecords =
        glob::glob("/mnt/cephfs/home/chenyaofo/datasets/imagenet-tfrec/val/*.tfrecord").unwrap();
    let filenames: Vec<_> = tfrecords.map(|p| p.unwrap()).collect();

    let start_time = Instant::now();

    let source = filenames
        .iter()
        .map(|filename| File::open(filename).unwrap());
    let reader = IoUringTfrecordReader::new(source, 64, true).unwrap();
    let num_blocks = reader.count();

    // let (sender, receiver) = bounded(1024 * 1024);
    // std::thread::spawn(move || {
    //     io_uring_loop(
    //         &mut filenames
    //             .iter()
    //             .map(|filename| File::open(filename).unwrap()),
    //         64,
    //         sender,
    //         false,
    //     );
    // });
    // let start_time = Instant::now();
    // let num_blocks = receiver.count();
    dbg!(
        start_time.elapsed(),
        num_blocks,
        num_blocks as f64 / start_time.elapsed().as_secs_f64()
    );
}
