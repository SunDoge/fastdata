use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use clap::{Parser, ValueEnum};
use fastdata_tfrecord::async_reader::{self, io_uring_single_file::AsyncDepthOneTfrecordReader};
use glob::glob;
use kanal::bounded;
use rayon::prelude::*;

#[derive(Debug, Parser)]
struct Cli {
    data: PathBuf,

    #[arg(long, short, default_value = "32")]
    queue_depth: u32,

    #[arg(long, short)]
    check_integrity: bool,

    #[arg(value_enum)]
    reader: Reader,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Reader {
    IoUringMultiFiles,
    IoUringSingleFileDepthOne,
}

fn main() {
    let cli = Cli::parse();
    dbg!(&cli);

    let pattern = cli.data.join("*.tfrecord");

    let tfrecords: Vec<PathBuf> = glob(pattern.to_str().unwrap())
        .expect("fail to glob")
        .map(|p| p.unwrap())
        .collect();

    println!("first record: {}", tfrecords[0].display());

    let (num_records, elapsed) = match cli.reader {
        Reader::IoUringMultiFiles => bench_io_uring_multi_files(&cli, tfrecords),
        Reader::IoUringSingleFileDepthOne => bench_io_uring_single_file_depth_one(&cli, tfrecords),
    };

    let secs = elapsed.as_secs_f64();
    println!(
        "num records: {}, elapsed: {} s, speed: {} record/s",
        num_records,
        secs,
        num_records as f64 / secs
    );
}

fn bench_io_uring_multi_files(cli: &Cli, tfrecords: Vec<PathBuf>) -> (usize, Duration) {
    let mut tfrecord_files = tfrecords
        .into_iter()
        .map(|p| std::fs::File::open(p).unwrap());

    let (sender, receiver) = bounded(cli.queue_depth as usize);

    let queue_depth = cli.queue_depth;
    let check_integrity = cli.check_integrity;
    let _ = std::thread::spawn(move || {
        async_reader::io_uring_multi_files::io_uring_loop(
            tfrecord_files,
            queue_depth,
            check_integrity,
            |buf| sender.send(buf).unwrap(),
        )
        .expect("exit loop");
    });

    let start_time = Instant::now();
    let num_records = receiver.count();
    (num_records, start_time.elapsed())
}

fn bench_io_uring_single_file_depth_one(cli: &Cli, tfrecords: Vec<PathBuf>) -> (usize, Duration) {
    // let mut tfrecord_files = tfrecords
    //     .into_iter()
    //     .map(|p| std::fs::File::open(p).unwrap());

    rayon::ThreadPoolBuilder::new()
        .num_threads(cli.queue_depth as usize)
        .build_global()
        .unwrap();

    let start_time = Instant::now();
    let num_records = tfrecords
        .par_iter()
        .flat_map_iter(|path| {
            let file = std::fs::File::open(path).unwrap();
            AsyncDepthOneTfrecordReader::new(file, cli.check_integrity).unwrap()
        })
        .map(|buf| buf.unwrap())
        .count();

    (num_records, start_time.elapsed())
}
