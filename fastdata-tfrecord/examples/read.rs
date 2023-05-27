use std::{
    io::BufReader,
    path::PathBuf,
    time::{Duration, Instant},
};

use clap::{Parser, ValueEnum};
use fastdata_tfrecord::{
    async_reader::{
        self,
        io_uring_random_reader::AsyncRandomReader,
        io_uring_single_file::{AsyncBufReader, AsyncDepthOneTfrecordReader},
    },
    sync_reader::TfrecordReader,
};
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

    #[arg(long, short = 'j', default_value = "4")]
    num_threads: usize,

    #[arg(value_enum)]
    reader: Reader,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Reader {
    IoUringMultiFiles,
    IoUringSingleFileDepthOne,
    Sync,
    SyncOverAsync,
    IoUringIndexed,
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
        Reader::Sync => bench_sync(&cli, tfrecords),
        Reader::SyncOverAsync => bench_sync_over_async(&cli, tfrecords),
        Reader::IoUringIndexed => bench_io_uring_indexed(&cli, tfrecords),
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
    let tfrecord_files = tfrecords
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

fn bench_sync(cli: &Cli, tfrecords: Vec<PathBuf>) -> (usize, Duration) {
    rayon::ThreadPoolBuilder::new()
        .num_threads(cli.queue_depth as usize)
        .build_global()
        .unwrap();

    let start_time = Instant::now();
    let num_records = tfrecords
        .par_iter()
        .flat_map_iter(|path| {
            let file = std::fs::File::open(path).unwrap();
            let buf_reader = BufReader::new(file);
            TfrecordReader::new(buf_reader, cli.check_integrity)
        })
        .map(|buf| buf.unwrap())
        .count();

    (num_records, start_time.elapsed())
}

fn bench_sync_over_async(cli: &Cli, tfrecords: Vec<PathBuf>) -> (usize, Duration) {
    rayon::ThreadPoolBuilder::new()
        .num_threads(cli.queue_depth as usize / 4)
        .build_global()
        .unwrap();

    // let queue_depth = cli.queue_depth;
    let queue_depth = 4;
    let buf_size = 1024 * 1024;
    let start_time = Instant::now();
    let num_records = tfrecords
        .par_iter()
        .flat_map_iter(|path| {
            // dbg!(path);
            let file = std::fs::File::open(path).unwrap();
            let (sender, reciver) = bounded(1024);
            std::thread::spawn(move || {
                async_reader::io_uring_single_file::io_uring_loop(
                    file,
                    queue_depth,
                    buf_size,
                    |buf| {
                        // dbg!(buf.data.len());
                        sender.send(buf).unwrap();
                    },
                )
                .unwrap();
            });
            // reciver
            let buf_reader = AsyncBufReader::new(reciver);
            TfrecordReader::new(buf_reader, cli.check_integrity)
        })
        .map(|buf| buf.unwrap())
        .count();

    (num_records, start_time.elapsed())
}

fn bench_io_uring_indexed(cli: &Cli, tfrecords: Vec<PathBuf>) -> (usize, Duration) {
    rayon::ThreadPoolBuilder::new()
        .num_threads(cli.num_threads)
        .build_global()
        .unwrap();

    let (sender, receiver) = bounded(1024 * 1024);

    let queue_depth = cli.queue_depth;
    let check_integrity = cli.check_integrity;

    let start_time = Instant::now();
    rayon::spawn(move || {
        tfrecords.par_iter().for_each_with(sender, |sender, path| {
            async_reader::io_uring_random_reader::io_uring_loop(
                path,
                None,
                queue_depth,
                check_integrity,
                |buf| sender.send(buf).unwrap(),
            )
            .unwrap();
        });
    });

    let num_records = receiver.count();

    (num_records, start_time.elapsed())
}
