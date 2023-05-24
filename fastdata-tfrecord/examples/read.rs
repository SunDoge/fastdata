use std::{path::PathBuf, time::Instant};

use clap::Parser;
use fastdata_tfrecord::async_reader;
use glob::glob;
use kanal::bounded;

#[derive(Debug, Parser)]
struct Cli {
    data: PathBuf,

    #[arg(long, short, default_value = "32")]
    queue_depth: u32,

    #[arg(long, short)]
    check_integrity: bool,
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

    let mut tfrecord_files = tfrecords
        .into_iter()
        .map(|p| std::fs::File::open(p).unwrap());

    let (sender, receiver) = bounded(cli.queue_depth as usize);

    let _ = std::thread::spawn(move || {
        async_reader::io_uring_loop(
            &mut tfrecord_files,
            cli.queue_depth,
            cli.check_integrity,
            |buf| sender.send(buf).unwrap(),
        )
        .expect("exit loop");
    });

    let start_time = Instant::now();
    let num_records = receiver.count();

    let secs = start_time.elapsed().as_secs_f64();
    println!(
        "num records: {}, elapsed: {} s, speed: {} record/s",
        num_records,
        secs,
        num_records as f64 / secs
    );
}
