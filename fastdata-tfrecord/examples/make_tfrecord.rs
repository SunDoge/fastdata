use std::{collections::HashMap, fs::File, io::Read, path::PathBuf};

use clap::Parser;
use fastdata_tfrecord::sync_writer::TfrecordWriter;
use kanal::bounded;
use rayon::{prelude::{ParallelBridge, ParallelIterator, IntoParallelRefIterator, IndexedParallelIterator}, slice::ParallelSlice};

#[derive(Debug, Parser)]
struct Cli {
    in_dir: PathBuf,

    out_dir: PathBuf,

    #[arg(long, short = 'j', default_value = "4")]
    num_threads: usize,
}

fn main() {
    let cli = Cli::parse();
    dbg!(&cli);

    rayon::ThreadPoolBuilder::new()
        .num_threads(cli.num_threads)
        .build_global()
        .unwrap();

    let mut dirs: Vec<_> = cli
        .in_dir
        .read_dir()
        .unwrap()
        .filter_map(|path| {
            let path = path.unwrap();
            if let Ok(file_type) = path.file_type() {
                if file_type.is_dir() {
                    Some(path.file_name())
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    dirs.sort();

    let class_map: HashMap<_, _> = dirs
        .iter()
        .enumerate()
        .map(|(i, p)| (p.as_os_str(), i))
        .collect();

    // let (sender, receiver) = bounded(1024 * 1024);

    let pattern = cli.in_dir.join("*/*");

    let samples: Vec<_> = glob::glob(pattern.to_str().unwrap())
        .unwrap()
        .par_bridge()
        .map(|path| {
            let path = path.unwrap();
            let class_name = path.parent().unwrap().file_name().unwrap();
            let class_index = class_map[class_name];

            // let mut img_buf = Vec::new();
            // File::open(&path)
            //     .unwrap()
            //     .read_to_end(&mut img_buf)
            //     .unwrap();

            (path, class_index)
        })
        .collect();

    dbg!(samples.len());

    samples.par_chunks(500).enumerate().map(|(index, chunk)| {
        // let mut writer = TfrecordWriter::create(path)
    });

}
