use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::{fs::File, path::PathBuf};

use clap::{Args, Parser, Subcommand};
use fastdata_tfrecord::indexing::sync_writer::SyncIndexWriter;
use fastdata_tfrecord::sync_reader::TfrecordReader;
use rayon::prelude::{ParallelBridge, ParallelIterator};

#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, short = 'j', default_value = "4")]
    num_threads: usize,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Make index files
    Make(MakeArgs),
    /// Check index files
    Check {},
}

#[derive(Debug, Args)]
struct MakeArgs {
    path: PathBuf,

    #[arg(short, long, default_value = "*.tfrecord")]
    masks: String,
}

fn main() {
    let cli = Cli::parse();
    dbg!(&cli);

    rayon::ThreadPoolBuilder::new()
        .num_threads(cli.num_threads)
        .build_global()
        .unwrap();

    match cli.command {
        Commands::Make(ref args) => make_index_files(args),
        Commands::Check {} => {}
    }
}

fn make_index_files(args: &MakeArgs) {
    if args.path.is_dir() {
        let pattern = args.path.join(&args.masks);
        glob::glob(pattern.to_str().unwrap())
            .unwrap()
            .par_bridge()
            .for_each(|path| {
                let path = path.unwrap();
                dbg!(&path);
                create_index(&path);
            });
    } else if args.path.is_file() {
        create_index(&args.path);
    }
}

fn create_index<P: AsRef<Path>>(path: P) {
    let in_file = File::open(&path).unwrap();
    // let index_path = format!("{}.idx", path.as_ref().to_str().unwrap());
    let index_path = path.as_ref().to_owned().with_extension("tfrecord.idx");
    let out_file = File::create(&index_path).unwrap();

    let buf_reader = BufReader::new(in_file);
    let reader = TfrecordReader::new(buf_reader, true);

    let buf_writer = BufWriter::new(out_file);
    let mut index_writer = SyncIndexWriter::new(buf_writer);

    reader.indices().for_each(|index| {
        let (offset, length) = index.unwrap();
        index_writer.write_index(offset, length).unwrap();
    });
}
