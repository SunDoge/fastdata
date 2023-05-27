use std::path::PathBuf;

use clap::Parser;

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
}
