use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    data: PathBuf,

    #[arg(long, default_value = "val")]
    split: String,
}

impl Cli {
    pub fn tfrecord_dir(&self) -> PathBuf {
        self.data.join(&self.split)
    }
}

fn main() {
    let cli = Cli::parse();
    dbg!(cli.tfrecord_dir());
}
