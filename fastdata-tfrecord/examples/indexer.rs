use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
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
}

fn main() {
    let cli = Cli::parse();
    dbg!(&cli);
}

fn make_index_files(args: &MakeArgs) {
    if args.path.is_dir() {
    } else if args.path.is_file() {
    }
}
