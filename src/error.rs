pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O Error {0}")]
    IoError(std::io::Error),

    #[error("checksum mismatch error: expect {expect:#010x}, but found {found:#010x}")]
    ChecksumMismatch { found: u32, expect: u32 },

    #[error("eof")]
    OutOfRange,

    #[error("{0}")]
    DataLoss(String),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}
