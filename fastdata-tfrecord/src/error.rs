pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O Error {0}")]
    IoError(#[from] std::io::Error),

    #[error("checksum mismatch error: expect {expect:#010x}, but found {found:#010x}")]
    ChecksumMismatch { found: u32, expect: u32 },

    #[error("eof")]
    OutOfRange,

    #[error("{0}")]
    DataLoss(String),
    // #[error("libvips")]
    // VipsError(libvips::error::Error),
    #[error("IoUring submission queue push failed: {0}")]
    PushError(#[from] io_uring::squeue::PushError),

    #[error("{0}")]
    DecodeError(#[from] prost::DecodeError),
}

impl Error {
    pub fn from_raw_os_io_error(code: i32) -> Self {
        Self::IoError(std::io::Error::from_raw_os_error(code))
    }
}
