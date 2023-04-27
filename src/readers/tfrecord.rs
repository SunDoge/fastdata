use std::{
    fs::File,
    io::{BufReader, Read},
    path::Path,
};

use crate::{
    error::{Error, Result},
    utils::crc32c::verify_masked_crc,
};

const U64_SIZE: usize = std::mem::size_of::<u64>();
const U32_SIZE: usize = std::mem::size_of::<u32>();

pub struct TfRecordReader {
    reader: BufReader<File>,
    check_integrity: bool,
    length_buf: [u8; U64_SIZE],
    masked_crc_buf: [u8; U32_SIZE],
    data_buf: Vec<u8>,
}

impl TfRecordReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        Ok(Self {
            reader,
            check_integrity: false,
            length_buf: [0; U64_SIZE],
            masked_crc_buf: [0; U32_SIZE],
            data_buf: Vec::with_capacity(1024),
        })
    }

    pub fn read(&mut self) -> Result<Option<Vec<u8>>> {
        let bytes_read = self.reader.read(&mut self.length_buf)?;
        if bytes_read == 0 {
            return Ok(None);
        } else if bytes_read != 8 {
            return Err(Error::DataLoss("Invalid tfrecord file".to_string()));
        }

        if self.reader.read(&mut self.masked_crc_buf)? != U32_SIZE {
            return Err(Error::DataLoss("Invalid tfrecord file".to_string()));
        }

        if self.check_integrity {
            self.verify_masked_crc32(&self.length_buf)?;
        }

        let length = u64::from_le_bytes(self.length_buf);
        if length as usize > self.data_buf.len() {
            self.data_buf.resize((length * 2) as usize, 0);
        }
        let bytes_read = self.reader.read(&mut self.data_buf[..length as usize])?;
        if bytes_read != length as usize {
            return Err(Error::DataLoss("Invalid tfrecord file".to_string()));
        }

        if self.reader.read(&mut self.masked_crc_buf)? != U32_SIZE {
            return Err(Error::DataLoss("Invalid tfrecord file".to_string()));
        }

        if self.check_integrity {
            self.verify_masked_crc32(&self.data_buf)?;
        }

        Ok(Some(self.data_buf[..length as usize].to_owned()))
    }

    fn verify_masked_crc32(&self, buf: &[u8]) -> Result<()> {
        let expect = u32::from_le_bytes(self.masked_crc_buf);
        verify_masked_crc(buf, expect)
    }
}

impl Iterator for TfRecordReader {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read().transpose()
    }
}
