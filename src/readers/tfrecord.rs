use std::{
    fs::File,
    io::{BufReader, Read, Seek},
    path::Path,
};

use crate::{error::Result, utils::crc32c::verify_masked_crc};

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
        match self.reader.read_exact(&mut self.length_buf) {
            Ok(_) => {}
            Err(err) => {
                return match err.kind() {
                    std::io::ErrorKind::UnexpectedEof => Ok(None),
                    _ => Err(err.into()),
                }
            }
        }

        self.reader.read_exact(&mut self.masked_crc_buf)?;

        if self.check_integrity {
            self.verify_masked_crc32(&self.length_buf)
                .expect("fail to pass length crc");
        }

        let length = u64::from_le_bytes(self.length_buf);
        // dbg!(length);

        if length as usize > self.data_buf.len() {
            self.data_buf.resize((length * 2) as usize, 0);
        }

        self.reader
            .read_exact(&mut self.data_buf[..length as usize])?;

        self.reader.read_exact(&mut self.masked_crc_buf)?;

        if self.check_integrity {
            self.verify_masked_crc32(&self.data_buf[..length as usize])?;
        }

        Ok(Some(self.data_buf[..length as usize].to_owned()))
    }

    fn verify_masked_crc32(&self, buf: &[u8]) -> Result<()> {
        let expect = u32::from_le_bytes(self.masked_crc_buf);
        verify_masked_crc(buf, expect)
    }

    pub fn set_check_integrity(&mut self, check_integrity: bool) {
        self.check_integrity = check_integrity;
    }

    pub fn iter(&mut self) -> Result<Iter<'_>> {
        self.reader.seek(std::io::SeekFrom::Start(0))?;
        Ok(Iter { reader: self })
    }
}

impl From<BufReader<File>> for TfRecordReader {
    fn from(reader: BufReader<File>) -> Self {
        Self {
            reader,
            check_integrity: false,
            length_buf: [0; U64_SIZE],
            masked_crc_buf: [0; U32_SIZE],
            data_buf: Vec::with_capacity(1024),
        }
    }
}

pub struct Iter<'a> {
    reader: &'a mut TfRecordReader,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.read().transpose()
    }
}

impl Iterator for TfRecordReader {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read().transpose()
    }
}
