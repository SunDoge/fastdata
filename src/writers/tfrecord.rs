use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

use crate::{error::Result, utils::crc32c::get_masked_crc};

pub struct TfRecordWriter {
    writer: BufWriter<File>,
}

impl TfRecordWriter {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::options().append(true).create(true).open(path)?;
        let writer = BufWriter::new(file);
        Ok(Self { writer })
    }

    pub fn write(&mut self, buf: &[u8]) -> Result<()> {
        let length = buf.len() as u64;
        let length_buf = length.to_le_bytes();
        let masked_crc_of_length = get_masked_crc(&length_buf);
        let masked_crc_of_length_buf = masked_crc_of_length.to_le_bytes();
        let masked_crc_of_data = get_masked_crc(buf);
        let masked_crc_of_data_buf = masked_crc_of_data.to_le_bytes();

        self.writer.write_all(&length_buf)?;
        self.writer.write_all(&masked_crc_of_length_buf)?;
        self.writer.write_all(&buf)?;
        self.writer.write_all(&masked_crc_of_data_buf)?;

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        Ok(self.writer.flush()?)
    }
}
