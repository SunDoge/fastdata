use crate::error::Result;
use std::io::Write;

pub struct SyncIndexWriter<T> {
    writer: T,
}

impl<T> SyncIndexWriter<T>
where
    T: Write,
{
    pub fn new(writer: T) -> Self {
        Self { writer }
    }

    pub fn write_index(&mut self, offset: u64, length: u64) -> Result<()> {
        self.writer.write_all(&offset.to_le_bytes())?;
        self.writer.write_all(&length.to_le_bytes())?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}
