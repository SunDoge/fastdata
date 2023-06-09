use std::{fs::File, io::Read, path::Path};

use crate::{constants::U64_SIZE, error::Result};
use memmap2::Mmap;

pub struct MmapIndexReader {
    mmap: Mmap,
}

impl MmapIndexReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        Self::new(file)
    }

    pub fn new(file: File) -> Result<Self> {
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(Self { mmap })
    }

    pub fn read_index(&self, index: usize) -> (u64, u64) {
        let bytes_index = index * U64_SIZE * 2;

        let buf = &self.mmap[bytes_index..bytes_index + U64_SIZE * 2];

        let offset = u64::from_le_bytes(buf[..U64_SIZE].try_into().unwrap());
        let length = u64::from_le_bytes(buf[U64_SIZE..].try_into().unwrap());
        (offset, length)
    }

    pub fn get(&self, index: usize) -> Option<(u64, u64)> {
        if index < self.len() {
            Some(self.read_index(index))
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.mmap.len() / (U64_SIZE * 2)
    }

    pub fn iter(&self) -> Iter<'_> {
        Iter {
            reader: self,
            index: 0,
        }
    }

    pub fn into_iter(self) -> IntoIter {
        IntoIter {
            reader: self,
            index: 0,
        }
    }
}

pub struct Iter<'a> {
    reader: &'a MmapIndexReader,
    index: usize,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (u64, u64);
    fn next(&mut self) -> Option<Self::Item> {
        self.index += 1;
        self.reader.get(self.index - 1)
    }
}

pub struct IntoIter {
    reader: MmapIndexReader,
    index: usize,
}

impl Iterator for IntoIter {
    type Item = (u64, u64);
    fn next(&mut self) -> Option<Self::Item> {
        self.index += 1;
        self.reader.get(self.index - 1)
    }
}

pub struct IndexReader {
    buf: Vec<u8>,
}

impl IndexReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = File::open(path)?;
        Self::new(&mut file)
    }

    pub fn new(file: &mut File) -> Result<Self> {
        let mut buf = Vec::new();
        file.read_to_end(&mut buf);
        Ok(Self { buf })
    }

    pub fn read_index(&self, index: usize) -> (u64, u64) {
        let bytes_index = index * U64_SIZE * 2;

        let buf = &self.buf[bytes_index..bytes_index + U64_SIZE * 2];

        let offset = u64::from_le_bytes(buf[..U64_SIZE].try_into().unwrap());
        let length = u64::from_le_bytes(buf[U64_SIZE..].try_into().unwrap());
        (offset, length)
    }

    pub fn get(&self, index: usize) -> Option<(u64, u64)> {
        if index < self.len() {
            Some(self.read_index(index))
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.buf.len() / (U64_SIZE * 2)
    }

    pub fn iter(&self) -> Iter2<'_> {
        Iter2 {
            reader: self,
            index: 0,
        }
    }

    pub fn into_iter(self) -> IntoIter2 {
        IntoIter2 {
            reader: self,
            index: 0,
        }
    }
}

pub struct Iter2<'a> {
    reader: &'a IndexReader,
    index: usize,
}

impl<'a> Iterator for Iter2<'a> {
    type Item = (u64, u64);
    fn next(&mut self) -> Option<Self::Item> {
        self.index += 1;
        self.reader.get(self.index - 1)
    }
}

pub struct IntoIter2 {
    reader: IndexReader,
    index: usize,
}

impl Iterator for IntoIter2 {
    type Item = (u64, u64);
    fn next(&mut self) -> Option<Self::Item> {
        self.index += 1;
        self.reader.get(self.index - 1)
    }
}
