use crate::utils::IoVec;
use crate::{crc32c::verify_masked_crc, error::Result};
use io_uring::{opcode, types, IoUring};
use slab::Slab;
use std::{collections::BinaryHeap, fs::File, os::fd::AsRawFd};

const U64_SIZE: usize = std::mem::size_of::<u64>();
const U32_SIZE: usize = std::mem::size_of::<u32>();

pub struct RawBuffer {
    pub io_vecs: Vec<IoVec>,
    pub offset: u64,
}

impl RawBuffer {
    pub fn build_readv_entry(&self, fd: &File, user_data: u64) -> io_uring::squeue::Entry {
        opcode::Readv::new(
            types::Fd(fd.as_raw_fd()),
            self.io_vecs.as_ptr() as *const _,
            self.io_vecs.len() as _,
        )
        .offset(self.offset)
        .build()
        .user_data(user_data)
    }
}

pub struct Buffer {
    pub data: Vec<u8>,
    pub offset: u64,
}

impl PartialEq for Buffer {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
    }
}

impl Eq for Buffer {}

impl PartialOrd for Buffer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.offset.partial_cmp(&other.offset)
    }
}

impl Ord for Buffer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.offset.cmp(&other.offset)
    }
}

pub fn io_uring_depth_one_loop<F>(fd: File, buf_size: usize, cb: F)
where
    F: Fn(Vec<u8>) -> Result<()>,
{
    let mut ring = IoUring::new(1);

    let mut buf = IoVec::from(vec![0; buf_size]);

    loop {}
}

pub struct AsyncDepthOneTfrecordReader {
    pub file: File,
    pub raw_buffer: RawBuffer,
    pub ring: IoUring,
    check_integrity: bool,
}

impl AsyncDepthOneTfrecordReader {
    pub fn new(file: File, check_integrity: bool) -> Result<Self> {
        let ring = IoUring::new(1)?;
        Ok(Self {
            file,
            raw_buffer: RawBuffer {
                io_vecs: Vec::new(),
                offset: 0,
            },
            ring,
            check_integrity,
        })
    }

    pub fn is_started(&self) -> bool {
        !self.raw_buffer.io_vecs.is_empty()
    }

    pub fn start(&mut self) -> Result<()> {
        self.raw_buffer.io_vecs = vec![
            IoVec::from(vec![0; U64_SIZE]), // length
            IoVec::from(vec![0; U32_SIZE]), // crc_of_length
        ];
        let read_e = self.raw_buffer.build_readv_entry(&self.file, 0x42);
        unsafe {
            self.ring.submission().push(&read_e)?;
        }
        self.ring.submit_and_wait(1)?;

        let cqe = self.ring.completion().next().unwrap();
        assert!(cqe.result() >= 0);
        assert!(cqe.user_data() == 0x42);
        // dbg!(cqe.result());

        let length_buf = self.raw_buffer.io_vecs[0]
            .as_slice()
            .try_into()
            .expect("fail to convert to array");
        let length = u64::from_le_bytes(length_buf);
        // dbg!(length);
        if self.check_integrity {
            let masked_crc_buf = self.raw_buffer.io_vecs[1]
                .as_slice()
                .try_into()
                .expect("fail to convert to array");
            let masked_crc = u32::from_le_bytes(masked_crc_buf);
            assert!(verify_masked_crc(&length_buf, masked_crc).is_ok());
        }

        self.raw_buffer.io_vecs = vec![
            IoVec::from(vec![0; length as usize]), // data
            IoVec::from(vec![0; U32_SIZE]),        // crc of data
            IoVec::from(vec![0; U64_SIZE]),        // length
            IoVec::from(vec![0; U32_SIZE]),
        ];
        self.raw_buffer.offset += (U32_SIZE + U64_SIZE) as u64;

        let read_e = self.raw_buffer.build_readv_entry(&self.file, 0x42);
        unsafe {
            self.ring.submission().push(&read_e)?;
        }
        self.ring.submit()?;

        Ok(())
    }

    pub fn read(&mut self) -> Result<Option<Vec<u8>>> {
        if !self.is_started() {
            self.start()?;
        }
        self.ring.submit_and_wait(1)?;
        let cqe = self.ring.completion().next().unwrap();
        assert!(cqe.result() >= 0);
        // dbg!(cqe.result());

        let bytes_read = cqe.result() as usize;
        if bytes_read == 0 {
            Ok(None)
        } else {
            let data_buf = Vec::from(self.raw_buffer.io_vecs[0]);
            if self.check_integrity {
                let masked_crc_buf = self.raw_buffer.io_vecs[1].as_slice().try_into().unwrap();
                let masked_crc = u32::from_le_bytes(masked_crc_buf);
                verify_masked_crc(&data_buf, masked_crc)?;
            }

            let data_length = data_buf.len();

            let length_buf = self.raw_buffer.io_vecs[2].as_slice().try_into().unwrap();
            let length = u64::from_le_bytes(length_buf);

            if self.check_integrity {
                let masked_crc_buf = self.raw_buffer.io_vecs[3].as_slice().try_into().unwrap();
                let masked_crc = u32::from_le_bytes(masked_crc_buf);
                verify_masked_crc(&length_buf, masked_crc)?;
            }

            // Reset data buffer
            self.raw_buffer.io_vecs[0] = IoVec::from(vec![0; length as usize]);
            self.raw_buffer.offset += (data_length + U32_SIZE + U64_SIZE + U32_SIZE) as u64;

            let read_e = self.raw_buffer.build_readv_entry(&self.file, 0x42);
            unsafe {
                self.ring.submission().push(&read_e)?;
            }
            self.ring.submit()?;

            Ok(Some(data_buf))
        }
    }
}

impl Iterator for AsyncDepthOneTfrecordReader {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read().transpose()
    }
}
