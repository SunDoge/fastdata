use crate::utils::IoVec;
use crate::{crc32c::verify_masked_crc, error::Result};
use io_uring::{opcode, types, IoUring};
use slab::Slab;
use std::cmp::Reverse;
use std::io::Read;
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

    pub fn is_read_header(&self) -> bool {
        self.io_vecs.len() == 2
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

pub fn io_uring_loop<F>(file: File, queue_depth: u32, buf_size: usize, cb: F) -> Result<()>
where
    F: Fn(Buffer),
{
    let mut ring = IoUring::new(queue_depth)?;

    let max_reads = queue_depth as usize;
    let mut buffers = Slab::with_capacity(max_reads);
    let mut pending = Vec::with_capacity(max_reads);
    let mut heap = BinaryHeap::with_capacity(max_reads);
    let mut offset = 0;
    let mut num_reads = 0;
    let mut heap_offset = 0; // for heap

    for _ in 0..max_reads {
        let raw_buf = RawBuffer {
            io_vecs: vec![IoVec::from(vec![0; buf_size])],
            offset: offset,
        };
        offset += buf_size as u64;
        let buf_idx = buffers.insert(raw_buf);
        let buf_ref = &buffers[buf_idx];
        let read_e = buf_ref.build_readv_entry(&file, buf_idx as _);
        pending.push(read_e);
    }

    for read_e in pending.drain(..) {
        unsafe {
            ring.submission().push(&read_e)?;
        }
        num_reads += 1;
    }

    loop {
        ring.submit_and_wait(1)?;
        for cqe in ring.completion() {
            assert!(cqe.result() >= 0);

            let buf_idx = cqe.user_data() as usize;
            let bytes_read = cqe.result() as usize;

            // if 0, do nothing
            if bytes_read > 0 {
                let buf_ref = &mut buffers[buf_idx];
                let data = buf_ref.io_vecs[0].as_slice()[..bytes_read].to_vec();

                let new_buffer = Buffer {
                    data,
                    offset: buf_ref.offset,
                };
                heap.push(Reverse(new_buffer));

                buf_ref.offset = offset;
                offset += buf_size as u64;

                let read_e = buf_ref.build_readv_entry(&file, buf_idx as _);
                pending.push(read_e);
            }

            num_reads -= 1;
        }

        for read_e in pending.drain(..) {
            unsafe {
                ring.submission().push(&read_e)?;
            }
            num_reads += 1;
        }

        ring.submit()?;

        while let Some(Reverse(ref buf_ref)) = heap.peek() {
            if buf_ref.offset == heap_offset {
                let Reverse(buf) = heap.pop().unwrap();
                cb(buf);
                heap_offset += buf_size as u64;
            } else {
                break;
            }
        }

        if num_reads == 0 {
            break;
        }
    }

    Ok(())
}

pub struct AsyncBufReader<T> {
    source: T,
    offset: usize,
    buf: Buffer,
    is_end: bool,
}

impl<T> AsyncBufReader<T>
where
    T: Iterator<Item = Buffer>,
{
    pub fn new(source: T) -> Self {
        Self {
            source,
            offset: 0,
            buf: Buffer {
                data: Vec::new(),
                offset: 0,
            },
            is_end: false,
        }
    }
}

impl<T> Read for AsyncBufReader<T>
where
    T: Iterator<Item = Buffer>,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.is_end {
            return Ok(0);
        }

        if self.buf.data.len() == 0 {
            match self.source.next() {
                Some(buf) => {
                    self.buf = buf;
                    self.offset = 0;
                }
                None => return Ok(0),
            }
        }

        let should_read = buf.len();

        let mut readed = 0;

        while readed < should_read {
            let current_buf_size = self.buf.data.len() - self.offset;

            let rest = should_read - readed;

            if current_buf_size <= rest && !self.is_end {
                buf[readed..readed + current_buf_size]
                    .copy_from_slice(&self.buf.data[self.offset..]);
                readed += current_buf_size;
                match self.source.next() {
                    Some(buf) => {
                        self.buf = buf;
                        self.offset = 0;
                    }
                    None => self.is_end = true,
                }
            } else {
                buf[readed..].copy_from_slice(&self.buf.data[self.offset..self.offset + rest]);
                readed += rest;
                self.offset += rest;
            }
        }

        Ok(readed)
    }
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
