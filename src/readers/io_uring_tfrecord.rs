use std::{mem::ManuallyDrop, os::fd::AsRawFd};

use crate::error::{Error, Result};
use io_uring::{opcode, types, IoUring};
use kanal::Sender;
use slab::Slab;

const U64_SIZE: usize = std::mem::size_of::<u64>();
const U32_SIZE: usize = std::mem::size_of::<u32>();

#[derive(Debug)]
#[repr(C)]
pub struct IoVec {
    pub iov_base: *mut std::ffi::c_void,
    pub iov_len: usize,
}

impl IoVec {
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.iov_base as *const _, self.iov_len) }
    }
}

impl From<Vec<u8>> for IoVec {
    fn from(value: Vec<u8>) -> Self {
        let value = ManuallyDrop::new(value);
        Self {
            iov_base: value.as_ptr() as *mut _,
            iov_len: value.len(),
        }
    }
}

impl<const N: usize> From<[u8; N]> for IoVec {
    fn from(value: [u8; N]) -> Self {
        let value = ManuallyDrop::new(value);
        Self {
            iov_base: value.as_ptr() as *mut _,
            iov_len: value.len(),
        }
    }
}

impl From<IoVec> for Vec<u8> {
    fn from(value: IoVec) -> Self {
        unsafe { Vec::from_raw_parts(value.iov_base as *mut _, value.iov_len, value.iov_len) }
    }
}

// impl From<IoVec> for &[u8] {
//     fn from(value: IoVec) -> Self {
//         unsafe { std::slice::from_raw_parts(value.iov_base as *const _, value.iov_len) }
//     }
// }

#[derive(Debug)]
pub struct Buffer {
    pub fd: std::fs::File,
    pub io_vecs: Vec<IoVec>,
    pub offset: u64,
}

impl Buffer {
    pub fn get_raw_fd(&self) -> types::Fd {
        types::Fd(self.fd.as_raw_fd())
    }

    pub fn get_readv(&self) -> opcode::Readv {
        opcode::Readv::new(
            self.get_raw_fd(),
            self.io_vecs.as_ptr() as *const _,
            self.io_vecs.len() as _,
        )
        .offset(self.offset)
    }

    pub fn is_read_header(&self) -> bool {
        self.io_vecs.len() == 2
    }
}

pub struct IoUringTfrecordReader<T> {
    pub source: T,
    pub buffers: Slab<Buffer>,
    pub ring: IoUring,
    pub num_reads: usize,
    pub queue_depth: usize,
    pub waiting: Vec<io_uring::squeue::Entry>,
}

impl<T> IoUringTfrecordReader<T>
where
    T: Iterator<Item = std::fs::File>,
{
    pub fn new(source: T, queue_depth: u32) -> Result<Self> {
        Ok(Self {
            source,
            buffers: Slab::with_capacity(queue_depth as usize),
            ring: IoUring::new(queue_depth)?,
            num_reads: 0,
            queue_depth: queue_depth as usize,
            waiting: Vec::with_capacity(queue_depth as usize),
        })
    }

    pub fn start(&mut self) {
        for _ in 0..self.queue_depth {
            if let Some(fd) = self.source.next() {
                self.read_header(fd);
            } else {
                break;
            }
        }

        self.push_submission();
        self.ring.submit().unwrap();
    }

    pub fn read_next_header(&mut self) {
        if let Some(fd) = self.source.next() {
            self.read_header(fd);
        }
    }

    pub fn read(&mut self) {}

    pub fn read_header(&mut self, fd: std::fs::File) {
        let buffer = Buffer {
            fd,
            io_vecs: vec![
                IoVec::from(vec![0; U64_SIZE]),
                IoVec::from(vec![0; U32_SIZE]),
            ],
            offset: 0,
        };
        let buffer_idx = self.buffers.insert(buffer);
        let buffer_ref = &mut self.buffers[buffer_idx];
        let read_e = buffer_ref.get_readv().build().user_data(buffer_idx as _);
        self.waiting.push(read_e);
    }

    pub fn read_data(&mut self, buffer_idx: usize, length: usize) {
        let buffer_ref = &mut self.buffers[buffer_idx];
        buffer_ref.io_vecs = vec![
            IoVec::from(vec![0; length]),
            IoVec::from(vec![0; U32_SIZE]),
            IoVec::from(vec![0; U64_SIZE]),
            IoVec::from(vec![0; U32_SIZE]),
        ];
        buffer_ref.offset;
        let read_e = buffer_ref.get_readv().build().user_data(buffer_idx as _);
        self.waiting.push(read_e);
    }

    pub fn push_submission(&mut self) {
        let n = self.waiting.len().min(self.queue_depth);
        for _ in 0..n {
            let read_e = self.waiting.pop().unwrap();
            unsafe {
                self.ring.submission().push(&read_e).unwrap();
            }
            self.num_reads += 1;
        }
    }
}

pub fn io_uring_loop<T>(source: &mut T, queue_depth: u32, sender: Sender<Vec<u8>>)
where
    T: Iterator<Item = std::fs::File>,
{
    let mut ring = IoUring::new(queue_depth).unwrap();
    let max_reads = queue_depth as usize;
    let mut buffers = Slab::with_capacity(max_reads);

    let mut waiting = Vec::new();

    for _ in 0..max_reads {
        if let Some(fd) = source.next() {
            let buffer = Buffer {
                fd,
                io_vecs: vec![
                    IoVec::from(vec![0; U64_SIZE]),
                    IoVec::from(vec![0; U32_SIZE]),
                ],
                offset: 0,
            };
            let buf_idx = buffers.insert(buffer);
            let buf_ref = &mut buffers[buf_idx];
            let read_e = buf_ref.get_readv().build().user_data(buf_idx as _);
            waiting.push(read_e);
        } else {
            break;
        }
    }

    let mut num_reads = 0;

    for read_e in waiting.drain(..) {
        unsafe {
            ring.submission().push(&read_e).unwrap();
        }
        num_reads += 1;
    }

    ring.submit_and_wait(1).unwrap();

    // let mut length_buf = [0u8; U64_SIZE];

    while num_reads > 0 && !buffers.is_empty() {
        for cqe in ring.completion() {
            // TODO: return Err
            assert!(cqe.result() >= 0);

            let buf_idx = cqe.user_data() as usize;
            let bytes_read = cqe.result();
            if bytes_read == 0 {
                let buffer = buffers.remove(buf_idx);
                println!("finished: {:?}", buffer);
                num_reads -= 1;
            } else {
                let buf_ref = &mut buffers[buf_idx];
                if buf_ref.is_read_header() {
                    let length_buf = buf_ref.io_vecs[0].as_slice().try_into().unwrap();
                    let length = u64::from_le_bytes(length_buf);

                    buf_ref.io_vecs = vec![
                        IoVec::from(vec![0; length as usize]),
                        IoVec::from(vec![0; U32_SIZE]),
                        IoVec::from(vec![0; U64_SIZE]),
                        IoVec::from(vec![0; U32_SIZE]),
                    ];
                    buf_ref.offset += (U64_SIZE + U32_SIZE) as u64;

                    let read_e = buf_ref.get_readv().build().user_data(buf_idx as _);
                    waiting.push(read_e);
                }
            }
        }
    }
}
