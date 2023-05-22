use std::collections::VecDeque;
use std::{mem::ManuallyDrop, os::fd::AsRawFd};

use crate::error::{Error, Result};
use crate::utils::crc32c::verify_masked_crc;
use io_uring::{opcode, types, IoUring};
use kanal::Sender;
use slab::Slab;

const U64_SIZE: usize = std::mem::size_of::<u64>();
const U32_SIZE: usize = std::mem::size_of::<u32>();

#[derive(Debug, Clone, Copy)]
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
    pub max_reads: usize,
    pub pending: Vec<io_uring::squeue::Entry>,
    pub finished: VecDeque<Vec<u8>>,
    pub check_integrity: bool,
}

impl<T> IoUringTfrecordReader<T>
where
    T: Iterator<Item = std::fs::File>,
{
    pub fn new(source: T, queue_depth: u32, check_integrity: bool) -> Result<Self> {
        let max_reads = queue_depth as usize;
        Ok(Self {
            source,
            buffers: Slab::with_capacity(max_reads),
            ring: IoUring::new(queue_depth)?,
            num_reads: 0,
            max_reads,
            pending: Vec::with_capacity(max_reads),
            finished: VecDeque::with_capacity(max_reads),
            check_integrity,
        })
    }

    pub fn start(&mut self) -> Result<()> {
        for _ in 0..self.max_reads {
            if let Some(fd) = self.source.next() {
                self.add_read_header(fd);
            } else {
                break;
            }
        }
        self.drain_pending()?;
        Ok(())
    }

    /// If tasks completed, drain them and submit new ones
    pub fn check_completion(&mut self) -> Result<()> {
        for cqe in self.ring.completion() {
            if cqe.result() < 0 {
                return Err(Error::from_raw_os_io_error(-cqe.result()));
            }

            let buf_idx = cqe.user_data() as usize;
            let bytes_read = cqe.result();

            // If finished
            if bytes_read == 0 {
                let buffer = self.buffers.remove(buf_idx);
                println!("finished: {:?}", buffer);

                // Try to add next file
                if let Some(fd) = self.source.next() {
                    let buffer = Buffer {
                        fd,
                        io_vecs: vec![
                            IoVec::from(vec![0; U64_SIZE]),
                            IoVec::from(vec![0; U32_SIZE]),
                        ],
                        offset: 0,
                    };

                    // Reload a new Buffer
                    let buf_idx = self.buffers.insert(buffer);
                    let buf_ref = &mut self.buffers[buf_idx];
                    let read_e = buf_ref.get_readv().build().user_data(buf_idx as _);
                    self.pending.push(read_e);
                }
            } else {
                let buf_ref = &mut self.buffers[buf_idx];
                if buf_ref.is_read_header() {
                    let length_buf = buf_ref.io_vecs[0].as_slice().try_into().unwrap();
                    let length = u64::from_le_bytes(length_buf);
                    if self.check_integrity {
                        let masked_crc_buf = buf_ref.io_vecs[1].as_slice().try_into().unwrap();
                        let masked_crc = u32::from_le_bytes(masked_crc_buf);
                        verify_masked_crc(&length_buf, masked_crc)?;
                    }
                    buf_ref.io_vecs = vec![
                        IoVec::from(vec![0; length as usize]),
                        IoVec::from(vec![0; U32_SIZE]),
                        IoVec::from(vec![0; U64_SIZE]),
                        IoVec::from(vec![0; U32_SIZE]),
                    ];

                    // Move to data start
                    buf_ref.offset += (U64_SIZE + U32_SIZE) as u64;
                    let read_e = buf_ref.get_readv().build().user_data(buf_idx as _);
                    self.pending.push(read_e);
                } else {
                    let data_buf = Vec::from(buf_ref.io_vecs[0]);
                    if self.check_integrity {
                        let masked_crc_buf = buf_ref.io_vecs[1].as_slice().try_into().unwrap();
                        let masked_crc = u32::from_le_bytes(masked_crc_buf);
                        assert!(verify_masked_crc(&data_buf, masked_crc).is_ok());
                    }

                    // Save finished data
                    let data_length = data_buf.len();
                    self.finished.push_back(data_buf);

                    // Get next record length
                    let length_buf = buf_ref.io_vecs[2].as_slice().try_into().unwrap();
                    let length = u64::from_le_bytes(length_buf);

                    if self.check_integrity {
                        let masked_crc_buf = buf_ref.io_vecs[3].as_slice().try_into().unwrap();
                        let masked_crc = u32::from_le_bytes(masked_crc_buf);
                        assert!(verify_masked_crc(&length_buf, masked_crc).is_ok());
                    }

                    // Update data buffer length is enough
                    buf_ref.io_vecs[0] = IoVec::from(vec![0; length as usize]);
                    buf_ref.offset += (data_length + U32_SIZE + U64_SIZE + U32_SIZE) as u64;

                    let read_e = buf_ref.get_readv().build().user_data(buf_idx as _);
                    self.pending.push(read_e);
                }
            }

            // For every cqe, decrease num_reads
            self.num_reads -= 1;
        }

        Ok(())
    }

    /// Must call start first
    pub fn read(&mut self) -> Result<Option<Vec<u8>>> {
        if self.num_reads == 0 && self.finished.is_empty() {
            Ok(None)
        } else {
            if self.finished.is_empty() {
                // We have to make sure we have at least one return buffer
                self.ring.submit_and_wait(1)?;
            }

            // Decrease num_reads, add pending, add finished
            self.check_completion()?;

            // drain pending, increase num_reads
            self.drain_pending()?;

            // We have at least one finished, so return it
            Ok(self.finished.pop_front())
        }
    }

    pub fn add_read_header(&mut self, fd: std::fs::File) {
        let buffer = Buffer {
            fd,
            io_vecs: vec![
                IoVec::from(vec![0; U64_SIZE]),
                IoVec::from(vec![0; U32_SIZE]),
            ],
            offset: 0,
        };
        let buf_idx = self.buffers.insert(buffer);
        let buf_ref = &mut self.buffers[buf_idx];
        let read_e = buf_ref.get_readv().build().user_data(buf_idx as _);
        self.pending.push(read_e);
    }

    pub fn read_data(&mut self, buf_idx: usize, length: usize) {
        let buf_ref = &mut self.buffers[buf_idx];
        buf_ref.io_vecs = vec![
            IoVec::from(vec![0; length]),
            IoVec::from(vec![0; U32_SIZE]),
            IoVec::from(vec![0; U64_SIZE]),
            IoVec::from(vec![0; U32_SIZE]),
        ];
        buf_ref.offset;
        let read_e = buf_ref.get_readv().build().user_data(buf_idx as _);
        self.pending.push(read_e);
    }

    pub fn drain_pending(&mut self) -> Result<()> {
        assert!(self.pending.len() <= self.max_reads - self.num_reads);
        if self.pending.is_empty() {
            Ok(())
        } else {
            for read_e in self.pending.drain(..) {
                unsafe {
                    self.ring.submission().push(&read_e)?;
                }
                self.num_reads += 1;
            }
            self.ring.submit()?;
            Ok(())
        }
    }
}

pub fn io_uring_loop<T>(
    source: &mut T,
    queue_depth: u32,
    sender: Sender<Vec<u8>>,
    check_integrity: bool,
) where
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

    loop {
        for cqe in ring.completion() {
            // TODO: return Err
            assert!(cqe.result() >= 0);

            let buf_idx = cqe.user_data() as usize;
            let bytes_read = cqe.result();
            if bytes_read == 0 {
                let buffer = buffers.remove(buf_idx);
                println!("finished: {:?}", buffer);

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
                }
            } else {
                // dbg!(bytes_read);
                let buf_ref = &mut buffers[buf_idx];
                if buf_ref.is_read_header() {
                    let length_buf = buf_ref.io_vecs[0].as_slice().try_into().unwrap();
                    let length = u64::from_le_bytes(length_buf);

                    if check_integrity {
                        let masked_crc_buf = buf_ref.io_vecs[1].as_slice().try_into().unwrap();
                        let masked_crc = u32::from_le_bytes(masked_crc_buf);
                        assert!(verify_masked_crc(&length_buf, masked_crc).is_ok());
                    }

                    buf_ref.io_vecs = vec![
                        IoVec::from(vec![0; length as usize]),
                        IoVec::from(vec![0; U32_SIZE]),
                        IoVec::from(vec![0; U64_SIZE]),
                        IoVec::from(vec![0; U32_SIZE]),
                    ];
                    buf_ref.offset += (U64_SIZE + U32_SIZE) as u64;

                    let read_e = buf_ref.get_readv().build().user_data(buf_idx as _);
                    waiting.push(read_e);
                } else {
                    let data_buf = Vec::from(buf_ref.io_vecs[0]);
                    if check_integrity {
                        let masked_crc_buf = buf_ref.io_vecs[1].as_slice().try_into().unwrap();
                        let masked_crc = u32::from_le_bytes(masked_crc_buf);
                        assert!(verify_masked_crc(&data_buf, masked_crc).is_ok());
                    }

                    let data_length = data_buf.len();
                    sender.send(data_buf).unwrap();

                    let length_buf = buf_ref.io_vecs[2].as_slice().try_into().unwrap();
                    let length = u64::from_le_bytes(length_buf);

                    if check_integrity {
                        let masked_crc_buf = buf_ref.io_vecs[3].as_slice().try_into().unwrap();
                        let masked_crc = u32::from_le_bytes(masked_crc_buf);
                        assert!(verify_masked_crc(&length_buf, masked_crc).is_ok());
                    }

                    buf_ref.io_vecs[0] = IoVec::from(vec![0; length as usize]);
                    buf_ref.offset += (data_length + U32_SIZE + U64_SIZE + U32_SIZE) as u64;

                    let read_e = buf_ref.get_readv().build().user_data(buf_idx as _);
                    waiting.push(read_e);
                }
            }
            num_reads -= 1;
        }

        // dbg!(num_reads);
        let n = waiting.len().min(max_reads - num_reads);
        for read_e in waiting.drain(0..n) {
            unsafe {
                ring.submission().push(&read_e).unwrap();
            }
            num_reads += 1;
        }

        // dbg!(num_reads == max_reads);

        if num_reads == 0 {
            break;
        }

        ring.submit_and_wait(1).unwrap();
    }
}
