use std::os::fd::AsRawFd;

use crate::crc32c::verify_masked_crc;
use crate::error::Result;
use crate::utils::IoVec;
use io_uring::{opcode, types, IoUring};
use slab::Slab;

const U64_SIZE: usize = std::mem::size_of::<u64>();
const U32_SIZE: usize = std::mem::size_of::<u32>();

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

    pub fn build_readv_entry(&self, user_data: u64) -> io_uring::squeue::Entry {
        self.get_readv().build().user_data(user_data)
    }
}

/// This function work without index
pub fn io_uring_loop<T, F>(
    mut source: T,
    queue_depth: u32,
    check_integrity: bool,
    cb: F,
) -> Result<()>
where
    T: Iterator<Item = std::fs::File>,
    F: Fn(Vec<u8>),
{
    let mut ring = IoUring::new(queue_depth)?;

    let max_reads = queue_depth as usize;
    let mut buffers = Slab::with_capacity(max_reads);
    let mut pending = Vec::with_capacity(max_reads);

    for _ in 0..max_reads {
        if let Some(fd) = source.next() {
            let buffer = Buffer {
                fd,
                io_vecs: vec![
                    IoVec::from(vec![0; U64_SIZE]), // length
                    IoVec::from(vec![0; U32_SIZE]), // crc_of_length
                ],
                offset: 0,
            };
            let buf_idx = buffers.insert(buffer);
            let buf_ref = &mut buffers[buf_idx];
            let read_e = buf_ref.build_readv_entry(buf_idx as _);
            pending.push(read_e);
        } else {
            break;
        }
    }

    let mut num_reads = 0;

    for read_e in pending.drain(..) {
        unsafe {
            ring.submission().push(&read_e)?;
        }
        num_reads += 1;
    }

    loop {
        ring.submit_and_wait(1)?;
        for cqe in ring.completion() {
            // TODO: return Err
            assert!(cqe.result() >= 0);

            let buf_idx = cqe.user_data() as usize;
            let bytes_read = cqe.result();
            if bytes_read == 0 {
                let _buffer = buffers.remove(buf_idx);

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
                    let buf_ref = &buffers[buf_idx];
                    let read_e = buf_ref.build_readv_entry(buf_idx as _);
                    pending.push(read_e);
                }
            } else {
                // dbg!(bytes_read);
                let buf_ref = &mut buffers[buf_idx];
                if buf_ref.is_read_header() {
                    let length_buf = buf_ref.io_vecs[0]
                        .as_slice()
                        .try_into()
                        .expect("fail to convert to array");
                    let length = u64::from_le_bytes(length_buf);

                    if check_integrity {
                        let masked_crc_buf = buf_ref.io_vecs[1]
                            .as_slice()
                            .try_into()
                            .expect("fail to convert to array");
                        let masked_crc = u32::from_le_bytes(masked_crc_buf);
                        assert!(verify_masked_crc(&length_buf, masked_crc).is_ok());
                    }

                    buf_ref.io_vecs = vec![
                        IoVec::from(vec![0; length as usize]), // data
                        IoVec::from(vec![0; U32_SIZE]),        // crc of data
                        IoVec::from(vec![0; U64_SIZE]),        // length
                        IoVec::from(vec![0; U32_SIZE]),        // crc of length
                    ];
                    buf_ref.offset += (U64_SIZE + U32_SIZE) as u64;

                    let read_e = buf_ref.build_readv_entry(buf_idx as _);
                    pending.push(read_e);
                } else {
                    let data_buf = Vec::from(buf_ref.io_vecs[0]);
                    if check_integrity {
                        let masked_crc_buf = buf_ref.io_vecs[1].as_slice().try_into().unwrap();
                        let masked_crc = u32::from_le_bytes(masked_crc_buf);
                        assert!(verify_masked_crc(&data_buf, masked_crc).is_ok());
                    }

                    let data_length = data_buf.len();

                    // Pass the buffer out
                    cb(data_buf);

                    let length_buf = buf_ref.io_vecs[2].as_slice().try_into().unwrap();
                    let length = u64::from_le_bytes(length_buf);

                    if check_integrity {
                        let masked_crc_buf = buf_ref.io_vecs[3].as_slice().try_into().unwrap();
                        let masked_crc = u32::from_le_bytes(masked_crc_buf);
                        assert!(verify_masked_crc(&length_buf, masked_crc).is_ok());
                    }

                    // Reset data buffer
                    buf_ref.io_vecs[0] = IoVec::from(vec![0; length as usize]);
                    buf_ref.offset += (data_length + U32_SIZE + U64_SIZE + U32_SIZE) as u64;

                    let read_e = buf_ref.build_readv_entry(buf_idx as _);
                    pending.push(read_e);
                }
            }
            num_reads -= 1;
        }

        // Will submit at the beginning
        for read_e in pending.drain(..) {
            unsafe {
                ring.submission().push(&read_e)?;
            }
            num_reads += 1;
        }

        if num_reads == 0 {
            break;
        }
    }

    Ok(())
}
