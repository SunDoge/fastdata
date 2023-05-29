use std::{fs::File, os::fd::AsRawFd, path::Path};

use crate::{
    constants::{U32_SIZE, U64_SIZE},
    crc32c::verify_masked_crc,
    error::Result,
    indexing::sync_reader::IndexReader,
    utils::IoVec,
};
use io_uring::{opcode, types, IoUring};
use memmap2::Mmap;
use slab::Slab;

use crate::indexing::sync_reader::MmapIndexReader;

pub struct AsyncRandomReader {
    file: File,
    index: Mmap,
    ring: IoUring,
}

// impl AsyncRandomReader {
//     pub fn new<P: AsRef<Path>>(path: P, index_path: Option<P>, queue_depth: u32) -> Result<Self> {
//         let index_path = index_path.unwrap_or_else(|| path.as_ref().with_extension("tfrecord.idx"));
//         let file = File::open(path)?;
//         let index_file = File::open(&index_path)?;
//         let index = unsafe { Mmap::map(&index_file).unwrap() };
//         let ring = IoUring::new(queue_depth)?;

//         Ok(Self { file, index, ring })
//     }
// }

#[derive(Debug)]
pub struct Buffer {
    pub io_vecs: Vec<IoVec>,
}

impl Buffer {
    pub fn build_readv_entry(
        &self,
        file: &File,
        offset: u64,
        user_data: u64,
    ) -> io_uring::squeue::Entry {
        opcode::Readv::new(
            types::Fd(file.as_raw_fd()),
            self.io_vecs.as_ptr() as *const _,
            self.io_vecs.len() as _,
        )
        .offset(offset)
        .build()
        .user_data(user_data)
    }

    pub fn total_length(&self) -> usize {
        self.io_vecs.iter().map(|v| v.iov_len).sum()
    }
}

pub fn io_uring_loop<P, F>(
    path: P,
    index_path: Option<P>,
    queue_depth: u32,
    check_integrity: bool,
    cb: F,
) -> Result<()>
where
    P: AsRef<Path>,
    F: Fn(Vec<u8>),
{
    let index_path = index_path
        .map(|p| p.as_ref().to_owned())
        .unwrap_or_else(|| path.as_ref().with_extension("tfrecord.idx"));
    let file = File::open(path)?;
    let index_reader = IndexReader::open(&index_path)?;
    let mut ring = IoUring::new(queue_depth)?;

    let mut index_iter = index_reader.into_iter();

    let mut num_reads = 0;
    let max_reads = queue_depth as usize;

    let mut pending = Vec::new();
    let mut buffers: Slab<_> = Slab::with_capacity(max_reads);

    for _ in 0..max_reads {
        if let Some((offset, length)) = index_iter.next() {
            let buffer = Buffer {
                io_vecs: vec![
                    IoVec::from(vec![0; U64_SIZE]), // length
                    IoVec::from(vec![0; U32_SIZE]), // crc_of_length
                    IoVec::from(vec![0; length as usize - U32_SIZE * 2 - U64_SIZE]), // data
                    IoVec::from(vec![0; U32_SIZE]), // crc_of_data
                ],
            };
            let buf_idx = buffers.insert(buffer);
            let buf_ref = &mut buffers[buf_idx];
            let read_e = buf_ref.build_readv_entry(&file, offset, buf_idx as _);
            pending.push(read_e);
        } else {
            break;
        }
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
            let _bytes_read = cqe.result() as usize;

            let buf_ref = &mut buffers[buf_idx];

            // assert!(bytes_read == buf_ref.total_length());

            if check_integrity {
                let masked_crc_of_length_buf = buf_ref.io_vecs[1]
                    .as_slice()
                    .try_into()
                    .expect("fail to convert to array");
                let masked_crc_of_length = u32::from_le_bytes(masked_crc_of_length_buf);
                let length_buf = buf_ref.io_vecs[0].as_slice();
                assert!(verify_masked_crc(length_buf, masked_crc_of_length).is_ok());

                let masked_crc_of_data_buf = buf_ref.io_vecs[3]
                    .as_slice()
                    .try_into()
                    .expect("fail to convert to array");
                let masked_crc_of_data = u32::from_le_bytes(masked_crc_of_data_buf);
                let data_buf = buf_ref.io_vecs[2].as_slice();
                assert!(verify_masked_crc(data_buf, masked_crc_of_data).is_ok());
            }

            // TODO: unsafe, please use other way
            let data_buf = Vec::from(buf_ref.io_vecs[2]);
            cb(data_buf);

            if let Some((offset, length)) = index_iter.next() {
                buf_ref.io_vecs[2] =
                    IoVec::from(vec![0; length as usize - U32_SIZE * 2 - U64_SIZE]);
                let read_e = buf_ref.build_readv_entry(&file, offset, buf_idx as _);
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

        if num_reads == 0 {
            break;
        }
    }

    Ok(())
}
