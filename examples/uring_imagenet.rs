use std::{collections::VecDeque, io::Read, os::fd::AsRawFd, path::PathBuf, time::Instant};

use io_uring::{opcode, types, IoUring};
use rayon::prelude::*;
use slab::Slab;

const QUEUE_DEPTH: usize = 32;
const BUFFER_SIZE: usize = 16 * 1024;

fn main() {
    // let tfrecords = glob::glob("/home/denghuang/datasets/imagenet-tfrec/val/*.tfrecord").unwrap();
    let tfrecords =
        glob::glob("/mnt/cephfs/home/chenyaofo/datasets/imagenet-tfrec/train/*.tfrecord").unwrap();
    let filenames: Vec<_> = tfrecords.map(|p| p.unwrap()).collect();

    let start_time = Instant::now();
    let num_blocks = seq_read(&filenames);
    dbg!(start_time.elapsed(), num_blocks);

    // let start_time = Instant::now();
    // let num_blocks = iouring_read(&filenames);
    // dbg!(start_time.elapsed(), num_blocks);
}

fn seq_read(filenames: &[PathBuf]) -> usize {
    filenames
        .par_iter()
        .map(|p| {
            let file = std::fs::File::open(p).unwrap();
            let mut reader = std::io::BufReader::new(file);
            let mut buf = vec![0; BUFFER_SIZE];

            let mut num_buffers = 0;
            loop {
                let bytes_read = reader.read(&mut buf).unwrap();
                if bytes_read == 0 {
                    break;
                }
                num_buffers += 1;
            }

            println!("finish: {}", p.display());
            num_buffers
        })
        .sum()
}

struct Buffer {
    pub fd: std::fs::File,
    pub buf: Vec<u8>,
    pub offset: u64,
}

fn iouring_read(filenames: &[PathBuf]) -> usize {
    // let mut fds: Slab<i32> = filenames
    //     .iter()
    //     .map(|p| std::fs::File::open(p).unwrap().as_raw_fd())
    //     .fold(Slab::new(), |mut sl, fd| {
    //         sl.insert(fd);
    //         sl
    //     });
    let mut buffers = Slab::new();
    let mut read_queue = VecDeque::new();
    for filename in filenames {
        let fd = std::fs::File::open(filename).unwrap();
        let buf = vec![0; BUFFER_SIZE];
        let buf_idx = buffers.insert(Buffer { fd, buf, offset: 0 });

        let buf_ref = &mut buffers[buf_idx];
        let read_e = opcode::Read::new(
            types::Fd(buf_ref.fd.as_raw_fd()),
            buf_ref.buf.as_mut_ptr(),
            buf_ref.buf.len() as _,
        )
        .offset(buf_ref.offset)
        .build()
        .user_data(buf_idx as u64);
        read_queue.push_back(read_e);
    }

    println!("read queue size: {}", read_queue.len());

    let mut ring = IoUring::new(QUEUE_DEPTH as u32).unwrap();

    let mut num_blocks = 0;
    let mut num_reads = 0;

    let n = read_queue.len().min(QUEUE_DEPTH);
    for _ in 0..n {
        let read_e = read_queue.pop_front().unwrap();
        unsafe {
            ring.submission().push(&read_e).unwrap();
        }

        num_reads += 1;
    }
    dbg!(num_reads);
    println!("read queue size: {}", read_queue.len());

    ring.submit_and_wait(1).unwrap();

    println!("start loop");
    while num_reads > 0 && !buffers.is_empty() {
        for cqe in ring.completion() {
            assert!(cqe.result() >= 0);
            let buf_idx = cqe.user_data() as usize;
            let bytes_read = cqe.result();
            if bytes_read == 0 {
                let buffer = buffers.remove(buf_idx);
                println!("finish: {:?}", buffer.fd);
                num_reads -= 1;
            } else {
                num_blocks += 1;
                buffers[buf_idx].buf = vec![0; BUFFER_SIZE];
                buffers[buf_idx].offset += bytes_read as u64;

                let buf_ref = &mut buffers[buf_idx];

                let read_e = opcode::Read::new(
                    types::Fd(buf_ref.fd.as_raw_fd()),
                    buf_ref.buf.as_mut_ptr(),
                    buf_ref.buf.len() as _,
                )
                .offset(buf_ref.offset)
                .build()
                .user_data(buf_idx as u64);
                read_queue.push_back(read_e);
                num_reads -= 1;
                // println!("add back read with buf_idx: {}", buf_idx);
            }
        }

        let n = read_queue.len().min(QUEUE_DEPTH - num_reads);

        for _ in 0..n {
            let read_e = read_queue.pop_front().unwrap();
            unsafe {
                ring.submission().push(&read_e).unwrap();
            }
            num_reads += 1;
        }

        if num_reads == 0 {
            break;
        }

        // ring.submit_and_wait(1).unwrap();
        ring.submit().unwrap();
        // dbg!(num_reads);
    }

    num_blocks
}
