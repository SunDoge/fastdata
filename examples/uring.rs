use std::{os::fd::AsRawFd, sync::Arc};

use crossbeam_channel::unbounded;

const BUFFER_SIZE: usize = 32;

fn main() {
    let (sender, receiver) = unbounded();
    let file_size = {
        let file = std::fs::File::open("build.rs").unwrap();
        let size = file.metadata().unwrap().len();
        drop(file);
        size
    };
    dbg!(file_size);

    let handle = std::thread::spawn(move || {
        let res = tokio_uring::start(async move {
            let mut offset = 0;
            let file = tokio_uring::fs::File::open("Cargo.lock").await.unwrap();
            // let mut handles = Vec::new();
            while offset < file_size {
                // let sender_cloned = sender.clone();
                // let handle = tokio_uring::spawn(async move {
                //     let file = tokio_uring::fs::File::open("Cargo.lock").await.unwrap();
                //     let buf = vec![0; BUFFER_SIZE];
                //     let (res, buf) = file.read_at(buf, offset).await;
                //     let bytes_read = res.unwrap();
                //     dbg!(bytes_read);
                //     sender_cloned.send((buf, bytes_read, offset)).unwrap();
                // });
                // handles.push(handle);

                let buf = vec![0; BUFFER_SIZE];
                let (res, buf) = file.read_at(buf, offset).await;
                let bytes_read = res.unwrap();
                dbg!(bytes_read);
                sender.send((buf, bytes_read, offset)).unwrap();
                offset += BUFFER_SIZE as u64;
            }
            // for handle in handles {
            //     handle.await.unwrap();
            // }
            1
        });
    });

    for (buf, bytes_read, offset) in receiver.iter() {
        dbg!(buf.len(), bytes_read, offset);
    }

    handle.join().unwrap();
}
