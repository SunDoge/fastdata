use std::{io::Cursor, time::Instant};

use crossbeam_channel::bounded;
use fastdata::ops::shuffler::Shuffle;
use fastdata::{error::Result, readers::tfrecord::TfRecordReader};
use image::{ImageFormat, RgbImage};
use prost::Message;
use rayon::prelude::{ParallelBridge, ParallelIterator};

fn read_image_from_buffer(buf: &[u8]) {
    let format = image::guess_format(buf).unwrap();
    match format {
        ImageFormat::Jpeg => {
            let mut decoder = zune_jpeg::JpegDecoder::new(buf);
            let img = decoder.decode().map_err(|err| {
                image::load_from_memory_with_format(buf, format).unwrap();
                println!("err {:?}", err);
            });
        }
        ImageFormat::Png => {
            let mut decoder = zune_png::PngDecoder::new(buf);
            let img = decoder.decode().map_err(|err| {
                image::load_from_memory_with_format(buf, format).unwrap();
                println!("err {:?}", err);
            });
        }
        _ => {
            image::load_from_memory_with_format(buf, format).unwrap();
        }
    }
}

fn main() {
    rayon::ThreadPoolBuilder::new()
        .num_threads(64)
        .build_global()
        .unwrap();

    let (sender, receiver) = bounded(1024 * 1024 * 10);

    let tfrecords =
        glob::glob("/mnt/cephfs/home/chenyaofo/datasets/imagenet-tfrec/val/*.tfrecord").unwrap();

    rayon::spawn(move || {
        tfrecords
            // .take(10)
            .flat_map(|path| {
                let path = path.unwrap();
                println!("tfrecord: {}", path.display());
                let reader = TfRecordReader::open(&path).expect("fail to open");
                reader
            })
            .for_each(|buf| {
                sender.send(buf.unwrap()).unwrap();
            })
    });

    let start_time = Instant::now();
    let num_records = receiver
        .iter()
        .par_bridge()
        .map(|buf| {
            let example = fastdata::tensorflow::Example::decode(&mut Cursor::new(buf)).unwrap();
            let image_bytes = example.get_bytes_list("image")[0];
            let label = example.get_int64_list("label")[0];

            read_image_from_buffer(image_bytes);

            // let img = aug.apply(&img).unwrap();
            // let image_buffer = img.image_write_to_memory();
            label
        })
        .count();

    let rate = num_records as f64 / start_time.elapsed().as_secs_f64();
    println!("rate: {rate} record/s");
}
