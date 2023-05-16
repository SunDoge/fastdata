use std::{io::Cursor, time::Instant};

use crossbeam_channel::bounded;

use fastdata::ops::image::opencv::{BgrToRgb, CenterCrop, SmallestMaxSize};
use fastdata::ops::shuffler::Shuffle;
use fastdata::{error::Result, readers::tfrecord::TfRecordReader};

use opencv::prelude::*;
use prost::Message;
use rayon::prelude::*;
use rayon::prelude::{ParallelBridge, ParallelIterator};

#[derive(Debug, Clone, Default)]
struct Aug {
    convert_color: BgrToRgb,
    resize: SmallestMaxSize,
    crop: CenterCrop,
}

impl Aug {
    pub fn apply(&mut self, img: &Mat) -> Mat {
        let img = self.convert_color.apply(img);
        let img = self.resize.apply(&img);
        let img = self.crop.apply(&img);
        img
    }
}

fn main() {
    rayon::ThreadPoolBuilder::new()
        .num_threads(32)
        .build_global()
        .unwrap();

    // let (sender, receiver) = bounded(1024 * 1024 * 10);
    // let pattern = "/mnt/cephfs/home/chenyaofo/datasets/imagenet-tfrec/val/*.tfrecord";
    let pattern = "/mnt/ssd/chenyf/val/*.tfrecord";
    let tfrecords: Vec<_> = glob::glob(pattern).unwrap().collect();

    opencv::core::set_num_threads(0).unwrap();
    println!(
        "use optimization {}",
        opencv::core::use_optimized().unwrap()
    );

    // rayon::spawn(move || {
    //     tfrecords
    //         // .take(10)
    //         .collect::<Vec<_>>()
    //         .()
    //         .flat_map(|path| {
    //             let path = path.unwrap();
    //             println!("tfrecord: {}", path.display());
    //             let reader = TfRecordReader::open(&path).expect("fail to open");
    //             reader
    //         })
    //         .for_each(|buf| {
    //             sender.send(buf.unwrap()).unwrap();
    //         })
    // });

    let start_time = Instant::now();

    let num_records: usize = tfrecords
        .par_iter()
        .flat_map(|path| {
            let path = path.as_ref().unwrap();
            println!("tfrecord: {}", path.display());
            let reader = TfRecordReader::open(&path).expect("fail to open");
            reader.par_bridge().map(|buf| {
                let example =
                    fastdata::tensorflow::Example::decode(&mut Cursor::new(buf.unwrap())).unwrap();
                // let image_bytes = example.get_bytes_list("image")[0];
                // let label = example.get_int64_list("label")[0];

                // let img = aug.apply(&img).unwrap();
                // let image_buffer = img.image_write_to_memory();
                // label
                example
            })
        })
        .map_with(Aug::default(), |aug, example| {
            let image_bytes = example.get_bytes_list("image")[0];
            let label = example.get_int64_list("label")[0];

            let img_buf = Mat::from_slice(image_bytes).unwrap();
            let img =
                opencv::imgcodecs::imdecode(&img_buf, opencv::imgcodecs::IMREAD_COLOR).unwrap();

            let img = aug.apply(&img);

            (img, label)
        })
        .count();

    let rate = num_records as f64 / start_time.elapsed().as_secs_f64();
    println!("rate: {rate} record/s records {num_records}");
}
