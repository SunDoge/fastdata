use std::io::Cursor;

use fastdata::readers::tfrecord::TfRecordReader;
use prost::Message;

fn main() {
    let mut reader = TfRecordReader::open(
        "/mnt/cephfs/home/chenyaofo/datasets/imagenet-tfrec/val/imagenet-1k-val-000100.tfrecord",
    )
    .expect("fail to open");
    reader.set_check_integrity(true);

    reader.iter().unwrap().for_each(|buf| {
        let example =
            fastdata::tensorflow::Example::decode(&mut Cursor::new(buf.unwrap())).unwrap();
        let image_bytes = example.get_bytes_list("image")[0];
        let label = example.get_int64_list("label")[0];
    });
}
