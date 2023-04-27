use fastdata::readers::tfrecord::TfRecordReader;

fn main() {
    let mut reader = TfRecordReader::open(
        "/mnt/cephfs/home/chenyaofo/datasets/imagenet-tfrec/val/imagenet-1k-val-000100.tfrecord",
    )
    .expect("fail to open");
    reader.set_check_integrity(true);

    for (i, data) in reader.iter().unwrap().enumerate() {
        let buf = data.unwrap();
        println!("index {} length {}", i, buf.len());
    }
}
