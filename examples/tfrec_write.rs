use fastdata::{error::Result, writers::tfrecord::TfRecordWriter};

fn main() {
    let mut writer = TfRecordWriter::create("ints.tfrecord").expect("fail to open");

    for i in 0..10000 {
        let data: Vec<u8> = vec![100; i as usize];

        writer.write(&data).expect("fail to write");
    }

    writer.flush().unwrap();
}
