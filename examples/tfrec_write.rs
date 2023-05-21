use bytes::Buf;
use fastdata::{
    error::Result,
    tensorflow::{BytesList, Example, Feature},
    writers::tfrecord::TfrecordWriter,
};
use prost::Message;

fn main() {
    let mut writer = TfrecordWriter::create("ints.tfrecord").expect("fail to open");

    for i in 0..10000 {
        let data: Vec<u8> = vec![100; i as usize];
        let bytes_feature = Feature::from(data);
        let example = Example::from([("data", bytes_feature)]);
        let buf = example.encode_to_vec();
        writer.write(&buf).expect("fail to write");
    }

    writer.flush().unwrap();
}
