include!(concat!(env!("OUT_DIR"), "/tensorflow.rs"));

pub fn get_bytes_list<'a>(example: &'a Example, key: &str) -> Vec<&'a [u8]> {
    let feat = example.features.as_ref().unwrap().feature.get(key).unwrap();
    match feat.kind {
        Some(feature::Kind::BytesList(ref list)) => {
            list.value.iter().map(|v| v.as_slice()).collect::<Vec<_>>()
        }
        _ => unreachable!(),
    }
}

pub fn get_float_list<'a>(example: &'a Example, key: &str) -> &'a [f32] {
    let feat = example.features.as_ref().unwrap().feature.get(key).unwrap();
    match feat.kind {
        Some(feature::Kind::FloatList(ref list)) => list.value.as_slice(),
        _ => unreachable!(),
    }
}

pub fn get_int64_list<'a>(example: &'a Example, key: &str) -> &'a [i64] {
    let feat = example.features.as_ref().unwrap().feature.get(key).unwrap();
    match feat.kind {
        Some(feature::Kind::Int64List(ref list)) => list.value.as_slice(),
        _ => unreachable!(),
    }
}
