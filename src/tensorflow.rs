

include!(concat!(env!("OUT_DIR"), "/tensorflow.rs"));

impl Example {
    pub fn get_bytes_list<'a>(&'a self, key: &str) -> Vec<&'a [u8]> {
        let feat = self.features.as_ref().unwrap().feature.get(key).unwrap();
        match feat.kind {
            Some(feature::Kind::BytesList(ref list)) => {
                list.value.iter().map(|v| v.as_slice()).collect::<Vec<_>>()
            }
            _ => unreachable!(),
        }
    }

    pub fn get_float_list<'a>(&'a self, key: &str) -> &'a [f32] {
        let feat = self.features.as_ref().unwrap().feature.get(key).unwrap();
        match feat.kind {
            Some(feature::Kind::FloatList(ref list)) => list.value.as_slice(),
            _ => unreachable!(),
        }
    }

    pub fn get_int64_list<'a>(&'a self, key: &str) -> &'a [i64] {
        let feat = self.features.as_ref().unwrap().feature.get(key).unwrap();
        match feat.kind {
            Some(feature::Kind::Int64List(ref list)) => list.value.as_slice(),
            _ => unreachable!(),
        }
    }
}

impl From<Vec<f32>> for Feature {
    fn from(value: Vec<f32>) -> Self {
        Self {
            kind: Some(feature::Kind::FloatList(FloatList { value })),
        }
    }
}

impl From<Vec<i64>> for Feature {
    fn from(value: Vec<i64>) -> Self {
        Self {
            kind: Some(feature::Kind::Int64List(Int64List { value })),
        }
    }
}

impl From<Vec<Vec<u8>>> for Feature {
    fn from(value: Vec<Vec<u8>>) -> Self {
        Self {
            kind: Some(feature::Kind::BytesList(BytesList { value })),
        }
    }
}
