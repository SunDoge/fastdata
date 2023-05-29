use crate::error::Result;
use prost::Message;

include!("proto/tensorflow.rs");

impl Example {
    pub fn get_bytes_list(&self, key: &str) -> Option<Vec<&[u8]>> {
        let feat = self.features.as_ref()?.feature.get(key)?;
        match feat.kind {
            Some(feature::Kind::BytesList(ref list)) => {
                Some(list.value.iter().map(|x| x.as_slice()).collect())
            }
            _ => None,
        }
    }

    pub fn get_float_list(&self, key: &str) -> Option<&[f32]> {
        let feat = self.features.as_ref()?.feature.get(key)?;
        match feat.kind {
            Some(feature::Kind::FloatList(ref list)) => Some(list.value.as_slice()),
            _ => None,
        }
    }

    pub fn get_int64_list(&self, key: &str) -> Option<&[i64]> {
        let feat = self.features.as_ref()?.feature.get(key)?;
        match feat.kind {
            Some(feature::Kind::Int64List(ref list)) => Some(list.value.as_slice()),
            _ => None,
        }
    }

    pub fn take_bytes_list(&mut self, key: &str) -> Option<Vec<Vec<u8>>> {
        let feat = self.features.as_mut()?.feature.remove(key)?;
        match feat.kind {
            Some(feature::Kind::BytesList(BytesList { value })) => Some(value),
            _ => None
        }
    }
    
    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        Self::decode(std::io::Cursor::new(buf)).map_err(Into::into)
    }
}

impl From<&[f32]> for Feature {
    fn from(value: &[f32]) -> Self {
        Self {
            kind: Some(feature::Kind::FloatList(FloatList {
                value: value.to_vec(),
            })),
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

impl From<&[i64]> for Feature {
    fn from(value: &[i64]) -> Self {
        Self {
            kind: Some(feature::Kind::Int64List(Int64List {
                value: value.to_vec(),
            })),
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

impl From<&[&[u8]]> for Feature {
    fn from(value: &[&[u8]]) -> Self {
        Self {
            kind: Some(feature::Kind::BytesList(BytesList {
                value: value.iter().map(|x| x.to_vec()).collect(),
            })),
        }
    }
}

impl From<&[u8]> for Feature {
    fn from(value: &[u8]) -> Self {
        Self {
            kind: Some(feature::Kind::BytesList(BytesList {
                value: vec![value.to_vec()],
            })),
        }
    }
}

impl From<Vec<u8>> for Feature {
    fn from(value: Vec<u8>) -> Self {
        Self {
            kind: Some(feature::Kind::BytesList(BytesList { value: vec![value] })),
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

impl<const N: usize> From<[(String, Feature); N]> for Features {
    fn from(value: [(String, Feature); N]) -> Self {
        Self {
            feature: value.into(),
        }
    }
}

impl<const N: usize> From<[(&str, Feature); N]> for Features {
    fn from(value: [(&str, Feature); N]) -> Self {
        Self {
            feature: value.map(|(k, v)| (k.to_string(), v)).into(),
        }
    }
}

impl<const N: usize> From<[(String, Feature); N]> for Example {
    fn from(value: [(String, Feature); N]) -> Self {
        Self {
            features: Some(Features::from(value)),
        }
    }
}

impl<const N: usize> From<[(&str, Feature); N]> for Example {
    fn from(value: [(&str, Feature); N]) -> Self {
        Self {
            features: Some(Features::from(value)),
        }
    }
}
