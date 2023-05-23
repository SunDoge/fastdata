/// LINT.IfChange
/// Containers to hold repeated fundamental values.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BytesList {
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub value: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FloatList {
    #[prost(float, repeated, tag = "1")]
    pub value: ::prost::alloc::vec::Vec<f32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Int64List {
    #[prost(int64, repeated, tag = "1")]
    pub value: ::prost::alloc::vec::Vec<i64>,
}
/// Containers for non-sequential data.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Feature {
    /// Each feature can be exactly one kind.
    #[prost(oneof = "feature::Kind", tags = "1, 2, 3")]
    pub kind: ::core::option::Option<feature::Kind>,
}
/// Nested message and enum types in `Feature`.
pub mod feature {
    /// Each feature can be exactly one kind.
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Kind {
        #[prost(message, tag = "1")]
        BytesList(super::BytesList),
        #[prost(message, tag = "2")]
        FloatList(super::FloatList),
        #[prost(message, tag = "3")]
        Int64List(super::Int64List),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Features {
    /// Map from feature name to feature.
    #[prost(map = "string, message", tag = "1")]
    pub feature: ::std::collections::HashMap<::prost::alloc::string::String, Feature>,
}
/// Containers for sequential data.
///
/// A FeatureList contains lists of Features.  These may hold zero or more
/// Feature values.
///
/// FeatureLists are organized into categories by name.  The FeatureLists message
/// contains the mapping from name to FeatureList.
///
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FeatureList {
    #[prost(message, repeated, tag = "1")]
    pub feature: ::prost::alloc::vec::Vec<Feature>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FeatureLists {
    /// Map from feature name to feature list.
    #[prost(map = "string, message", tag = "1")]
    pub feature_list: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        FeatureList,
    >,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Example {
    #[prost(message, optional, tag = "1")]
    pub features: ::core::option::Option<Features>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SequenceExample {
    #[prost(message, optional, tag = "1")]
    pub context: ::core::option::Option<Features>,
    #[prost(message, optional, tag = "2")]
    pub feature_lists: ::core::option::Option<FeatureLists>,
}
