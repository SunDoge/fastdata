use opencv::prelude::*;

#[derive(Debug, Clone)]
pub struct SmallestMaxSize {
    pub max_size: u32,
    pub interpolation: i32,
    pub mat: Mat,
}

impl SmallestMaxSize {
    pub fn new(max_size: u32, interpolation: i32) -> Self {
        Self {
            max_size,
            interpolation,
            mat: Mat::default(),
        }
    }
}
