use dlpark::{prelude::*, tensor::traits::HasStrides};
use opencv::imgproc;
use opencv::prelude::*;

pub struct PyMat(pub Mat);

impl HasData for PyMat {
    fn data(&self) -> *mut std::ffi::c_void {
        self.0.data() as *const std::ffi::c_void as *mut _
    }
}

impl HasDevice for PyMat {
    fn device(&self) -> Device {
        Device::CPU
    }
}

impl HasDtype for PyMat {
    fn dtype(&self) -> dlpark::ffi::DataType {
        dlpark::ffi::DataType::U8
    }
}

impl HasShape for PyMat {
    fn shape(&self) -> Shape {
        Shape::Owned(
            [self.0.cols(), self.0.rows(), self.0.channels()]
                .map(|x| x as i64)
                .to_vec(),
        )
    }
}

impl HasByteOffset for PyMat {
    fn byte_offset(&self) -> u64 {
        0
    }
}
impl HasStrides for PyMat {}

#[derive(Debug, Clone)]
pub struct SmallestMaxSize {
    pub max_size: u32,
    pub interpolation: i32,
    pub out: Mat,
}

impl Default for SmallestMaxSize {
    fn default() -> Self {
        Self {
            max_size: 256,
            interpolation: imgproc::INTER_LINEAR,
            out: Mat::default(),
        }
    }
}

impl SmallestMaxSize {
    pub fn new(max_size: u32, interpolation: i32) -> Self {
        Self {
            max_size,
            interpolation,
            out: Mat::default(),
        }
    }

    pub fn apply(&mut self, img: &Mat) -> Mat {
        let scale = self.max_size as f64 / img.cols().min(img.rows()) as f64;
        imgproc::resize(
            img,
            &mut self.out,
            Default::default(),
            scale,
            scale,
            self.interpolation,
        )
        .unwrap();
        self.out.clone()
    }
}

#[derive(Debug, Clone)]
pub struct CenterCrop {
    pub width: i32,
    pub height: i32,
}

impl Default for CenterCrop {
    fn default() -> Self {
        Self {
            width: 224,
            height: 224,
        }
    }
}

impl CenterCrop {
    pub fn apply(&self, img: &Mat) -> Mat {
        let left = img.cols() / 2 - self.width / 2;
        let top = img.rows() / 2 - self.height / 2;
        let roi = opencv::core::Rect::new(left, top, self.width, self.height);
        Mat::roi(img, roi).unwrap()
    }
}

#[derive(Debug, Clone)]
pub struct BgrToRgb {
    pub out: Mat,
}

impl Default for BgrToRgb {
    fn default() -> Self {
        Self {
            out: Mat::default(),
        }
    }
}

impl BgrToRgb {
    pub fn apply(&mut self, img: &Mat) -> Mat {
        opencv::imgproc::cvt_color(img, &mut self.out, opencv::imgproc::COLOR_BGR2RGB, 0).unwrap();
        self.out.clone()
    }
}
