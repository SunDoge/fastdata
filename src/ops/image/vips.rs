use libvips::{
    ops::{ExtractBandOptions, ResizeOptions, ScaleOptions, ThumbnailImageOptions},
    VipsImage,
};

use crate::error::{Error, Result};

pub fn resize_pixel(
    img: &VipsImage,
    width: u32,
    height: u32,
    kernel: libvips::ops::Kernel,
) -> Result<VipsImage> {
    let original_width = img.get_width();
    let original_height = img.get_height();
    let hscale = width as f64 / original_width as f64;
    let vscale = height as f64 / original_height as f64;
    libvips::ops::resize_with_opts(
        img,
        hscale,
        &ResizeOptions {
            vscale,
            kernel,
            ..Default::default()
        },
    )
    .map_err(Error::VipsError)
}

/// hscale for height, vscale for width
pub fn resize_scale(
    img: &VipsImage,
    hscale: f64,
    vscale: f64,
    kernel: libvips::ops::Kernel,
) -> Result<VipsImage> {
    libvips::ops::resize_with_opts(
        img,
        hscale,
        &ResizeOptions {
            vscale,
            kernel,
            ..Default::default()
        },
    )
    .map_err(Error::VipsError)
}

pub fn crop(img: &VipsImage, left: i32, top: i32, width: i32, height: i32) -> Result<VipsImage> {
    libvips::ops::extract_area(img, left, top, width, height).map_err(Error::VipsError)
}

pub fn convert_to_rgb(img: &VipsImage) -> Result<VipsImage> {
    if img.image_hasalpha() {
        let img_no_alpha = remove_alpha(img)?;
        libvips::ops::colourspace(&img_no_alpha, libvips::ops::Interpretation::Srgb)
            .map_err(Error::VipsError)
    } else {
        libvips::ops::colourspace(img, libvips::ops::Interpretation::Srgb).map_err(Error::VipsError)
    }
}

pub fn remove_alpha(img: &VipsImage) -> Result<VipsImage> {
    libvips::ops::extract_band_with_opts(
        img,
        0,
        &ExtractBandOptions {
            n: img.get_bands() - 1,
        },
    )
    .map_err(Error::VipsError)
}

#[derive(Debug, Clone, Copy)]
pub struct SmallestMaxSize {
    pub max_size: u32,
    pub kernel: libvips::ops::Kernel,
}

impl SmallestMaxSize {
    pub fn apply(&self, img: &VipsImage) -> Result<VipsImage> {
        let scale = self.max_size as f64 / img.get_width().min(img.get_height()) as f64;
        resize_scale(img, scale, scale, self.kernel)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CenterCrop {
    pub width: i32,
    pub height: i32,
}

impl CenterCrop {
    pub fn apply(&self, img: &VipsImage) -> Result<VipsImage> {
        let left = img.get_width() / 2 - self.width / 2;
        let top = img.get_height() / 2 - self.height / 2;
        crop(img, left, top, self.width, self.height)
    }
}
