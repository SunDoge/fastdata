use image::{imageops::FilterType, DynamicImage};

#[derive(Debug, Clone, Copy)]
pub struct SmallestMaxSize {
    pub max_size: u32,
    pub filter: FilterType,
}

impl SmallestMaxSize {
    pub fn apply(&self, img: &DynamicImage) -> DynamicImage {
        let scale = self.max_size as f64 / img.width().min(img.height()) as f64;
        let nwidth = img.width() as f64 * scale;
        let nheight = img.height() as f64 * scale;
        img.resize_exact(nwidth as u32, nheight as u32, self.filter)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CenterCrop {
    pub width: u32,
    pub height: u32,
}

impl CenterCrop {
    pub fn apply(&self, img: &DynamicImage) -> DynamicImage {
        let left = img.width() / 2 - self.width / 2;
        let top = img.height() / 2 - self.height / 2;
        img.crop_imm(left, top, self.width, self.height)
    }
}
