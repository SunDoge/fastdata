use dlpark::prelude::*;
use fastdata::ops::image::opencv::{BgrToRgb, CenterCrop, PyMat, SmallestMaxSize};
use fastdata::readers::tfrecord::TfRecordReader;
use fastdata::utils::data_source::{DataSource, IntoDataSource};
use opencv::prelude::*;
use prost::Message;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use rayon::prelude::*;
use std::{io::Cursor, time::Instant};

use kanal::{bounded, unbounded};

#[derive(Debug, Clone, Default)]
struct Aug {
    convert_color: BgrToRgb,
    resize: SmallestMaxSize,
    crop: CenterCrop,
}

impl Aug {
    pub fn apply(&mut self, img: &Mat) -> Mat {
        let img = self.convert_color.apply(img);
        let img = self.resize.apply(&img);
        let img = self.crop.apply(&img);
        img
    }
}

#[pyfunction]
pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[pyfunction]
pub fn one_tfrecord(py: Python<'_>, pattern: &str, num_workers: usize) -> DataSource {
    // let pattern = "/mnt/ssd/chenyf/val/*.tfrecord";
    let tfrecords: Vec<_> = glob::glob(pattern).unwrap().collect();
    opencv::core::set_num_threads(0).unwrap();
    println!(
        "use optimization {}",
        opencv::core::use_optimized().unwrap()
    );

    rayon::ThreadPoolBuilder::new()
        .num_threads(num_workers)
        .build_global()
        .unwrap();

    let (sender, receiver) = bounded(10240);

    rayon::spawn(move || {
        tfrecords.par_iter().for_each(|path| {
            let path = path.as_ref().unwrap();
            // println!("tfrecord: {}", path.display());
            // let reader = TfRecordReader::open(&path).expect("fail to open");
            // reader
            (0..256)
                .par_bridge()
                .for_each_with(Aug::default(), |aug, buf| {
                    // let example =
                    //     fastdata::tensorflow::Example::decode(&mut Cursor::new(buf.unwrap()))
                    //         .unwrap();

                    // let image_bytes = example.get_bytes_list("image")[0];
                    // let label = example.get_int64_list("label")[0];

                    // let img_buf = Mat::from_slice(image_bytes).unwrap();
                    // let img =
                    //     opencv::imgcodecs::imdecode(&img_buf, opencv::imgcodecs::IMREAD_COLOR)
                    //         .unwrap();
                    let img = Mat::zeros(720, 840, opencv::core::CV_8U)
                        .unwrap()
                        .to_mat()
                        .unwrap();

                    let img = aug.apply(&img);
                    let label = buf;

                    sender.send((ManagerCtx::from(PyMat(img)), label)).unwrap();
                });
        });
        // .for_each_with((Aug::default(), sender), |(aug, sender), buf| {

        // });
    });

    receiver
        .map(|(img, label)| {
            Python::with_gil(|py| {
                let dic = PyDict::new(py);
                dic.set_item("image", img.into_py(py)).unwrap();
                // dic.set_item("image", ManagerCtx::from(PyMat(img)).into_py(py)).unwrap();
                dic.set_item("label", label).unwrap();
                dic.into_py(py)
            })
        })
        .data_source()
}

#[pyfunction]
fn pure_data(n: usize) -> DataSource {
    (0..n)
        .map(|x| {
            Python::with_gil(|py| {
                let dic = PyDict::new(py);
                dic.set_item(
                    "image",
                    ManagerCtx::from(vec![1.0f32; 3 * 224 * 224]).into_py(py),
                )
                .unwrap();
                dic.into_py(py)
            })
        })
        .data_source()
}

#[pymodule]
fn mylib(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(add, m)?)?;
    m.add_function(wrap_pyfunction!(one_tfrecord, m)?)?;
    m.add_function(wrap_pyfunction!(pure_data, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
