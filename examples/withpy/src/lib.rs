use std::io::Cursor;

use crossbeam_channel::bounded;
use dlpark::tensor::TensorWrapper;
use fastdata::{python::MyIterator, readers::tfrecord::TfRecordReader, tensorflow::Example};
use prost::Message;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use rayon::{prelude::*, ThreadPoolBuilder};

#[pyfunction]
pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[pyfunction]
pub fn make_record_dataset(root: &str, num_threads: usize) -> MyIterator {
    // let it = (0..10).map(|x| Python::with_gil(|py| x.to_object(py)));

    let thread_pool = ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .unwrap();

    let (sender, receiver) = bounded(32);

    let root_string = root.to_string();
    let _iter = thread_pool.spawn(move || {
        let record = TfRecordReader::open(&root_string).unwrap();
        println!("start loop");

        let (record_sender, record_receiver) = bounded(1024);

        rayon::spawn(move || {
            record.for_each(|buf| {
                record_sender.send(buf).unwrap();
            });
        });

        record_receiver
            .into_iter()
            .par_bridge()
            .map(|buf| {
                let example = Example::decode(&mut Cursor::new(buf.unwrap())).unwrap();
                let image_bytes = example.get_bytes_list("image")[0];
                let label = example.get_int64_list("label")[0];
                let img = image::load_from_memory(image_bytes).unwrap();
                let resized = img.resize(224, 224, image::imageops::FilterType::Triangle);
                let rgb_img_vec = resized.to_rgb8().to_vec();
                (rgb_img_vec, label)
            })
            .for_each_with(sender, |sender, (img_vec, label)| {
                sender.send((img_vec, label)).unwrap();
            });
    });

    let it: Box<dyn Iterator<Item = PyObject> + Send> =
        Box::new(receiver.into_iter().map(|(img_vec, label)| {
            Python::with_gil(|py| {
                let dic = PyDict::new(py);
                dic.set_item("image", TensorWrapper::from(img_vec).into_py(py))
                    .unwrap();
                dic.set_item("label", label).unwrap();
                dic.into()
            })
        }));

    MyIterator { iter: it }
}

#[pymodule]
fn mylib(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(add, m)?)?;
    m.add_function(wrap_pyfunction!(make_record_dataset, m)?)?;
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
