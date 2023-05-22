use pyo3::prelude::*;
use pyo3::types::PyDict;

#[pyclass]
struct MyIterator {
    iter: Box<dyn Iterator<Item = PyObject> + Send>,
}

#[pymethods]
impl MyIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyObject> {
        slf.iter.next()
    }
}

#[pyfunction]
fn my_iter(py: Python<'_>, n: usize) -> MyIterator {
    let it = (0..n).map(|x| {
        let dic: PyObject = Python::with_gil(|py| {
            let dic = PyDict::new(py);
            dic.set_item("x", x).unwrap();
            dic.into()
        });
        dic
    });
    MyIterator { iter: Box::new(it) }
}

fn main() {}
