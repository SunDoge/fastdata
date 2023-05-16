use pyo3::prelude::*;

#[pyclass]
pub struct MyIterator {
    pub iter: Box<dyn Iterator<Item = PyObject> + Send>,
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
