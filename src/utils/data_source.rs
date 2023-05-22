use pyo3::prelude::*;

#[pyclass]
pub struct DataSource {
    pub iter: Box<dyn Iterator<Item = PyObject> + Send>,
}

impl DataSource {
    pub fn new(iter: impl Iterator<Item = PyObject> + Send + 'static) -> Self {
        Self {
            iter: Box::new(iter),
        }
    }
}

#[pymethods]
impl DataSource {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyObject> {
        slf.iter.next()
    }
}

pub trait IntoDataSource: Iterator<Item = PyObject> + Send + Sized + 'static {
    fn data_source(self) -> DataSource {
        DataSource::new(self)
    }
}

impl<T> IntoDataSource for T where T: Iterator<Item = PyObject> + Send + 'static {}
