use fastdata::python::MyIterator;
use pyo3::prelude::*;
use pyo3::types::PyDict;

#[pyfunction]
pub fn add(left: usize, right: usize) -> usize {
    left + right
}

// #[pyfunction]
// pub fn make_imagenette_val(py: Python<'_>, root: &str, num_workers: usize) -> MyIterator {
//     MyIterator {
//         iter: Box::new(vec![1, 2, 3].iter().map(|x| x.into_py(py))),
//     }
// }

#[pymodule]
fn mylib(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(add, m)?)?;
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
