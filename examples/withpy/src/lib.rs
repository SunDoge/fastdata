use pyo3::prelude::*;


pub fn add(left: usize, right: usize) -> usize {
    left + right
}




#[pymodule]
fn mylib(_py: Python<'_>, m: &PyModule) -> PyResult<()> {

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
