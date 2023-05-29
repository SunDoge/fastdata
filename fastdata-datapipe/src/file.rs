use std::path::Path;

pub fn list_with_glob<P: AsRef<Path>>(root: P, masks: &str) -> glob::Paths {
    let pattern = root.as_ref().join(masks);
    glob::glob(pattern.to_str().unwrap()).unwrap()
}
