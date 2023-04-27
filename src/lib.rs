pub mod error;
pub mod readers;
mod utils;
pub mod writers;

pub mod tensorflow {
    include!(concat!(env!("OUT_DIR"), "/tensorflow.rs"));
}

pub fn add(left: usize, right: usize) -> usize {
    left + right
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
