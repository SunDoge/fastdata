use std::mem::ManuallyDrop;

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct IoVec {
    pub iov_base: *mut std::ffi::c_void,
    pub iov_len: usize,
}

unsafe impl Send for IoVec {}

impl IoVec {
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.iov_base as *const _, self.iov_len) }
    }
}

impl From<Vec<u8>> for IoVec {
    fn from(value: Vec<u8>) -> Self {
        let value = ManuallyDrop::new(value);
        Self {
            iov_base: value.as_ptr() as *mut _,
            iov_len: value.len(),
        }
    }
}

impl<const N: usize> From<[u8; N]> for IoVec {
    fn from(value: [u8; N]) -> Self {
        let value = ManuallyDrop::new(value);
        Self {
            iov_base: value.as_ptr() as *mut _,
            iov_len: value.len(),
        }
    }
}

impl From<IoVec> for Vec<u8> {
    // TODO: this is really unsafe, don't use it
    fn from(value: IoVec) -> Self {
        unsafe { Vec::from_raw_parts(value.iov_base as *mut _, value.iov_len, value.iov_len) }
    }
}

impl From<IoVec> for Box<[u8]> {
    fn from(value: IoVec) -> Self {
        unsafe {
            let v = std::slice::from_raw_parts(value.iov_base as *const u8, value.iov_len);
            Box::from_raw(v as *const [u8] as *mut [u8])
        }
    }
}

// impl From<IoVec> for Box<[u8]> {
//     fn from(value: IoVec) -> Self {
//         unsafe {
//             // let slice = std::slice::from_raw_parts(value.iov_base as *const u8, value.iov_len);
//             // // Box::from_raw(slice.as_ptr() as *const [u8] as *mut [u8])
//             let v = Vec::from_raw_parts(value.iov_base as *mut _, value.iov_len, value.iov_len);
//             v.into_boxed_slice()
//         }
//     }
// }

// impl From<IoVec> for &[u8] {
//     fn from(value: IoVec) -> Self {
//         unsafe { std::slice::from_raw_parts(value.iov_base as *const _, value.iov_len) }
//     }
// }
