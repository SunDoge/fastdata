use crate::error::{Error, Result};
use crc::Crc;

const MASK_DELTA: u32 = 0xa282ead8;

#[inline]
pub fn get_masked_crc(buf: &[u8]) -> u32 {
    const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&crc::CRC_32_ISCSI);
    let crc = CASTAGNOLI.checksum(buf);
    let masked_crc = ((crc >> 15) | (crc << 17)).wrapping_add(MASK_DELTA);
    masked_crc
}

#[inline]
pub fn verify_masked_crc(buf: &[u8], expect: u32) -> Result<()> {
    let found = get_masked_crc(buf);
    if found == expect {
        Ok(())
    } else {
        Err(Error::ChecksumMismatch { found, expect })
    }
}
