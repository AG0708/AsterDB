use crate::{Lsn, PageId, Result, StorageError, checksum::crc32};

pub const PAGE_SIZE: usize = 4096;
pub const PAGE_HEADER_SIZE: usize = 64;
pub const PAGE_SIZE_U16: u16 = 4096;
pub const PAGE_HEADER_SIZE_U16: u16 = 64;
pub const PAGE_MAGIC: [u8; 4] = *b"ASTP";
pub const PAGE_FORMAT_VERSION: u16 = 1;
const CHECKSUM_OFFSET: usize = 44;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PageKind {
    Uninitialized = 0,
    Superblock = 1,
    Heap = 2,
    BTreeLeaf = 3,
    BTreeInternal = 4,
    TreeDirectory = 5,
    FreeList = 6,
}

impl TryFrom<u8> for PageKind {
    type Error = StorageError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Uninitialized),
            1 => Ok(Self::Superblock),
            2 => Ok(Self::Heap),
            3 => Ok(Self::BTreeLeaf),
            4 => Ok(Self::BTreeInternal),
            5 => Ok(Self::TreeDirectory),
            6 => Ok(Self::FreeList),
            _ => Err(StorageError::InvalidPage(format!(
                "unknown page kind {value}"
            ))),
        }
    }
}

/// Parsed common header. The header is encoded manually and is never written
/// using Rust's memory representation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PageHeader {
    pub kind: PageKind,
    pub flags: u8,
    pub page_id: PageId,
    pub lsn: Lsn,
    /// Serialized Raft/state-machine apply index that produced this image.
    /// This is not a WAL byte offset; `lsn` and `page_epoch` have separate
    /// domains and must never be compared.
    pub page_epoch: u64,
    pub lower: u16,
    pub upper: u16,
    pub slot_count: u16,
}

#[derive(Clone, Eq, PartialEq)]
pub struct Page {
    bytes: [u8; PAGE_SIZE],
}

impl std::fmt::Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Page")
            .field("header", &self.header())
            .finish()
    }
}

impl Page {
    #[must_use]
    pub fn new(page_id: PageId, kind: PageKind) -> Self {
        let mut page = Self {
            bytes: [0; PAGE_SIZE],
        };
        page.bytes[0..4].copy_from_slice(&PAGE_MAGIC);
        page.put_u16(4, PAGE_FORMAT_VERSION);
        page.bytes[6] = kind as u8;
        page.put_u64(8, page_id.0);
        page.put_u16(32, PAGE_HEADER_SIZE_U16);
        page.put_u16(34, PAGE_SIZE_U16);
        page.seal();
        page
    }

    pub fn decode(bytes: [u8; PAGE_SIZE]) -> Result<Self> {
        let page = Self { bytes };
        page.validate()?;
        Ok(page)
    }

    pub fn validate(&self) -> Result<()> {
        if self.bytes[0..4] != PAGE_MAGIC {
            return Err(StorageError::InvalidPage("bad page magic".into()));
        }
        let version = self.get_u16(4);
        if version != PAGE_FORMAT_VERSION {
            return Err(StorageError::InvalidPage(format!(
                "unsupported page version {version}"
            )));
        }
        PageKind::try_from(self.bytes[6])?;
        if self.bytes[38..44].iter().any(|byte| *byte != 0)
            || self.bytes[48..PAGE_HEADER_SIZE]
                .iter()
                .any(|byte| *byte != 0)
        {
            return Err(StorageError::InvalidPage(
                "nonzero reserved page-header bytes".into(),
            ));
        }
        let lower = usize::from(self.get_u16(32));
        let upper = usize::from(self.get_u16(34));
        if !(PAGE_HEADER_SIZE..=upper).contains(&lower) || upper > PAGE_SIZE {
            return Err(StorageError::InvalidPage(format!(
                "invalid free-space bounds {lower}..{upper}"
            )));
        }
        let expected = self.get_u32(CHECKSUM_OFFSET);
        let actual = self.computed_checksum();
        if expected != actual {
            return Err(StorageError::ChecksumMismatch { expected, actual });
        }
        Ok(())
    }

    #[must_use]
    pub fn header(&self) -> PageHeader {
        PageHeader {
            kind: PageKind::try_from(self.bytes[6]).unwrap_or(PageKind::Uninitialized),
            flags: self.bytes[7],
            page_id: PageId(self.get_u64(8)),
            lsn: Lsn(self.get_u64(16)),
            page_epoch: self.get_u64(24),
            lower: self.get_u16(32),
            upper: self.get_u16(34),
            slot_count: self.get_u16(36),
        }
    }

    #[must_use]
    pub fn id(&self) -> PageId {
        PageId(self.get_u64(8))
    }

    #[must_use]
    pub fn kind(&self) -> PageKind {
        PageKind::try_from(self.bytes[6]).unwrap_or(PageKind::Uninitialized)
    }

    #[must_use]
    pub fn lsn(&self) -> Lsn {
        Lsn(self.get_u64(16))
    }

    pub fn set_lsn(&mut self, lsn: Lsn) {
        self.put_u64(16, lsn.0);
        self.seal();
    }

    pub fn set_page_epoch(&mut self, page_epoch: u64) {
        self.put_u64(24, page_epoch);
        self.seal();
    }

    pub fn set_flags(&mut self, flags: u8) {
        self.bytes[7] = flags;
        self.seal();
    }

    pub(crate) fn set_layout(&mut self, lower: u16, upper: u16, slot_count: u16) {
        self.put_u16(32, lower);
        self.put_u16(34, upper);
        self.put_u16(36, slot_count);
    }

    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.bytes[PAGE_HEADER_SIZE..]
    }

    pub(crate) fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.bytes[PAGE_HEADER_SIZE..]
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.bytes
    }

    #[must_use]
    pub fn into_bytes(mut self) -> [u8; PAGE_SIZE] {
        self.seal();
        self.bytes
    }

    pub fn seal(&mut self) {
        self.bytes[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4].fill(0);
        let checksum = crc32(&self.bytes);
        self.put_u32(CHECKSUM_OFFSET, checksum);
    }

    #[must_use]
    pub fn computed_checksum(&self) -> u32 {
        let mut copy = self.bytes;
        copy[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4].fill(0);
        crc32(&copy)
    }

    pub(crate) fn bytes_range(&self, start: usize, len: usize) -> Result<&[u8]> {
        let end = start
            .checked_add(len)
            .ok_or_else(|| StorageError::InvalidPage("page byte-range overflow".into()))?;
        self.bytes.get(start..end).ok_or_else(|| {
            StorageError::InvalidPage(format!("byte range {start}..{end} outside page"))
        })
    }

    pub(crate) fn bytes_range_mut(&mut self, start: usize, len: usize) -> Result<&mut [u8]> {
        let end = start
            .checked_add(len)
            .ok_or_else(|| StorageError::InvalidPage("page byte-range overflow".into()))?;
        self.bytes.get_mut(start..end).ok_or_else(|| {
            StorageError::InvalidPage(format!("byte range {start}..{end} outside page"))
        })
    }

    pub(crate) fn get_u16(&self, offset: usize) -> u16 {
        u16::from_le_bytes([self.bytes[offset], self.bytes[offset + 1]])
    }

    pub(crate) fn get_u32(&self, offset: usize) -> u32 {
        u32::from_le_bytes([
            self.bytes[offset],
            self.bytes[offset + 1],
            self.bytes[offset + 2],
            self.bytes[offset + 3],
        ])
    }

    pub(crate) fn get_u64(&self, offset: usize) -> u64 {
        let mut value = [0; 8];
        value.copy_from_slice(&self.bytes[offset..offset + 8]);
        u64::from_le_bytes(value)
    }

    pub(crate) fn put_u16(&mut self, offset: usize, value: u16) {
        self.bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn put_u32(&mut self, offset: usize, value: u32) {
        self.bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn put_u64(&mut self, offset: usize, value: u64) {
        self.bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_round_trip_and_corruption_detection() {
        let mut page = Page::new(PageId(42), PageKind::Heap);
        page.payload_mut()[5..10].copy_from_slice(b"aster");
        page.set_lsn(Lsn(99));
        let encoded = page.clone().into_bytes();
        assert_eq!(Page::decode(encoded).unwrap(), page);

        let mut corrupt = encoded;
        corrupt[100] ^= 1;
        assert!(matches!(
            Page::decode(corrupt),
            Err(StorageError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn integers_are_little_endian() {
        let page = Page::new(PageId(0x0102_0304_0506_0708), PageKind::Heap);
        assert_eq!(&page.as_bytes()[8..16], &[8, 7, 6, 5, 4, 3, 2, 1]);
    }
}
