use crate::{
    PageId, Result, StorageError,
    page::{PAGE_HEADER_SIZE, PAGE_SIZE, Page, PageKind},
};

const SLOT_SIZE: usize = 8;
const SLOT_LIVE: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RecordId {
    pub page_id: PageId,
    pub slot_id: u16,
    pub generation: u16,
}

#[derive(Clone, Copy, Debug)]
struct Slot {
    offset: u16,
    length: u16,
    flags: u16,
    generation: u16,
}

impl Slot {
    fn live(self) -> bool {
        self.flags & SLOT_LIVE != 0
    }
}

/// Stable-slot variable-length heap page. Deletes leave reusable slot IDs and
/// compaction only moves record bodies, never logical record identifiers.
#[derive(Clone, Debug)]
pub struct HeapPage {
    page: Page,
}

impl HeapPage {
    #[must_use]
    pub fn new(page_id: PageId) -> Self {
        Self {
            page: Page::new(page_id, PageKind::Heap),
        }
    }

    pub fn from_page(page: Page) -> Result<Self> {
        if page.kind() != PageKind::Heap {
            return Err(StorageError::InvalidPage(format!(
                "expected heap page, found {:?}",
                page.kind()
            )));
        }
        let heap = Self { page };
        heap.validate()?;
        Ok(heap)
    }

    #[must_use]
    pub fn page_id(&self) -> PageId {
        self.page.id()
    }

    #[must_use]
    pub fn slot_count(&self) -> u16 {
        self.page.header().slot_count
    }

    #[must_use]
    pub fn live_count(&self) -> usize {
        (0..self.slot_count())
            .filter(|slot_id| self.read_slot(*slot_id).is_ok_and(Slot::live))
            .count()
    }

    #[must_use]
    pub fn contiguous_free_bytes(&self) -> usize {
        let header = self.page.header();
        usize::from(header.upper.saturating_sub(header.lower))
    }

    #[must_use]
    pub fn reclaimable_bytes(&self) -> usize {
        (0..self.slot_count())
            .filter_map(|slot_id| self.read_slot(slot_id).ok())
            .filter(|slot| !slot.live())
            .map(|slot| usize::from(slot.length))
            .sum()
    }

    pub fn insert(&mut self, record: &[u8]) -> Result<RecordId> {
        if record.is_empty() || record.len() > usize::from(u16::MAX) {
            return Err(StorageError::RecordTooLarge {
                bytes: record.len(),
                available: self.contiguous_free_bytes(),
            });
        }
        let reusable = (0..self.slot_count())
            .find(|slot_id| self.read_slot(*slot_id).is_ok_and(|slot| !slot.live()));
        let slot_overhead = if reusable.is_some() { 0 } else { SLOT_SIZE };
        if record.len() + slot_overhead > self.contiguous_free_bytes() {
            self.compact()?;
        }
        let available = self.contiguous_free_bytes();
        if record.len() + slot_overhead > available {
            return Err(StorageError::RecordTooLarge {
                bytes: record.len() + slot_overhead,
                available,
            });
        }

        let mut header = self.page.header();
        let offset = usize::from(header.upper) - record.len();
        self.page
            .bytes_range_mut(offset, record.len())?
            .copy_from_slice(record);
        header.upper = checked_u16(offset, "heap record offset")?;

        let (slot_id, generation) = if let Some(slot_id) = reusable {
            let old = self.read_slot(slot_id)?;
            let generation = old.generation.wrapping_add(1).max(1);
            (slot_id, generation)
        } else {
            let slot_id = header.slot_count;
            header.slot_count = header
                .slot_count
                .checked_add(1)
                .ok_or_else(|| StorageError::InvalidPage("heap slot identifier overflow".into()))?;
            header.lower = checked_u16(
                PAGE_HEADER_SIZE + usize::from(header.slot_count) * SLOT_SIZE,
                "heap slot-directory end",
            )?;
            (slot_id, 1)
        };
        self.write_slot(
            slot_id,
            Slot {
                offset: checked_u16(offset, "heap record offset")?,
                length: checked_u16(record.len(), "heap record length")?,
                flags: SLOT_LIVE,
                generation,
            },
        )?;
        self.page
            .set_layout(header.lower, header.upper, header.slot_count);
        self.page.seal();
        Ok(RecordId {
            page_id: self.page.id(),
            slot_id,
            generation,
        })
    }

    pub fn get(&self, rid: RecordId) -> Result<&[u8]> {
        self.check_rid(rid)?;
        let slot = self.read_slot(rid.slot_id)?;
        self.page
            .bytes_range(usize::from(slot.offset), usize::from(slot.length))
    }

    pub fn update(&mut self, rid: RecordId, record: &[u8]) -> Result<()> {
        // Stage on a private page image so any size/codec failure leaves the
        // caller-visible heap page byte-for-byte unchanged.
        let mut staged = self.clone();
        staged.update_in_place(rid, record)?;
        *self = staged;
        Ok(())
    }

    fn update_in_place(&mut self, rid: RecordId, record: &[u8]) -> Result<()> {
        self.check_rid(rid)?;
        let old = self.get(rid)?.to_vec();
        let slot = self.read_slot(rid.slot_id)?;
        if record.len() <= old.len() {
            self.page
                .bytes_range_mut(usize::from(slot.offset), old.len())?
                .fill(0);
            self.page
                .bytes_range_mut(usize::from(slot.offset), record.len())?
                .copy_from_slice(record);
            self.write_slot(
                rid.slot_id,
                Slot {
                    length: checked_u16(record.len(), "heap record length")?,
                    ..slot
                },
            )?;
            self.page.seal();
            return Ok(());
        }

        // Preserve the stable slot and generation while relocating the body.
        self.write_slot(rid.slot_id, Slot { flags: 0, ..slot })?;
        self.compact()?;
        if record.len() > self.contiguous_free_bytes() {
            return Err(StorageError::RecordTooLarge {
                bytes: record.len(),
                available: self.contiguous_free_bytes(),
            });
        }
        let mut header = self.page.header();
        let offset = usize::from(header.upper) - record.len();
        self.page
            .bytes_range_mut(offset, record.len())?
            .copy_from_slice(record);
        header.upper = checked_u16(offset, "heap record offset")?;
        self.write_slot(
            rid.slot_id,
            Slot {
                offset: checked_u16(offset, "heap record offset")?,
                length: checked_u16(record.len(), "heap record length")?,
                flags: SLOT_LIVE,
                generation: rid.generation,
            },
        )?;
        self.page
            .set_layout(header.lower, header.upper, header.slot_count);
        self.page.seal();
        Ok(())
    }

    pub fn delete(&mut self, rid: RecordId) -> Result<bool> {
        self.check_rid(rid)?;
        let slot = self.read_slot(rid.slot_id)?;
        self.write_slot(rid.slot_id, Slot { flags: 0, ..slot })?;
        self.page.seal();
        Ok(true)
    }

    pub fn compact(&mut self) -> Result<()> {
        let count = self.slot_count();
        let mut slots = Vec::with_capacity(usize::from(count));
        let mut records = Vec::new();
        for slot_id in 0..count {
            let slot = self.read_slot(slot_id)?;
            if slot.live() {
                let record = self
                    .page
                    .bytes_range(usize::from(slot.offset), usize::from(slot.length))?
                    .to_vec();
                records.push((slot_id, slot, record));
            }
            slots.push(slot);
        }
        self.page
            .bytes_range_mut(PAGE_HEADER_SIZE, PAGE_SIZE - PAGE_HEADER_SIZE)?
            .fill(0);
        // Preserve dead-slot generations so a reused slot cannot resurrect an
        // old RecordId after compaction.
        for (slot_id, slot) in slots.into_iter().enumerate() {
            self.write_slot(checked_u16(slot_id, "heap slot id")?, slot)?;
        }
        let mut upper = PAGE_SIZE;
        for (slot_id, mut slot, record) in records {
            upper -= record.len();
            self.page
                .bytes_range_mut(upper, record.len())?
                .copy_from_slice(&record);
            slot.offset = checked_u16(upper, "compacted heap record offset")?;
            self.write_slot(slot_id, slot)?;
        }
        let lower = PAGE_HEADER_SIZE + usize::from(count) * SLOT_SIZE;
        self.page.set_layout(
            checked_u16(lower, "heap lower bound")?,
            checked_u16(upper, "heap upper bound")?,
            count,
        );
        self.page.seal();
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        self.page.validate()?;
        let header = self.page.header();
        let expected_lower = PAGE_HEADER_SIZE + usize::from(header.slot_count) * SLOT_SIZE;
        if usize::from(header.lower) != expected_lower || expected_lower > PAGE_SIZE {
            return Err(StorageError::InvalidPage(format!(
                "heap slot directory ends at {}, header says {}",
                expected_lower, header.lower
            )));
        }
        let mut intervals = Vec::new();
        for slot_id in 0..header.slot_count {
            let slot = self.read_slot(slot_id)?;
            if !slot.live() {
                continue;
            }
            if slot.length == 0 {
                return Err(StorageError::InvalidPage(format!(
                    "live heap slot {slot_id} is empty"
                )));
            }
            let start = usize::from(slot.offset);
            let end = start + usize::from(slot.length);
            if start < usize::from(header.upper) || end > PAGE_SIZE {
                return Err(StorageError::InvalidPage(format!(
                    "heap slot {slot_id} has out-of-bounds body {start}..{end}"
                )));
            }
            intervals.push((start, end, slot_id));
        }
        intervals.sort_unstable();
        for pair in intervals.windows(2) {
            if pair[0].1 > pair[1].0 {
                return Err(StorageError::InvalidPage(format!(
                    "heap slots {} and {} overlap",
                    pair[0].2, pair[1].2
                )));
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn into_page(mut self) -> Page {
        self.page.seal();
        self.page
    }

    #[must_use]
    pub fn page(&self) -> &Page {
        &self.page
    }

    fn check_rid(&self, rid: RecordId) -> Result<()> {
        if rid.page_id != self.page.id() || rid.slot_id >= self.slot_count() {
            return Err(StorageError::NotFound(format!("record {rid:?}")));
        }
        let slot = self.read_slot(rid.slot_id)?;
        if !slot.live() || slot.generation != rid.generation {
            return Err(StorageError::NotFound(format!("stale record {rid:?}")));
        }
        Ok(())
    }

    fn slot_offset(slot_id: u16) -> usize {
        PAGE_HEADER_SIZE + usize::from(slot_id) * SLOT_SIZE
    }

    fn read_slot(&self, slot_id: u16) -> Result<Slot> {
        if slot_id >= self.slot_count() {
            return Err(StorageError::NotFound(format!("heap slot {slot_id}")));
        }
        let offset = Self::slot_offset(slot_id);
        let bytes = self.page.bytes_range(offset, SLOT_SIZE)?;
        Ok(Slot {
            offset: u16::from_le_bytes([bytes[0], bytes[1]]),
            length: u16::from_le_bytes([bytes[2], bytes[3]]),
            flags: u16::from_le_bytes([bytes[4], bytes[5]]),
            generation: u16::from_le_bytes([bytes[6], bytes[7]]),
        })
    }

    fn write_slot(&mut self, slot_id: u16, slot: Slot) -> Result<()> {
        let offset = Self::slot_offset(slot_id);
        let bytes = self.page.bytes_range_mut(offset, SLOT_SIZE)?;
        bytes[0..2].copy_from_slice(&slot.offset.to_le_bytes());
        bytes[2..4].copy_from_slice(&slot.length.to_le_bytes());
        bytes[4..6].copy_from_slice(&slot.flags.to_le_bytes());
        bytes[6..8].copy_from_slice(&slot.generation.to_le_bytes());
        Ok(())
    }
}

fn checked_u16(value: usize, field: &str) -> Result<u16> {
    u16::try_from(value).map_err(|_| {
        StorageError::InvalidPage(format!("{field} value {value} exceeds on-disk u16"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_survive_compaction_and_detect_aba() {
        let mut heap = HeapPage::new(PageId(7));
        let a = heap.insert(&vec![1; 900]).unwrap();
        let b = heap.insert(&vec![2; 700]).unwrap();
        let c = heap.insert(&vec![3; 800]).unwrap();
        heap.delete(b).unwrap();
        heap.compact().unwrap();
        assert_eq!(heap.get(a).unwrap(), vec![1; 900]);
        assert_eq!(heap.get(c).unwrap(), vec![3; 800]);
        let reused = heap.insert(b"replacement").unwrap();
        assert_eq!(reused.slot_id, b.slot_id);
        assert_ne!(reused.generation, b.generation);
        assert!(heap.get(b).is_err());
        heap.validate().unwrap();
        HeapPage::from_page(heap.into_page()).unwrap();
    }

    #[test]
    fn failed_growth_update_is_atomic() {
        let mut heap = HeapPage::new(PageId(8));
        let target = heap.insert(b"original").unwrap();
        heap.insert(&vec![9; 3_500]).unwrap();
        let before = heap.page().clone().into_bytes();
        assert!(heap.update(target, &vec![4; 2_000]).is_err());
        assert_eq!(heap.page().as_bytes(), &before);
        assert_eq!(heap.get(target).unwrap(), b"original");
        heap.validate().unwrap();
    }

    #[test]
    fn randomized_model() {
        let mut seed = 0x5eed_1234_dead_beef_u64;
        let mut heap = HeapPage::new(PageId(3));
        let mut model = Vec::<(RecordId, Vec<u8>)>::new();
        for step in 0..2_000 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            if seed.is_multiple_of(3) && !model.is_empty() {
                let index = usize::try_from(seed % model.len() as u64).unwrap();
                let (rid, _) = model.swap_remove(index);
                heap.delete(rid).unwrap();
            } else {
                let length = 1 + usize::try_from(seed % 80).unwrap();
                let value = vec![u8::try_from(step % 251).unwrap(); length];
                if let Ok(rid) = heap.insert(&value) {
                    model.push((rid, value));
                } else if !model.is_empty() {
                    let (rid, _) = model.swap_remove(0);
                    heap.delete(rid).unwrap();
                    heap.compact().unwrap();
                }
            }
            heap.validate().unwrap();
            for (rid, expected) in &model {
                assert_eq!(heap.get(*rid).unwrap(), expected);
            }
        }
    }
}
