use std::{
    collections::{BTreeMap, VecDeque},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use parking_lot::Mutex;

use crate::{PageId, Result, StorageError, page::PAGE_SIZE};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IoOperation {
    Read,
    Write,
    Sync,
    PageCount,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IoEvent {
    pub ordinal: u64,
    pub operation: IoOperation,
    pub page_id: Option<PageId>,
}

/// Minimal random-access page device. Implementations must either transfer a
/// whole page or return an error; partial I/O never leaks into callers.
pub trait Disk: Send + Sync + 'static {
    fn read_page(&self, page_id: PageId) -> Result<[u8; PAGE_SIZE]>;
    fn write_page(&self, page_id: PageId, page: &[u8; PAGE_SIZE]) -> Result<()>;
    fn sync(&self) -> Result<()>;
    fn page_count(&self) -> Result<u64>;
}

pub struct FileDisk {
    file: Mutex<File>,
}

impl FileDisk {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }
}

impl Disk for FileDisk {
    fn read_page(&self, page_id: PageId) -> Result<[u8; PAGE_SIZE]> {
        let mut file = self.file.lock();
        let offset = page_id.0.checked_mul(PAGE_SIZE as u64).ok_or_else(|| {
            StorageError::InvalidPage(format!("page id {} overflows file offset", page_id.0))
        })?;
        let length = file.metadata()?.len();
        let end = offset.checked_add(PAGE_SIZE as u64).ok_or_else(|| {
            StorageError::InvalidPage(format!("page id {} overflows file extent", page_id.0))
        })?;
        if end > length {
            return Err(StorageError::NotFound(format!("page {}", page_id.0)));
        }
        file.seek(SeekFrom::Start(offset))?;
        let mut bytes = [0; PAGE_SIZE];
        file.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    fn write_page(&self, page_id: PageId, page: &[u8; PAGE_SIZE]) -> Result<()> {
        let mut file = self.file.lock();
        let offset = page_id.0.checked_mul(PAGE_SIZE as u64).ok_or_else(|| {
            StorageError::InvalidPage(format!("page id {} overflows file offset", page_id.0))
        })?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(page)?;
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        self.file.lock().sync_data()?;
        Ok(())
    }

    fn page_count(&self) -> Result<u64> {
        let length = self.file.lock().metadata()?.len();
        if length % PAGE_SIZE as u64 != 0 {
            return Err(StorageError::InvalidPage(format!(
                "database file length {length} is not page aligned"
            )));
        }
        Ok(length / PAGE_SIZE as u64)
    }
}

/// In-memory durability model. Writes affect the volatile image; `sync`
/// advances the durable image, and `crash` discards everything else.
#[derive(Default)]
pub struct MemoryDisk {
    state: Mutex<MemoryDiskState>,
}

#[derive(Default)]
struct MemoryDiskState {
    volatile: BTreeMap<PageId, [u8; PAGE_SIZE]>,
    durable: BTreeMap<PageId, [u8; PAGE_SIZE]>,
    events: Vec<IoEvent>,
    ordinal: u64,
}

impl MemoryDisk {
    pub fn crash(&self) {
        let mut state = self.state.lock();
        state.volatile = state.durable.clone();
    }

    #[must_use]
    pub fn events(&self) -> Vec<IoEvent> {
        self.state.lock().events.clone()
    }

    pub fn clear_events(&self) {
        self.state.lock().events.clear();
    }

    fn record(state: &mut MemoryDiskState, operation: IoOperation, page_id: Option<PageId>) {
        state.ordinal += 1;
        let ordinal = state.ordinal;
        state.events.push(IoEvent {
            ordinal,
            operation,
            page_id,
        });
    }
}

impl Disk for MemoryDisk {
    fn read_page(&self, page_id: PageId) -> Result<[u8; PAGE_SIZE]> {
        let mut state = self.state.lock();
        Self::record(&mut state, IoOperation::Read, Some(page_id));
        state
            .volatile
            .get(&page_id)
            .copied()
            .ok_or_else(|| StorageError::NotFound(format!("page {}", page_id.0)))
    }

    fn write_page(&self, page_id: PageId, page: &[u8; PAGE_SIZE]) -> Result<()> {
        let mut state = self.state.lock();
        Self::record(&mut state, IoOperation::Write, Some(page_id));
        state.volatile.insert(page_id, *page);
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        let mut state = self.state.lock();
        Self::record(&mut state, IoOperation::Sync, None);
        state.durable = state.volatile.clone();
        Ok(())
    }

    fn page_count(&self) -> Result<u64> {
        let mut state = self.state.lock();
        Self::record(&mut state, IoOperation::PageCount, None);
        Ok(state.volatile.keys().next_back().map_or(0, |id| id.0 + 1))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FaultAction {
    Error,
    /// Persist only the first N bytes of a page write in the volatile image,
    /// then report an error. Useful for checksum and recovery tests.
    TornWrite(usize),
    /// Pretend the operation succeeded without changing the underlying disk.
    Drop,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Fault {
    pub operation: IoOperation,
    /// One-based ordinal among matching operations.
    pub matching_ordinal: u64,
    pub action: FaultAction,
}

/// Scriptable fault-injecting disk decorator. Faults are deterministic and
/// each rule is consumed once.
pub struct FaultyFile<D: Disk> {
    inner: D,
    state: Mutex<FaultState>,
}

#[derive(Default)]
struct FaultState {
    faults: VecDeque<Fault>,
    matching_counts: BTreeMap<u8, u64>,
    events: Vec<IoEvent>,
    ordinal: u64,
}

impl<D: Disk> FaultyFile<D> {
    #[must_use]
    pub fn new(inner: D) -> Self {
        Self {
            inner,
            state: Mutex::new(FaultState::default()),
        }
    }

    pub fn push_fault(&self, fault: Fault) {
        self.state.lock().faults.push_back(fault);
    }

    #[must_use]
    pub const fn inner(&self) -> &D {
        &self.inner
    }

    #[must_use]
    pub fn events(&self) -> Vec<IoEvent> {
        self.state.lock().events.clone()
    }

    fn before(
        &self,
        operation: IoOperation,
        page_id: Option<PageId>,
    ) -> Option<(u64, FaultAction)> {
        let mut state = self.state.lock();
        state.ordinal += 1;
        let ordinal = state.ordinal;
        state.events.push(IoEvent {
            ordinal,
            operation,
            page_id,
        });
        let key = operation as u8;
        let matching = state.matching_counts.entry(key).or_default();
        *matching += 1;
        let matching = *matching;
        if state
            .faults
            .front()
            .is_some_and(|fault| fault.operation == operation && fault.matching_ordinal == matching)
        {
            state
                .faults
                .pop_front()
                .map(|fault| (ordinal, fault.action))
        } else {
            None
        }
    }

    fn injected(operation: IoOperation, ordinal: u64) -> StorageError {
        StorageError::InjectedFault {
            operation: format!("{operation:?}"),
            ordinal,
        }
    }
}

impl<D: Disk> Disk for FaultyFile<D> {
    fn read_page(&self, page_id: PageId) -> Result<[u8; PAGE_SIZE]> {
        if let Some((ordinal, action)) = self.before(IoOperation::Read, Some(page_id)) {
            return match action {
                FaultAction::Drop | FaultAction::Error | FaultAction::TornWrite(_) => {
                    Err(Self::injected(IoOperation::Read, ordinal))
                }
            };
        }
        self.inner.read_page(page_id)
    }

    fn write_page(&self, page_id: PageId, page: &[u8; PAGE_SIZE]) -> Result<()> {
        if let Some((ordinal, action)) = self.before(IoOperation::Write, Some(page_id)) {
            return match action {
                FaultAction::Drop => Ok(()),
                FaultAction::Error => Err(Self::injected(IoOperation::Write, ordinal)),
                FaultAction::TornWrite(bytes) => {
                    let mut old = self.inner.read_page(page_id).unwrap_or([0; PAGE_SIZE]);
                    let bytes = bytes.min(PAGE_SIZE);
                    old[..bytes].copy_from_slice(&page[..bytes]);
                    self.inner.write_page(page_id, &old)?;
                    Err(Self::injected(IoOperation::Write, ordinal))
                }
            };
        }
        self.inner.write_page(page_id, page)
    }

    fn sync(&self) -> Result<()> {
        if let Some((ordinal, action)) = self.before(IoOperation::Sync, None) {
            return match action {
                FaultAction::Drop => Ok(()),
                FaultAction::Error | FaultAction::TornWrite(_) => {
                    Err(Self::injected(IoOperation::Sync, ordinal))
                }
            };
        }
        self.inner.sync()
    }

    fn page_count(&self) -> Result<u64> {
        if let Some((ordinal, _)) = self.before(IoOperation::PageCount, None) {
            return Err(Self::injected(IoOperation::PageCount, ordinal));
        }
        self.inner.page_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_disk_models_flush_and_crash() {
        let disk = MemoryDisk::default();
        let first = [1; PAGE_SIZE];
        let second = [2; PAGE_SIZE];
        disk.write_page(PageId(2), &first).unwrap();
        disk.sync().unwrap();
        disk.write_page(PageId(2), &second).unwrap();
        assert_eq!(disk.read_page(PageId(2)).unwrap()[0], 2);
        disk.crash();
        assert_eq!(disk.read_page(PageId(2)).unwrap()[0], 1);
    }

    #[test]
    fn torn_write_is_visible_and_detectable() {
        let faulty = FaultyFile::new(MemoryDisk::default());
        faulty.inner.write_page(PageId(0), &[7; PAGE_SIZE]).unwrap();
        faulty.push_fault(Fault {
            operation: IoOperation::Write,
            matching_ordinal: 1,
            action: FaultAction::TornWrite(100),
        });
        assert!(faulty.write_page(PageId(0), &[9; PAGE_SIZE]).is_err());
        let bytes = faulty.inner.read_page(PageId(0)).unwrap();
        assert!(bytes[..100].iter().all(|value| *value == 9));
        assert!(bytes[100..].iter().all(|value| *value == 7));
    }
}
