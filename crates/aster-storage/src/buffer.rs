use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use parking_lot::Mutex;

use crate::{Lsn, PageId, Result, StorageError, disk::Disk, page::Page};

/// WAL durability boundary used by the buffer pool. A dirty committed page is
/// never written until its page LSN is known durable.
pub trait WalSync: Send + Sync + 'static {
    fn flush_through(&self, lsn: Lsn) -> Result<()>;
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BufferStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub writes: u64,
}

struct Frame {
    page: Page,
    pin_count: usize,
    dirty: bool,
    referenced: bool,
}

struct PoolState {
    frames: Vec<Option<Frame>>,
    table: HashMap<PageId, usize>,
    clock_hand: usize,
    stats: BufferStats,
}

struct BufferInner<D: Disk> {
    disk: Arc<D>,
    state: Mutex<PoolState>,
    wal: Option<Arc<dyn WalSync>>,
}

/// Bounded clock buffer pool. Dirty frames follow a no-steal policy: the clock
/// will not evict them. Publication/flush is explicit and WAL-ordered.
pub struct BufferPool<D: Disk> {
    inner: Arc<BufferInner<D>>,
}

impl<D: Disk> Clone for BufferPool<D> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<D: Disk> BufferPool<D> {
    pub fn new(disk: Arc<D>, capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(StorageError::Invariant(
                "buffer-pool capacity must be non-zero".into(),
            ));
        }
        Ok(Self {
            inner: Arc::new(BufferInner {
                disk,
                state: Mutex::new(PoolState {
                    frames: (0..capacity).map(|_| None).collect(),
                    table: HashMap::new(),
                    clock_hand: 0,
                    stats: BufferStats::default(),
                }),
                wal: None,
            }),
        })
    }

    pub fn with_wal(disk: Arc<D>, capacity: usize, wal: Arc<dyn WalSync>) -> Result<Self> {
        if capacity == 0 {
            return Err(StorageError::Invariant(
                "buffer-pool capacity must be non-zero".into(),
            ));
        }
        Ok(Self {
            inner: Arc::new(BufferInner {
                disk,
                state: Mutex::new(PoolState {
                    frames: (0..capacity).map(|_| None).collect(),
                    table: HashMap::new(),
                    clock_hand: 0,
                    stats: BufferStats::default(),
                }),
                wal: Some(wal),
            }),
        })
    }

    pub fn fetch(&self, page_id: PageId) -> Result<PageGuard<D>> {
        let mut state = self.inner.state.lock();
        if let Some(&index) = state.table.get(&page_id) {
            let frame = state
                .frames
                .get_mut(index)
                .and_then(Option::as_mut)
                .ok_or_else(|| {
                    StorageError::Invariant("buffer page table points to empty frame".into())
                })?;
            frame.pin_count += 1;
            frame.referenced = true;
            state.stats.hits += 1;
            return Ok(PageGuard {
                inner: Arc::clone(&self.inner),
                index,
                page_id,
            });
        }

        state.stats.misses += 1;
        let bytes = self.inner.disk.read_page(page_id)?;
        let page = Page::decode(bytes)?;
        if page.id() != page_id {
            return Err(StorageError::InvalidPage(format!(
                "physical page {} encodes page id {}",
                page_id.0,
                page.id().0
            )));
        }
        let index = Self::choose_frame(&mut state)?;
        if let Some(victim) = state.frames[index].take() {
            debug_assert_eq!(victim.pin_count, 0);
            debug_assert!(!victim.dirty);
            state.table.remove(&victim.page.id());
            state.stats.evictions += 1;
        }
        state.frames[index] = Some(Frame {
            page,
            pin_count: 1,
            dirty: false,
            referenced: true,
        });
        state.table.insert(page_id, index);
        Ok(PageGuard {
            inner: Arc::clone(&self.inner),
            index,
            page_id,
        })
    }

    /// Installs a newly allocated page in the pool as dirty. The caller is
    /// responsible for reserving a unique page ID in persistent metadata.
    pub fn install(&self, page: Page) -> Result<PageGuard<D>> {
        page.validate()?;
        let page_id = page.id();
        let mut state = self.inner.state.lock();
        if state.table.contains_key(&page_id) {
            return Err(StorageError::Invariant(format!(
                "page {} already resident",
                page_id.0
            )));
        }
        let index = Self::choose_frame(&mut state)?;
        if let Some(victim) = state.frames[index].take() {
            state.table.remove(&victim.page.id());
            state.stats.evictions += 1;
        }
        state.frames[index] = Some(Frame {
            page,
            pin_count: 1,
            dirty: true,
            referenced: true,
        });
        state.table.insert(page_id, index);
        Ok(PageGuard {
            inner: Arc::clone(&self.inner),
            index,
            page_id,
        })
    }

    /// Flush all unpinned dirty pages. If any dirty page remains pinned, no
    /// data-file sync is issued and an error identifies the first page.
    pub fn flush_all(&self) -> Result<()> {
        let mut state = self.inner.state.lock();
        if let Some(frame) = state
            .frames
            .iter()
            .flatten()
            .find(|frame| frame.dirty && frame.pin_count != 0)
        {
            return Err(StorageError::PagePinned(frame.page.id()));
        }
        let maximum_lsn = state
            .frames
            .iter()
            .flatten()
            .filter(|frame| frame.dirty)
            .map(|frame| frame.page.lsn())
            .max();
        if let (Some(wal), Some(lsn)) = (&self.inner.wal, maximum_lsn) {
            wal.flush_through(lsn)?;
        }
        let mut writes = 0;
        for frame in state
            .frames
            .iter_mut()
            .flatten()
            .filter(|frame| frame.dirty)
        {
            frame.page.seal();
            frame.page.validate()?;
            self.inner
                .disk
                .write_page(frame.page.id(), frame.page.as_bytes())?;
            frame.dirty = false;
            writes += 1;
        }
        state.stats.writes += writes;
        self.inner.disk.sync()?;
        Ok(())
    }

    #[must_use]
    pub fn stats(&self) -> BufferStats {
        self.inner.state.lock().stats
    }

    #[must_use]
    pub fn resident_pages(&self) -> Vec<(PageId, usize, bool)> {
        self.inner
            .state
            .lock()
            .frames
            .iter()
            .flatten()
            .map(|frame| (frame.page.id(), frame.pin_count, frame.dirty))
            .collect()
    }

    fn choose_frame(state: &mut PoolState) -> Result<usize> {
        if let Some(index) = state.frames.iter().position(Option::is_none) {
            return Ok(index);
        }
        let capacity = state.frames.len();
        for _ in 0..capacity * 2 {
            let index = state.clock_hand;
            state.clock_hand = (state.clock_hand + 1) % capacity;
            let Some(frame) = state.frames[index].as_mut() else {
                continue;
            };
            if frame.pin_count != 0 || frame.dirty {
                continue;
            }
            if frame.referenced {
                frame.referenced = false;
                continue;
            }
            return Ok(index);
        }
        Err(StorageError::BufferPoolExhausted)
    }
}

pub struct PageGuard<D: Disk> {
    inner: Arc<BufferInner<D>>,
    index: usize,
    page_id: PageId,
}

impl<D: Disk> PageGuard<D> {
    #[must_use]
    pub const fn page_id(&self) -> PageId {
        self.page_id
    }

    pub fn snapshot(&self) -> Result<Page> {
        let state = self.inner.state.lock();
        state
            .frames
            .get(self.index)
            .and_then(Option::as_ref)
            .filter(|frame| frame.page.id() == self.page_id)
            .map(|frame| frame.page.clone())
            .ok_or_else(|| StorageError::Invariant("pinned buffer frame disappeared".into()))
    }

    /// Replace a resident page with a validated committed image. Mutations are
    /// performed on the caller's private copy, preserving no-steal semantics.
    pub fn replace(&self, mut page: Page, lsn: Lsn) -> Result<()> {
        if page.id() != self.page_id {
            return Err(StorageError::Invariant(format!(
                "cannot replace page {} with page {}",
                self.page_id.0,
                page.id().0
            )));
        }
        page.set_lsn(lsn);
        page.seal();
        page.validate()?;
        let mut state = self.inner.state.lock();
        let frame = state.frames[self.index]
            .as_mut()
            .ok_or_else(|| StorageError::Invariant("pinned buffer frame disappeared".into()))?;
        frame.page = page;
        frame.dirty = true;
        frame.referenced = true;
        Ok(())
    }
}

impl<D: Disk> Drop for PageGuard<D> {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock();
        if let Some(frame) = state.frames[self.index].as_mut()
            && frame.page.id() == self.page_id
        {
            frame.pin_count = frame.pin_count.saturating_sub(1);
        }
    }
}

/// Transaction-private page images. They cannot be observed or evicted by the
/// buffer pool until a durable WAL commit has assigned a page LSN.
#[derive(Default)]
pub struct PrivatePageBatch {
    pages: BTreeMap<PageId, Page>,
}

impl PrivatePageBatch {
    pub fn stage(&mut self, mut page: Page) -> Result<()> {
        page.seal();
        page.validate()?;
        self.pages.insert(page.id(), page);
        Ok(())
    }

    #[must_use]
    pub fn get(&self, page_id: PageId) -> Option<&Page> {
        self.pages.get(&page_id)
    }

    pub fn get_or_load<D: Disk>(
        &mut self,
        pool: &BufferPool<D>,
        page_id: PageId,
    ) -> Result<&mut Page> {
        if let std::collections::btree_map::Entry::Vacant(entry) = self.pages.entry(page_id) {
            let page = pool.fetch(page_id)?.snapshot()?;
            entry.insert(page);
        }
        self.pages.get_mut(&page_id).ok_or_else(|| {
            StorageError::Invariant("private page disappeared after insertion".into())
        })
    }

    #[must_use]
    pub fn into_pages(self) -> Vec<Page> {
        self.pages.into_values().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::{Disk, MemoryDisk};
    use crate::page::PageKind;

    fn put(disk: &MemoryDisk, id: u64) {
        let page = Page::new(PageId(id), PageKind::Heap);
        disk.write_page(PageId(id), page.as_bytes()).unwrap();
    }

    #[test]
    fn clock_respects_pins_and_dirty_no_steal() {
        let disk = Arc::new(MemoryDisk::default());
        for id in 0..4 {
            put(&disk, id);
        }
        let pool = BufferPool::new(Arc::clone(&disk), 2).unwrap();
        let pinned = pool.fetch(PageId(0)).unwrap();
        let dirty = pool.fetch(PageId(1)).unwrap();
        let mut changed = dirty.snapshot().unwrap();
        changed.set_page_epoch(1);
        dirty.replace(changed, Lsn(8)).unwrap();
        drop(dirty);
        assert!(matches!(
            pool.fetch(PageId(2)),
            Err(StorageError::BufferPoolExhausted)
        ));
        drop(pinned);
        assert!(pool.fetch(PageId(2)).is_ok());
    }

    #[test]
    fn private_batches_are_invisible_until_publish() {
        let disk = Arc::new(MemoryDisk::default());
        put(&disk, 0);
        let pool = BufferPool::new(Arc::clone(&disk), 2).unwrap();
        let mut batch = PrivatePageBatch::default();
        batch
            .get_or_load(&pool, PageId(0))
            .unwrap()
            .set_page_epoch(99);
        assert_eq!(
            pool.fetch(PageId(0))
                .unwrap()
                .snapshot()
                .unwrap()
                .header()
                .page_epoch,
            0
        );
    }
}
