use std::sync::Arc;

use parking_lot::Mutex;

use crate::{
    Lsn, PageId, Result, StorageError,
    disk::Disk,
    page::{PAGE_SIZE_U16, Page, PageKind},
    wal::{Checkpoint, CommittedApply, WalIo, WalScan, WriteAheadLog},
};

const SUPER_MAGIC: [u8; 8] = *b"ASTERDB\0";
const SUPER_VERSION: u16 = 1;
const NO_ROOT: u64 = u64::MAX;

/// Independently checksummed superblock stored on physical page 0 or 1.
/// `applied_index` is the Raft/state-machine epoch; it is intentionally
/// separate from byte-oriented WAL LSNs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Superblock {
    pub slot: u8,
    pub generation: u64,
    pub applied_index: u64,
    pub state_hash: [u8; 32],
    pub checkpoint_lsn: Lsn,
    pub next_page_id: PageId,
    pub root_directory: Option<PageId>,
}

impl Superblock {
    #[must_use]
    pub fn genesis() -> Self {
        Self {
            slot: 0,
            generation: 1,
            applied_index: 0,
            state_hash: [0; 32],
            checkpoint_lsn: Lsn(0),
            next_page_id: PageId(2),
            root_directory: None,
        }
    }

    pub fn encode(&self) -> Result<Page> {
        if self.slot > 1 {
            return Err(StorageError::Invariant(format!(
                "invalid superblock slot {}",
                self.slot
            )));
        }
        let mut page = Page::new(PageId(u64::from(self.slot)), PageKind::Superblock);
        let payload = page.payload_mut();
        payload[0..8].copy_from_slice(&SUPER_MAGIC);
        payload[8..10].copy_from_slice(&SUPER_VERSION.to_le_bytes());
        payload[10] = self.slot;
        payload[11..16].fill(0);
        payload[16..24].copy_from_slice(&self.generation.to_le_bytes());
        payload[24..32].copy_from_slice(&self.applied_index.to_le_bytes());
        payload[32..64].copy_from_slice(&self.state_hash);
        payload[64..72].copy_from_slice(&self.checkpoint_lsn.0.to_le_bytes());
        payload[72..80].copy_from_slice(&self.next_page_id.0.to_le_bytes());
        payload[80..88].copy_from_slice(
            &self
                .root_directory
                .map_or(NO_ROOT, |page_id| page_id.0)
                .to_le_bytes(),
        );
        page.set_layout(152, PAGE_SIZE_U16, 0);
        page.set_page_epoch(self.applied_index);
        page.seal();
        Ok(page)
    }

    pub fn decode(page: &Page) -> Result<Self> {
        page.validate()?;
        if page.kind() != PageKind::Superblock || page.id().0 > 1 {
            return Err(StorageError::InvalidPage(
                "expected superblock page 0 or 1".into(),
            ));
        }
        let payload = page.payload();
        if payload[0..8] != SUPER_MAGIC {
            return Err(StorageError::InvalidPage("bad superblock magic".into()));
        }
        let version = u16::from_le_bytes([payload[8], payload[9]]);
        if version != SUPER_VERSION {
            return Err(StorageError::InvalidPage(format!(
                "unsupported superblock version {version}"
            )));
        }
        let slot = payload[10];
        if u64::from(slot) != page.id().0 || payload[11..16].iter().any(|byte| *byte != 0) {
            return Err(StorageError::InvalidPage(
                "superblock slot or reserved bytes invalid".into(),
            ));
        }
        let generation = read_u64(payload, 16)?;
        let applied_index = read_u64(payload, 24)?;
        let mut state_hash = [0; 32];
        state_hash.copy_from_slice(&payload[32..64]);
        let checkpoint_lsn = Lsn(read_u64(payload, 64)?);
        let next_page_id = PageId(read_u64(payload, 72)?);
        let root = read_u64(payload, 80)?;
        if next_page_id.0 < 2 {
            return Err(StorageError::InvalidPage(
                "superblock allocator overlaps metadata".into(),
            ));
        }
        Ok(Self {
            slot,
            generation,
            applied_index,
            state_hash,
            checkpoint_lsn,
            next_page_id,
            root_directory: (root != NO_ROOT).then_some(PageId(root)),
        })
    }
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| StorageError::InvalidPage("superblock offset overflow".into()))?;
    let source = bytes
        .get(offset..end)
        .ok_or_else(|| StorageError::InvalidPage("truncated superblock".into()))?;
    let mut value = [0; 8];
    value.copy_from_slice(source);
    Ok(u64::from_le_bytes(value))
}

pub fn read_superblock<D: Disk>(disk: &D) -> Result<Option<Superblock>> {
    let mut valid = Vec::new();
    let mut failures = Vec::new();
    for slot in 0..=1 {
        match disk
            .read_page(PageId(slot))
            .and_then(Page::decode)
            .and_then(|page| Superblock::decode(&page))
        {
            Ok(superblock) => valid.push(superblock),
            Err(StorageError::NotFound(_)) => {}
            Err(error) => failures.push((slot, error.to_string())),
        }
    }
    valid.sort_by_key(|superblock| superblock.generation);
    if let Some(latest) = valid.pop() {
        return Ok(Some(latest));
    }
    if failures.is_empty() {
        Ok(None)
    } else {
        Err(StorageError::InvalidPage(format!(
            "neither superblock is valid: {failures:?}"
        )))
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RecoveryReport {
    pub groups_examined: usize,
    pub groups_replayed: usize,
    pub pages_replayed: usize,
    pub pages_already_current: usize,
    pub final_applied_index: u64,
}

/// Replay complete, digest-verified full-page groups. Incomplete groups are
/// invisible. A page image is idempotent by page LSN; apply-group identity is
/// separately checked by Raft index and state hash in the superblock.
pub fn recover<D: Disk>(disk: &D, scan: &WalScan) -> Result<(Superblock, RecoveryReport)> {
    let current = read_superblock(disk)?.unwrap_or_else(Superblock::genesis);
    if current.checkpoint_lsn.0 > scan.safe_append_offset {
        return Err(StorageError::Invariant(format!(
            "superblock checkpoint LSN {} exceeds verified WAL end {}",
            current.checkpoint_lsn.0, scan.safe_append_offset
        )));
    }
    if current.checkpoint_lsn.0 != 0 {
        let checkpoint = scan
            .checkpoints
            .iter()
            .find(|checkpoint| checkpoint.end_lsn == current.checkpoint_lsn)
            .ok_or_else(|| {
                StorageError::Invariant(format!(
                    "superblock references missing checkpoint {}",
                    current.checkpoint_lsn.0
                ))
            })?;
        if checkpoint.applied_index != current.applied_index
            || checkpoint.state_hash != current.state_hash
        {
            return Err(StorageError::Invariant(
                "checkpoint index/hash disagrees with superblock".into(),
            ));
        }
    }
    let mut state = current.clone();
    let mut report = RecoveryReport::default();
    for group in &scan.groups {
        report.groups_examined += 1;
        if group.apply_index < state.applied_index {
            continue;
        }
        if group.apply_index == state.applied_index {
            if group.state_hash != state.state_hash {
                return Err(StorageError::Invariant(format!(
                    "WAL group {} conflicts with durable state hash",
                    group.apply_index
                )));
            }
            continue;
        }
        if state.applied_index != 0 && group.apply_index != state.applied_index + 1 {
            return Err(StorageError::Invariant(format!(
                "WAL apply gap: durable {}, next {}",
                state.applied_index, group.apply_index
            )));
        }
        replay_group(disk, group, &mut report)?;
        disk.sync()?;
        state = publish_superblock(
            disk,
            &state,
            group.apply_index,
            group.state_hash,
            state.checkpoint_lsn,
            group.next_page_id,
            group.root_directory,
        )?;
        report.groups_replayed += 1;
    }
    report.final_applied_index = state.applied_index;
    Ok((state, report))
}

fn replay_group<D: Disk>(
    disk: &D,
    group: &CommittedApply,
    report: &mut RecoveryReport,
) -> Result<()> {
    for image in &group.pages {
        let current_lsn = match disk.read_page(image.id()) {
            Ok(bytes) => match Page::decode(bytes) {
                Ok(page) if page.id() == image.id() => Some(page.lsn()),
                Ok(page) => {
                    return Err(StorageError::InvalidPage(format!(
                        "physical page {} contains page {}",
                        image.id().0,
                        page.id().0
                    )));
                }
                // A committed full-page image repairs a torn/corrupt data page.
                Err(_) => None,
            },
            Err(StorageError::NotFound(_)) => None,
            Err(error) => return Err(error),
        };
        if current_lsn.is_some_and(|lsn| lsn >= image.lsn()) {
            report.pages_already_current += 1;
            continue;
        }
        disk.write_page(image.id(), image.as_bytes())?;
        report.pages_replayed += 1;
    }
    Ok(())
}

fn publish_superblock<D: Disk>(
    disk: &D,
    previous: &Superblock,
    applied_index: u64,
    state_hash: [u8; 32],
    checkpoint_lsn: Lsn,
    next_page_id: PageId,
    root_directory: Option<PageId>,
) -> Result<Superblock> {
    let next = Superblock {
        slot: 1 - previous.slot,
        generation: previous
            .generation
            .checked_add(1)
            .ok_or_else(|| StorageError::Invariant("superblock generation exhausted".into()))?,
        applied_index,
        state_hash,
        checkpoint_lsn,
        next_page_id,
        root_directory,
    };
    let page = next.encode()?;
    disk.write_page(page.id(), page.as_bytes())?;
    disk.sync()?;
    Ok(next)
}

/// Durable serialized-apply API that enforces WAL-before-data and alternating
/// superblock publication. It is the vertical storage boundary used by the
/// replicated engine.
pub struct DurablePager<D: Disk, W: WalIo> {
    disk: Arc<D>,
    wal: Arc<WriteAheadLog<W>>,
    state: Mutex<Superblock>,
}

impl<D: Disk, W: WalIo> DurablePager<D, W> {
    pub fn open(disk: Arc<D>, wal_io: Arc<W>) -> Result<(Self, RecoveryReport)> {
        let wal = Arc::new(WriteAheadLog::open(wal_io)?);
        let scan = wal.scan()?;
        let had_superblock = read_superblock(disk.as_ref())?.is_some();
        let (mut state, report) = recover(disk.as_ref(), &scan)?;
        if !had_superblock {
            // Establish both independent slots. A crash after either sync still
            // leaves one complete authoritative copy.
            let first = state.encode()?;
            disk.write_page(first.id(), first.as_bytes())?;
            disk.sync()?;
            state = publish_superblock(
                disk.as_ref(),
                &state,
                state.applied_index,
                state.state_hash,
                state.checkpoint_lsn,
                state.next_page_id,
                state.root_directory,
            )?;
        }
        Ok((
            Self {
                disk,
                wal,
                state: Mutex::new(state),
            },
            report,
        ))
    }

    pub fn apply(
        &self,
        apply_index: u64,
        state_hash: [u8; 32],
        pages: Vec<Page>,
        next_page_id: PageId,
        root_directory: Option<PageId>,
    ) -> Result<CommittedApply> {
        if pages.iter().any(|page| page.id().0 < 2) {
            return Err(StorageError::Invariant(
                "apply group may not overwrite superblocks".into(),
            ));
        }
        let mut state = self.state.lock();
        if apply_index == state.applied_index {
            if state_hash != state.state_hash {
                return Err(StorageError::Invariant(
                    "duplicate apply index has different hash".into(),
                ));
            }
            let existing = self
                .wal
                .scan()?
                .groups
                .into_iter()
                .find(|group| group.apply_index == apply_index)
                .ok_or_else(|| StorageError::NotFound(format!("WAL group {apply_index}")))?;
            return Ok(existing);
        }
        if apply_index != state.applied_index + 1 {
            return Err(StorageError::Invariant(format!(
                "serialized apply expected index {}, got {apply_index}",
                state.applied_index + 1
            )));
        }
        if next_page_id.0 < state.next_page_id.0
            || pages.iter().any(|page| page.id().0 >= next_page_id.0)
        {
            return Err(StorageError::Invariant(
                "invalid next-page high-water mark".into(),
            ));
        }
        let group = self.wal.append_apply_with_metadata(
            apply_index,
            state_hash,
            next_page_id,
            root_directory,
            pages,
        )?;
        let mut ignored_report = RecoveryReport::default();
        replay_group(self.disk.as_ref(), &group, &mut ignored_report)?;
        self.disk.sync()?;
        *state = publish_superblock(
            self.disk.as_ref(),
            &state,
            apply_index,
            state_hash,
            // A checkpoint marker is bound to one exact apply index and state
            // hash. Advancing the state invalidates that binding; recovery can
            // still scan the retained WAL from its beginning until the next
            // checkpoint publishes a new marker.
            Lsn(0),
            next_page_id,
            root_directory,
        )?;
        Ok(group)
    }

    /// Atomically installs a copy-on-write state-machine snapshot at a newer
    /// apply index. Every supplied page must be freshly allocated above the
    /// current high-water mark, so the old superblock continues to reference a
    /// complete database until the new pages are durable. The alternate
    /// superblock is the sole publication point.
    pub fn install_snapshot(
        &self,
        apply_index: u64,
        state_hash: [u8; 32],
        mut pages: Vec<Page>,
        next_page_id: PageId,
        root_directory: PageId,
    ) -> Result<()> {
        let mut state = self.state.lock();
        if apply_index == state.applied_index {
            if state_hash != state.state_hash || state.root_directory != Some(root_directory) {
                return Err(StorageError::Invariant(
                    "duplicate snapshot index disagrees with durable state".into(),
                ));
            }
            return Ok(());
        }
        if apply_index < state.applied_index {
            return Err(StorageError::Invariant(format!(
                "snapshot index {apply_index} is behind durable index {}",
                state.applied_index
            )));
        }
        if next_page_id.0 <= state.next_page_id.0
            || pages.is_empty()
            || pages
                .iter()
                .any(|page| page.id().0 < state.next_page_id.0 || page.id().0 >= next_page_id.0)
            || !pages.iter().any(|page| page.id() == root_directory)
        {
            return Err(StorageError::Invariant(
                "snapshot pages are not a fresh, bounded allocation range".into(),
            ));
        }
        pages.sort_by_key(Page::id);
        if pages.windows(2).any(|pair| pair[0].id() == pair[1].id()) {
            return Err(StorageError::Invariant(
                "snapshot contains duplicate page ids".into(),
            ));
        }
        for page in &mut pages {
            if page.id().0 < 2 {
                return Err(StorageError::Invariant(
                    "snapshot may not overwrite superblocks".into(),
                ));
            }
            page.set_lsn(Lsn(0));
            page.set_page_epoch(apply_index);
            page.validate()?;
            self.disk.write_page(page.id(), page.as_bytes())?;
        }
        // Copy-on-write pages become durable before the alternate superblock
        // can make them reachable. A crash before this sync keeps the old root.
        self.disk.sync()?;
        *state = publish_superblock(
            self.disk.as_ref(),
            &state,
            apply_index,
            state_hash,
            // The installed image is self-contained and does not depend on an
            // older WAL checkpoint marker.
            Lsn(0),
            next_page_id,
            Some(root_directory),
        )?;
        Ok(())
    }

    /// Complete the checkpoint durability sequence. WAL prefix retirement is
    /// intentionally left to a generation-aware log recycler; this method
    /// never renumbers byte LSNs or risks skipping newer pages.
    pub fn checkpoint(&self) -> Result<Checkpoint> {
        let mut state = self.state.lock();
        self.disk.sync()?;
        let through_lsn = self
            .wal
            .scan()?
            .groups
            .last()
            .map_or(Lsn(0), |group| group.end_lsn);
        let marker =
            self.wal
                .append_checkpoint(through_lsn, state.applied_index, state.state_hash)?;
        *state = publish_superblock(
            self.disk.as_ref(),
            &state,
            state.applied_index,
            state.state_hash,
            marker.end_lsn,
            state.next_page_id,
            state.root_directory,
        )?;
        Ok(marker)
    }

    pub fn superblock(&self) -> Result<Superblock> {
        Ok(self.state.lock().clone())
    }

    #[must_use]
    pub fn wal(&self) -> &Arc<WriteAheadLog<W>> {
        &self.wal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        disk::{Disk, Fault, FaultAction, FaultyFile, IoOperation, MemoryDisk},
        page::PageKind,
        wal::{FaultyWal, MemoryWal},
    };

    fn initialized_disk() -> MemoryDisk {
        let disk = MemoryDisk::default();
        let first = Superblock::genesis();
        let first_page = first.encode().unwrap();
        disk.write_page(first_page.id(), first_page.as_bytes())
            .unwrap();
        disk.sync().unwrap();
        let second = Superblock {
            slot: 1,
            generation: 2,
            ..first
        };
        let second_page = second.encode().unwrap();
        disk.write_page(second_page.id(), second_page.as_bytes())
            .unwrap();
        disk.sync().unwrap();
        disk.clear_events();
        disk
    }

    #[test]
    fn durable_apply_survives_crash_and_reopens_idempotently() {
        let disk = Arc::new(MemoryDisk::default());
        let wal = Arc::new(MemoryWal::default());
        let (pager, _) = DurablePager::open(Arc::clone(&disk), Arc::clone(&wal)).unwrap();
        let page = Page::new(PageId(2), PageKind::Heap);
        pager
            .apply(1, [7; 32], vec![page], PageId(3), None)
            .unwrap();
        disk.crash();
        wal.crash();
        let (reopened, report) = DurablePager::open(disk, wal).unwrap();
        assert_eq!(reopened.superblock().unwrap().applied_index, 1);
        assert_eq!(report.groups_replayed, 0);
    }

    #[test]
    fn recovery_repairs_page_write_missing_after_wal_commit() {
        let disk = Arc::new(MemoryDisk::default());
        let wal_io = Arc::new(MemoryWal::default());
        let wal = WriteAheadLog::open(Arc::clone(&wal_io)).unwrap();
        wal.append_apply(1, [4; 32], vec![Page::new(PageId(2), PageKind::Heap)])
            .unwrap();
        wal_io.crash();
        let scan = wal.scan().unwrap();
        let (state, report) = recover(disk.as_ref(), &scan).unwrap();
        assert_eq!(state.applied_index, 1);
        assert_eq!(report.pages_replayed, 1);
        Page::decode(disk.read_page(PageId(2)).unwrap()).unwrap();
    }

    #[test]
    fn checkpoint_is_fsynced_and_bound_to_published_superblock() {
        let disk = Arc::new(MemoryDisk::default());
        let wal = Arc::new(MemoryWal::default());
        let (pager, _) = DurablePager::open(Arc::clone(&disk), Arc::clone(&wal)).unwrap();
        pager
            .apply(
                1,
                [6; 32],
                vec![Page::new(PageId(2), PageKind::TreeDirectory)],
                PageId(3),
                Some(PageId(2)),
            )
            .unwrap();
        let checkpoint = pager.checkpoint().unwrap();
        assert_eq!(
            pager.superblock().unwrap().checkpoint_lsn,
            checkpoint.end_lsn
        );
        drop(pager);
        disk.crash();
        wal.crash();
        let (reopened, _) = DurablePager::open(disk, wal).unwrap();
        let state = reopened.superblock().unwrap();
        assert_eq!(state.applied_index, 1);
        assert_eq!(state.root_directory, Some(PageId(2)));
        assert_eq!(state.checkpoint_lsn, checkpoint.end_lsn);
    }

    #[test]
    fn apply_after_checkpoint_clears_exact_checkpoint_binding() {
        let disk = Arc::new(MemoryDisk::default());
        let wal = Arc::new(MemoryWal::default());
        let (pager, _) = DurablePager::open(Arc::clone(&disk), Arc::clone(&wal)).unwrap();
        pager
            .apply(
                1,
                [6; 32],
                vec![Page::new(PageId(2), PageKind::TreeDirectory)],
                PageId(3),
                Some(PageId(2)),
            )
            .unwrap();
        assert_ne!(pager.checkpoint().unwrap().end_lsn, Lsn(0));
        pager
            .apply(
                2,
                [7; 32],
                vec![Page::new(PageId(2), PageKind::TreeDirectory)],
                PageId(3),
                Some(PageId(2)),
            )
            .unwrap();
        assert_eq!(pager.superblock().unwrap().checkpoint_lsn, Lsn(0));
        drop(pager);
        disk.crash();
        wal.crash();
        let (reopened, _) = DurablePager::open(disk, wal).unwrap();
        assert_eq!(reopened.superblock().unwrap().applied_index, 2);
        assert_eq!(reopened.superblock().unwrap().state_hash, [7; 32]);
    }

    #[test]
    fn snapshot_install_faults_keep_old_superblock_visible() {
        let failpoints = [
            (IoOperation::Write, 1, FaultAction::Error, "first page"),
            (
                IoOperation::Write,
                2,
                FaultAction::TornWrite(137),
                "torn second page",
            ),
            (IoOperation::Sync, 1, FaultAction::Error, "page fsync"),
            (
                IoOperation::Write,
                3,
                FaultAction::TornWrite(211),
                "torn superblock",
            ),
            (IoOperation::Sync, 2, FaultAction::Error, "superblock fsync"),
        ];
        for (operation, relative_ordinal, action, label) in failpoints {
            let disk = Arc::new(FaultyFile::new(initialized_disk()));
            let wal = Arc::new(MemoryWal::default());
            let (pager, _) = DurablePager::open(Arc::clone(&disk), Arc::clone(&wal)).unwrap();
            let prior_matching = u64::try_from(
                disk.events()
                    .iter()
                    .filter(|event| event.operation == operation)
                    .count(),
            )
            .unwrap();
            disk.push_fault(Fault {
                operation,
                matching_ordinal: prior_matching + relative_ordinal,
                action,
            });
            let result = pager.install_snapshot(
                8,
                [8; 32],
                vec![
                    Page::new(PageId(2), PageKind::TreeDirectory),
                    Page::new(PageId(3), PageKind::Heap),
                ],
                PageId(4),
                PageId(2),
            );
            assert!(result.is_err(), "{label}");
            drop(pager);
            disk.inner().crash();
            wal.crash();
            let (reopened, _) = DurablePager::open(disk, wal).unwrap();
            assert_eq!(reopened.superblock().unwrap().applied_index, 0, "{label}");
        }
    }

    #[test]
    fn wal_failure_before_fsync_never_publishes_apply() {
        for short_append in 1..=3 {
            let disk = Arc::new(initialized_disk());
            let wal_io = Arc::new(FaultyWal::new(MemoryWal::default()));
            wal_io.short_append_at(short_append, 17);
            let (pager, _) = DurablePager::open(Arc::clone(&disk), Arc::clone(&wal_io)).unwrap();
            let result = pager.apply(
                1,
                [u8::try_from(short_append).unwrap(); 32],
                vec![Page::new(PageId(2), PageKind::Heap)],
                PageId(3),
                Some(PageId(2)),
            );
            assert!(result.is_err(), "append failpoint {short_append}");
            drop(pager);
            disk.crash();
            wal_io.inner().crash();
            let (reopened, _) = DurablePager::open(disk, wal_io).unwrap();
            assert_eq!(reopened.superblock().unwrap().applied_index, 0);
        }

        let disk = Arc::new(initialized_disk());
        let wal_io = Arc::new(FaultyWal::new(MemoryWal::default()));
        wal_io.fail_sync_at(1);
        let (pager, _) = DurablePager::open(Arc::clone(&disk), Arc::clone(&wal_io)).unwrap();
        assert!(
            pager
                .apply(
                    1,
                    [9; 32],
                    vec![Page::new(PageId(2), PageKind::Heap)],
                    PageId(3),
                    None,
                )
                .is_err()
        );
        drop(pager);
        disk.crash();
        wal_io.inner().crash();
        let (reopened, _) = DurablePager::open(disk, wal_io).unwrap();
        assert_eq!(reopened.superblock().unwrap().applied_index, 0);
    }

    #[test]
    fn every_post_wal_disk_failure_recovers_committed_group() {
        let failpoints = [
            (IoOperation::Write, 1, "data-page write"),
            (IoOperation::Sync, 1, "data-page fsync"),
            (IoOperation::Write, 2, "superblock write"),
            (IoOperation::Sync, 2, "superblock fsync"),
        ];
        for (operation, matching_ordinal, label) in failpoints {
            let disk = Arc::new(FaultyFile::new(initialized_disk()));
            disk.push_fault(Fault {
                operation,
                matching_ordinal,
                action: FaultAction::Error,
            });
            let wal = Arc::new(MemoryWal::default());
            let (pager, _) = DurablePager::open(Arc::clone(&disk), Arc::clone(&wal)).unwrap();
            let result = pager.apply(
                1,
                [5; 32],
                vec![Page::new(PageId(2), PageKind::Heap)],
                PageId(3),
                Some(PageId(2)),
            );
            assert!(result.is_err(), "{label}");
            drop(pager);
            disk.inner().crash();
            wal.crash();
            let (reopened, report) =
                DurablePager::open(Arc::clone(&disk), Arc::clone(&wal)).unwrap();
            let recovered = reopened.superblock().unwrap();
            assert_eq!(recovered.applied_index, 1, "{label}");
            assert_eq!(recovered.next_page_id, PageId(3), "{label}");
            assert_eq!(recovered.root_directory, Some(PageId(2)), "{label}");
            assert_eq!(report.groups_replayed, 1, "{label}");
            Page::decode(disk.read_page(PageId(2)).unwrap()).unwrap();
        }
    }
}
