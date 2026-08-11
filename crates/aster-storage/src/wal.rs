use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::Arc,
};

use parking_lot::Mutex;

use crate::{
    Lsn, PageId, Result, StorageError,
    buffer::WalSync,
    checksum::{crc32, sha256},
    page::{PAGE_SIZE, Page},
};

const FRAME_MAGIC: [u8; 4] = *b"ASWL";
const WAL_VERSION: u16 = 1;
const FRAME_FIXED: usize = 40;
const MAX_FRAME: usize = PAGE_SIZE + 256;

pub trait WalIo: Send + Sync + 'static {
    fn len(&self) -> Result<u64>;
    fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }
    fn read_all(&self) -> Result<Vec<u8>>;
    fn append(&self, bytes: &[u8]) -> Result<()>;
    fn sync(&self) -> Result<()>;
    fn truncate(&self, length: u64) -> Result<()>;
}

pub struct FileWal {
    file: Mutex<File>,
}

impl FileWal {
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

impl WalIo for FileWal {
    fn len(&self) -> Result<u64> {
        Ok(self.file.lock().metadata()?.len())
    }

    fn read_all(&self) -> Result<Vec<u8>> {
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    fn append(&self, bytes: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        file.seek(SeekFrom::End(0))?;
        file.write_all(bytes)?;
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        self.file.lock().sync_data()?;
        Ok(())
    }

    fn truncate(&self, length: u64) -> Result<()> {
        let mut file = self.file.lock();
        file.set_len(length)?;
        file.seek(SeekFrom::Start(length))?;
        Ok(())
    }
}

/// Byte-accurate volatile/durable WAL model for exhaustive crash-boundary
/// testing.
#[derive(Default)]
pub struct MemoryWal {
    state: Mutex<MemoryWalState>,
}

#[derive(Default)]
struct MemoryWalState {
    volatile: Vec<u8>,
    durable: Vec<u8>,
    append_sizes: Vec<usize>,
    sync_count: u64,
}

impl MemoryWal {
    pub fn crash(&self) {
        let mut state = self.state.lock();
        let MemoryWalState {
            volatile, durable, ..
        } = &mut *state;
        volatile.clone_from(durable);
    }

    #[must_use]
    pub fn bytes(&self) -> Vec<u8> {
        self.state.lock().volatile.clone()
    }

    pub fn replace_bytes(&self, bytes: Vec<u8>, durable: bool) {
        let mut state = self.state.lock();
        state.volatile.clone_from(&bytes);
        if durable {
            state.durable = bytes;
        }
    }

    #[must_use]
    pub fn append_sizes(&self) -> Vec<usize> {
        self.state.lock().append_sizes.clone()
    }
}

impl WalIo for MemoryWal {
    fn len(&self) -> Result<u64> {
        u64::try_from(self.state.lock().volatile.len()).map_err(|_| StorageError::CorruptWal {
            offset: u64::MAX,
            reason: "WAL length exceeds u64".into(),
        })
    }

    fn read_all(&self) -> Result<Vec<u8>> {
        Ok(self.state.lock().volatile.clone())
    }

    fn append(&self, bytes: &[u8]) -> Result<()> {
        let mut state = self.state.lock();
        state.volatile.extend_from_slice(bytes);
        state.append_sizes.push(bytes.len());
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        let mut state = self.state.lock();
        let MemoryWalState {
            volatile, durable, ..
        } = &mut *state;
        durable.clone_from(volatile);
        state.sync_count += 1;
        Ok(())
    }

    fn truncate(&self, length: u64) -> Result<()> {
        let length = usize::try_from(length).map_err(|_| StorageError::CorruptWal {
            offset: length,
            reason: "length overflow".into(),
        })?;
        let mut state = self.state.lock();
        if length > state.volatile.len() {
            return Err(StorageError::CorruptWal {
                offset: length as u64,
                reason: "cannot extend WAL through truncate".into(),
            });
        }
        state.volatile.truncate(length);
        Ok(())
    }
}

/// Fault injection for WAL byte writes. A short append writes a deterministic
/// prefix then returns an error, exactly modeling process-visible short I/O.
pub struct FaultyWal<W: WalIo> {
    inner: W,
    state: Mutex<WalFaultState>,
}

#[derive(Default)]
struct WalFaultState {
    append_ordinal: u64,
    sync_ordinal: u64,
    short_append: Option<(u64, usize)>,
    fail_sync: Option<u64>,
}

impl<W: WalIo> FaultyWal<W> {
    #[must_use]
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            state: Mutex::new(WalFaultState::default()),
        }
    }

    pub fn short_append_at(&self, ordinal: u64, bytes: usize) {
        self.state.lock().short_append = Some((ordinal, bytes));
    }

    pub fn fail_sync_at(&self, ordinal: u64) {
        self.state.lock().fail_sync = Some(ordinal);
    }

    #[must_use]
    pub const fn inner(&self) -> &W {
        &self.inner
    }
}

impl<W: WalIo> WalIo for FaultyWal<W> {
    fn len(&self) -> Result<u64> {
        self.inner.len()
    }

    fn read_all(&self) -> Result<Vec<u8>> {
        self.inner.read_all()
    }

    fn append(&self, bytes: &[u8]) -> Result<()> {
        let action = {
            let mut state = self.state.lock();
            state.append_ordinal += 1;
            let ordinal = state.append_ordinal;
            state
                .short_append
                .take_if(|(target, _)| *target == ordinal)
                .map(|(_, length)| (ordinal, length))
        };
        if let Some((ordinal, length)) = action {
            self.inner.append(&bytes[..length.min(bytes.len())])?;
            return Err(StorageError::InjectedFault {
                operation: "WAL short append".into(),
                ordinal,
            });
        }
        self.inner.append(bytes)
    }

    fn sync(&self) -> Result<()> {
        let fail = {
            let mut state = self.state.lock();
            state.sync_ordinal += 1;
            let ordinal = state.sync_ordinal;
            state
                .fail_sync
                .take_if(|target| *target == ordinal)
                .map(|_| ordinal)
        };
        if let Some(ordinal) = fail {
            return Err(StorageError::InjectedFault {
                operation: "WAL sync".into(),
                ordinal,
            });
        }
        self.inner.sync()
    }

    fn truncate(&self, length: u64) -> Result<()> {
        self.inner.truncate(length)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FrameKind {
    BeginApply = 1,
    PageImage = 2,
    CommitApply = 3,
    Checkpoint = 4,
}

impl TryFrom<u8> for FrameKind {
    type Error = String;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::BeginApply),
            2 => Ok(Self::PageImage),
            3 => Ok(Self::CommitApply),
            4 => Ok(Self::Checkpoint),
            _ => Err(format!("unknown WAL frame kind {value}")),
        }
    }
}

#[derive(Clone, Debug)]
struct Frame {
    kind: FrameKind,
    lsn: Lsn,
    apply_index: u64,
    payload: Vec<u8>,
    end: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedApply {
    pub apply_index: u64,
    pub state_hash: [u8; 32],
    /// Allocator and root-directory metadata are part of the WAL commit
    /// digest, so recovery cannot publish page images with stale roots.
    pub next_page_id: PageId,
    pub root_directory: Option<PageId>,
    pub pages: Vec<Page>,
    pub begin_lsn: Lsn,
    pub commit_lsn: Lsn,
    pub end_lsn: Lsn,
    pub digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Checkpoint {
    pub frame_lsn: Lsn,
    pub through_lsn: Lsn,
    pub applied_index: u64,
    pub state_hash: [u8; 32],
    pub end_lsn: Lsn,
}

#[derive(Clone, Debug, Default)]
pub struct WalScan {
    pub groups: Vec<CommittedApply>,
    pub checkpoints: Vec<Checkpoint>,
    /// Last byte that is safe to retain before appending. Any complete but
    /// uncommitted group after this boundary is discarded.
    pub safe_append_offset: u64,
    pub torn_tail_at: Option<u64>,
}

pub struct WriteAheadLog<W: WalIo> {
    io: Arc<W>,
    append_lock: Mutex<()>,
    durable_end: Mutex<Lsn>,
}

impl<W: WalIo> WriteAheadLog<W> {
    pub fn open(io: Arc<W>) -> Result<Self> {
        let scan = scan_bytes(&io.read_all()?)?;
        if io.len()? != scan.safe_append_offset {
            io.truncate(scan.safe_append_offset)?;
        }
        Ok(Self {
            io,
            append_lock: Mutex::new(()),
            durable_end: Mutex::new(Lsn(scan.safe_append_offset)),
        })
    }

    #[must_use]
    pub fn io(&self) -> &Arc<W> {
        &self.io
    }

    pub fn scan(&self) -> Result<WalScan> {
        scan_bytes(&self.io.read_all()?)
    }

    /// Append and durably commit one serialized state-machine apply. Repeating
    /// the same `(index, hash)` is an idempotent no-op; reusing an index with a
    /// different hash is rejected.
    pub fn append_apply(
        &self,
        apply_index: u64,
        state_hash: [u8; 32],
        pages: Vec<Page>,
    ) -> Result<CommittedApply> {
        let next_page_id = PageId(
            pages
                .iter()
                .map(Page::id)
                .map(|id| id.0)
                .max()
                .unwrap_or(1)
                .checked_add(1)
                .ok_or_else(|| StorageError::Invariant("page identifier space exhausted".into()))?
                .max(2),
        );
        self.append_apply_with_metadata(apply_index, state_hash, next_page_id, None, pages)
    }

    #[allow(clippy::too_many_lines)]
    pub fn append_apply_with_metadata(
        &self,
        apply_index: u64,
        state_hash: [u8; 32],
        next_page_id: PageId,
        root_directory: Option<PageId>,
        mut pages: Vec<Page>,
    ) -> Result<CommittedApply> {
        let _guard = self.append_lock.lock();
        let scan = scan_bytes(&self.io.read_all()?)?;
        if self.io.len()? != scan.safe_append_offset {
            self.io.truncate(scan.safe_append_offset)?;
        }
        if let Some(existing) = scan
            .groups
            .iter()
            .find(|group| group.apply_index == apply_index)
        {
            if existing.state_hash == state_hash {
                return Ok(existing.clone());
            }
            return Err(StorageError::Invariant(format!(
                "Raft apply index {apply_index} already committed with a different state hash"
            )));
        }
        if let Some(last) = scan.groups.last()
            && apply_index <= last.apply_index
        {
            return Err(StorageError::Invariant(format!(
                "apply index {apply_index} does not advance committed index {}",
                last.apply_index
            )));
        }
        pages.sort_by_key(Page::id);
        if next_page_id.0 < 2
            || pages.iter().any(|page| page.id().0 >= next_page_id.0)
            || root_directory.is_some_and(|root| root.0 >= next_page_id.0)
        {
            return Err(StorageError::Invariant(
                "WAL group has invalid allocator/root metadata".into(),
            ));
        }
        let mut ids = BTreeSet::new();
        for page in &pages {
            page.validate()?;
            if !ids.insert(page.id()) {
                return Err(StorageError::Invariant(format!(
                    "duplicate page {} in WAL group",
                    page.id().0
                )));
            }
        }

        let mut offset = self.io.len()?;
        let begin_lsn = Lsn(offset);
        let mut begin_payload = Vec::with_capacity(48);
        begin_payload.extend_from_slice(&state_hash);
        begin_payload.extend_from_slice(&next_page_id.0.to_le_bytes());
        begin_payload
            .extend_from_slice(&root_directory.map_or(u64::MAX, |root| root.0).to_le_bytes());
        let begin = encode_frame(
            FrameKind::BeginApply,
            begin_lsn,
            apply_index,
            &begin_payload,
        )?;
        self.io.append(&begin)?;
        offset += begin.len() as u64;

        let mut durable_pages = Vec::with_capacity(pages.len());
        let mut digest_material = Vec::with_capacity(64 + pages.len() * (PAGE_SIZE + 8));
        digest_material.extend_from_slice(&apply_index.to_le_bytes());
        digest_material.extend_from_slice(&begin_payload);
        for mut page in pages {
            let page_lsn = Lsn(offset);
            page.set_lsn(page_lsn);
            page.set_page_epoch(apply_index);
            page.seal();
            let mut payload = Vec::with_capacity(8 + PAGE_SIZE);
            payload.extend_from_slice(&page.id().0.to_le_bytes());
            payload.extend_from_slice(page.as_bytes());
            digest_material.extend_from_slice(&payload);
            let frame = encode_frame(FrameKind::PageImage, page_lsn, apply_index, &payload)?;
            self.io.append(&frame)?;
            offset += frame.len() as u64;
            durable_pages.push(page);
        }

        let digest = sha256(&digest_material);
        let commit_lsn = Lsn(offset);
        let mut commit_payload = Vec::with_capacity(36);
        let page_count =
            u32::try_from(durable_pages.len()).map_err(|_| StorageError::CorruptWal {
                offset: commit_lsn.0,
                reason: "apply group page count exceeds u32".into(),
            })?;
        commit_payload.extend_from_slice(&page_count.to_le_bytes());
        commit_payload.extend_from_slice(&digest);
        let commit = encode_frame(
            FrameKind::CommitApply,
            commit_lsn,
            apply_index,
            &commit_payload,
        )?;
        self.io.append(&commit)?;
        offset += commit.len() as u64;
        self.io.sync()?;
        *self.durable_end.lock() = Lsn(offset);
        Ok(CommittedApply {
            apply_index,
            state_hash,
            next_page_id,
            root_directory,
            pages: durable_pages,
            begin_lsn,
            commit_lsn,
            end_lsn: Lsn(offset),
            digest,
        })
    }

    /// Append a checkpoint only after the caller has synced all data pages
    /// through `through_lsn`. Recovery may ignore older redo groups.
    pub fn append_checkpoint(
        &self,
        through_lsn: Lsn,
        applied_index: u64,
        state_hash: [u8; 32],
    ) -> Result<Checkpoint> {
        let _guard = self.append_lock.lock();
        let scan = scan_bytes(&self.io.read_all()?)?;
        if self.io.len()? != scan.safe_append_offset {
            self.io.truncate(scan.safe_append_offset)?;
        }
        if through_lsn.0 > scan.safe_append_offset {
            return Err(StorageError::Invariant(format!(
                "checkpoint LSN {} exceeds WAL end {}",
                through_lsn.0, scan.safe_append_offset
            )));
        }
        if let Some(group) = scan.groups.last()
            && (applied_index != group.apply_index || state_hash != group.state_hash)
        {
            return Err(StorageError::Invariant(
                "checkpoint applied index/hash does not match WAL prefix".into(),
            ));
        }
        let frame_lsn = Lsn(self.io.len()?);
        let mut payload = Vec::with_capacity(48);
        payload.extend_from_slice(&through_lsn.0.to_le_bytes());
        payload.extend_from_slice(&applied_index.to_le_bytes());
        payload.extend_from_slice(&state_hash);
        let frame = encode_frame(FrameKind::Checkpoint, frame_lsn, applied_index, &payload)?;
        self.io.append(&frame)?;
        let end_lsn = Lsn(frame_lsn.0 + frame.len() as u64);
        self.io.sync()?;
        *self.durable_end.lock() = end_lsn;
        Ok(Checkpoint {
            frame_lsn,
            through_lsn,
            applied_index,
            state_hash,
            end_lsn,
        })
    }
}

impl<W: WalIo> WalSync for WriteAheadLog<W> {
    fn flush_through(&self, lsn: Lsn) -> Result<()> {
        if self.durable_end.lock().0 >= lsn.0 {
            return Ok(());
        }
        self.io.sync()?;
        *self.durable_end.lock() = Lsn(self.io.len()?);
        if self.durable_end.lock().0 < lsn.0 {
            return Err(StorageError::Invariant(format!(
                "requested WAL LSN {} beyond end",
                lsn.0
            )));
        }
        Ok(())
    }
}

fn encode_frame(kind: FrameKind, lsn: Lsn, apply_index: u64, payload: &[u8]) -> Result<Vec<u8>> {
    let total_len =
        FRAME_FIXED
            .checked_add(payload.len())
            .ok_or_else(|| StorageError::CorruptWal {
                offset: lsn.0,
                reason: "frame length overflow".into(),
            })?;
    if total_len > MAX_FRAME {
        return Err(StorageError::CorruptWal {
            offset: lsn.0,
            reason: format!("frame length {total_len} exceeds limit {MAX_FRAME}"),
        });
    }
    let total_u32 = u32::try_from(total_len).map_err(|_| StorageError::CorruptWal {
        offset: lsn.0,
        reason: "frame length exceeds u32".into(),
    })?;
    let mut bytes = Vec::with_capacity(total_len);
    bytes.extend_from_slice(&total_u32.to_le_bytes());
    bytes.extend_from_slice(&FRAME_MAGIC);
    bytes.extend_from_slice(&WAL_VERSION.to_le_bytes());
    bytes.push(kind as u8);
    bytes.push(0);
    bytes.extend_from_slice(&lsn.0.to_le_bytes());
    bytes.extend_from_slice(&apply_index.to_le_bytes());
    let payload_u32 = u32::try_from(payload.len()).map_err(|_| StorageError::CorruptWal {
        offset: lsn.0,
        reason: "frame payload exceeds u32".into(),
    })?;
    bytes.extend_from_slice(&payload_u32.to_le_bytes());
    bytes.extend_from_slice(payload);
    let checksum = crc32(&bytes[4..]);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    bytes.extend_from_slice(&total_u32.to_le_bytes());
    debug_assert_eq!(bytes.len(), total_len);
    Ok(bytes)
}

#[allow(clippy::too_many_lines)]
fn decode_frames(bytes: &[u8]) -> Result<(Vec<Frame>, Option<u64>)> {
    let mut frames = Vec::new();
    let mut offset = 0_usize;
    while offset < bytes.len() {
        if bytes.len() - offset < 4 {
            return Ok((frames, Some(offset as u64)));
        }
        let length = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        if !(FRAME_FIXED..=MAX_FRAME).contains(&length) {
            if offset + 4 == bytes.len() {
                return Ok((frames, Some(offset as u64)));
            }
            return Err(StorageError::CorruptWal {
                offset: offset as u64,
                reason: format!("invalid frame length {length}"),
            });
        }
        let Some(end) = offset.checked_add(length) else {
            return Err(StorageError::CorruptWal {
                offset: offset as u64,
                reason: "frame length overflow".into(),
            });
        };
        if end > bytes.len() {
            return Ok((frames, Some(offset as u64)));
        }
        let frame = &bytes[offset..end];
        if frame[4..8] != FRAME_MAGIC {
            return Err(StorageError::CorruptWal {
                offset: offset as u64,
                reason: "bad frame magic".into(),
            });
        }
        let version = u16::from_le_bytes([frame[8], frame[9]]);
        if version != WAL_VERSION {
            return Err(StorageError::CorruptWal {
                offset: offset as u64,
                reason: format!("unsupported WAL version {version}"),
            });
        }
        if frame[11] != 0 {
            return Err(StorageError::CorruptWal {
                offset: offset as u64,
                reason: "nonzero reserved frame flags".into(),
            });
        }
        let kind = FrameKind::try_from(frame[10]).map_err(|reason| StorageError::CorruptWal {
            offset: offset as u64,
            reason,
        })?;
        let mut encoded_lsn = [0; 8];
        encoded_lsn.copy_from_slice(&frame[12..20]);
        let lsn = Lsn(u64::from_le_bytes(encoded_lsn));
        if lsn.0 != offset as u64 {
            return Err(StorageError::CorruptWal {
                offset: offset as u64,
                reason: format!("frame encodes LSN {}, expected {offset}", lsn.0),
            });
        }
        let mut encoded_index = [0; 8];
        encoded_index.copy_from_slice(&frame[20..28]);
        let apply_index = u64::from_le_bytes(encoded_index);
        let payload_len = u32::from_le_bytes([frame[28], frame[29], frame[30], frame[31]]) as usize;
        if payload_len.checked_add(FRAME_FIXED) != Some(length) {
            return Err(StorageError::CorruptWal {
                offset: offset as u64,
                reason: "payload length disagrees with frame".into(),
            });
        }
        let checksum_at = 32 + payload_len;
        let expected = u32::from_le_bytes([
            frame[checksum_at],
            frame[checksum_at + 1],
            frame[checksum_at + 2],
            frame[checksum_at + 3],
        ]);
        let actual = crc32(&frame[4..checksum_at]);
        let footer = u32::from_le_bytes([
            frame[checksum_at + 4],
            frame[checksum_at + 5],
            frame[checksum_at + 6],
            frame[checksum_at + 7],
        ]);
        if expected != actual || footer != u32::try_from(length).unwrap_or(u32::MAX) {
            if end == bytes.len() {
                return Ok((frames, Some(offset as u64)));
            }
            return Err(StorageError::CorruptWal {
                offset: offset as u64,
                reason: "checksum or trailing-length mismatch".into(),
            });
        }
        frames.push(Frame {
            kind,
            lsn,
            apply_index,
            payload: frame[32..checksum_at].to_vec(),
            end: end as u64,
        });
        offset = end;
    }
    Ok((frames, None))
}

struct PendingApply {
    index: u64,
    state_hash: [u8; 32],
    next_page_id: PageId,
    root_directory: Option<PageId>,
    begin_lsn: Lsn,
    pages: Vec<Page>,
    material: Vec<u8>,
}

#[allow(clippy::too_many_lines)]
pub fn scan_bytes(bytes: &[u8]) -> Result<WalScan> {
    let (frames, torn_tail_at) = decode_frames(bytes)?;
    let mut scan = WalScan {
        torn_tail_at,
        ..WalScan::default()
    };
    let mut pending: Option<PendingApply> = None;
    for frame in frames {
        match frame.kind {
            FrameKind::BeginApply => {
                if frame.payload.len() != 48 {
                    return Err(StorageError::CorruptWal {
                        offset: frame.lsn.0,
                        reason: "BEGIN payload must be 48 bytes".into(),
                    });
                }
                if pending.is_some() {
                    // A previous complete-frame but uncommitted group is a
                    // crash tail. Never append through it.
                    break;
                }
                let mut state_hash = [0; 32];
                state_hash.copy_from_slice(&frame.payload[..32]);
                let mut encoded_next = [0; 8];
                encoded_next.copy_from_slice(&frame.payload[32..40]);
                let next_page_id = PageId(u64::from_le_bytes(encoded_next));
                let mut encoded_root = [0; 8];
                encoded_root.copy_from_slice(&frame.payload[40..48]);
                let root = u64::from_le_bytes(encoded_root);
                let root_directory = (root != u64::MAX).then_some(PageId(root));
                if next_page_id.0 < 2 || root_directory.is_some_and(|root| root.0 >= next_page_id.0)
                {
                    return Err(StorageError::CorruptWal {
                        offset: frame.lsn.0,
                        reason: "invalid BEGIN allocator/root metadata".into(),
                    });
                }
                let mut material = Vec::with_capacity(56);
                material.extend_from_slice(&frame.apply_index.to_le_bytes());
                material.extend_from_slice(&frame.payload);
                pending = Some(PendingApply {
                    index: frame.apply_index,
                    state_hash,
                    next_page_id,
                    root_directory,
                    begin_lsn: frame.lsn,
                    pages: Vec::new(),
                    material,
                });
            }
            FrameKind::PageImage => {
                let Some(group) = pending.as_mut() else {
                    return Err(StorageError::CorruptWal {
                        offset: frame.lsn.0,
                        reason: "PAGE_IMAGE outside apply group".into(),
                    });
                };
                if group.index != frame.apply_index || frame.payload.len() != 8 + PAGE_SIZE {
                    return Err(StorageError::CorruptWal {
                        offset: frame.lsn.0,
                        reason: "invalid PAGE_IMAGE group or length".into(),
                    });
                }
                let mut encoded_page_id = [0; 8];
                encoded_page_id.copy_from_slice(&frame.payload[0..8]);
                let page_id = PageId(u64::from_le_bytes(encoded_page_id));
                if page_id.0 >= group.next_page_id.0 {
                    return Err(StorageError::CorruptWal {
                        offset: frame.lsn.0,
                        reason: "PAGE_IMAGE exceeds group allocator high-water mark".into(),
                    });
                }
                let mut bytes = [0; PAGE_SIZE];
                bytes.copy_from_slice(&frame.payload[8..]);
                let page = Page::decode(bytes)?;
                if page.id() != page_id || page.lsn() != frame.lsn {
                    return Err(StorageError::CorruptWal {
                        offset: frame.lsn.0,
                        reason: "PAGE_IMAGE id or page LSN mismatch".into(),
                    });
                }
                if group.pages.iter().any(|existing| existing.id() == page_id) {
                    return Err(StorageError::CorruptWal {
                        offset: frame.lsn.0,
                        reason: "duplicate PAGE_IMAGE".into(),
                    });
                }
                group.material.extend_from_slice(&frame.payload);
                group.pages.push(page);
            }
            FrameKind::CommitApply => {
                let Some(group) = pending.take() else {
                    return Err(StorageError::CorruptWal {
                        offset: frame.lsn.0,
                        reason: "COMMIT outside apply group".into(),
                    });
                };
                if group.index != frame.apply_index || frame.payload.len() != 36 {
                    return Err(StorageError::CorruptWal {
                        offset: frame.lsn.0,
                        reason: "invalid COMMIT group or length".into(),
                    });
                }
                let count = u32::from_le_bytes([
                    frame.payload[0],
                    frame.payload[1],
                    frame.payload[2],
                    frame.payload[3],
                ]) as usize;
                let mut digest = [0; 32];
                digest.copy_from_slice(&frame.payload[4..36]);
                if count != group.pages.len() || digest != sha256(&group.material) {
                    return Err(StorageError::CorruptWal {
                        offset: frame.lsn.0,
                        reason: "COMMIT count or digest mismatch".into(),
                    });
                }
                if scan
                    .groups
                    .last()
                    .is_some_and(|last| last.apply_index >= group.index)
                {
                    return Err(StorageError::CorruptWal {
                        offset: frame.lsn.0,
                        reason: "non-monotonic apply index".into(),
                    });
                }
                scan.groups.push(CommittedApply {
                    apply_index: group.index,
                    state_hash: group.state_hash,
                    next_page_id: group.next_page_id,
                    root_directory: group.root_directory,
                    pages: group.pages,
                    begin_lsn: group.begin_lsn,
                    commit_lsn: frame.lsn,
                    end_lsn: Lsn(frame.end),
                    digest,
                });
                scan.safe_append_offset = frame.end;
            }
            FrameKind::Checkpoint => {
                if pending.is_some() {
                    break;
                }
                if frame.payload.len() != 48 {
                    return Err(StorageError::CorruptWal {
                        offset: frame.lsn.0,
                        reason: "checkpoint payload must be 48 bytes".into(),
                    });
                }
                let mut encoded_through = [0; 8];
                encoded_through.copy_from_slice(&frame.payload[0..8]);
                let through_lsn = Lsn(u64::from_le_bytes(encoded_through));
                let mut encoded_index = [0; 8];
                encoded_index.copy_from_slice(&frame.payload[8..16]);
                let applied_index = u64::from_le_bytes(encoded_index);
                let mut state_hash = [0; 32];
                state_hash.copy_from_slice(&frame.payload[16..48]);
                scan.checkpoints.push(Checkpoint {
                    frame_lsn: frame.lsn,
                    through_lsn,
                    applied_index,
                    state_hash,
                    end_lsn: Lsn(frame.end),
                });
                scan.safe_append_offset = frame.end;
            }
        }
    }
    Ok(scan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::PageKind;

    #[test]
    fn committed_group_round_trips_and_is_idempotent() {
        let io = Arc::new(MemoryWal::default());
        let wal = WriteAheadLog::open(Arc::clone(&io)).unwrap();
        let page = Page::new(PageId(9), PageKind::Heap);
        let group = wal.append_apply(7, [3; 32], vec![page]).unwrap();
        assert!(group.pages[0].lsn().0 > group.begin_lsn.0);
        let again = wal
            .append_apply(7, [3; 32], vec![Page::new(PageId(99), PageKind::Heap)])
            .unwrap();
        assert_eq!(again.digest, group.digest);
        assert!(wal.append_apply(7, [4; 32], Vec::new()).is_err());
        assert_eq!(wal.scan().unwrap().groups, vec![group]);
    }

    #[test]
    fn every_truncated_tail_exposes_only_committed_prefix() {
        let io = Arc::new(MemoryWal::default());
        let wal = WriteAheadLog::open(Arc::clone(&io)).unwrap();
        wal.append_apply(1, [1; 32], vec![Page::new(PageId(2), PageKind::Heap)])
            .unwrap();
        let first_end = io.bytes().len();
        wal.append_apply(2, [2; 32], vec![Page::new(PageId(3), PageKind::Heap)])
            .unwrap();
        let complete = io.bytes();
        for cut in 0..=complete.len() {
            let scan = scan_bytes(&complete[..cut]).unwrap();
            let expected = if cut < first_end {
                0
            } else if cut < complete.len() {
                1
            } else {
                2
            };
            assert_eq!(scan.groups.len(), expected, "cut={cut}");
        }
    }

    #[test]
    fn corruption_in_middle_is_not_a_torn_tail() {
        let io = Arc::new(MemoryWal::default());
        let wal = WriteAheadLog::open(Arc::clone(&io)).unwrap();
        wal.append_apply(1, [1; 32], vec![]).unwrap();
        wal.append_apply(2, [2; 32], vec![]).unwrap();
        let mut bytes = io.bytes();
        bytes[20] ^= 1;
        assert!(matches!(
            scan_bytes(&bytes),
            Err(StorageError::CorruptWal { .. })
        ));
    }
}
