use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

use aster_engine::{CommitResult, Engine, SnapshotRecord};
use aster_storage::btree::{BPlusTree, DiskPageStore, PageStore, ValidationReport};
use aster_storage::disk::{Disk, FileDisk};
use aster_storage::page::{Page, PageKind};
use aster_storage::recovery::{DurablePager, RecoveryReport, Superblock};
use aster_storage::wal::{Checkpoint, FileWal};
use aster_storage::{PageId, StorageError};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::MAX_SNAPSHOT_BYTES;
use crate::error::{DatabaseError, Result};

const DATA_FILE: &str = "aster.data";
const WAL_FILE: &str = "aster.wal";
const RECORD_MAGIC: &[u8; 8] = b"ASTREC\0\x01";
const PHYSICAL_KEY_MAGIC: &[u8; 8] = b"ASTKEY\0\x01";
const PHYSICAL_KEY_BYTES: usize = PHYSICAL_KEY_MAGIC.len() + 32 + 4;
const RECORD_HEADER_BYTES: usize = RECORD_MAGIC.len() + 1 + 4 + 8;
const REQUEST_RECORD_KIND: u8 = 240;
// Keep every physical entry below one third of a leaf's usable space. The
// storage tree's byte-balanced split can then always place both halves on a
// page even when neighboring records have very different sizes.
const CHUNK_BYTES: usize = 1_024;
const MAX_PHYSICAL_ENTRIES: usize = MAX_SNAPSHOT_BYTES.div_ceil(CHUNK_BYTES) + 4_096;

pub(crate) struct FilePersistence {
    disk: Arc<FileDisk>,
    pager: DurablePager<FileDisk, FileWal>,
    recovery: RecoveryReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DurableRequestRecord {
    pub sequence: u64,
    pub fingerprint: [u8; 32],
    pub result: CommitResult,
    pub affected_rows: u64,
}

pub(crate) type DurableRequests = BTreeMap<[u8; 16], DurableRequestRecord>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PersistenceStatus {
    pub superblock: Superblock,
    pub database_pages: u64,
    pub wal_bytes: u64,
    pub recovery: RecoveryReport,
}

impl FilePersistence {
    pub(crate) fn open(directory: impl AsRef<Path>) -> Result<(Self, Engine, DurableRequests)> {
        let directory = directory.as_ref().to_path_buf();
        fs::create_dir_all(&directory)?;
        let data_path = directory.join(DATA_FILE);
        let wal_path = directory.join(WAL_FILE);
        let data_was_missing = !data_path.exists();
        let wal_was_missing = !wal_path.exists();

        let disk = Arc::new(FileDisk::open(&data_path)?);
        let wal = Arc::new(FileWal::open(&wal_path)?);
        if data_was_missing || wal_was_missing {
            // File creation is not durable until the containing directory is
            // synced. This is supported on the Unix targets in scope.
            File::open(&directory)?.sync_all()?;
        }
        let (pager, recovery) = DurablePager::open(Arc::clone(&disk), wal)?;
        let persistence = Self {
            disk,
            pager,
            recovery,
        };
        let (engine, requests) = persistence.load_state()?;
        Ok((persistence, engine, requests))
    }

    pub(crate) fn persist_state(
        &self,
        engine: &Engine,
        requests: &DurableRequests,
    ) -> Result<ValidationReport> {
        let superblock = self.pager.superblock()?;
        let expected_index = superblock
            .applied_index
            .checked_add(1)
            .ok_or_else(|| DatabaseError::Invariant("applied index overflow".into()))?;
        if engine.last_applied() != expected_index {
            return Err(DatabaseError::Invariant(format!(
                "candidate engine is at index {}, durable pager expects {expected_index}",
                engine.last_applied()
            )));
        }
        let state_hash = engine
            .last_apply_hash()
            .ok_or_else(|| DatabaseError::Invariant("applied engine has no command hash".into()))?;
        let expected = encode_records(&state_records(engine, requests)?)?;

        let store = StagedPageStore::new(Arc::clone(&self.disk), superblock.next_page_id);
        let tree = if let Some(root) = superblock.root_directory {
            BPlusTree::open(store.clone(), root)?
        } else {
            BPlusTree::create(store.clone())?
        };
        let current: BTreeMap<_, _> = tree.range(&[], None)?.into_iter().collect();
        validate_physical_map(&current)?;

        for key in current.keys().filter(|key| !expected.contains_key(*key)) {
            let removed = tree.delete(key)?;
            if removed.is_none() {
                return Err(DatabaseError::Invariant(
                    "B+ tree key disappeared during snapshot replacement".into(),
                ));
            }
        }
        for (key, value) in &expected {
            if current.get(key) != Some(value) {
                tree.insert(key.clone(), value.clone())?;
            }
        }
        let validation = tree.validate()?;
        if validation.entries != expected.len() {
            return Err(DatabaseError::Invariant(format!(
                "B+ tree has {} entries after replacement, expected {}",
                validation.entries,
                expected.len()
            )));
        }
        let root = tree.root_page();
        let (pages, next_page_id) = store.finish()?;
        if pages.is_empty() {
            return Err(DatabaseError::Invariant(
                "advancing an apply index produced no durable page images".into(),
            ));
        }
        self.pager.apply(
            engine.last_applied(),
            state_hash,
            pages,
            next_page_id,
            Some(root),
        )?;
        Ok(validation)
    }

    /// Installs a logical snapshot with a copy-on-write page tree. Unlike a
    /// normal apply, this may jump across compacted Raft indexes. Publication
    /// remains atomic because all pages are freshly allocated and synced before
    /// the alternate superblock names their root.
    pub(crate) fn install_state(
        &self,
        engine: &Engine,
        requests: &DurableRequests,
    ) -> Result<ValidationReport> {
        validate_durable_requests(engine, requests)?;
        let superblock = self.pager.superblock()?;
        if engine.last_applied() <= superblock.applied_index {
            return Err(DatabaseError::Invariant(format!(
                "installed snapshot index {} must be newer than durable index {}",
                engine.last_applied(),
                superblock.applied_index
            )));
        }
        let state_hash = engine
            .last_apply_hash()
            .ok_or_else(|| DatabaseError::Invariant("snapshot engine has no apply hash".into()))?;
        let expected = encode_records(&state_records(engine, requests)?)?;
        let store = StagedPageStore::new(Arc::clone(&self.disk), superblock.next_page_id);
        let tree = BPlusTree::create(store.clone())?;
        for (key, value) in &expected {
            tree.insert(key.clone(), value.clone())?;
        }
        let validation = tree.validate()?;
        if validation.entries != expected.len() {
            return Err(DatabaseError::Invariant(format!(
                "snapshot tree has {} entries, expected {}",
                validation.entries,
                expected.len()
            )));
        }
        let root = tree.root_page();
        let (pages, next_page_id) = store.finish()?;
        self.pager.install_snapshot(
            engine.last_applied(),
            state_hash,
            pages,
            next_page_id,
            root,
        )?;
        Ok(validation)
    }

    pub(crate) fn checkpoint(&self) -> Result<Checkpoint> {
        Ok(self.pager.checkpoint()?)
    }

    pub(crate) fn status(&self) -> Result<PersistenceStatus> {
        let scan = self.pager.wal().scan()?;
        Ok(PersistenceStatus {
            superblock: self.pager.superblock()?,
            database_pages: self.disk.page_count()?,
            wal_bytes: scan.safe_append_offset,
            recovery: self.recovery.clone(),
        })
    }

    fn load_state(&self) -> Result<(Engine, DurableRequests)> {
        let superblock = self.pager.superblock()?;
        match (superblock.applied_index, superblock.root_directory) {
            (0, None) => return Ok((Engine::new(), BTreeMap::new())),
            (0, Some(_)) => {
                return Err(DatabaseError::Corruption(
                    "genesis superblock unexpectedly names a snapshot tree".into(),
                ));
            }
            (_, None) => {
                return Err(DatabaseError::Corruption(
                    "applied database has no snapshot-tree root".into(),
                ));
            }
            (_, Some(_)) => {}
        }
        let root = superblock
            .root_directory
            .ok_or_else(|| DatabaseError::Invariant("checked root is missing".into()))?;
        let store = DiskPageStore::open(Arc::clone(&self.disk))?;
        let tree = BPlusTree::open(store, root)?;
        let physical: BTreeMap<_, _> = tree.range(&[], None)?.into_iter().collect();
        let records = decode_records(&physical)?;
        let (engine_records, request_records): (Vec<_>, Vec<_>) = records
            .into_iter()
            .partition(|record| record.kind != REQUEST_RECORD_KIND);
        let engine =
            Engine::from_snapshot(aster_engine::EngineSnapshot::from_records(&engine_records)?)?;
        let requests = decode_request_records(&request_records, &engine)?;
        if engine.last_applied() != superblock.applied_index {
            return Err(DatabaseError::Corruption(format!(
                "engine snapshot index {} disagrees with superblock {}",
                engine.last_applied(),
                superblock.applied_index
            )));
        }
        if engine.last_apply_hash() != Some(superblock.state_hash) {
            return Err(DatabaseError::Corruption(
                "engine snapshot hash disagrees with superblock".into(),
            ));
        }
        Ok((engine, requests))
    }
}

fn encode_request_records(requests: &DurableRequests) -> Result<Vec<SnapshotRecord>> {
    requests
        .iter()
        .map(|(client_id, record)| {
            let value = serde_json::to_vec(record).map_err(|error| {
                DatabaseError::Invariant(format!("request record encode failed: {error}"))
            })?;
            Ok(SnapshotRecord {
                kind: REQUEST_RECORD_KIND,
                key: client_id.to_vec(),
                value,
            })
        })
        .collect()
}

fn state_records(engine: &Engine, requests: &DurableRequests) -> Result<Vec<SnapshotRecord>> {
    validate_durable_requests(engine, requests)?;
    let mut records = engine.snapshot().to_records()?;
    records.extend(encode_request_records(requests)?);
    Ok(records)
}

pub(crate) fn validate_durable_requests(engine: &Engine, requests: &DurableRequests) -> Result<()> {
    if engine.client_count() != requests.len() {
        return Err(DatabaseError::Corruption(format!(
            "engine has {} client records but snapshot has {} request records",
            engine.client_count(),
            requests.len()
        )));
    }
    for (client_id, request) in requests {
        let engine_record = engine.client_record(client_id).ok_or_else(|| {
            DatabaseError::Corruption(
                "request fingerprint has no matching engine client record".into(),
            )
        })?;
        if request.sequence != engine_record.sequence
            || request.fingerprint != engine_record.request_hash
            || request.result != engine_record.result
        {
            return Err(DatabaseError::Corruption(
                "request fingerprint disagrees with engine client result".into(),
            ));
        }
    }
    Ok(())
}

fn decode_request_records(records: &[SnapshotRecord], engine: &Engine) -> Result<DurableRequests> {
    let mut requests = BTreeMap::new();
    for record in records {
        if record.kind != REQUEST_RECORD_KIND || record.key.len() != 16 {
            return Err(DatabaseError::Corruption(
                "durable request record has an invalid kind or key".into(),
            ));
        }
        let mut client_id = [0; 16];
        client_id.copy_from_slice(&record.key);
        let request: DurableRequestRecord =
            serde_json::from_slice(&record.value).map_err(|error| {
                DatabaseError::Corruption(format!("request record decode failed: {error}"))
            })?;
        if requests.insert(client_id, request).is_some() {
            return Err(DatabaseError::Corruption(
                "duplicate durable request fingerprint".into(),
            ));
        }
    }
    validate_durable_requests(engine, &requests)?;
    Ok(requests)
}

#[derive(Clone)]
struct StagedPageStore {
    state: Arc<Mutex<StagedPageState>>,
}

struct StagedPageState {
    disk: Arc<FileDisk>,
    pages: BTreeMap<PageId, Page>,
    next_page_id: PageId,
    finished: bool,
}

impl StagedPageStore {
    fn new(disk: Arc<FileDisk>, next_page_id: PageId) -> Self {
        Self {
            state: Arc::new(Mutex::new(StagedPageState {
                disk,
                pages: BTreeMap::new(),
                next_page_id,
                finished: false,
            })),
        }
    }

    fn finish(&self) -> Result<(Vec<Page>, PageId)> {
        let mut state = self.state.lock();
        if state.finished {
            return Err(DatabaseError::Invariant(
                "staged page store was already consumed".into(),
            ));
        }
        state.finished = true;
        Ok((
            std::mem::take(&mut state.pages).into_values().collect(),
            state.next_page_id,
        ))
    }
}

impl PageStore for StagedPageStore {
    fn load(&self, page_id: PageId) -> aster_storage::Result<Page> {
        let state = self.state.lock();
        if let Some(page) = state.pages.get(&page_id) {
            return Ok(page.clone());
        }
        let page = Page::decode(state.disk.read_page(page_id)?)?;
        if page.id() != page_id {
            return Err(StorageError::InvalidPage(format!(
                "physical page {} contains page {}",
                page_id.0,
                page.id().0
            )));
        }
        Ok(page)
    }

    fn save(&self, page: &Page) -> aster_storage::Result<()> {
        page.validate()?;
        let mut state = self.state.lock();
        if state.finished {
            return Err(StorageError::Invariant(
                "cannot save after staged page store is consumed".into(),
            ));
        }
        state.pages.insert(page.id(), page.clone());
        Ok(())
    }

    fn allocate(&self, _kind: PageKind) -> aster_storage::Result<PageId> {
        let mut state = self.state.lock();
        if state.finished {
            return Err(StorageError::Invariant(
                "cannot allocate after staged page store is consumed".into(),
            ));
        }
        let allocated = state.next_page_id;
        state.next_page_id = PageId(
            allocated
                .0
                .checked_add(1)
                .ok_or_else(|| StorageError::Invariant("page id space exhausted".into()))?,
        );
        Ok(allocated)
    }
}

fn encode_records(records: &[SnapshotRecord]) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
    let mut output = BTreeMap::new();
    let mut total_bytes = 0_usize;
    let mut digests = BTreeSet::new();
    for record in records {
        let encoded = encode_record(record)?;
        total_bytes = total_bytes
            .checked_add(encoded.len())
            .ok_or_else(|| DatabaseError::ResourceLimit("snapshot byte count overflow".into()))?;
        if total_bytes > MAX_SNAPSHOT_BYTES {
            return Err(DatabaseError::ResourceLimit(format!(
                "canonical snapshot is {total_bytes} bytes; maximum is {MAX_SNAPSHOT_BYTES}"
            )));
        }
        let digest: [u8; 32] = Sha256::digest(&encoded).into();
        if !digests.insert(digest) {
            return Err(DatabaseError::Invariant(
                "two logical snapshot records have the same digest".into(),
            ));
        }
        for (chunk_index, chunk) in encoded.chunks(CHUNK_BYTES).enumerate() {
            let chunk_index = u32::try_from(chunk_index).map_err(|_| {
                DatabaseError::ResourceLimit("snapshot record has too many chunks".into())
            })?;
            let key = physical_key(&digest, chunk_index);
            if output.insert(key, chunk.to_vec()).is_some() {
                return Err(DatabaseError::Invariant(
                    "duplicate physical snapshot chunk".into(),
                ));
            }
            if output.len() > MAX_PHYSICAL_ENTRIES {
                return Err(DatabaseError::ResourceLimit(format!(
                    "snapshot needs more than {MAX_PHYSICAL_ENTRIES} B+ tree entries"
                )));
            }
        }
    }
    Ok(output)
}

fn decode_records(physical: &BTreeMap<Vec<u8>, Vec<u8>>) -> Result<Vec<SnapshotRecord>> {
    validate_physical_map(physical)?;
    let mut grouped: BTreeMap<[u8; 32], Vec<(u32, &[u8])>> = BTreeMap::new();
    for (key, value) in physical {
        let (digest, chunk_index) = parse_physical_key(key)?;
        grouped
            .entry(digest)
            .or_default()
            .push((chunk_index, value));
    }
    let mut records = Vec::with_capacity(grouped.len());
    let mut total_bytes = 0_usize;
    for (digest, chunks) in grouped {
        let mut encoded = Vec::new();
        for (expected, (actual, chunk)) in chunks.into_iter().enumerate() {
            let expected = u32::try_from(expected)
                .map_err(|_| DatabaseError::Corruption("snapshot chunk index overflow".into()))?;
            if actual != expected {
                return Err(DatabaseError::Corruption(format!(
                    "snapshot record chunk gap: expected {expected}, found {actual}"
                )));
            }
            encoded.extend_from_slice(chunk);
            total_bytes = total_bytes
                .checked_add(chunk.len())
                .ok_or_else(|| DatabaseError::Corruption("snapshot byte count overflow".into()))?;
            if total_bytes > MAX_SNAPSHOT_BYTES {
                return Err(DatabaseError::Corruption(format!(
                    "snapshot exceeds {MAX_SNAPSHOT_BYTES} byte limit"
                )));
            }
        }
        if <[u8; 32]>::from(Sha256::digest(&encoded)) != digest {
            return Err(DatabaseError::Corruption(
                "snapshot record digest mismatch".into(),
            ));
        }
        records.push(decode_record(&encoded)?);
    }
    records.sort();
    Ok(records)
}

fn validate_physical_map(physical: &BTreeMap<Vec<u8>, Vec<u8>>) -> Result<()> {
    if physical.len() > MAX_PHYSICAL_ENTRIES {
        return Err(DatabaseError::Corruption(format!(
            "snapshot tree has {} entries; maximum is {MAX_PHYSICAL_ENTRIES}",
            physical.len()
        )));
    }
    let mut previous: Option<([u8; 32], u32)> = None;
    for (key, value) in physical {
        let current = parse_physical_key(key)?;
        if value.is_empty() || value.len() > CHUNK_BYTES {
            return Err(DatabaseError::Corruption(format!(
                "snapshot chunk has invalid length {}",
                value.len()
            )));
        }
        if let Some((previous_digest, previous_index)) = previous
            && current.0 == previous_digest
            && current.1 != previous_index.saturating_add(1)
        {
            return Err(DatabaseError::Corruption(
                "snapshot chunks are not contiguous".into(),
            ));
        }
        previous = Some(current);
    }
    Ok(())
}

fn encode_record(record: &SnapshotRecord) -> Result<Vec<u8>> {
    let key_length = u32::try_from(record.key.len())
        .map_err(|_| DatabaseError::ResourceLimit("snapshot key exceeds u32".into()))?;
    let value_length = u64::try_from(record.value.len())
        .map_err(|_| DatabaseError::ResourceLimit("snapshot value exceeds u64".into()))?;
    let capacity = RECORD_HEADER_BYTES
        .checked_add(record.key.len())
        .and_then(|bytes| bytes.checked_add(record.value.len()))
        .ok_or_else(|| DatabaseError::ResourceLimit("snapshot record size overflow".into()))?;
    let mut encoded = Vec::with_capacity(capacity);
    encoded.extend_from_slice(RECORD_MAGIC);
    encoded.push(record.kind);
    encoded.extend_from_slice(&key_length.to_be_bytes());
    encoded.extend_from_slice(&value_length.to_be_bytes());
    encoded.extend_from_slice(&record.key);
    encoded.extend_from_slice(&record.value);
    Ok(encoded)
}

fn decode_record(encoded: &[u8]) -> Result<SnapshotRecord> {
    if encoded.len() < RECORD_HEADER_BYTES || &encoded[..RECORD_MAGIC.len()] != RECORD_MAGIC {
        return Err(DatabaseError::Corruption(
            "snapshot record header is invalid".into(),
        ));
    }
    let kind = encoded[RECORD_MAGIC.len()];
    let mut key_length = [0; 4];
    key_length.copy_from_slice(&encoded[RECORD_MAGIC.len() + 1..RECORD_MAGIC.len() + 5]);
    let key_length = usize::try_from(u32::from_be_bytes(key_length))
        .map_err(|_| DatabaseError::Corruption("snapshot key length overflow".into()))?;
    let mut value_length = [0; 8];
    value_length.copy_from_slice(&encoded[RECORD_MAGIC.len() + 5..RECORD_MAGIC.len() + 13]);
    let value_length = usize::try_from(u64::from_be_bytes(value_length))
        .map_err(|_| DatabaseError::Corruption("snapshot value length overflow".into()))?;
    let key_end = RECORD_HEADER_BYTES
        .checked_add(key_length)
        .ok_or_else(|| DatabaseError::Corruption("snapshot key extent overflow".into()))?;
    let value_end = key_end
        .checked_add(value_length)
        .ok_or_else(|| DatabaseError::Corruption("snapshot value extent overflow".into()))?;
    if value_end != encoded.len() {
        return Err(DatabaseError::Corruption(
            "snapshot record lengths disagree with payload".into(),
        ));
    }
    Ok(SnapshotRecord {
        kind,
        key: encoded[RECORD_HEADER_BYTES..key_end].to_vec(),
        value: encoded[key_end..value_end].to_vec(),
    })
}

fn physical_key(digest: &[u8; 32], chunk_index: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(PHYSICAL_KEY_BYTES);
    key.extend_from_slice(PHYSICAL_KEY_MAGIC);
    key.extend_from_slice(digest);
    key.extend_from_slice(&chunk_index.to_be_bytes());
    key
}

fn parse_physical_key(key: &[u8]) -> Result<([u8; 32], u32)> {
    if key.len() != PHYSICAL_KEY_BYTES || &key[..PHYSICAL_KEY_MAGIC.len()] != PHYSICAL_KEY_MAGIC {
        return Err(DatabaseError::Corruption(
            "snapshot tree contains an unknown physical key".into(),
        ));
    }
    let mut digest = [0; 32];
    digest.copy_from_slice(&key[PHYSICAL_KEY_MAGIC.len()..PHYSICAL_KEY_MAGIC.len() + 32]);
    let mut chunk_index = [0; 4];
    chunk_index.copy_from_slice(&key[PHYSICAL_KEY_MAGIC.len() + 32..]);
    Ok((digest, u32::from_be_bytes(chunk_index)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_records_round_trip_across_chunks() {
        let records = vec![
            SnapshotRecord {
                kind: 7,
                key: vec![0, 1, 2],
                value: vec![9; CHUNK_BYTES * 3 + 17],
            },
            SnapshotRecord {
                kind: 8,
                key: b"second".to_vec(),
                value: b"small".to_vec(),
            },
        ];
        let physical = encode_records(&records).unwrap();
        assert!(physical.len() > records.len());
        assert_eq!(decode_records(&physical).unwrap(), records);
    }

    #[test]
    fn missing_chunk_is_rejected() {
        let records = vec![SnapshotRecord {
            kind: 9,
            key: Vec::new(),
            value: vec![5; CHUNK_BYTES * 2],
        }];
        let mut physical = encode_records(&records).unwrap();
        let second = physical.keys().nth(1).unwrap().clone();
        physical.remove(&second);
        assert!(decode_records(&physical).is_err());
    }
}
