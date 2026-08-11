use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use aster_raft::{
    HardState, LogEntry, PersistentSnapshot, SnapshotMetadata, StableState, StorageMutation,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Result, RuntimeError};

const STATE_FILE: &str = "raft.state";
const STATE_TEMP_FILE: &str = "raft.state.tmp";
const INSTALL_FILE: &str = "snapshot.install";
const INSTALL_TEMP_FILE: &str = "snapshot.install.tmp";
const SNAPSHOT_TEMP_FILE: &str = "raft.snapshot.tmp";
const SNAPSHOT_PREFIX: &str = "snapshot-";
const SNAPSHOT_SUFFIX: &str = ".bin";
const STATE_MAGIC_V1: [u8; 8] = *b"ASTRFT\0\x01";
const STATE_MAGIC_V2: [u8; 8] = *b"ASTRFT\0\x02";
const SNAPSHOT_MAGIC: [u8; 8] = *b"ASTSNP\0\x01";
const INSTALL_MAGIC: [u8; 8] = *b"ASTINS\0\x01";
const HEADER_BYTES: usize = 8 + 8 + 32;
const MAX_STATE_BYTES: usize = 512 * 1024 * 1024;
const MAX_SNAPSHOT_BYTES: usize = 512 * 1024 * 1024;
const MAX_METADATA_BYTES: usize = 64 * 1024;
const STORED_STATE_FORMAT: u16 = 2;
const INSTALL_FORMAT: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredState {
    format: u16,
    hard_state: HardState,
    snapshot: Option<SnapshotMetadata>,
    entries: Vec<LogEntry>,
    applied_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredInstallIntent {
    format: u16,
    snapshot: SnapshotMetadata,
    retained_entries: Vec<LogEntry>,
    hard_state: HardState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingSnapshotInstall {
    pub(crate) snapshot: PersistentSnapshot,
    pub(crate) retained_entries: Vec<LogEntry>,
    pub(crate) hard_state: HardState,
}

impl PendingSnapshotInstall {
    fn stable_state(&self) -> StableState {
        StableState {
            hard_state: self.hard_state,
            snapshot: Some(self.snapshot.clone()),
            entries: self.retained_entries.clone(),
            applied_index: self.snapshot.metadata.last_included_index,
        }
    }
}

pub(crate) struct StableStore {
    directory: PathBuf,
    path: PathBuf,
    temporary: PathBuf,
    install_path: PathBuf,
    install_temporary: PathBuf,
    snapshot_temporary: PathBuf,
    state: StableState,
    pending_install: Option<PendingSnapshotInstall>,
    retained_entry_bytes: usize,
}

impl StableStore {
    pub(crate) fn open(directory: impl AsRef<Path>) -> Result<Self> {
        let directory = directory.as_ref().to_path_buf();
        let directory_was_missing = !directory.exists();
        fs::create_dir_all(&directory)?;
        if directory_was_missing {
            sync_directory(&directory)?;
        }
        let path = directory.join(STATE_FILE);
        let temporary = directory.join(STATE_TEMP_FILE);
        let install_path = directory.join(INSTALL_FILE);
        let install_temporary = directory.join(INSTALL_TEMP_FILE);
        let snapshot_temporary = directory.join(SNAPSHOT_TEMP_FILE);
        remove_temporary(&temporary)?;
        remove_temporary(&install_temporary)?;
        remove_temporary(&snapshot_temporary)?;

        let mut migrated_v1 = false;
        let state = if path.exists() {
            let encoded = read_bounded(&path, MAX_STATE_BYTES, "Raft state")?;
            if encoded.starts_with(&STATE_MAGIC_V1) {
                migrated_v1 = true;
                decode_v1_state(&encoded)?
            } else {
                let stored: StoredState =
                    decode_json_frame(&encoded, STATE_MAGIC_V2, MAX_STATE_BYTES, "Raft state")?;
                load_stored_state(&directory, stored)?
            }
        } else {
            StableState::default()
        };
        validate_indexes(&state)?;

        let pending_install = if install_path.exists() {
            let encoded = read_bounded(&install_path, MAX_STATE_BYTES, "snapshot install intent")?;
            let stored: StoredInstallIntent = decode_json_frame(
                &encoded,
                INSTALL_MAGIC,
                MAX_STATE_BYTES,
                "snapshot install intent",
            )?;
            Some(load_install_intent(&directory, stored)?)
        } else {
            None
        };
        let retained_entry_bytes = encoded_entry_bytes(&state.entries)?;
        let store = Self {
            directory,
            path,
            temporary,
            install_path,
            install_temporary,
            snapshot_temporary,
            state,
            pending_install,
            retained_entry_bytes,
        };
        if !store.path.exists() || migrated_v1 {
            store.publish(&store.state)?;
        }
        store.cleanup_snapshot_files()?;
        Ok(store)
    }

    pub(crate) fn state(&self) -> StableState {
        self.state.clone()
    }

    pub(crate) fn pending_install(&self) -> Option<&PendingSnapshotInstall> {
        self.pending_install.as_ref()
    }

    pub(crate) fn term_at(&self, index: u64) -> Option<u64> {
        term_at(&self.state, index)
    }

    pub(crate) fn snapshot_index(&self) -> Option<u64> {
        self.state
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.metadata.last_included_index)
    }

    pub(crate) fn snapshot_bytes(&self) -> Option<u64> {
        self.state
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.metadata.byte_len)
    }

    pub(crate) fn retained_entry_count(&self) -> usize {
        self.state.entries.len()
    }

    pub(crate) const fn retained_entry_bytes(&self) -> usize {
        self.retained_entry_bytes
    }

    pub(crate) fn persist(&mut self, mutation: &StorageMutation) -> Result<()> {
        let mut next = self.state.clone();
        match mutation {
            StorageMutation::HardState(hard_state) => {
                validate_hard_state(&next, *hard_state)?;
                next.hard_state = *hard_state;
            }
            StorageMutation::Append(entries) => {
                let expected = last_index(&next)
                    .checked_add(1)
                    .ok_or_else(|| RuntimeError::Corruption("Raft log index overflow".into()))?;
                if entries.first().is_some_and(|entry| entry.index != expected) {
                    return Err(RuntimeError::Corruption(format!(
                        "Raft append starts at {}, expected {expected}",
                        entries[0].index
                    )));
                }
                validate_entry_sequence(&next, entries)?;
                next.entries.extend_from_slice(entries);
            }
            StorageMutation::TruncateAndAppend { from, entries } => {
                if *from <= next.hard_state.commit_index {
                    return Err(RuntimeError::Corruption(format!(
                        "attempted to truncate committed Raft index {from}"
                    )));
                }
                let base = base_index(&next);
                if *from <= base {
                    return Err(RuntimeError::Corruption(
                        "attempted to truncate before compacted Raft base".into(),
                    ));
                }
                let keep = usize::try_from(from - base - 1).map_err(|_| {
                    RuntimeError::Corruption("Raft truncate offset overflow".into())
                })?;
                if keep > next.entries.len() {
                    return Err(RuntimeError::Corruption(
                        "Raft truncate starts beyond log end".into(),
                    ));
                }
                next.entries.truncate(keep);
                if entries.first().is_some_and(|entry| entry.index != *from) {
                    return Err(RuntimeError::Corruption(
                        "replacement Raft suffix starts at the wrong index".into(),
                    ));
                }
                validate_entry_sequence(&next, entries)?;
                next.entries.extend_from_slice(entries);
            }
            StorageMutation::CompactSnapshot {
                snapshot,
                retained_entries,
            } => {
                validate_snapshot(snapshot)?;
                if snapshot.metadata.last_included_index > next.applied_index {
                    return Err(RuntimeError::Corruption(
                        "cannot compact beyond the durable applied index".into(),
                    ));
                }
                if term_at(&next, snapshot.metadata.last_included_index)
                    != Some(snapshot.metadata.last_included_term)
                    || suffix_after(&next, snapshot.metadata.last_included_index)
                        != Some(retained_entries.as_slice())
                {
                    return Err(RuntimeError::Corruption(
                        "compacted snapshot boundary or retained suffix disagrees with the durable log"
                            .into(),
                    ));
                }
                next.snapshot = Some(snapshot.clone());
                next.entries.clone_from(retained_entries);
            }
            StorageMutation::InstallSnapshot { .. } => {
                return Err(RuntimeError::Corruption(
                    "snapshot install must use the durable cross-file intent path".into(),
                ));
            }
        }
        validate_indexes(&next)?;
        let retained_entry_bytes = encoded_entry_bytes(&next.entries)?;
        self.publish(&next)?;
        self.state = next;
        self.retained_entry_bytes = retained_entry_bytes;
        self.cleanup_snapshot_files()
    }

    pub(crate) fn begin_snapshot_install(
        &mut self,
        snapshot: &PersistentSnapshot,
        retained_entries: &[LogEntry],
        hard_state: HardState,
    ) -> Result<()> {
        validate_snapshot(snapshot)?;
        let intent = PendingSnapshotInstall {
            snapshot: snapshot.clone(),
            retained_entries: retained_entries.to_vec(),
            hard_state,
        };
        let desired = intent.stable_state();
        validate_hard_state_monotonic(self.state.hard_state, hard_state)?;
        validate_indexes(&desired)?;
        let matching_boundary = term_at(&self.state, snapshot.metadata.last_included_index)
            == Some(snapshot.metadata.last_included_term);
        let expected_suffix = if matching_boundary {
            suffix_after(&self.state, snapshot.metadata.last_included_index).unwrap_or(&[])
        } else {
            &[]
        };
        if retained_entries != expected_suffix {
            return Err(RuntimeError::Corruption(
                "snapshot install retained a suffix that does not match the durable log".into(),
            ));
        }
        if snapshot.metadata.last_included_index <= self.state.applied_index {
            return Err(RuntimeError::Corruption(format!(
                "snapshot install index {} is not newer than applied index {}",
                snapshot.metadata.last_included_index, self.state.applied_index
            )));
        }
        if let Some(existing) = &self.pending_install {
            if existing == &intent {
                return Ok(());
            }
            return Err(RuntimeError::Corruption(
                "a different snapshot install intent is already durable".into(),
            ));
        }

        self.publish_snapshot(snapshot)?;
        let stored = StoredInstallIntent {
            format: INSTALL_FORMAT,
            snapshot: snapshot.metadata.clone(),
            retained_entries: retained_entries.to_vec(),
            hard_state,
        };
        let encoded = encode_json_frame(
            INSTALL_MAGIC,
            &stored,
            MAX_STATE_BYTES,
            "snapshot install intent",
        )?;
        atomic_publish(
            &self.directory,
            &self.install_temporary,
            &self.install_path,
            &encoded,
        )?;
        self.pending_install = Some(intent);
        Ok(())
    }

    pub(crate) fn complete_snapshot_install(&mut self) -> Result<()> {
        let pending = self.pending_install.clone().ok_or_else(|| {
            RuntimeError::Corruption("no durable snapshot install intent exists".into())
        })?;
        let desired = pending.stable_state();
        validate_indexes(&desired)?;
        if self.state != desired {
            let retained_entry_bytes = encoded_entry_bytes(&desired.entries)?;
            self.publish(&desired)?;
            self.state = desired;
            self.retained_entry_bytes = retained_entry_bytes;
        }
        if self.install_path.exists() {
            fs::remove_file(&self.install_path)?;
            sync_directory(&self.directory)?;
        }
        self.pending_install = None;
        self.cleanup_snapshot_files()
    }

    pub(crate) fn mark_applied(&mut self, index: u64) -> Result<()> {
        if index == self.state.applied_index {
            return Ok(());
        }
        let expected = self
            .state
            .applied_index
            .checked_add(1)
            .ok_or_else(|| RuntimeError::Corruption("Raft applied index overflow".into()))?;
        if index != expected || index > self.state.hard_state.commit_index {
            return Err(RuntimeError::Corruption(format!(
                "cannot mark applied index {index}; expected {expected}, commit is {}",
                self.state.hard_state.commit_index
            )));
        }
        let mut next = self.state.clone();
        next.applied_index = index;
        self.publish(&next)?;
        self.state = next;
        Ok(())
    }

    fn publish(&self, state: &StableState) -> Result<()> {
        validate_indexes(state)?;
        if let Some(snapshot) = &state.snapshot {
            self.publish_snapshot(snapshot)?;
        }
        let stored = StoredState {
            format: STORED_STATE_FORMAT,
            hard_state: state.hard_state,
            snapshot: state
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.metadata.clone()),
            entries: state.entries.clone(),
            applied_index: state.applied_index,
        };
        let encoded = encode_json_frame(STATE_MAGIC_V2, &stored, MAX_STATE_BYTES, "Raft state")?;
        atomic_publish(&self.directory, &self.temporary, &self.path, &encoded)
    }

    fn publish_snapshot(&self, snapshot: &PersistentSnapshot) -> Result<()> {
        validate_snapshot(snapshot)?;
        let path = self.directory.join(snapshot_file_name(&snapshot.metadata));
        if path.exists() {
            let existing = decode_snapshot(&read_bounded(
                &path,
                MAX_SNAPSHOT_BYTES + MAX_METADATA_BYTES,
                "Raft snapshot",
            )?)?;
            if existing != *snapshot {
                return Err(RuntimeError::Corruption(
                    "content-addressed snapshot file has different bytes".into(),
                ));
            }
            return Ok(());
        }
        let encoded = encode_snapshot(snapshot)?;
        atomic_publish(&self.directory, &self.snapshot_temporary, &path, &encoded)
    }

    fn cleanup_snapshot_files(&self) -> Result<()> {
        let mut keep = BTreeSet::new();
        if let Some(snapshot) = &self.state.snapshot {
            keep.insert(snapshot_file_name(&snapshot.metadata));
        }
        if let Some(pending) = &self.pending_install {
            keep.insert(snapshot_file_name(&pending.snapshot.metadata));
        }
        let mut removed = false;
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if name.starts_with(SNAPSHOT_PREFIX)
                && name.ends_with(SNAPSHOT_SUFFIX)
                && !keep.contains(name)
            {
                fs::remove_file(entry.path())?;
                removed = true;
            }
        }
        if removed {
            sync_directory(&self.directory)?;
        }
        Ok(())
    }
}

fn encoded_entry_bytes(entries: &[LogEntry]) -> Result<usize> {
    serde_json::to_vec(entries)
        .map(|encoded| encoded.len())
        .map_err(|error| {
            RuntimeError::Corruption(format!("encode retained Raft log for sizing: {error}"))
        })
}

fn load_stored_state(directory: &Path, stored: StoredState) -> Result<StableState> {
    if stored.format != STORED_STATE_FORMAT {
        return Err(RuntimeError::Corruption(format!(
            "unsupported stored Raft-state format {}",
            stored.format
        )));
    }
    let snapshot = stored
        .snapshot
        .map(|metadata| load_snapshot(directory, &metadata))
        .transpose()?;
    let state = StableState {
        hard_state: stored.hard_state,
        snapshot,
        entries: stored.entries,
        applied_index: stored.applied_index,
    };
    validate_indexes(&state)?;
    Ok(state)
}

fn load_install_intent(
    directory: &Path,
    stored: StoredInstallIntent,
) -> Result<PendingSnapshotInstall> {
    if stored.format != INSTALL_FORMAT {
        return Err(RuntimeError::Corruption(format!(
            "unsupported snapshot-install format {}",
            stored.format
        )));
    }
    let snapshot = load_snapshot(directory, &stored.snapshot)?;
    let pending = PendingSnapshotInstall {
        snapshot,
        retained_entries: stored.retained_entries,
        hard_state: stored.hard_state,
    };
    validate_indexes(&pending.stable_state())?;
    Ok(pending)
}

fn load_snapshot(directory: &Path, metadata: &SnapshotMetadata) -> Result<PersistentSnapshot> {
    let path = directory.join(snapshot_file_name(metadata));
    let snapshot = decode_snapshot(&read_bounded(
        &path,
        MAX_SNAPSHOT_BYTES + MAX_METADATA_BYTES,
        "Raft snapshot",
    )?)?;
    if snapshot.metadata != *metadata {
        return Err(RuntimeError::Corruption(
            "snapshot sidecar metadata disagrees with its manifest".into(),
        ));
    }
    Ok(snapshot)
}

fn validate_hard_state(state: &StableState, next: HardState) -> Result<()> {
    let current = state.hard_state;
    validate_hard_state_monotonic(current, next)?;
    if next.commit_index > last_index(state) {
        return Err(RuntimeError::Corruption(format!(
            "commit {} is beyond durable log {}",
            next.commit_index,
            last_index(state)
        )));
    }
    Ok(())
}

fn validate_hard_state_monotonic(current: HardState, next: HardState) -> Result<()> {
    if next.current_term < current.current_term || next.commit_index < current.commit_index {
        return Err(RuntimeError::Corruption(
            "Raft hard state term or commit index regressed".into(),
        ));
    }
    if next.current_term == current.current_term
        && current.voted_for.is_some()
        && next.voted_for != current.voted_for
    {
        return Err(RuntimeError::Corruption(
            "persisted vote changed within one term".into(),
        ));
    }
    Ok(())
}

fn validate_entry_sequence(state: &StableState, entries: &[LogEntry]) -> Result<()> {
    let mut expected = last_index(state)
        .checked_add(1)
        .ok_or_else(|| RuntimeError::Corruption("Raft log index overflow".into()))?;
    let mut previous_term = state
        .entries
        .last()
        .map_or_else(|| base_term(state), |entry| entry.term);
    for entry in entries {
        if entry.index != expected || entry.term < previous_term {
            return Err(RuntimeError::Corruption(format!(
                "invalid Raft entry sequence at index {} term {}",
                entry.index, entry.term
            )));
        }
        expected = expected
            .checked_add(1)
            .ok_or_else(|| RuntimeError::Corruption("Raft log index overflow".into()))?;
        previous_term = entry.term;
    }
    Ok(())
}

fn validate_indexes(state: &StableState) -> Result<()> {
    if let Some(snapshot) = &state.snapshot {
        validate_snapshot(snapshot)?;
    }
    let base = base_index(state);
    let last = last_index(state);
    if state.hard_state.commit_index < base
        || state.hard_state.commit_index > last
        || state.applied_index < base
        || state.applied_index > state.hard_state.commit_index
    {
        return Err(RuntimeError::Corruption(format!(
            "invalid Raft indexes base={base} applied={} commit={} last={last}",
            state.applied_index, state.hard_state.commit_index
        )));
    }
    if let Some(first) = state.entries.first()
        && first.index != base.saturating_add(1)
    {
        return Err(RuntimeError::Corruption(format!(
            "retained Raft log begins at {}, expected {}",
            first.index,
            base.saturating_add(1)
        )));
    }
    validate_entry_sequence(
        &StableState {
            hard_state: HardState::default(),
            snapshot: state.snapshot.clone(),
            entries: Vec::new(),
            applied_index: base,
        },
        &state.entries,
    )?;
    Ok(())
}

fn validate_snapshot(snapshot: &PersistentSnapshot) -> Result<()> {
    if !snapshot.validate()
        || snapshot.data.is_empty()
        || snapshot.data.len() > MAX_SNAPSHOT_BYTES
        || snapshot.metadata.last_included_index == 0
    {
        return Err(RuntimeError::Corruption(
            "snapshot length, checksum, or boundary is invalid".into(),
        ));
    }
    Ok(())
}

fn term_at(state: &StableState, index: u64) -> Option<u64> {
    if index == base_index(state) && index != 0 {
        return Some(base_term(state));
    }
    if index <= base_index(state) {
        return None;
    }
    let offset = usize::try_from(index - base_index(state) - 1).ok()?;
    state.entries.get(offset).map(|entry| entry.term)
}

fn suffix_after(state: &StableState, index: u64) -> Option<&[LogEntry]> {
    if index < base_index(state) || index > last_index(state) {
        return None;
    }
    let offset = usize::try_from(index - base_index(state)).ok()?;
    state.entries.get(offset..)
}

fn base_index(state: &StableState) -> u64 {
    state
        .snapshot
        .as_ref()
        .map_or(0, |snapshot| snapshot.metadata.last_included_index)
}

fn base_term(state: &StableState) -> u64 {
    state
        .snapshot
        .as_ref()
        .map_or(0, |snapshot| snapshot.metadata.last_included_term)
}

fn last_index(state: &StableState) -> u64 {
    state
        .entries
        .last()
        .map_or_else(|| base_index(state), |entry| entry.index)
}

fn encode_json_frame<T: Serialize>(
    magic: [u8; 8],
    value: &T,
    maximum: usize,
    label: &str,
) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(value)
        .map_err(|error| RuntimeError::Corruption(format!("encode {label}: {error}")))?;
    encode_frame(magic, &payload, maximum, label)
}

fn decode_json_frame<T: for<'de> Deserialize<'de>>(
    encoded: &[u8],
    magic: [u8; 8],
    maximum: usize,
    label: &str,
) -> Result<T> {
    let payload = decode_frame(encoded, magic, maximum, label)?;
    serde_json::from_slice(payload)
        .map_err(|error| RuntimeError::Corruption(format!("decode {label}: {error}")))
}

fn encode_frame(magic: [u8; 8], payload: &[u8], maximum: usize, label: &str) -> Result<Vec<u8>> {
    if payload.len() > maximum {
        return Err(RuntimeError::Limit(format!(
            "{label} is {} bytes; maximum is {maximum}",
            payload.len()
        )));
    }
    let length = u64::try_from(payload.len())
        .map_err(|_| RuntimeError::Limit(format!("{label} length exceeds u64")))?;
    let checksum: [u8; 32] = Sha256::digest(payload).into();
    let mut encoded = Vec::with_capacity(HEADER_BYTES + payload.len());
    encoded.extend_from_slice(&magic);
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(&checksum);
    encoded.extend_from_slice(payload);
    Ok(encoded)
}

fn decode_frame<'a>(
    encoded: &'a [u8],
    magic: [u8; 8],
    maximum: usize,
    label: &str,
) -> Result<&'a [u8]> {
    if encoded.len() < HEADER_BYTES || encoded[..8] != magic {
        return Err(RuntimeError::Corruption(format!(
            "{label} header is invalid"
        )));
    }
    let mut length = [0; 8];
    length.copy_from_slice(&encoded[8..16]);
    let length = usize::try_from(u64::from_be_bytes(length))
        .map_err(|_| RuntimeError::Corruption(format!("{label} length overflow")))?;
    if length > maximum || HEADER_BYTES.checked_add(length) != Some(encoded.len()) {
        return Err(RuntimeError::Corruption(format!(
            "{label} length is invalid"
        )));
    }
    let mut expected = [0; 32];
    expected.copy_from_slice(&encoded[16..HEADER_BYTES]);
    let payload = &encoded[HEADER_BYTES..];
    if <[u8; 32]>::from(Sha256::digest(payload)) != expected {
        return Err(RuntimeError::Corruption(format!(
            "{label} checksum mismatch"
        )));
    }
    Ok(payload)
}

fn encode_snapshot(snapshot: &PersistentSnapshot) -> Result<Vec<u8>> {
    validate_snapshot(snapshot)?;
    let metadata = serde_json::to_vec(&snapshot.metadata)
        .map_err(|error| RuntimeError::Corruption(format!("encode snapshot metadata: {error}")))?;
    if metadata.len() > MAX_METADATA_BYTES {
        return Err(RuntimeError::Limit(
            "snapshot metadata exceeds its size bound".into(),
        ));
    }
    let metadata_length = u64::try_from(metadata.len())
        .map_err(|_| RuntimeError::Limit("snapshot metadata length exceeds u64".into()))?;
    let mut payload = Vec::with_capacity(8 + metadata.len() + snapshot.data.len());
    payload.extend_from_slice(&metadata_length.to_be_bytes());
    payload.extend_from_slice(&metadata);
    payload.extend_from_slice(&snapshot.data);
    encode_frame(
        SNAPSHOT_MAGIC,
        &payload,
        MAX_SNAPSHOT_BYTES + MAX_METADATA_BYTES + 8,
        "Raft snapshot",
    )
}

fn decode_snapshot(encoded: &[u8]) -> Result<PersistentSnapshot> {
    let payload = decode_frame(
        encoded,
        SNAPSHOT_MAGIC,
        MAX_SNAPSHOT_BYTES + MAX_METADATA_BYTES + 8,
        "Raft snapshot",
    )?;
    if payload.len() < 8 {
        return Err(RuntimeError::Corruption(
            "snapshot metadata length is missing".into(),
        ));
    }
    let mut metadata_length = [0; 8];
    metadata_length.copy_from_slice(&payload[..8]);
    let metadata_length = usize::try_from(u64::from_be_bytes(metadata_length))
        .map_err(|_| RuntimeError::Corruption("snapshot metadata length overflow".into()))?;
    let metadata_end = 8_usize
        .checked_add(metadata_length)
        .ok_or_else(|| RuntimeError::Corruption("snapshot metadata extent overflow".into()))?;
    if metadata_length > MAX_METADATA_BYTES || metadata_end > payload.len() {
        return Err(RuntimeError::Corruption(
            "snapshot metadata extent is invalid".into(),
        ));
    }
    let metadata: SnapshotMetadata = serde_json::from_slice(&payload[8..metadata_end])
        .map_err(|error| RuntimeError::Corruption(format!("decode snapshot metadata: {error}")))?;
    let snapshot = PersistentSnapshot {
        metadata,
        data: payload[metadata_end..].to_vec(),
    };
    validate_snapshot(&snapshot)?;
    Ok(snapshot)
}

fn snapshot_file_name(metadata: &SnapshotMetadata) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut hash = String::with_capacity(64);
    for byte in metadata.sha256 {
        hash.push(char::from(HEX[usize::from(byte >> 4)]));
        hash.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    format!(
        "{SNAPSHOT_PREFIX}{:020}-{hash}{SNAPSHOT_SUFFIX}",
        metadata.last_included_index
    )
}

fn decode_v1_state(encoded: &[u8]) -> Result<StableState> {
    let payload = decode_frame(encoded, STATE_MAGIC_V1, MAX_STATE_BYTES, "Raft v1 state")?;
    let state: StableState = serde_json::from_slice(payload)
        .map_err(|error| RuntimeError::Corruption(format!("decode Raft v1 state: {error}")))?;
    validate_indexes(&state)?;
    Ok(state)
}

fn atomic_publish(directory: &Path, temporary: &Path, path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    sync_directory(directory)
}

fn read_bounded(path: &Path, maximum: usize, label: &str) -> Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let length = usize::try_from(file.metadata()?.len())
        .map_err(|_| RuntimeError::Limit(format!("{label} file length exceeds usize")))?;
    let maximum_file = HEADER_BYTES
        .checked_add(maximum)
        .and_then(|value| value.checked_add(MAX_METADATA_BYTES + 8))
        .ok_or_else(|| RuntimeError::Limit(format!("{label} size bound overflow")))?;
    if length > maximum_file {
        return Err(RuntimeError::Limit(format!("{label} file is too large")));
    }
    let mut bytes = Vec::with_capacity(length);
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn remove_temporary(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_file(path)?;
        if let Some(directory) = path.parent() {
            sync_directory(directory)?;
        }
    }
    Ok(())
}

fn sync_directory(directory: &Path) -> Result<()> {
    File::open(directory)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use aster_raft::{Configuration, EntryPayload};
    use tempfile::TempDir;

    use super::*;

    fn append_committed(store: &mut StableStore, through: u64) {
        store
            .persist(&StorageMutation::HardState(HardState {
                current_term: 1,
                voted_for: Some(1),
                commit_index: 0,
            }))
            .unwrap();
        for index in 1..=through {
            store
                .persist(&StorageMutation::Append(vec![LogEntry {
                    index,
                    term: 1,
                    payload: EntryPayload::Noop,
                }]))
                .unwrap();
            store
                .persist(&StorageMutation::HardState(HardState {
                    current_term: 1,
                    voted_for: Some(1),
                    commit_index: index,
                }))
                .unwrap();
            store.mark_applied(index).unwrap();
        }
    }

    #[test]
    fn hard_state_log_and_applied_marker_survive_reopen() {
        let directory = TempDir::new().unwrap();
        {
            let mut store = StableStore::open(directory.path()).unwrap();
            append_committed(&mut store, 1);
        }
        let reopened = StableStore::open(directory.path()).unwrap();
        assert_eq!(reopened.state().hard_state.current_term, 1);
        assert_eq!(reopened.state().hard_state.voted_for, Some(1));
        assert_eq!(reopened.state().hard_state.commit_index, 1);
        assert_eq!(reopened.state().applied_index, 1);
        assert_eq!(reopened.state().entries.len(), 1);
    }

    #[test]
    fn corruption_and_vote_rewrite_are_rejected() {
        let directory = TempDir::new().unwrap();
        let mut store = StableStore::open(directory.path()).unwrap();
        store
            .persist(&StorageMutation::HardState(HardState {
                current_term: 2,
                voted_for: Some(1),
                commit_index: 0,
            }))
            .unwrap();
        assert!(
            store
                .persist(&StorageMutation::HardState(HardState {
                    current_term: 2,
                    voted_for: Some(2),
                    commit_index: 0,
                }))
                .is_err()
        );
        let path = directory.path().join(STATE_FILE);
        let mut bytes = read_bounded(&path, MAX_STATE_BYTES, "test state").unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert!(StableStore::open(directory.path()).is_err());
    }

    #[test]
    fn compact_snapshot_uses_checked_sidecar_and_bounds_log() {
        let directory = TempDir::new().unwrap();
        let configuration = Configuration::new([1, 2, 3]).unwrap();
        {
            let mut store = StableStore::open(directory.path()).unwrap();
            append_committed(&mut store, 4);
            let snapshot = PersistentSnapshot::new(4, 1, configuration, b"database-v4".to_vec());
            assert!(
                store
                    .persist(&StorageMutation::CompactSnapshot {
                        snapshot: snapshot.clone(),
                        retained_entries: vec![LogEntry {
                            index: 5,
                            term: 1,
                            payload: EntryPayload::Noop,
                        }],
                    })
                    .is_err()
            );
            store
                .persist(&StorageMutation::CompactSnapshot {
                    snapshot,
                    retained_entries: Vec::new(),
                })
                .unwrap();
            assert!(store.state().entries.is_empty());
        }
        let reopened = StableStore::open(directory.path()).unwrap();
        let snapshot = reopened.state().snapshot.unwrap();
        assert!(snapshot.validate());
        assert_eq!(snapshot.data, b"database-v4");
        assert_eq!(reopened.state().entries.len(), 0);

        let sidecar = directory
            .path()
            .join(snapshot_file_name(&snapshot.metadata));
        let mut bytes = read_bounded(&sidecar, MAX_SNAPSHOT_BYTES, "test snapshot").unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        fs::write(sidecar, bytes).unwrap();
        assert!(StableStore::open(directory.path()).is_err());
    }

    #[test]
    fn successive_compactions_retain_only_the_published_sidecar() {
        let directory = TempDir::new().unwrap();
        let configuration = Configuration::new([1, 2, 3]).unwrap();
        let mut store = StableStore::open(directory.path()).unwrap();
        append_committed(&mut store, 4);
        let first = PersistentSnapshot::new(4, 1, configuration.clone(), b"database-v4".to_vec());
        let first_path = directory.path().join(snapshot_file_name(&first.metadata));
        store
            .persist(&StorageMutation::CompactSnapshot {
                snapshot: first,
                retained_entries: Vec::new(),
            })
            .unwrap();
        assert!(first_path.exists());

        store
            .persist(&StorageMutation::Append(vec![LogEntry {
                index: 5,
                term: 1,
                payload: EntryPayload::Noop,
            }]))
            .unwrap();
        store
            .persist(&StorageMutation::HardState(HardState {
                current_term: 1,
                voted_for: Some(1),
                commit_index: 5,
            }))
            .unwrap();
        store.mark_applied(5).unwrap();
        let second = PersistentSnapshot::new(5, 1, configuration, b"database-v5".to_vec());
        let second_path = directory.path().join(snapshot_file_name(&second.metadata));
        store
            .persist(&StorageMutation::CompactSnapshot {
                snapshot: second,
                retained_entries: Vec::new(),
            })
            .unwrap();
        assert!(!first_path.exists());
        assert!(second_path.exists());
    }

    #[test]
    fn install_intent_survives_every_cross_file_restart_phase() {
        let directory = TempDir::new().unwrap();
        let configuration = Configuration::new([1, 2, 3]).unwrap();
        let snapshot = PersistentSnapshot::new(5, 2, configuration, b"database-v5".to_vec());
        let retained = Vec::new();
        let hard_state = HardState {
            current_term: 2,
            voted_for: None,
            commit_index: 5,
        };
        {
            let mut store = StableStore::open(directory.path()).unwrap();
            append_committed(&mut store, 1);
            store
                .begin_snapshot_install(&snapshot, &retained, hard_state)
                .unwrap();
            assert_eq!(store.state().applied_index, 1);
            assert!(store.pending_install().is_some());
        }
        {
            let mut reopened = StableStore::open(directory.path()).unwrap();
            assert_eq!(reopened.state().applied_index, 1);
            assert_eq!(reopened.pending_install().unwrap().snapshot, snapshot);
            reopened.complete_snapshot_install().unwrap();
            assert_eq!(reopened.state().applied_index, 5);
            assert_eq!(reopened.state().hard_state.commit_index, 5);
            assert_eq!(reopened.state().entries, retained);
        }
        let reopened = StableStore::open(directory.path()).unwrap();
        assert!(reopened.pending_install().is_none());
        assert_eq!(reopened.state().snapshot.unwrap(), snapshot);
    }
}
