use crate::{LogEntry, PersistentSnapshot};

#[derive(Debug, Clone)]
pub(crate) struct RaftLog {
    base_index: u64,
    base_term: u64,
    entries: Vec<LogEntry>,
}

impl RaftLog {
    pub(crate) fn new(snapshot: Option<&PersistentSnapshot>, entries: Vec<LogEntry>) -> Self {
        let (base_index, base_term) = snapshot.map_or((0, 0), |snapshot| {
            (
                snapshot.metadata.last_included_index,
                snapshot.metadata.last_included_term,
            )
        });
        Self {
            base_index,
            base_term,
            entries,
        }
    }

    pub(crate) const fn base_index(&self) -> u64 {
        self.base_index
    }

    pub(crate) const fn base_term(&self) -> u64 {
        self.base_term
    }

    pub(crate) fn last_index(&self) -> u64 {
        self.entries
            .last()
            .map_or(self.base_index, |entry| entry.index)
    }

    pub(crate) fn last_term(&self) -> u64 {
        self.entries
            .last()
            .map_or(self.base_term, |entry| entry.term)
    }

    pub(crate) fn term(&self, index: u64) -> Option<u64> {
        if index == self.base_index {
            return Some(self.base_term);
        }
        if index < self.base_index {
            return None;
        }
        let offset = usize::try_from(index - self.base_index - 1).ok()?;
        self.entries.get(offset).map(|entry| entry.term)
    }

    pub(crate) fn entry(&self, index: u64) -> Option<&LogEntry> {
        if index <= self.base_index {
            return None;
        }
        let offset = usize::try_from(index - self.base_index - 1).ok()?;
        self.entries.get(offset)
    }

    pub(crate) fn entries_from(&self, index: u64, limit: usize) -> Vec<LogEntry> {
        if index <= self.base_index {
            return Vec::new();
        }
        let Ok(offset) = usize::try_from(index - self.base_index - 1) else {
            return Vec::new();
        };
        self.entries
            .get(offset..)
            .unwrap_or_default()
            .iter()
            .take(limit)
            .cloned()
            .collect()
    }

    pub(crate) fn all_entries(&self) -> &[LogEntry] {
        &self.entries
    }

    pub(crate) fn append(&mut self, entries: &[LogEntry]) {
        self.entries.extend_from_slice(entries);
    }

    pub(crate) fn truncate_and_append(&mut self, from: u64, entries: &[LogEntry]) {
        let keep = usize::try_from(from.saturating_sub(self.base_index + 1)).unwrap_or(usize::MAX);
        self.entries.truncate(keep);
        self.entries.extend_from_slice(entries);
    }

    pub(crate) fn first_index_of_term(&self, index: u64, term: u64) -> u64 {
        let mut cursor = index;
        while cursor > self.base_index && self.term(cursor - 1) == Some(term) {
            cursor -= 1;
        }
        cursor
    }

    pub(crate) fn last_index_of_term(&self, term: u64) -> Option<u64> {
        self.entries
            .iter()
            .rev()
            .find(|entry| entry.term == term)
            .map(|entry| entry.index)
            .or_else(|| (self.base_term == term).then_some(self.base_index))
    }

    pub(crate) fn compact(&mut self, index: u64, term: u64, retain_suffix: bool) {
        let entries = if retain_suffix {
            self.entries_from(index + 1, usize::MAX)
        } else {
            Vec::new()
        };
        self.base_index = index;
        self.base_term = term;
        self.entries = entries;
    }
}
