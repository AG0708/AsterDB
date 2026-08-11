use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use parking_lot::Mutex;

use crate::{
    PageId, Result, StorageError,
    disk::Disk,
    page::{PAGE_HEADER_SIZE, PAGE_SIZE, PAGE_SIZE_U16, Page, PageKind},
};

const NODE_DATA: usize = 96;
const NONE_PAGE: u64 = u64::MAX;

/// Version-one underflow policy. Non-empty nodes may remain underfull after a
/// delete, avoiding write amplification. Empty leaves are unlinked and removed
/// from their parent; the stable root page is collapsed when it becomes unary.
pub const UNDERFLOW_POLICY: &str =
    "lazy underflow: retain non-empty nodes; unlink empty leaves; collapse unary stable root";

pub trait PageStore: Clone + Send + Sync + 'static {
    fn load(&self, page_id: PageId) -> Result<Page>;
    fn save(&self, page: &Page) -> Result<()>;
    fn allocate(&self, kind: PageKind) -> Result<PageId>;
}

pub struct DiskPageStore<D: Disk> {
    disk: Arc<D>,
    next_page: Arc<Mutex<u64>>,
}

impl<D: Disk> Clone for DiskPageStore<D> {
    fn clone(&self) -> Self {
        Self {
            disk: Arc::clone(&self.disk),
            next_page: Arc::clone(&self.next_page),
        }
    }
}

impl<D: Disk> DiskPageStore<D> {
    pub fn open(disk: Arc<D>) -> Result<Self> {
        let next = disk.page_count()?.max(2);
        Ok(Self {
            disk,
            next_page: Arc::new(Mutex::new(next)),
        })
    }

    pub fn sync(&self) -> Result<()> {
        self.disk.sync()
    }
}

impl<D: Disk> PageStore for DiskPageStore<D> {
    fn load(&self, page_id: PageId) -> Result<Page> {
        let page = Page::decode(self.disk.read_page(page_id)?)?;
        if page.id() != page_id {
            return Err(StorageError::InvalidPage(format!(
                "physical page {} contains page {}",
                page_id.0,
                page.id().0
            )));
        }
        Ok(page)
    }

    fn save(&self, page: &Page) -> Result<()> {
        page.validate()?;
        self.disk.write_page(page.id(), page.as_bytes())
    }

    fn allocate(&self, _kind: PageKind) -> Result<PageId> {
        let mut next = self.next_page.lock();
        let id = PageId(*next);
        *next = next
            .checked_add(1)
            .ok_or_else(|| StorageError::Invariant("page identifier space exhausted".into()))?;
        Ok(id)
    }
}

#[derive(Clone, Default)]
pub struct MemoryPageStore {
    state: Arc<Mutex<MemoryPageState>>,
}

#[derive(Default)]
struct MemoryPageState {
    pages: BTreeMap<PageId, Page>,
    next_page: u64,
}

impl MemoryPageStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(MemoryPageState {
                pages: BTreeMap::new(),
                next_page: 2,
            })),
        }
    }

    #[must_use]
    pub fn page_count(&self) -> usize {
        self.state.lock().pages.len()
    }
}

impl PageStore for MemoryPageStore {
    fn load(&self, page_id: PageId) -> Result<Page> {
        self.state
            .lock()
            .pages
            .get(&page_id)
            .cloned()
            .ok_or_else(|| StorageError::NotFound(format!("B+ tree page {}", page_id.0)))
    }

    fn save(&self, page: &Page) -> Result<()> {
        page.validate()?;
        self.state.lock().pages.insert(page.id(), page.clone());
        Ok(())
    }

    fn allocate(&self, _kind: PageKind) -> Result<PageId> {
        let mut state = self.state.lock();
        let id = PageId(state.next_page);
        state.next_page = state
            .next_page
            .checked_add(1)
            .ok_or_else(|| StorageError::Invariant("page identifier space exhausted".into()))?;
        Ok(id)
    }
}

#[derive(Clone, Debug)]
enum Node {
    Leaf {
        id: PageId,
        parent: Option<PageId>,
        previous: Option<PageId>,
        next: Option<PageId>,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    },
    Internal {
        id: PageId,
        parent: Option<PageId>,
        keys: Vec<Vec<u8>>,
        children: Vec<PageId>,
    },
}

impl Node {
    fn id(&self) -> PageId {
        match self {
            Self::Leaf { id, .. } | Self::Internal { id, .. } => *id,
        }
    }

    fn parent(&self) -> Option<PageId> {
        match self {
            Self::Leaf { parent, .. } | Self::Internal { parent, .. } => *parent,
        }
    }

    fn set_parent(&mut self, value: Option<PageId>) {
        match self {
            Self::Leaf { parent, .. } | Self::Internal { parent, .. } => *parent = value,
        }
    }
}

#[derive(Clone)]
pub struct BPlusTree<S: PageStore> {
    store: S,
    /// Stable for the life of the tree, including root splits and collapses.
    root: PageId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationReport {
    pub nodes: usize,
    pub leaves: usize,
    pub height: usize,
    pub entries: usize,
}

impl<S: PageStore> BPlusTree<S> {
    pub fn create(store: S) -> Result<Self> {
        let root = store.allocate(PageKind::BTreeLeaf)?;
        let tree = Self { store, root };
        tree.save_node(&Node::Leaf {
            id: root,
            parent: None,
            previous: None,
            next: None,
            entries: Vec::new(),
        })?;
        Ok(tree)
    }

    pub fn open(store: S, root: PageId) -> Result<Self> {
        let tree = Self { store, root };
        let root_node = tree.load_node(root)?;
        if root_node.parent().is_some() {
            return Err(StorageError::Invariant("B+ tree root has a parent".into()));
        }
        tree.validate()?;
        Ok(tree)
    }

    #[must_use]
    pub const fn root_page(&self) -> PageId {
        self.root
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let leaf = self.find_leaf(key)?;
        let Node::Leaf { entries, .. } = leaf else {
            return Err(StorageError::Invariant(
                "tree descent did not reach leaf".into(),
            ));
        };
        Ok(entries
            .binary_search_by(|(candidate, _)| candidate.as_slice().cmp(key))
            .ok()
            .map(|index| entries[index].1.clone()))
    }

    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
        if key.is_empty() {
            return Err(StorageError::KeyTooLarge { bytes: 0 });
        }
        if key.len() + value.len() + NODE_DATA + 4 > PAGE_SIZE {
            return Err(StorageError::KeyTooLarge {
                bytes: key.len() + value.len(),
            });
        }
        let mut leaf = self.find_leaf(&key)?;
        let Node::Leaf { entries, .. } = &mut leaf else {
            return Err(StorageError::Invariant(
                "tree descent did not reach leaf".into(),
            ));
        };
        let old_first = entries.first().map(|entry| entry.0.clone());
        let replaced = match entries.binary_search_by(|(candidate, _)| candidate.cmp(&key)) {
            Ok(index) => Some(std::mem::replace(&mut entries[index].1, value)),
            Err(index) => {
                entries.insert(index, (key, value));
                None
            }
        };
        if Self::node_fits(&leaf)? {
            self.save_node(&leaf)?;
            let new_first = self.node_min(&leaf);
            if old_first != new_first {
                self.propagate_min_change(leaf.id())?;
            }
        } else {
            self.split_leaf(leaf)?;
        }
        Ok(replaced)
    }

    pub fn delete(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut leaf = self.find_leaf(key)?;
        let leaf_id = leaf.id();
        let (value, changed_min, now_empty) = {
            let Node::Leaf { entries, .. } = &mut leaf else {
                return Err(StorageError::Invariant(
                    "tree descent did not reach leaf".into(),
                ));
            };
            let Ok(index) =
                entries.binary_search_by(|(candidate, _)| candidate.as_slice().cmp(key))
            else {
                return Ok(None);
            };
            let changed_min = index == 0;
            let (_, value) = entries.remove(index);
            (value, changed_min, entries.is_empty())
        };
        let empty_non_root = now_empty && leaf_id != self.root;
        if empty_non_root {
            self.unlink_empty_leaf(&leaf)?;
        } else {
            self.save_node(&leaf)?;
            if changed_min && !now_empty {
                self.propagate_min_change(leaf_id)?;
            }
        }
        Ok(Some(value))
    }

    /// Half-open range `[start, end)`. `None` means unbounded above.
    pub fn range(&self, start: &[u8], end: Option<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let leaf = self.find_leaf(start)?;
        let mut leaf_id = Some(leaf.id());
        let mut output = Vec::new();
        let mut visited = BTreeSet::new();
        while let Some(id) = leaf_id {
            if !visited.insert(id) {
                return Err(StorageError::Invariant("cycle in B+ leaf chain".into()));
            }
            let node = self.load_node(id)?;
            let Node::Leaf { entries, next, .. } = node else {
                return Err(StorageError::Invariant(
                    "leaf chain points to internal node".into(),
                ));
            };
            for (key, value) in entries {
                if key.as_slice() < start {
                    continue;
                }
                if end.is_some_and(|bound| key.as_slice() >= bound) {
                    return Ok(output);
                }
                output.push((key, value));
            }
            leaf_id = next;
        }
        Ok(output)
    }

    pub fn validate(&self) -> Result<ValidationReport> {
        let mut visited = BTreeSet::new();
        let mut leaf_order = Vec::new();
        let info = self.validate_subtree(self.root, None, &mut visited, &mut leaf_order)?;
        for (index, &leaf_id) in leaf_order.iter().enumerate() {
            let Node::Leaf { previous, next, .. } = self.load_node(leaf_id)? else {
                return Err(StorageError::Invariant(
                    "leaf order contains internal page".into(),
                ));
            };
            let expected_previous = index.checked_sub(1).map(|prior| leaf_order[prior]);
            let expected_next = leaf_order.get(index + 1).copied();
            if previous != expected_previous || next != expected_next {
                return Err(StorageError::Invariant(format!(
                    "leaf {} links {:?}/{:?}, expected {:?}/{:?}",
                    leaf_id.0, previous, next, expected_previous, expected_next
                )));
            }
        }
        Ok(ValidationReport {
            nodes: visited.len(),
            leaves: leaf_order.len(),
            height: info.depth + 1,
            entries: info.entries,
        })
    }

    fn find_leaf(&self, key: &[u8]) -> Result<Node> {
        let mut id = self.root;
        let mut visited = BTreeSet::new();
        loop {
            if !visited.insert(id) {
                return Err(StorageError::Invariant(
                    "cycle during B+ tree descent".into(),
                ));
            }
            let node = self.load_node(id)?;
            match &node {
                Node::Leaf { .. } => return Ok(node),
                Node::Internal { keys, children, .. } => {
                    let child_index = keys.partition_point(|separator| separator.as_slice() <= key);
                    id = *children.get(child_index).ok_or_else(|| {
                        StorageError::Invariant(format!(
                            "internal page {} has missing child",
                            node.id().0
                        ))
                    })?;
                }
            }
        }
    }

    fn split_leaf(&self, node: Node) -> Result<()> {
        let Node::Leaf {
            id,
            parent,
            previous,
            next,
            entries,
        } = node
        else {
            return Err(StorageError::Invariant(
                "split_leaf called on internal node".into(),
            ));
        };
        if entries.len() < 2 {
            return Err(StorageError::KeyTooLarge {
                bytes: entries
                    .first()
                    .map_or(0, |entry| entry.0.len() + entry.1.len()),
            });
        }
        let split = balanced_leaf_split(&entries);
        let left_entries = entries[..split].to_vec();
        let right_entries = entries[split..].to_vec();
        let separator = right_entries[0].0.clone();
        if parent.is_none() {
            let left_id = self.store.allocate(PageKind::BTreeLeaf)?;
            let right_id = self.store.allocate(PageKind::BTreeLeaf)?;
            let left = Node::Leaf {
                id: left_id,
                parent: Some(self.root),
                previous,
                next: Some(right_id),
                entries: left_entries,
            };
            let right = Node::Leaf {
                id: right_id,
                parent: Some(self.root),
                previous: Some(left_id),
                next,
                entries: right_entries,
            };
            self.relink_neighbor(previous, true, left_id)?;
            self.relink_neighbor(next, false, right_id)?;
            self.save_node(&left)?;
            self.save_node(&right)?;
            self.save_node(&Node::Internal {
                id: self.root,
                parent: None,
                keys: vec![separator],
                children: vec![left_id, right_id],
            })?;
            return Ok(());
        }

        let right_id = self.store.allocate(PageKind::BTreeLeaf)?;
        let left = Node::Leaf {
            id,
            parent,
            previous,
            next: Some(right_id),
            entries: left_entries,
        };
        let right = Node::Leaf {
            id: right_id,
            parent,
            previous: Some(id),
            next,
            entries: right_entries,
        };
        self.relink_neighbor(next, false, right_id)?;
        self.save_node(&left)?;
        self.save_node(&right)?;
        self.insert_parent(id, separator, right_id, parent.unwrap_or(self.root))
    }

    fn insert_parent(
        &self,
        left: PageId,
        separator: Vec<u8>,
        right: PageId,
        parent_id: PageId,
    ) -> Result<()> {
        let mut parent = self.load_node(parent_id)?;
        let Node::Internal { keys, children, .. } = &mut parent else {
            return Err(StorageError::Invariant(
                "leaf parent is not internal".into(),
            ));
        };
        let position = children
            .iter()
            .position(|child| *child == left)
            .ok_or_else(|| {
                StorageError::Invariant("parent does not reference split child".into())
            })?;
        children.insert(position + 1, right);
        keys.insert(position, separator);
        let mut right_node = self.load_node(right)?;
        right_node.set_parent(Some(parent_id));
        self.save_node(&right_node)?;
        if Self::node_fits(&parent)? {
            self.save_node(&parent)
        } else {
            self.split_internal(parent)
        }
    }

    fn split_internal(&self, node: Node) -> Result<()> {
        let Node::Internal {
            id,
            parent,
            keys,
            children,
        } = node
        else {
            return Err(StorageError::Invariant(
                "split_internal called on leaf".into(),
            ));
        };
        if children.len() < 3 || keys.len() + 1 != children.len() {
            return Err(StorageError::Invariant(
                "cannot split malformed internal node".into(),
            ));
        }
        let middle = keys.len() / 2;
        let promoted = keys[middle].clone();
        let left_keys = keys[..middle].to_vec();
        let right_keys = keys[middle + 1..].to_vec();
        let left_children = children[..=middle].to_vec();
        let right_children = children[middle + 1..].to_vec();

        if parent.is_none() {
            let left_id = self.store.allocate(PageKind::BTreeInternal)?;
            let right_id = self.store.allocate(PageKind::BTreeInternal)?;
            let left = Node::Internal {
                id: left_id,
                parent: Some(self.root),
                keys: left_keys,
                children: left_children,
            };
            let right = Node::Internal {
                id: right_id,
                parent: Some(self.root),
                keys: right_keys,
                children: right_children,
            };
            self.reparent_children(&left)?;
            self.reparent_children(&right)?;
            self.save_node(&left)?;
            self.save_node(&right)?;
            return self.save_node(&Node::Internal {
                id: self.root,
                parent: None,
                keys: vec![promoted],
                children: vec![left_id, right_id],
            });
        }

        let right_id = self.store.allocate(PageKind::BTreeInternal)?;
        let left = Node::Internal {
            id,
            parent,
            keys: left_keys,
            children: left_children,
        };
        let right = Node::Internal {
            id: right_id,
            parent,
            keys: right_keys,
            children: right_children,
        };
        self.reparent_children(&left)?;
        self.reparent_children(&right)?;
        self.save_node(&left)?;
        self.save_node(&right)?;
        self.insert_parent(id, promoted, right_id, parent.unwrap_or(self.root))
    }

    fn unlink_empty_leaf(&self, leaf: &Node) -> Result<()> {
        let Node::Leaf {
            id,
            parent: Some(parent_id),
            previous,
            next,
            entries,
        } = leaf
        else {
            return Err(StorageError::Invariant(
                "expected empty non-root leaf".into(),
            ));
        };
        if !entries.is_empty() {
            return Err(StorageError::Invariant(
                "attempted to unlink non-empty leaf".into(),
            ));
        }
        self.relink_neighbor(*previous, true, next.unwrap_or(PageId(NONE_PAGE)))?;
        self.relink_neighbor(*next, false, previous.unwrap_or(PageId(NONE_PAGE)))?;
        self.remove_child(*parent_id, *id)
    }

    fn remove_child(&self, parent_id: PageId, child_id: PageId) -> Result<()> {
        let mut parent = self.load_node(parent_id)?;
        let grandparent = parent.parent();
        let parent_is_root = parent.id() == self.root;
        let Node::Internal { keys, children, .. } = &mut parent else {
            return Err(StorageError::Invariant(
                "removed child parent is not internal".into(),
            ));
        };
        let position = children
            .iter()
            .position(|child| *child == child_id)
            .ok_or_else(|| StorageError::Invariant("parent missing removed child".into()))?;
        children.remove(position);
        if !keys.is_empty() {
            keys.remove(position.saturating_sub(1).min(keys.len() - 1));
        }
        if children.is_empty() {
            if parent_is_root {
                self.save_node(&Node::Leaf {
                    id: self.root,
                    parent: None,
                    previous: None,
                    next: None,
                    entries: Vec::new(),
                })?;
            } else {
                let grandparent = grandparent.ok_or_else(|| {
                    StorageError::Invariant("empty internal node has no parent".into())
                })?;
                self.remove_child(grandparent, parent_id)?;
            }
        } else if parent_is_root && children.len() == 1 {
            self.collapse_root(children[0])?;
        } else {
            self.save_node(&parent)?;
            self.recompute_internal_separators(parent.id())?;
        }
        Ok(())
    }

    fn collapse_root(&self, only_child: PageId) -> Result<()> {
        let mut child = self.load_node(only_child)?;
        if let Node::Internal { children, .. } = &child
            && children.len() == 1
        {
            return self.collapse_root(children[0]);
        }
        match &mut child {
            Node::Leaf {
                id,
                parent,
                previous,
                next,
                ..
            } => {
                *id = self.root;
                *parent = None;
                self.relink_neighbor(*previous, true, self.root)?;
                self.relink_neighbor(*next, false, self.root)?;
            }
            Node::Internal { id, parent, .. } => {
                *id = self.root;
                *parent = None;
                self.reparent_children(&child)?;
            }
        }
        self.save_node(&child)
    }

    fn propagate_min_change(&self, child_id: PageId) -> Result<()> {
        let child = self.load_node(child_id)?;
        let Some(parent_id) = child.parent() else {
            return Ok(());
        };
        let mut parent = self.load_node(parent_id)?;
        let Node::Internal { keys, children, .. } = &mut parent else {
            return Err(StorageError::Invariant(
                "child parent is not internal".into(),
            ));
        };
        let position = children
            .iter()
            .position(|child| *child == child_id)
            .ok_or_else(|| StorageError::Invariant("parent does not contain child".into()))?;
        if position > 0 {
            keys[position - 1] = self.subtree_min(child_id)?;
            self.save_node(&parent)
        } else {
            self.propagate_min_change(parent_id)
        }
    }

    fn recompute_internal_separators(&self, id: PageId) -> Result<()> {
        let mut node = self.load_node(id)?;
        let Node::Internal { keys, children, .. } = &mut node else {
            return Ok(());
        };
        *keys = children
            .iter()
            .skip(1)
            .map(|child| self.subtree_min(*child))
            .collect::<Result<Vec<_>>>()?;
        self.save_node(&node)?;
        self.propagate_min_change(id)
    }

    fn subtree_min(&self, mut id: PageId) -> Result<Vec<u8>> {
        loop {
            match self.load_node(id)? {
                Node::Leaf { entries, .. } => {
                    return entries.first().map(|entry| entry.0.clone()).ok_or_else(|| {
                        StorageError::Invariant("empty non-root leaf has no minimum".into())
                    });
                }
                Node::Internal { children, .. } => {
                    id = *children.first().ok_or_else(|| {
                        StorageError::Invariant("internal node has no children".into())
                    })?;
                }
            }
        }
    }

    fn relink_neighbor(
        &self,
        neighbor: Option<PageId>,
        update_next: bool,
        new: PageId,
    ) -> Result<()> {
        let Some(neighbor) = neighbor else {
            return Ok(());
        };
        let mut node = self.load_node(neighbor)?;
        let Node::Leaf { previous, next, .. } = &mut node else {
            return Err(StorageError::Invariant("leaf sibling is internal".into()));
        };
        let target = if new.0 == NONE_PAGE { None } else { Some(new) };
        if update_next {
            *next = target;
        } else {
            *previous = target;
        }
        self.save_node(&node)
    }

    fn reparent_children(&self, node: &Node) -> Result<()> {
        let Node::Internal { id, children, .. } = node else {
            return Ok(());
        };
        for child_id in children {
            let mut child = self.load_node(*child_id)?;
            child.set_parent(Some(*id));
            self.save_node(&child)?;
        }
        Ok(())
    }

    fn node_min(&self, node: &Node) -> Option<Vec<u8>> {
        match node {
            Node::Leaf { entries, .. } => entries.first().map(|entry| entry.0.clone()),
            Node::Internal { children, .. } => children
                .first()
                .and_then(|child| self.subtree_min(*child).ok()),
        }
    }

    fn node_fits(node: &Node) -> Result<bool> {
        match encode_node(node) {
            Ok(_) => Ok(true),
            Err(StorageError::KeyTooLarge { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn load_node(&self, id: PageId) -> Result<Node> {
        let page = self.store.load(id)?;
        decode_node(&page)
    }

    fn save_node(&self, node: &Node) -> Result<()> {
        self.store.save(&encode_node(node)?)
    }

    fn validate_subtree(
        &self,
        id: PageId,
        expected_parent: Option<PageId>,
        visited: &mut BTreeSet<PageId>,
        leaf_order: &mut Vec<PageId>,
    ) -> Result<SubtreeInfo> {
        if !visited.insert(id) {
            return Err(StorageError::Invariant(format!(
                "page {} is cyclic or multiply reachable",
                id.0
            )));
        }
        let node = self.load_node(id)?;
        if node.parent() != expected_parent {
            return Err(StorageError::Invariant(format!(
                "page {} has wrong parent",
                id.0
            )));
        }
        match node {
            Node::Leaf { entries, .. } => {
                if id != self.root && entries.is_empty() {
                    return Err(StorageError::Invariant(format!(
                        "non-root leaf {} is empty",
                        id.0
                    )));
                }
                if entries.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
                    return Err(StorageError::Invariant(format!(
                        "leaf {} keys are not strictly ordered",
                        id.0
                    )));
                }
                leaf_order.push(id);
                Ok(SubtreeInfo {
                    min: entries.first().map(|entry| entry.0.clone()),
                    max: entries.last().map(|entry| entry.0.clone()),
                    depth: 0,
                    entries: entries.len(),
                })
            }
            Node::Internal { keys, children, .. } => {
                if children.len() != keys.len() + 1 || children.is_empty() {
                    return Err(StorageError::Invariant(format!(
                        "internal page {} has invalid arity",
                        id.0
                    )));
                }
                if keys.windows(2).any(|pair| pair[0] >= pair[1]) {
                    return Err(StorageError::Invariant(format!(
                        "internal page {} separators are unordered",
                        id.0
                    )));
                }
                let mut infos = Vec::with_capacity(children.len());
                for child in &children {
                    infos.push(self.validate_subtree(*child, Some(id), visited, leaf_order)?);
                }
                let depth = infos[0].depth;
                if infos.iter().any(|info| info.depth != depth) {
                    return Err(StorageError::Invariant(format!(
                        "leaves below page {} have unequal depth",
                        id.0
                    )));
                }
                for index in 1..infos.len() {
                    let expected = infos[index].min.as_ref().ok_or_else(|| {
                        StorageError::Invariant("empty subtree under internal node".into())
                    })?;
                    if &keys[index - 1] != expected {
                        return Err(StorageError::Invariant(format!(
                            "page {} separator {} is not right-child minimum",
                            id.0,
                            index - 1
                        )));
                    }
                    if infos[index - 1]
                        .max
                        .as_ref()
                        .is_some_and(|maximum| maximum >= expected)
                    {
                        return Err(StorageError::Invariant(format!(
                            "page {} child ranges overlap",
                            id.0
                        )));
                    }
                }
                Ok(SubtreeInfo {
                    min: infos.first().and_then(|info| info.min.clone()),
                    max: infos.last().and_then(|info| info.max.clone()),
                    depth: depth + 1,
                    entries: infos.iter().map(|info| info.entries).sum(),
                })
            }
        }
    }
}

struct SubtreeInfo {
    min: Option<Vec<u8>>,
    max: Option<Vec<u8>>,
    depth: usize,
    entries: usize,
}

fn balanced_leaf_split(entries: &[(Vec<u8>, Vec<u8>)]) -> usize {
    let total: usize = entries
        .iter()
        .map(|entry| 4 + entry.0.len() + entry.1.len())
        .sum();
    let mut used = 0;
    for (index, entry) in entries.iter().enumerate().take(entries.len() - 1) {
        used += 4 + entry.0.len() + entry.1.len();
        if used * 2 >= total {
            return index + 1;
        }
    }
    entries.len() / 2
}

fn encode_node(node: &Node) -> Result<Page> {
    let (kind, id, parent) = match node {
        Node::Leaf { id, parent, .. } => (PageKind::BTreeLeaf, *id, *parent),
        Node::Internal { id, parent, .. } => (PageKind::BTreeInternal, *id, *parent),
    };
    let mut data = Vec::new();
    match node {
        Node::Leaf {
            previous,
            next,
            entries,
            ..
        } => {
            for (key, value) in entries {
                let key_len = u16::try_from(key.len())
                    .map_err(|_| StorageError::KeyTooLarge { bytes: key.len() })?;
                let value_len = u16::try_from(value.len())
                    .map_err(|_| StorageError::KeyTooLarge { bytes: value.len() })?;
                data.extend_from_slice(&key_len.to_le_bytes());
                data.extend_from_slice(&value_len.to_le_bytes());
                data.extend_from_slice(key);
                data.extend_from_slice(value);
            }
            let end = NODE_DATA
                .checked_add(data.len())
                .ok_or(StorageError::KeyTooLarge { bytes: data.len() })?;
            if end > PAGE_SIZE {
                return Err(StorageError::KeyTooLarge { bytes: data.len() });
            }
            let mut page = Page::new(id, kind);
            put_u64(
                &mut page,
                PAGE_HEADER_SIZE,
                parent.map_or(NONE_PAGE, |id| id.0),
            )?;
            put_u64(
                &mut page,
                PAGE_HEADER_SIZE + 8,
                previous.map_or(NONE_PAGE, |id| id.0),
            )?;
            put_u64(
                &mut page,
                PAGE_HEADER_SIZE + 16,
                next.map_or(NONE_PAGE, |id| id.0),
            )?;
            let entry_count = checked_u16(entries.len(), "B+ leaf entry count")?;
            put_u16(&mut page, PAGE_HEADER_SIZE + 24, entry_count)?;
            page.bytes_range_mut(NODE_DATA, data.len())?
                .copy_from_slice(&data);
            page.set_layout(
                checked_u16(end, "B+ leaf data end")?,
                PAGE_SIZE_U16,
                entry_count,
            );
            page.seal();
            Ok(page)
        }
        Node::Internal { keys, children, .. } => {
            if children.len() != keys.len() + 1 || children.is_empty() {
                return Err(StorageError::Invariant(
                    "internal node arity mismatch".into(),
                ));
            }
            data.extend_from_slice(&children[0].0.to_le_bytes());
            for (key, child) in keys.iter().zip(children.iter().skip(1)) {
                let key_len = u16::try_from(key.len())
                    .map_err(|_| StorageError::KeyTooLarge { bytes: key.len() })?;
                data.extend_from_slice(&key_len.to_le_bytes());
                data.extend_from_slice(key);
                data.extend_from_slice(&child.0.to_le_bytes());
            }
            let end = NODE_DATA
                .checked_add(data.len())
                .ok_or(StorageError::KeyTooLarge { bytes: data.len() })?;
            if end > PAGE_SIZE {
                return Err(StorageError::KeyTooLarge { bytes: data.len() });
            }
            let mut page = Page::new(id, kind);
            put_u64(
                &mut page,
                PAGE_HEADER_SIZE,
                parent.map_or(NONE_PAGE, |id| id.0),
            )?;
            let key_count = checked_u16(keys.len(), "B+ internal key count")?;
            put_u16(&mut page, PAGE_HEADER_SIZE + 24, key_count)?;
            page.bytes_range_mut(NODE_DATA, data.len())?
                .copy_from_slice(&data);
            page.set_layout(
                checked_u16(end, "B+ internal data end")?,
                PAGE_SIZE_U16,
                key_count,
            );
            page.seal();
            Ok(page)
        }
    }
}

fn decode_node(page: &Page) -> Result<Node> {
    page.validate()?;
    let id = page.id();
    let parent = page_id_option(get_u64(page, PAGE_HEADER_SIZE)?);
    let count = usize::from(get_u16(page, PAGE_HEADER_SIZE + 24)?);
    let data_end = usize::from(page.header().lower);
    if !(NODE_DATA..=PAGE_SIZE).contains(&data_end) {
        return Err(StorageError::InvalidPage(
            "B+ node data end is invalid".into(),
        ));
    }
    let mut cursor = NODE_DATA;
    match page.kind() {
        PageKind::BTreeLeaf => {
            let previous = page_id_option(get_u64(page, PAGE_HEADER_SIZE + 8)?);
            let next = page_id_option(get_u64(page, PAGE_HEADER_SIZE + 16)?);
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let key_len = usize::from(read_u16_at(page, &mut cursor, data_end)?);
                let value_len = usize::from(read_u16_at(page, &mut cursor, data_end)?);
                let key = read_bytes_at(page, &mut cursor, key_len, data_end)?.to_vec();
                let value = read_bytes_at(page, &mut cursor, value_len, data_end)?.to_vec();
                entries.push((key, value));
            }
            if cursor != data_end || entries.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
                return Err(StorageError::InvalidPage(
                    "malformed or unordered B+ leaf".into(),
                ));
            }
            Ok(Node::Leaf {
                id,
                parent,
                previous,
                next,
                entries,
            })
        }
        PageKind::BTreeInternal => {
            let first = read_u64_at(page, &mut cursor, data_end)?;
            let mut children = vec![PageId(first)];
            let mut keys = Vec::with_capacity(count);
            for _ in 0..count {
                let key_len = usize::from(read_u16_at(page, &mut cursor, data_end)?);
                keys.push(read_bytes_at(page, &mut cursor, key_len, data_end)?.to_vec());
                children.push(PageId(read_u64_at(page, &mut cursor, data_end)?));
            }
            if cursor != data_end || keys.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err(StorageError::InvalidPage(
                    "malformed or unordered B+ internal node".into(),
                ));
            }
            Ok(Node::Internal {
                id,
                parent,
                keys,
                children,
            })
        }
        other => Err(StorageError::InvalidPage(format!(
            "page {id:?} has non-B+ kind {other:?}"
        ))),
    }
}

fn page_id_option(value: u64) -> Option<PageId> {
    (value != NONE_PAGE).then_some(PageId(value))
}

fn checked_u16(value: usize, field: &str) -> Result<u16> {
    u16::try_from(value).map_err(|_| {
        StorageError::InvalidPage(format!("{field} value {value} exceeds on-disk u16"))
    })
}

fn get_u16(page: &Page, offset: usize) -> Result<u16> {
    let bytes = page.bytes_range(offset, 2)?;
    Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
}

fn get_u64(page: &Page, offset: usize) -> Result<u64> {
    let bytes = page.bytes_range(offset, 8)?;
    let mut array = [0; 8];
    array.copy_from_slice(bytes);
    Ok(u64::from_le_bytes(array))
}

fn put_u16(page: &mut Page, offset: usize, value: u16) -> Result<()> {
    page.bytes_range_mut(offset, 2)?
        .copy_from_slice(&value.to_le_bytes());
    Ok(())
}

fn put_u64(page: &mut Page, offset: usize, value: u64) -> Result<()> {
    page.bytes_range_mut(offset, 8)?
        .copy_from_slice(&value.to_le_bytes());
    Ok(())
}

fn read_u16_at(page: &Page, cursor: &mut usize, end: usize) -> Result<u16> {
    let next = cursor
        .checked_add(2)
        .ok_or_else(|| StorageError::InvalidPage("B+ cursor overflow".into()))?;
    if next > end {
        return Err(StorageError::InvalidPage("truncated B+ entry".into()));
    }
    let value = get_u16(page, *cursor)?;
    *cursor = next;
    Ok(value)
}

fn read_u64_at(page: &Page, cursor: &mut usize, end: usize) -> Result<u64> {
    let next = cursor
        .checked_add(8)
        .ok_or_else(|| StorageError::InvalidPage("B+ cursor overflow".into()))?;
    if next > end {
        return Err(StorageError::InvalidPage(
            "truncated B+ child pointer".into(),
        ));
    }
    let value = get_u64(page, *cursor)?;
    *cursor = next;
    Ok(value)
}

fn read_bytes_at<'a>(
    page: &'a Page,
    cursor: &mut usize,
    length: usize,
    end: usize,
) -> Result<&'a [u8]> {
    let next = cursor
        .checked_add(length)
        .ok_or_else(|| StorageError::InvalidPage("B+ cursor overflow".into()))?;
    if next > end {
        return Err(StorageError::InvalidPage("truncated B+ key/value".into()));
    }
    let value = page.bytes_range(*cursor, length)?;
    *cursor = next;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(value: u32) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    #[test]
    fn randomized_model_survives_splits_reopen_and_deletes() {
        let store = MemoryPageStore::new();
        let tree = BPlusTree::create(store.clone()).unwrap();
        let root = tree.root_page();
        let mut model = BTreeMap::new();
        let mut seed = 0xa57e_daba_5eed_u64;
        for step in 0..5_000_u32 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let number = (seed % 1_500) as u32;
            if seed.is_multiple_of(4) {
                assert_eq!(tree.delete(&key(number)).unwrap(), model.remove(&number));
            } else {
                let length = 24 + usize::try_from(seed % 80).unwrap();
                let value = vec![u8::try_from(step % 251).unwrap(); length];
                assert_eq!(
                    tree.insert(key(number), value.clone()).unwrap(),
                    model.insert(number, value)
                );
            }
            if step % 100 == 0 {
                let report = tree.validate().unwrap();
                assert_eq!(report.entries, model.len());
            }
        }
        let reopened = BPlusTree::open(store, root).unwrap();
        assert_eq!(reopened.root_page(), root);
        let actual = reopened.range(&[], None).unwrap();
        let expected: Vec<_> = model
            .into_iter()
            .map(|(number, value)| (key(number), value))
            .collect();
        assert_eq!(actual, expected);
        reopened.validate().unwrap();
    }

    #[test]
    fn root_page_stays_stable_across_height_growth() {
        let store = MemoryPageStore::new();
        let tree = BPlusTree::create(store).unwrap();
        let root = tree.root_page();
        for number in 0..10_000 {
            tree.insert(key(number), vec![7; 100]).unwrap();
        }
        let report = tree.validate().unwrap();
        assert!(report.height >= 3);
        assert_eq!(tree.root_page(), root);
    }

    #[test]
    fn deleting_every_key_contracts_all_levels_to_empty_stable_root() {
        let store = MemoryPageStore::new();
        let tree = BPlusTree::create(store.clone()).unwrap();
        let root = tree.root_page();
        let internal_a = store.allocate(PageKind::BTreeInternal).unwrap();
        let internal_b = store.allocate(PageKind::BTreeInternal).unwrap();
        let leaf_a = store.allocate(PageKind::BTreeLeaf).unwrap();
        let leaf_b = store.allocate(PageKind::BTreeLeaf).unwrap();
        tree.save_node(&Node::Leaf {
            id: leaf_a,
            parent: Some(internal_a),
            previous: None,
            next: Some(leaf_b),
            entries: vec![(key(1), vec![1])],
        })
        .unwrap();
        tree.save_node(&Node::Leaf {
            id: leaf_b,
            parent: Some(internal_b),
            previous: Some(leaf_a),
            next: None,
            entries: vec![(key(2), vec![2])],
        })
        .unwrap();
        tree.save_node(&Node::Internal {
            id: internal_a,
            parent: Some(root),
            keys: Vec::new(),
            children: vec![leaf_a],
        })
        .unwrap();
        tree.save_node(&Node::Internal {
            id: internal_b,
            parent: Some(root),
            keys: Vec::new(),
            children: vec![leaf_b],
        })
        .unwrap();
        tree.save_node(&Node::Internal {
            id: root,
            parent: None,
            keys: vec![key(2)],
            children: vec![internal_a, internal_b],
        })
        .unwrap();
        assert_eq!(tree.validate().unwrap().height, 3);
        assert_eq!(tree.delete(&key(1)).unwrap(), Some(vec![1]));
        let contracted = tree.validate().unwrap();
        assert_eq!(contracted.height, 1);
        assert_eq!(contracted.entries, 1);
        assert_eq!(tree.delete(&key(2)).unwrap(), Some(vec![2]));
        let report = tree.validate().unwrap();
        assert_eq!(report.entries, 0);
        assert_eq!(report.height, 1);
        assert_eq!(tree.root_page(), root);
    }
}
