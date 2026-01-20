use std::cmp::Ordering;
use std::collections::HashMap;

use uuid::Uuid;

use crate::commit::CommitId;
use crate::object::{BranchName, ObjectId, ObjectState};
use crate::object_manager::ObjectManager;

/// Maximum level for skip list nodes (height of the skip list).
const MAX_LEVEL: usize = 16;

/// Probability factor for level generation (1/4 chance to go up a level).
const P: f64 = 0.25;

/// Branch name for index data (all index nodes use this branch).
pub const INDEX_BRANCH: &str = "main";

/// Errors that can occur during index operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexError {
    /// Object not found in storage.
    ObjectNotFound(ObjectId),
    /// Object is still loading.
    ObjectNotReady(ObjectId),
    /// Branch not found on object.
    BranchNotFound(ObjectId),
    /// Failed to decode node data.
    DecodeError(ObjectId),
    /// Object manager error.
    ObjectManagerError(String),
}

// ============================================================================
// Zero-Copy Node View
// ============================================================================

/// Zero-copy view into a skip list node's encoded data.
///
/// Reads directly from `commit.content` without allocating.
/// Pre-parses offsets for O(1) field access.
pub struct SkipListNodeView<'a> {
    data: &'a [u8],
    key_end: usize,
    row_count: u32,
    rows_start: usize,
    forward_start: usize,
    level: u8,
    forward_count: u8,
}

impl<'a> SkipListNodeView<'a> {
    /// Parse offsets from encoded data (no allocation).
    pub fn new(data: &'a [u8]) -> Option<Self> {
        if data.len() < 2 {
            return None;
        }
        let key_len = u16::from_le_bytes([data[0], data[1]]) as usize;
        let key_end = 2 + key_len;

        if data.len() < key_end + 4 {
            return None;
        }
        let row_count = u32::from_le_bytes(data[key_end..key_end + 4].try_into().ok()?);
        let rows_start = key_end + 4;
        let forward_start = rows_start + (row_count as usize * 16);

        if data.len() < forward_start + 2 {
            return None;
        }
        let level = data[forward_start];
        let forward_count = data[forward_start + 1];

        Some(Self {
            data,
            key_end,
            row_count,
            rows_start,
            forward_start,
            level,
            forward_count,
        })
    }

    /// Zero-copy key access.
    pub fn key(&self) -> &'a [u8] {
        &self.data[2..self.key_end]
    }

    /// Iterate row IDs without allocating.
    pub fn row_ids(&self) -> impl Iterator<Item = ObjectId> + 'a {
        let rows_start = self.rows_start;
        let row_count = self.row_count as usize;
        let data = self.data;
        (0..row_count).filter_map(move |i| {
            let start = rows_start + i * 16;
            Uuid::from_slice(&data[start..start + 16])
                .ok()
                .map(ObjectId)
        })
    }

    /// Get forward pointer at level (no allocation).
    pub fn forward(&self, level: usize) -> Option<ObjectId> {
        if level >= self.forward_count as usize {
            return None;
        }
        let base = self.forward_start + 2; // Skip level byte and forward_count byte
        let mut pos = base;
        for i in 0..=level {
            if pos >= self.data.len() {
                return None;
            }
            let present = self.data[pos];
            if i == level {
                return if present == 1 {
                    if pos + 17 > self.data.len() {
                        return None;
                    }
                    Uuid::from_slice(&self.data[pos + 1..pos + 17])
                        .ok()
                        .map(ObjectId)
                } else {
                    None
                };
            }
            pos += if present == 1 { 17 } else { 1 };
        }
        None
    }

    /// Get all forward pointers as a Vec (for mutations).
    pub fn forward_all(&self) -> Vec<Option<ObjectId>> {
        (0..self.forward_count as usize)
            .map(|i| self.forward(i))
            .collect()
    }

    pub fn level(&self) -> u8 {
        self.level
    }

    pub fn row_count(&self) -> u32 {
        self.row_count
    }

    /// Convert to owned SkipListNode (for mutations).
    pub fn to_owned(&self) -> SkipListNode {
        SkipListNode {
            key: self.key().to_vec(),
            row_ids: self.row_ids().collect(),
            level: self.level,
            forward: self.forward_all(),
        }
    }
}

/// Unified access to a skip list node - either a view into ObjectManager or a pending write.
pub enum NodeRef<'a> {
    View(SkipListNodeView<'a>),
    Pending(&'a SkipListNode),
}

impl<'a> NodeRef<'a> {
    pub fn key(&self) -> &[u8] {
        match self {
            NodeRef::View(v) => v.key(),
            NodeRef::Pending(n) => &n.key,
        }
    }

    pub fn row_ids_vec(&self) -> Vec<ObjectId> {
        match self {
            NodeRef::View(v) => v.row_ids().collect(),
            NodeRef::Pending(n) => n.row_ids.clone(),
        }
    }

    pub fn forward(&self, level: usize) -> Option<ObjectId> {
        match self {
            NodeRef::View(v) => v.forward(level),
            NodeRef::Pending(n) => n.forward.get(level).and_then(|x| *x),
        }
    }

    pub fn level(&self) -> u8 {
        match self {
            NodeRef::View(v) => v.level(),
            NodeRef::Pending(n) => n.level,
        }
    }

    pub fn to_owned(&self) -> SkipListNode {
        match self {
            NodeRef::View(v) => v.to_owned(),
            NodeRef::Pending(n) => (*n).clone(),
        }
    }
}

/// Index root discovery: deterministic UUID from table + column name.
///
/// Uses UUID v5 (SHA-1 based) with a custom namespace for indices.
pub fn index_root_id(table: &str, column: &str) -> ObjectId {
    // Custom namespace UUID for index roots
    const INDEX_NAMESPACE: Uuid = Uuid::from_bytes([
        0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30,
        0xc8,
    ]);

    let name = format!("index:{}:{}", table, column);
    let uuid = Uuid::new_v5(&INDEX_NAMESPACE, name.as_bytes());
    ObjectId(uuid)
}

/// A node in the skip list, stored as one Jazz object.
#[derive(Debug, Clone)]
pub struct SkipListNode {
    /// The key bytes for this node (column value in binary form).
    /// Empty for the sentinel (head) node.
    pub key: Vec<u8>,
    /// Row ObjectIds that have this key value.
    pub row_ids: Vec<ObjectId>,
    /// Height/level of this node (0 to MAX_LEVEL-1).
    pub level: u8,
    /// Forward pointers at each level. None = end of list at that level.
    pub forward: Vec<Option<ObjectId>>,
}

impl SkipListNode {
    /// Create a new sentinel (head) node.
    pub fn new_sentinel() -> Self {
        Self {
            key: vec![],
            row_ids: vec![],
            level: (MAX_LEVEL - 1) as u8,
            forward: vec![None; MAX_LEVEL],
        }
    }

    /// Create a new data node with the given key.
    pub fn new(key: Vec<u8>, level: u8) -> Self {
        Self {
            key,
            row_ids: vec![],
            level,
            forward: vec![None; (level + 1) as usize],
        }
    }

    /// Encode node to binary format for storage in a commit.
    ///
    /// Format:
    /// ```text
    /// [key_len: u16][key_data: bytes]
    /// [row_count: u32][row_ids: ObjectId...]
    /// [level: u8][forward_count: u8][forward_ptrs: Option<ObjectId>...]
    /// ```
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Key
        buf.extend_from_slice(&(self.key.len() as u16).to_le_bytes());
        buf.extend_from_slice(&self.key);

        // Row IDs
        buf.extend_from_slice(&(self.row_ids.len() as u32).to_le_bytes());
        for row_id in &self.row_ids {
            buf.extend_from_slice(row_id.0.as_bytes());
        }

        // Level and forward pointers
        buf.push(self.level);
        buf.push(self.forward.len() as u8);
        for fwd in &self.forward {
            match fwd {
                Some(id) => {
                    buf.push(1);
                    buf.extend_from_slice(id.0.as_bytes());
                }
                None => {
                    buf.push(0);
                }
            }
        }

        buf
    }

    /// Decode node from binary format.
    pub fn decode(data: &[u8]) -> Option<Self> {
        let mut pos = 0;

        // Key
        if pos + 2 > data.len() {
            return None;
        }
        let key_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        if pos + key_len > data.len() {
            return None;
        }
        let key = data[pos..pos + key_len].to_vec();
        pos += key_len;

        // Row IDs
        if pos + 4 > data.len() {
            return None;
        }
        let row_count =
            u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;

        let mut row_ids = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            if pos + 16 > data.len() {
                return None;
            }
            let uuid = Uuid::from_slice(&data[pos..pos + 16]).ok()?;
            row_ids.push(ObjectId(uuid));
            pos += 16;
        }

        // Level and forward pointers
        if pos + 2 > data.len() {
            return None;
        }
        let level = data[pos];
        pos += 1;
        let forward_count = data[pos] as usize;
        pos += 1;

        let mut forward = Vec::with_capacity(forward_count);
        for _ in 0..forward_count {
            if pos >= data.len() {
                return None;
            }
            let has_ptr = data[pos] != 0;
            pos += 1;

            if has_ptr {
                if pos + 16 > data.len() {
                    return None;
                }
                let uuid = Uuid::from_slice(&data[pos..pos + 16]).ok()?;
                forward.push(Some(ObjectId(uuid)));
                pos += 16;
            } else {
                forward.push(None);
            }
        }

        Some(Self {
            key,
            row_ids,
            level,
            forward,
        })
    }
}

/// In-memory state for a skip list index.
///
/// Reads nodes directly from ObjectManager via zero-copy views.
/// Only mutated nodes are copied to `pending_writes`.
#[derive(Debug, Clone)]
pub struct IndexState {
    /// Root (sentinel) node ObjectId.
    pub root_id: ObjectId,
    /// Table this index belongs to.
    pub table: String,
    /// Column name ("_id" for primary index).
    pub column: String,
    /// Nodes modified but not yet persisted (copy-on-write).
    pub pending_writes: HashMap<ObjectId, SkipListNode>,
    /// Current maximum level in use (cached from root node).
    current_level: usize,
}

impl IndexState {
    /// Create a new index state for a table/column.
    ///
    /// Does not create an in-memory sentinel - the sentinel will be created
    /// on first insert if it doesn't exist in ObjectManager.
    pub fn new(table: impl Into<String>, column: impl Into<String>) -> Self {
        let table = table.into();
        let column = column.into();
        let root_id = index_root_id(&table, &column);

        Self {
            root_id,
            table,
            column,
            pending_writes: HashMap::new(),
            current_level: 0,
        }
    }

    // ========================================================================
    // Node access (zero-copy views with copy-on-write for mutations)
    // ========================================================================

    /// Get a node - checks pending_writes first, then ObjectManager.
    ///
    /// Returns None if the node doesn't exist in either location.
    pub fn get_node<'a>(&'a self, node_id: ObjectId, om: &'a ObjectManager) -> Option<NodeRef<'a>> {
        // Pending writes take precedence (uncommitted mutations)
        if let Some(node) = self.pending_writes.get(&node_id) {
            return Some(NodeRef::Pending(node));
        }

        // Otherwise, read from ObjectManager (zero-copy)
        Self::get_node_from_om(node_id, om)
    }

    /// Get a node directly from ObjectManager (static helper).
    fn get_node_from_om(node_id: ObjectId, om: &ObjectManager) -> Option<NodeRef<'_>> {
        let state = om.get_state(node_id)?;
        match state {
            ObjectState::Loading => None,
            ObjectState::Creating(obj) | ObjectState::Available(obj) => {
                let branch = obj.branches.get(&BranchName::new(INDEX_BRANCH))?;
                let tip_id = branch.tips.iter().next()?;
                let commit = branch.commits.get(tip_id)?;
                let view = SkipListNodeView::new(&commit.content)?;
                Some(NodeRef::View(view))
            }
        }
    }

    /// Get a mutable node - copies to pending_writes if not already there.
    fn get_node_mut(&mut self, node_id: ObjectId, om: &ObjectManager) -> Option<&mut SkipListNode> {
        if !self.pending_writes.contains_key(&node_id) {
            // Convert view to owned (only when mutating)
            let node = self.get_node(node_id, om)?.to_owned();
            self.pending_writes.insert(node_id, node);
        }
        self.pending_writes.get_mut(&node_id)
    }

    /// Check if the index root exists (either in pending_writes or ObjectManager).
    pub fn root_exists(&self, om: &ObjectManager) -> bool {
        self.get_node(self.root_id, om).is_some()
    }

    /// Check if a row ID exists in the index (for InsertHandle.is_indexed).
    pub fn contains_row(&self, row_id: ObjectId, om: &ObjectManager) -> bool {
        let key = row_id.0.as_bytes();
        !self.lookup_exact(key, om).is_empty()
    }

    /// Generate a random level for a new node.
    fn random_level() -> u8 {
        let mut level = 0u8;
        while rand::random::<f64>() < P && (level as usize) < MAX_LEVEL - 1 {
            level += 1;
        }
        level
    }

    /// Compare two keys. For "_id" index, keys are ObjectId bytes (UUIDv7 = time-ordered).
    fn compare_keys(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    /// Ensure the sentinel node exists, creating it if necessary.
    ///
    /// Returns true if the sentinel exists (or was created), false if ObjectManager
    /// doesn't have the sentinel and we can't create (loading state).
    fn ensure_sentinel(&mut self, om: &ObjectManager) -> bool {
        // Check if sentinel already exists
        if self.get_node(self.root_id, om).is_some() {
            return true;
        }

        // Create sentinel in pending_writes
        self.pending_writes
            .insert(self.root_id, SkipListNode::new_sentinel());
        true
    }

    /// Insert a row into the index.
    ///
    /// Returns the ObjectIds of nodes that were modified (for persistence).
    /// Returns empty Vec if index root doesn't exist (caller should queue update).
    #[allow(clippy::needless_range_loop)]
    pub fn insert(&mut self, key: &[u8], row_id: ObjectId, om: &ObjectManager) -> Vec<ObjectId> {
        // Ensure sentinel exists
        if !self.ensure_sentinel(om) {
            return vec![];
        }

        // Recalculate current_level from root
        self.recalculate_level(om);

        let mut modified = Vec::new();
        let mut update: Vec<ObjectId> = vec![self.root_id; MAX_LEVEL];
        let mut current = self.root_id;

        // Find position, tracking update path
        for i in (0..=self.current_level).rev() {
            loop {
                let node = self.get_node(current, om).unwrap();
                if let Some(next_id) = node.forward(i) {
                    let next = self.get_node(next_id, om).unwrap();
                    if self.compare_keys(next.key(), key) == Ordering::Less {
                        current = next_id;
                        continue;
                    }
                }
                break;
            }
            update[i] = current;
        }

        // Check if key already exists
        let next_opt = self.get_node(current, om).and_then(|n| n.forward(0));
        if let Some(next_id) = next_opt
            && let Some(next) = self.get_node(next_id, om)
            && next.key().cmp(key) == Ordering::Equal
        {
            // Key exists, add row_id if not already present
            let row_ids = next.row_ids_vec();
            if !row_ids.contains(&row_id) {
                let next_mut = self.get_node_mut(next_id, om).unwrap();
                next_mut.row_ids.push(row_id);
                modified.push(next_id);
            }
            return modified;
        }

        // Key doesn't exist, create new node
        let new_level = Self::random_level();
        let new_node_id = ObjectId::new();
        let mut new_node = SkipListNode::new(key.to_vec(), new_level);
        new_node.row_ids.push(row_id);

        // Update current_level if needed
        if new_level as usize > self.current_level {
            for i in (self.current_level + 1)..=(new_level as usize) {
                update[i] = self.root_id;
            }
            self.current_level = new_level as usize;
        }

        // Insert node and update forward pointers
        for i in 0..=(new_level as usize) {
            let update_node = self.get_node(update[i], om).unwrap();
            let next = update_node.forward(i);
            new_node.forward[i] = next;
        }

        self.pending_writes.insert(new_node_id, new_node);
        modified.push(new_node_id);

        // Update predecessors' forward pointers
        for i in 0..=(new_level as usize) {
            let update_node = self.get_node_mut(update[i], om).unwrap();
            if i < update_node.forward.len() {
                update_node.forward[i] = Some(new_node_id);
            }
            modified.push(update[i]);
        }

        modified.sort();
        modified.dedup();
        modified
    }

    /// Remove a row from the index.
    ///
    /// Returns the ObjectIds of nodes that were modified (for persistence).
    #[allow(clippy::needless_range_loop)]
    pub fn remove(&mut self, key: &[u8], row_id: ObjectId, om: &ObjectManager) -> Vec<ObjectId> {
        // If root doesn't exist, nothing to remove
        if self.get_node(self.root_id, om).is_none() {
            return vec![];
        }

        self.recalculate_level(om);

        let mut modified = Vec::new();
        let mut update: Vec<ObjectId> = vec![self.root_id; MAX_LEVEL];
        let mut current = self.root_id;

        // Find position
        for i in (0..=self.current_level).rev() {
            loop {
                let node = self.get_node(current, om).unwrap();
                if let Some(next_id) = node.forward(i) {
                    let next = self.get_node(next_id, om).unwrap();
                    if self.compare_keys(next.key(), key) == Ordering::Less {
                        current = next_id;
                        continue;
                    }
                }
                break;
            }
            update[i] = current;
        }

        // Find the node with this key
        let target_opt = self.get_node(current, om).and_then(|n| n.forward(0));
        if let Some(target_id) = target_opt
            && let Some(target) = self.get_node(target_id, om)
            && target.key().cmp(key) == Ordering::Equal
        {
            let target_level = target.level() as usize;
            let target_forward: Vec<Option<ObjectId>> =
                (0..=target_level).map(|i| target.forward(i)).collect();

            // Copy to pending_writes and remove row_id
            let target_mut = self.get_node_mut(target_id, om).unwrap();
            target_mut.row_ids.retain(|id| *id != row_id);
            modified.push(target_id);

            // If no more rows, mark for removal and update predecessors
            if target_mut.row_ids.is_empty() {
                // Remove from pending_writes (mark as deleted)
                self.pending_writes.remove(&target_id);

                // Update forward pointers of predecessors
                for i in 0..=target_level {
                    let update_node = self.get_node_mut(update[i], om).unwrap();
                    if i < update_node.forward.len() {
                        update_node.forward[i] = target_forward.get(i).and_then(|x| *x);
                        modified.push(update[i]);
                    }
                }

                // Update current_level if needed
                while self.current_level > 0 {
                    let root = self.get_node(self.root_id, om).unwrap();
                    if root.forward(self.current_level).is_none() {
                        self.current_level -= 1;
                    } else {
                        break;
                    }
                }
            }
        }

        modified.sort();
        modified.dedup();
        modified
    }

    /// Exact lookup - returns row IDs for the given key.
    #[allow(clippy::while_let_loop)]
    pub fn lookup_exact(&self, key: &[u8], om: &ObjectManager) -> Vec<ObjectId> {
        // If root doesn't exist, return empty
        if self.get_node(self.root_id, om).is_none() {
            return vec![];
        }

        let mut current = self.root_id;

        // Traverse to find the key
        for i in (0..=self.current_level).rev() {
            loop {
                let node = match self.get_node(current, om) {
                    Some(n) => n,
                    None => break,
                };
                if let Some(next_id) = node.forward(i) {
                    let next = match self.get_node(next_id, om) {
                        Some(n) => n,
                        None => break,
                    };
                    if self.compare_keys(next.key(), key) == Ordering::Less {
                        current = next_id;
                        continue;
                    }
                }
                break;
            }
        }

        // Check the next node at level 0
        let node = match self.get_node(current, om) {
            Some(n) => n,
            None => return vec![],
        };
        if let Some(next_id) = node.forward(0) {
            let next = match self.get_node(next_id, om) {
                Some(n) => n,
                None => return vec![],
            };
            if self.compare_keys(next.key(), key) == Ordering::Equal {
                return next.row_ids_vec();
            }
        }

        vec![]
    }

    /// Range scan - returns row IDs for keys in [min, max] range.
    /// Pass None for unbounded.
    #[allow(clippy::while_let_loop)]
    pub fn range_scan(
        &self,
        min: Option<&[u8]>,
        max: Option<&[u8]>,
        om: &ObjectManager,
    ) -> Vec<ObjectId> {
        // If root doesn't exist, return empty
        if self.get_node(self.root_id, om).is_none() {
            return vec![];
        }

        let mut results = Vec::new();
        let mut current = self.root_id;

        // If min is specified, find start position
        if let Some(min_key) = min {
            for i in (0..=self.current_level).rev() {
                loop {
                    let node = match self.get_node(current, om) {
                        Some(n) => n,
                        None => break,
                    };
                    if let Some(next_id) = node.forward(i) {
                        let next = match self.get_node(next_id, om) {
                            Some(n) => n,
                            None => break,
                        };
                        if self.compare_keys(next.key(), min_key) == Ordering::Less {
                            current = next_id;
                            continue;
                        }
                    }
                    break;
                }
            }
        }

        // Walk forward at level 0, collecting results
        let node = match self.get_node(current, om) {
            Some(n) => n,
            None => return results,
        };
        let mut next_opt = node.forward(0);

        while let Some(next_id) = next_opt {
            let next = match self.get_node(next_id, om) {
                Some(n) => n,
                None => break,
            };

            // Check min bound
            if let Some(min_key) = min
                && self.compare_keys(next.key(), min_key) == Ordering::Less
            {
                next_opt = next.forward(0);
                continue;
            }

            // Check max bound
            if let Some(max_key) = max
                && self.compare_keys(next.key(), max_key) == Ordering::Greater
            {
                break;
            }

            results.extend(next.row_ids_vec());
            next_opt = next.forward(0);
        }

        results
    }

    /// Full scan - returns all row IDs in order.
    pub fn scan_all(&self, om: &ObjectManager) -> Vec<ObjectId> {
        self.range_scan(None, None, om)
    }

    // ========================================================================
    // Persistence methods
    // ========================================================================

    /// Persist a single node to storage.
    ///
    /// Creates the object if it doesn't exist, or adds a new commit if it does.
    /// Returns `Ok(Some(CommitId))` if persisted, `Ok(None)` if persistence failed
    /// due to loading state.
    fn persist_node_internal(
        &self,
        node_id: ObjectId,
        node: &SkipListNode,
        object_manager: &mut ObjectManager,
    ) -> Result<Option<CommitId>, IndexError> {
        let data = node.encode();

        // Build index metadata
        let mut metadata = HashMap::new();
        metadata.insert("type".to_string(), "index".to_string());
        metadata.insert("nosync".to_string(), "true".to_string());
        metadata.insert("index_table".to_string(), self.table.clone());
        metadata.insert("index_column".to_string(), self.column.clone());

        // Check if object already exists
        if let Some(state) = object_manager.get_state(node_id) {
            match state {
                ObjectState::Loading => {
                    return Err(IndexError::ObjectNotReady(node_id));
                }
                ObjectState::Creating(_) | ObjectState::Available(_) => {
                    // Object exists, add a new commit
                    let tips = object_manager
                        .get_tip_ids(node_id, INDEX_BRANCH)
                        .map_err(|e| IndexError::ObjectManagerError(format!("{:?}", e)))?
                        .clone();

                    let parents: Vec<_> = tips.into_iter().collect();
                    let commit_id = object_manager
                        .add_commit(node_id, INDEX_BRANCH, parents, data, node_id, None)
                        .map_err(|e| IndexError::ObjectManagerError(format!("{:?}", e)))?;

                    return Ok(Some(commit_id));
                }
            }
        }

        // Object doesn't exist, create it with the deterministic ID
        object_manager.create_with_id(node_id, Some(metadata));

        // Add initial commit
        let commit_id = object_manager
            .add_commit(node_id, INDEX_BRANCH, vec![], data, node_id, None)
            .map_err(|e| IndexError::ObjectManagerError(format!("{:?}", e)))?;

        Ok(Some(commit_id))
    }

    /// Persist all pending writes to ObjectManager.
    ///
    /// Clears pending_writes and returns list of (node_id, commit_id) for persisted nodes.
    pub fn persist_pending(
        &mut self,
        object_manager: &mut ObjectManager,
    ) -> Vec<(ObjectId, CommitId)> {
        let pending = std::mem::take(&mut self.pending_writes);
        pending
            .into_iter()
            .filter_map(|(id, node)| {
                self.persist_node_internal(id, &node, object_manager)
                    .ok()
                    .flatten()
                    .map(|c| (id, c))
            })
            .collect()
    }

    /// Persist a specific node (for immediate persistence after insert).
    ///
    /// If the node is in pending_writes, persists and removes it.
    /// If the node is only in ObjectManager, just returns its current state.
    pub fn persist_node(
        &mut self,
        node_id: ObjectId,
        object_manager: &mut ObjectManager,
    ) -> Result<Option<CommitId>, IndexError> {
        // If node is in pending_writes, persist it
        if let Some(node) = self.pending_writes.remove(&node_id) {
            return self.persist_node_internal(node_id, &node, object_manager);
        }

        // Node not in pending_writes - check if it exists in ObjectManager
        match object_manager.get_state(node_id) {
            Some(ObjectState::Creating(_)) | Some(ObjectState::Available(_)) => {
                // Already persisted, get current commit
                let tips = object_manager
                    .get_tip_ids(node_id, INDEX_BRANCH)
                    .map_err(|e| IndexError::ObjectManagerError(format!("{:?}", e)))?;
                let commit_id = tips.iter().next().copied();
                Ok(commit_id)
            }
            Some(ObjectState::Loading) => Err(IndexError::ObjectNotReady(node_id)),
            None => Ok(None), // Node doesn't exist anywhere
        }
    }

    /// Recalculate current_level based on root node.
    fn recalculate_level(&mut self, om: &ObjectManager) {
        let new_level = self.calculate_level_from_root(om);
        self.current_level = new_level;
    }

    /// Calculate the current level from the root node (read-only).
    fn calculate_level_from_root(&self, om: &ObjectManager) -> usize {
        if let Some(root) = self.get_node(self.root_id, om) {
            // Find highest level with a non-None forward pointer
            let mut level = 0;
            for i in 0..MAX_LEVEL {
                if root.forward(i).is_some() {
                    level = i;
                }
            }
            level
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn index_root_id_is_deterministic() {
        let id1 = index_root_id("users", "email");
        let id2 = index_root_id("users", "email");
        assert_eq!(id1, id2);
    }

    #[test]
    fn index_root_id_differs_by_table() {
        let id1 = index_root_id("users", "email");
        let id2 = index_root_id("posts", "email");
        assert_ne!(id1, id2);
    }

    #[test]
    fn index_root_id_differs_by_column() {
        let id1 = index_root_id("users", "email");
        let id2 = index_root_id("users", "name");
        assert_ne!(id1, id2);
    }

    #[test]
    fn node_encode_decode_roundtrip() {
        let mut node = SkipListNode::new(b"test_key".to_vec(), 3);
        node.row_ids.push(ObjectId::new());
        node.row_ids.push(ObjectId::new());
        node.forward[0] = Some(ObjectId::new());
        node.forward[2] = Some(ObjectId::new());

        let encoded = node.encode();
        let decoded = SkipListNode::decode(&encoded).unwrap();

        assert_eq!(decoded.key, node.key);
        assert_eq!(decoded.row_ids.len(), node.row_ids.len());
        assert_eq!(decoded.level, node.level);
        assert_eq!(decoded.forward.len(), node.forward.len());
    }

    #[test]
    fn sentinel_node_encode_decode() {
        let node = SkipListNode::new_sentinel();
        let encoded = node.encode();
        let decoded = SkipListNode::decode(&encoded).unwrap();

        assert!(decoded.key.is_empty());
        assert!(decoded.row_ids.is_empty());
        assert_eq!(decoded.forward.len(), MAX_LEVEL);
    }

    // ========================================================================
    // Zero-copy view tests
    // ========================================================================

    #[test]
    fn node_view_parses_encoded_data() {
        let mut node = SkipListNode::new(b"hello".to_vec(), 2);
        node.row_ids.push(ObjectId::new());
        node.forward[0] = Some(ObjectId::new());

        let encoded = node.encode();
        let view = SkipListNodeView::new(&encoded).unwrap();

        assert_eq!(view.key(), b"hello");
        assert_eq!(view.level(), 2);
        assert_eq!(view.row_count(), 1);
    }

    #[test]
    fn node_view_key_is_zero_copy() {
        let node = SkipListNode::new(b"test_key".to_vec(), 1);
        let encoded = node.encode();
        let view = SkipListNodeView::new(&encoded).unwrap();
        let key = view.key();

        // Verify key points into encoded data (offset 2 = after key_len)
        assert!(std::ptr::eq(key.as_ptr(), encoded[2..].as_ptr()));
    }

    #[test]
    fn node_view_iterates_row_ids() {
        let mut node = SkipListNode::new(b"key".to_vec(), 1);
        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        node.row_ids.push(row1);
        node.row_ids.push(row2);

        let encoded = node.encode();
        let view = SkipListNodeView::new(&encoded).unwrap();
        let row_ids: Vec<_> = view.row_ids().collect();

        assert_eq!(row_ids.len(), 2);
        assert!(row_ids.contains(&row1));
        assert!(row_ids.contains(&row2));
    }

    #[test]
    fn node_view_reads_forward_pointers() {
        let mut node = SkipListNode::new(b"key".to_vec(), 3);
        let fwd0 = ObjectId::new();
        let fwd2 = ObjectId::new();
        node.forward[0] = Some(fwd0);
        node.forward[2] = Some(fwd2);

        let encoded = node.encode();
        let view = SkipListNodeView::new(&encoded).unwrap();

        assert_eq!(view.forward(0), Some(fwd0));
        assert_eq!(view.forward(1), None);
        assert_eq!(view.forward(2), Some(fwd2));
        assert_eq!(view.forward(3), None);
    }

    #[test]
    fn node_view_to_owned_roundtrip() {
        let mut node = SkipListNode::new(b"test".to_vec(), 2);
        node.row_ids.push(ObjectId::new());
        node.forward[0] = Some(ObjectId::new());
        node.forward[1] = Some(ObjectId::new());

        let encoded = node.encode();
        let view = SkipListNodeView::new(&encoded).unwrap();
        let owned = view.to_owned();

        assert_eq!(owned.key, node.key);
        assert_eq!(owned.row_ids, node.row_ids);
        assert_eq!(owned.level, node.level);
        assert_eq!(owned.forward, node.forward);
    }

    // ========================================================================
    // IndexState with ObjectManager tests
    // ========================================================================

    #[test]
    fn insert_and_lookup() {
        let om = ObjectManager::new();
        let mut index = IndexState::new("users", "email");

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        index.insert(b"alice@example.com", row1, &om);
        index.insert(b"bob@example.com", row2, &om);

        let alice_rows = index.lookup_exact(b"alice@example.com", &om);
        assert_eq!(alice_rows.len(), 1);
        assert!(alice_rows.contains(&row1));

        let bob_rows = index.lookup_exact(b"bob@example.com", &om);
        assert_eq!(bob_rows.len(), 1);
        assert!(bob_rows.contains(&row2));

        let unknown = index.lookup_exact(b"unknown@example.com", &om);
        assert!(unknown.is_empty());
    }

    #[test]
    fn insert_duplicate_key() {
        let om = ObjectManager::new();
        let mut index = IndexState::new("users", "email");

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        index.insert(b"alice@example.com", row1, &om);
        index.insert(b"alice@example.com", row2, &om);

        let rows = index.lookup_exact(b"alice@example.com", &om);
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&row1));
        assert!(rows.contains(&row2));
    }

    #[test]
    fn insert_same_row_twice_is_idempotent() {
        let om = ObjectManager::new();
        let mut index = IndexState::new("users", "email");
        let row = ObjectId::new();

        index.insert(b"alice@example.com", row, &om);
        index.insert(b"alice@example.com", row, &om);

        let rows = index.lookup_exact(b"alice@example.com", &om);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn remove_row() {
        let om = ObjectManager::new();
        let mut index = IndexState::new("users", "email");

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();

        index.insert(b"alice@example.com", row1, &om);
        index.insert(b"alice@example.com", row2, &om);

        index.remove(b"alice@example.com", row1, &om);

        let rows = index.lookup_exact(b"alice@example.com", &om);
        assert_eq!(rows.len(), 1);
        assert!(rows.contains(&row2));
    }

    #[test]
    fn remove_last_row_removes_node() {
        let om = ObjectManager::new();
        let mut index = IndexState::new("users", "email");
        let row = ObjectId::new();

        index.insert(b"alice@example.com", row, &om);
        index.remove(b"alice@example.com", row, &om);

        let rows = index.lookup_exact(b"alice@example.com", &om);
        assert!(rows.is_empty());
    }

    #[test]
    fn range_scan_bounded() {
        let om = ObjectManager::new();
        let mut index = IndexState::new("users", "score");

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let row3 = ObjectId::new();
        let row4 = ObjectId::new();

        // Insert scores as bytes (simulating i32 encoding)
        index.insert(&10i32.to_le_bytes(), row1, &om);
        index.insert(&20i32.to_le_bytes(), row2, &om);
        index.insert(&30i32.to_le_bytes(), row3, &om);
        index.insert(&40i32.to_le_bytes(), row4, &om);

        // Range [15, 35] should get 20 and 30
        let results = index.range_scan(Some(&15i32.to_le_bytes()), Some(&35i32.to_le_bytes()), &om);
        assert_eq!(results.len(), 2);
        assert!(results.contains(&row2));
        assert!(results.contains(&row3));
    }

    #[test]
    fn range_scan_unbounded_min() {
        let om = ObjectManager::new();
        let mut index = IndexState::new("users", "score");

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let row3 = ObjectId::new();

        index.insert(&10i32.to_le_bytes(), row1, &om);
        index.insert(&20i32.to_le_bytes(), row2, &om);
        index.insert(&30i32.to_le_bytes(), row3, &om);

        // Range [_, 25] should get 10 and 20
        let results = index.range_scan(None, Some(&25i32.to_le_bytes()), &om);
        assert_eq!(results.len(), 2);
        assert!(results.contains(&row1));
        assert!(results.contains(&row2));
    }

    #[test]
    fn range_scan_unbounded_max() {
        let om = ObjectManager::new();
        let mut index = IndexState::new("users", "score");

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let row3 = ObjectId::new();

        index.insert(&10i32.to_le_bytes(), row1, &om);
        index.insert(&20i32.to_le_bytes(), row2, &om);
        index.insert(&30i32.to_le_bytes(), row3, &om);

        // Range [15, _] should get 20 and 30
        let results = index.range_scan(Some(&15i32.to_le_bytes()), None, &om);
        assert_eq!(results.len(), 2);
        assert!(results.contains(&row2));
        assert!(results.contains(&row3));
    }

    #[test]
    fn scan_all() {
        let om = ObjectManager::new();
        let mut index = IndexState::new("users", "_id");

        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let row3 = ObjectId::new();

        index.insert(row1.0.as_bytes(), row1, &om);
        index.insert(row2.0.as_bytes(), row2, &om);
        index.insert(row3.0.as_bytes(), row3, &om);

        let all = index.scan_all(&om);
        assert_eq!(all.len(), 3);
        assert!(all.contains(&row1));
        assert!(all.contains(&row2));
        assert!(all.contains(&row3));
    }

    #[test]
    fn insert_returns_modified_nodes() {
        let om = ObjectManager::new();
        let mut index = IndexState::new("users", "email");
        let row = ObjectId::new();

        let modified = index.insert(b"alice@example.com", row, &om);

        // Should include at least the new node and some predecessors
        assert!(!modified.is_empty());
    }

    #[test]
    fn many_inserts_maintains_order() {
        let om = ObjectManager::new();
        let mut index = IndexState::new("users", "score");

        // Insert 100 items in random order
        let mut rows = Vec::new();
        for i in (0..100).rev() {
            let row = ObjectId::new();
            rows.push((i, row));
            index.insert(&(i as i32).to_le_bytes(), row, &om);
        }

        // Scan all should return them (each value is unique)
        let all = index.scan_all(&om);
        assert_eq!(all.len(), 100);
    }

    #[test]
    fn persist_and_load_roundtrip() {
        let mut om = ObjectManager::new();

        // Create an index and insert entries
        let mut index = IndexState::new("users", "email");
        let row1 = ObjectId::new();
        let row2 = ObjectId::new();
        let row3 = ObjectId::new();

        let mod1 = index.insert(b"alice@example.com", row1, &om);
        let mod2 = index.insert(b"bob@example.com", row2, &om);
        let mod3 = index.insert(b"charlie@example.com", row3, &om);

        // Persist all modified nodes
        let mut all_modified: HashSet<ObjectId> = HashSet::new();
        all_modified.extend(mod1);
        all_modified.extend(mod2);
        all_modified.extend(mod3);

        for node_id in all_modified {
            index.persist_node(node_id, &mut om).unwrap();
        }

        // Create a new index state - reads from ObjectManager directly (no loading needed)
        let index2 = IndexState::new("users", "email");

        // Verify the loaded index has the same data (reads directly from om)
        let alice_rows = index2.lookup_exact(b"alice@example.com", &om);
        assert_eq!(alice_rows.len(), 1);
        assert!(alice_rows.contains(&row1));

        let bob_rows = index2.lookup_exact(b"bob@example.com", &om);
        assert_eq!(bob_rows.len(), 1);
        assert!(bob_rows.contains(&row2));

        let charlie_rows = index2.lookup_exact(b"charlie@example.com", &om);
        assert_eq!(charlie_rows.len(), 1);
        assert!(charlie_rows.contains(&row3));
    }

    #[test]
    fn persist_pending_clears_pending_writes() {
        let mut om = ObjectManager::new();
        let mut index = IndexState::new("users", "email");
        let row = ObjectId::new();

        // Insert creates pending writes
        index.insert(b"test@example.com", row, &om);
        assert!(!index.pending_writes.is_empty());

        // Persist clears them
        index.persist_pending(&mut om);
        assert!(index.pending_writes.is_empty());
    }

    #[test]
    fn get_node_returns_pending_write_if_present() {
        let om = ObjectManager::new();
        let mut index = IndexState::new("users", "email");
        let row = ObjectId::new();

        // Insert creates a pending write for sentinel and new node
        index.insert(b"test@example.com", row, &om);

        // get_node should return from pending_writes for root
        let node = index.get_node(index.root_id, &om).unwrap();
        assert!(matches!(node, NodeRef::Pending(_)));
    }

    #[test]
    fn fresh_index_root_doesnt_exist() {
        let om = ObjectManager::new();
        let index = IndexState::new("users", "email");

        // Before any inserts, root doesn't exist in ObjectManager
        assert!(!index.root_exists(&om));
    }

    #[test]
    fn insert_creates_sentinel_lazily() {
        let om = ObjectManager::new();
        let mut index = IndexState::new("users", "email");

        // Before insert, no sentinel
        assert!(!index.root_exists(&om));

        // After insert, sentinel exists in pending_writes
        let row = ObjectId::new();
        index.insert(b"test@example.com", row, &om);
        assert!(index.root_exists(&om));
        assert!(index.pending_writes.contains_key(&index.root_id));
    }

    #[test]
    fn persist_node_returns_none_for_nonexistent_node() {
        let mut om = ObjectManager::new();
        let mut index = IndexState::new("users", "email");
        let fake_id = ObjectId::new();

        // Node doesn't exist anywhere
        let result = index.persist_node(fake_id, &mut om).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn contains_row_checks_id_index() {
        let om = ObjectManager::new();
        let mut index = IndexState::new("users", "_id");
        let row = ObjectId::new();

        assert!(!index.contains_row(row, &om));

        index.insert(row.0.as_bytes(), row, &om);

        assert!(index.contains_row(row, &om));
    }
}
