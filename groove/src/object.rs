use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use crate::branch::Branch;
use crate::commit::{Commit, CommitId};
use crate::merge::MergeStrategy;

// ========== ObjectId Type ==========

/// Crockford Base32 alphabet (excludes I, L, O, U to avoid confusion).
/// Uses lowercase for output (parsing is case-insensitive).
const CROCKFORD_ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// Decode table for Crockford Base32 (maps ASCII byte to 5-bit value, or 0xFF for invalid).
const CROCKFORD_DECODE: [u8; 128] = {
    let mut table = [0xFFu8; 128];
    // Digits
    table[b'0' as usize] = 0;
    table[b'1' as usize] = 1;
    table[b'2' as usize] = 2;
    table[b'3' as usize] = 3;
    table[b'4' as usize] = 4;
    table[b'5' as usize] = 5;
    table[b'6' as usize] = 6;
    table[b'7' as usize] = 7;
    table[b'8' as usize] = 8;
    table[b'9' as usize] = 9;
    // Letters (uppercase)
    table[b'A' as usize] = 10;
    table[b'B' as usize] = 11;
    table[b'C' as usize] = 12;
    table[b'D' as usize] = 13;
    table[b'E' as usize] = 14;
    table[b'F' as usize] = 15;
    table[b'G' as usize] = 16;
    table[b'H' as usize] = 17;
    table[b'J' as usize] = 18; // I is skipped
    table[b'K' as usize] = 19;
    table[b'M' as usize] = 20; // L is skipped
    table[b'N' as usize] = 21;
    table[b'P' as usize] = 22; // O is skipped
    table[b'Q' as usize] = 23;
    table[b'R' as usize] = 24;
    table[b'S' as usize] = 25;
    table[b'T' as usize] = 26;
    table[b'V' as usize] = 27; // U is skipped
    table[b'W' as usize] = 28;
    table[b'X' as usize] = 29;
    table[b'Y' as usize] = 30;
    table[b'Z' as usize] = 31;
    // Letters (lowercase) - map to same values
    table[b'a' as usize] = 10;
    table[b'b' as usize] = 11;
    table[b'c' as usize] = 12;
    table[b'd' as usize] = 13;
    table[b'e' as usize] = 14;
    table[b'f' as usize] = 15;
    table[b'g' as usize] = 16;
    table[b'h' as usize] = 17;
    table[b'j' as usize] = 18;
    table[b'k' as usize] = 19;
    table[b'm' as usize] = 20;
    table[b'n' as usize] = 21;
    table[b'p' as usize] = 22;
    table[b'q' as usize] = 23;
    table[b'r' as usize] = 24;
    table[b's' as usize] = 25;
    table[b't' as usize] = 26;
    table[b'v' as usize] = 27;
    table[b'w' as usize] = 28;
    table[b'x' as usize] = 29;
    table[b'y' as usize] = 30;
    table[b'z' as usize] = 31;
    // Common substitutions
    table[b'I' as usize] = 1; // I -> 1
    table[b'i' as usize] = 1;
    table[b'L' as usize] = 1; // L -> 1
    table[b'l' as usize] = 1;
    table[b'O' as usize] = 0; // O -> 0
    table[b'o' as usize] = 0;
    table
};

/// Object ID - a 128-bit unique identifier.
///
/// Displayed and parsed as Crockford Base32 (26 characters for 128 bits).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ObjectId(pub u128);

impl ObjectId {
    /// Create a new ObjectId from a u128 value.
    pub const fn new(value: u128) -> Self {
        ObjectId(value)
    }

    /// Create a new random ObjectId using UUID v7 (timestamp + random).
    pub fn new_random() -> Self {
        ObjectId(uuid::Uuid::now_v7().as_u128())
    }

    /// Create a deterministic ObjectId from a string key.
    /// Uses FNV-1a hash to generate a reproducible ID.
    /// This is useful when multiple clients need to reference the same object
    /// without explicitly syncing the ID.
    pub fn from_key(key: &str) -> Self {
        // FNV-1a hash constants for 128-bit
        const FNV_OFFSET: u128 = 0x6c62272e07bb014262b821756295c58d;
        const FNV_PRIME: u128 = 0x0000000001000000000000000000013b;

        let mut hash = FNV_OFFSET;
        for byte in key.as_bytes() {
            hash ^= *byte as u128;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        ObjectId(hash)
    }

    /// Get the inner u128 value.
    pub const fn inner(self) -> u128 {
        self.0
    }

    /// Convert to little-endian bytes.
    pub fn to_le_bytes(self) -> [u8; 16] {
        self.0.to_le_bytes()
    }

    /// Create from little-endian bytes.
    pub fn from_le_bytes(bytes: [u8; 16]) -> Self {
        ObjectId(u128::from_le_bytes(bytes))
    }

    /// Encode as Crockford Base32 string.
    /// Returns a 26-character string (128 bits = 26 * 5 bits, with 2 bits padding).
    fn to_base32(self) -> String {
        let mut result = [0u8; 26];
        let mut value = self.0;

        // Encode from right to left (least significant first)
        for i in (0..26).rev() {
            result[i] = CROCKFORD_ALPHABET[(value & 0x1F) as usize];
            value >>= 5;
        }

        // Safety: CROCKFORD_ALPHABET only contains ASCII characters
        unsafe { String::from_utf8_unchecked(result.to_vec()) }
    }

    /// Parse from Crockford Base32 string.
    fn from_base32(s: &str) -> Result<Self, ObjectIdParseError> {
        let s = s.trim();

        if s.is_empty() {
            return Err(ObjectIdParseError::Empty);
        }

        // Allow variable length - pad with leading zeros
        if s.len() > 26 {
            return Err(ObjectIdParseError::TooLong);
        }

        let mut value: u128 = 0;

        for c in s.bytes() {
            if c >= 128 {
                return Err(ObjectIdParseError::InvalidChar(c as char));
            }

            let digit = CROCKFORD_DECODE[c as usize];
            if digit == 0xFF {
                return Err(ObjectIdParseError::InvalidChar(c as char));
            }

            // Check for overflow
            if value > (u128::MAX >> 5) {
                return Err(ObjectIdParseError::Overflow);
            }

            value = (value << 5) | (digit as u128);
        }

        Ok(ObjectId(value))
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_base32())
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectId({})", self.to_base32())
    }
}

impl FromStr for ObjectId {
    type Err = ObjectIdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ObjectId::from_base32(s)
    }
}

impl From<u128> for ObjectId {
    fn from(value: u128) -> Self {
        ObjectId(value)
    }
}

impl From<ObjectId> for u128 {
    fn from(id: ObjectId) -> Self {
        id.0
    }
}

/// Error parsing an ObjectId from a string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectIdParseError {
    /// Empty string.
    Empty,
    /// String too long (more than 26 characters).
    TooLong,
    /// Invalid character in string.
    InvalidChar(char),
    /// Value overflow (shouldn't happen with <= 26 chars).
    Overflow,
}

impl fmt::Display for ObjectIdParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObjectIdParseError::Empty => write!(f, "empty object ID string"),
            ObjectIdParseError::TooLong => {
                write!(f, "object ID string too long (max 26 characters)")
            }
            ObjectIdParseError::InvalidChar(c) => {
                write!(f, "invalid character '{}' in object ID", c)
            }
            ObjectIdParseError::Overflow => write!(f, "object ID value overflow"),
        }
    }
}

impl std::error::Error for ObjectIdParseError {}

/// Schema ID type alias (object ID of schema object).
pub type SchemaId = ObjectId;

// ========== Object Type ==========

/// An object (CoValue) with its commit graph.
#[derive(Debug)]
pub struct Object {
    /// Unique object ID (UUIDv7)
    pub id: ObjectId,
    /// Type prefix (e.g., "chat", "message")
    pub prefix: String,
    /// Named branches (wrapped in Arc<RwLock<>> for signal access)
    branches: HashMap<String, Arc<RwLock<Branch>>>,
    /// Object-level metadata
    pub meta: Option<BTreeMap<String, String>>,
}

impl Object {
    /// Create a new object with the given ID and prefix.
    /// Automatically creates a "main" branch.
    pub fn new(id: ObjectId, prefix: impl Into<String>) -> Self {
        Self::new_with_meta(id, prefix, None)
    }

    /// Create a new object with the given ID, prefix, and optional metadata.
    /// Automatically creates a "main" branch.
    pub fn new_with_meta(
        id: ObjectId,
        prefix: impl Into<String>,
        meta: Option<BTreeMap<String, String>>,
    ) -> Self {
        let mut branches = HashMap::new();
        branches.insert(
            "main".to_string(),
            Arc::new(RwLock::new(Branch::new("main"))),
        );

        Object {
            id,
            prefix: prefix.into(),
            branches,
            meta,
        }
    }

    /// Set object metadata.
    pub fn set_meta(&mut self, meta: BTreeMap<String, String>) {
        self.meta = Some(meta);
    }

    /// Get a reference to a branch (Arc<RwLock<Branch>>).
    /// Use this when you need to share the branch with signals.
    pub fn branch_ref(&self, name: &str) -> Option<Arc<RwLock<Branch>>> {
        self.branches.get(name).cloned()
    }

    /// Get a branch by name (read lock).
    pub fn branch(&self, name: &str) -> Option<std::sync::RwLockReadGuard<'_, Branch>> {
        self.branches.get(name).map(|b| b.read().unwrap())
    }

    /// Get a branch by name mutably (write lock).
    pub fn branch_mut(&self, name: &str) -> Option<std::sync::RwLockWriteGuard<'_, Branch>> {
        self.branches.get(name).map(|b| b.write().unwrap())
    }

    /// Create a new branch starting from a commit in an existing branch.
    /// Returns error if the source branch or commit doesn't exist.
    pub fn create_branch(
        &mut self,
        name: impl Into<String>,
        from_branch: &str,
        from_commit: &CommitId,
    ) -> Result<(), &'static str> {
        let name = name.into();

        if self.branches.contains_key(&name) {
            return Err("branch already exists");
        }

        let source = self
            .branches
            .get(from_branch)
            .ok_or("source branch not found")?
            .read()
            .unwrap();

        if !source.commits.contains_key(from_commit) {
            return Err("commit not found in source branch");
        }

        // Create new branch with commits up to and including from_commit
        let mut new_branch = Branch::new(&name);

        // Copy all ancestors of from_commit (including itself)
        let mut to_copy = vec![*from_commit];
        let mut copied = HashSet::new();

        while let Some(id) = to_copy.pop() {
            if copied.contains(&id) {
                continue;
            }
            if let Some(commit) = source.commits.get(&id) {
                // Add parents to copy list
                for parent in &commit.parents {
                    if !copied.contains(parent) {
                        to_copy.push(*parent);
                    }
                }
                // Copy commit (re-add to build proper indices)
                new_branch.commits.insert(id, commit.clone());
                copied.insert(id);

                // Rebuild children index
                for parent in &commit.parents {
                    new_branch.children.entry(*parent).or_default().push(id);
                }
            }
        }

        // Set frontier to just the starting commit
        new_branch.frontier = vec![*from_commit];

        drop(source); // Release read lock before modifying self.branches
        self.branches
            .insert(name, Arc::new(RwLock::new(new_branch)));
        Ok(())
    }

    /// List all branch names.
    pub fn branch_names(&self) -> Vec<&str> {
        self.branches.keys().map(|s| s.as_str()).collect()
    }

    /// Merge a source branch into a target branch.
    /// Creates a merge commit in the target branch that combines the tips of both.
    /// Returns the new merge commit ID.
    pub fn merge_branches(
        &self,
        target_branch: &str,
        source_branch: &str,
        strategy: &dyn MergeStrategy,
        author: &str,
        timestamp: u64,
    ) -> Result<CommitId, &'static str> {
        // Get source frontier
        let source_lock = self
            .branches
            .get(source_branch)
            .ok_or("source branch not found")?;
        let source = source_lock.read().unwrap();

        let source_frontier: Vec<CommitId> = source.frontier().to_vec();
        let source_commits: HashMap<CommitId, Commit> = source.commits().clone();
        drop(source); // Release read lock

        // Get target branch
        let target_lock = self
            .branches
            .get(target_branch)
            .ok_or("target branch not found")?;
        let mut target = target_lock.write().unwrap();

        let target_frontier = target.frontier().to_vec();

        if target_frontier.is_empty() {
            return Err("target branch is empty");
        }
        if source_frontier.is_empty() {
            return Err("source branch is empty");
        }

        // First, copy any commits from source that aren't in target
        for (id, commit) in &source_commits {
            if !target.commits.contains_key(id) {
                target.commits.insert(*id, commit.clone());
                for parent in &commit.parents {
                    target.children.entry(*parent).or_default().push(*id);
                }
            }
        }

        // Collect all tips we need to merge
        let mut all_tips: Vec<CommitId> = target_frontier.clone();
        for tip in &source_frontier {
            if !all_tips.contains(tip) {
                all_tips.push(*tip);
            }
        }

        // If there's only one unique tip, nothing to merge
        if all_tips.len() == 1 {
            return Ok(all_tips[0]);
        }

        // Find LCA of first two tips, then extend
        let mut lca_commits = target.find_lca(&all_tips[0], &all_tips[1]);
        for tip in all_tips.iter().skip(2) {
            // Find LCA with each additional tip
            if let Some(first_lca) = lca_commits.first() {
                lca_commits = target.find_lca(first_lca, tip);
            }
        }

        // Get base content (from first LCA if exists)
        let base_content: Option<Vec<u8>> = lca_commits
            .first()
            .and_then(|id| target.commits.get(id))
            .map(|c| c.content.to_vec());

        // Collect tip contents
        let tip_contents: Vec<Vec<u8>> = all_tips
            .iter()
            .filter_map(|id| target.commits.get(id))
            .map(|c| c.content.to_vec())
            .collect();

        let tip_refs: Vec<&[u8]> = tip_contents.iter().map(|v| v.as_slice()).collect();

        // Perform merge
        let merged_content = strategy.merge(base_content.as_deref(), &tip_refs)?;

        // Create merge commit
        let merge_commit = Commit {
            parents: all_tips,
            content: merged_content,
            author: author.to_string(),
            timestamp,
            meta: None,
        };

        // Manually handle frontier update for merge
        let merge_id = merge_commit.compute_id();

        // Remove all merged tips from frontier
        for parent in &merge_commit.parents {
            target.frontier.retain(|f| f != parent);
            target.children.entry(*parent).or_default().push(merge_id);
        }

        target.frontier.push(merge_id);
        target.commits.insert(merge_id, merge_commit);

        Ok(merge_id)
    }

    // ========== Sync Read/Write Methods ==========

    /// Read content from the frontier of a branch (sync).
    /// Returns None if the branch is empty or has multiple tips.
    pub fn read_sync(&self, branch_name: &str) -> Option<Vec<u8>> {
        let branch = self.branches.get(branch_name)?.read().unwrap();
        let frontier = branch.frontier();

        // Only return content if there's exactly one tip
        if frontier.len() != 1 {
            return None;
        }

        let commit = branch.get_commit(&frontier[0])?;
        Some(commit.content.to_vec())
    }

    /// Write content to a branch (sync).
    /// Returns the new commit ID.
    pub fn write_sync(
        &self,
        branch_name: &str,
        content: &[u8],
        author: &str,
        timestamp: u64,
    ) -> CommitId {
        self.write_sync_with_meta(branch_name, content, author, timestamp, None)
    }

    /// Write content to a branch with optional metadata (sync).
    /// Returns the new commit ID.
    pub fn write_sync_with_meta(
        &self,
        branch_name: &str,
        content: &[u8],
        author: &str,
        timestamp: u64,
        meta: Option<std::collections::BTreeMap<String, String>>,
    ) -> CommitId {
        let mut branch = self
            .branches
            .get(branch_name)
            .expect("branch not found")
            .write()
            .unwrap();

        let parents = branch.frontier().to_vec();

        let commit = Commit {
            parents,
            content: content.to_vec().into_boxed_slice(),
            author: author.to_string(),
            timestamp,
            meta,
        };

        branch.add_commit(commit)
    }

    /// Get the frontier commit IDs for a branch.
    pub fn frontier(&self, branch_name: &str) -> Option<Vec<CommitId>> {
        let branch = self.branches.get(branch_name)?.read().unwrap();
        Some(branch.frontier().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_id_roundtrip() {
        let test_values = [
            0u128,
            1,
            255,
            256,
            u128::MAX,
            0x0123456789ABCDEF0123456789ABCDEF,
        ];

        for value in test_values {
            let id = ObjectId::new(value);
            let s = id.to_string();
            let parsed: ObjectId = s.parse().unwrap();
            assert_eq!(id, parsed, "roundtrip failed for {:#x}", value);
        }
    }

    #[test]
    fn object_id_display_format() {
        // Zero should be all zeros in base32
        let id = ObjectId::new(0);
        assert_eq!(id.to_string(), "00000000000000000000000000");

        // Max value
        let id = ObjectId::new(u128::MAX);
        // 128 bits = 26 * 5 - 2 = 128 bits with 2 padding bits
        // So max is 0x3FFFFFFF... which in base32 is 7zzzzzzzzzzzzzzzzzzzzzzzzz
        assert_eq!(id.to_string(), "7zzzzzzzzzzzzzzzzzzzzzzzzz");
    }

    #[test]
    fn object_id_case_insensitive() {
        let lower: ObjectId = "abc123".parse().unwrap();
        let upper: ObjectId = "ABC123".parse().unwrap();
        assert_eq!(lower, upper);
    }

    #[test]
    fn object_id_common_substitutions() {
        // I, L -> 1
        let id1: ObjectId = "1".parse().unwrap();
        let id_i: ObjectId = "I".parse().unwrap();
        let id_l: ObjectId = "L".parse().unwrap();
        assert_eq!(id1, id_i);
        assert_eq!(id1, id_l);

        // O -> 0
        let id0: ObjectId = "0".parse().unwrap();
        let id_o: ObjectId = "O".parse().unwrap();
        assert_eq!(id0, id_o);
    }

    #[test]
    fn object_id_debug_format() {
        let id = ObjectId::new(42);
        let debug = format!("{:?}", id);
        assert!(debug.starts_with("ObjectId("));
        assert!(debug.ends_with(")"));
    }

    #[test]
    fn object_id_bytes_roundtrip() {
        let id = ObjectId::new(0x0123456789ABCDEF0123456789ABCDEF);
        let bytes = id.to_le_bytes();
        let parsed = ObjectId::from_le_bytes(bytes);
        assert_eq!(id, parsed);
    }

    #[test]
    fn object_id_from_u128() {
        let id: ObjectId = 42u128.into();
        assert_eq!(id.inner(), 42);
    }

    #[test]
    fn object_id_into_u128() {
        let id = ObjectId::new(42);
        let value: u128 = id.into();
        assert_eq!(value, 42);
    }

    #[test]
    fn object_id_parse_errors() {
        assert!(matches!(
            "".parse::<ObjectId>(),
            Err(ObjectIdParseError::Empty)
        ));
        assert!(matches!(
            "000000000000000000000000000".parse::<ObjectId>(),
            Err(ObjectIdParseError::TooLong)
        ));
        assert!(matches!(
            "hello!".parse::<ObjectId>(),
            Err(ObjectIdParseError::InvalidChar('!'))
        ));
    }

    #[test]
    fn object_id_string_sortability() {
        // Test that string representation maintains sort order.
        // This is critical for database indices and range queries.
        // Crockford Base32 alphabet (0-9, A-Z minus I,L,O,U) is in ASCII order,
        // and MSB-first encoding ensures lexicographic ordering matches numeric ordering.
        let values = [0u128, 1, 31, 32, 255, 256, 1000, u128::MAX / 2, u128::MAX];

        for i in 0..values.len() - 1 {
            let id1 = ObjectId::new(values[i]);
            let id2 = ObjectId::new(values[i + 1]);

            // Numeric ordering
            assert!(id1 < id2);

            // String ordering should match
            assert!(
                id1.to_string() < id2.to_string(),
                "String ordering mismatch: {} ({}) vs {} ({})",
                id1,
                values[i],
                id2,
                values[i + 1]
            );
        }
    }

    #[test]
    fn object_id_string_sort_matches_numeric_sort() {
        // Verify that sorting a collection by string gives same order as sorting by value
        let ids: Vec<ObjectId> = vec![
            ObjectId::new(1000),
            ObjectId::new(1),
            ObjectId::new(u128::MAX),
            ObjectId::new(0),
            ObjectId::new(500),
            ObjectId::new(31), // boundary: last single-char value
            ObjectId::new(32), // boundary: first two-char value
        ];

        // Sort by numeric value (uses derived Ord on u128)
        let mut numeric_sorted = ids.clone();
        numeric_sorted.sort();

        // Sort by string representation
        let mut string_sorted = ids.clone();
        string_sorted.sort_by_key(|id| id.to_string());

        assert_eq!(numeric_sorted, string_sorted);
    }

    #[test]
    fn object_id_boundary_values_sortability() {
        // Test specific boundary cases where encoding "rolls over"

        // 31 -> 32 boundary (Z -> 10 in least significant position)
        let id31 = ObjectId::new(31);
        let id32 = ObjectId::new(32);
        assert!(id31 < id32);
        assert!(id31.to_string() < id32.to_string());

        // 1023 -> 1024 boundary (ZZ -> 100)
        let id1023 = ObjectId::new(1023);
        let id1024 = ObjectId::new(1024);
        assert!(id1023 < id1024);
        assert!(id1023.to_string() < id1024.to_string());

        // First character differences (very large values)
        // Value that starts with '0' vs value that starts with '1'
        let small_first_char = ObjectId::new(1u128 << 125); // starts with low digit
        let large_first_char = ObjectId::new(3u128 << 125); // starts with higher digit
        assert!(small_first_char < large_first_char);
        assert!(small_first_char.to_string() < large_first_char.to_string());
    }
}

// Tests for Object have been moved to tests/object.rs
