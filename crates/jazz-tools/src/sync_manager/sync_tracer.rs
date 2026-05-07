//! Test-only message recorder for the sync protocol.
//!
//! Captures every sync message flowing between named participants (alice, bob,
//! server) and provides querying and pretty-printing for test assertions and
//! debugging.
//!
//! # Example
//!
//! ```ignore
//! let tracer = SyncTracer::new();
//! // ... attach to server + clients via builders ...
//!
//! println!("{}", tracer.dump());
//! // [001] +  0ms  alice    -> server   ObjectUpdated        obj:a1b2c3d4 branch:main commits:[e5f6a7b8]
//! // [002] +  3ms  server   -> alice    PersistenceAck       obj:a1b2c3d4 confirmed:[e5f6a7b8] tier:EdgeServer
//! // [003] +  5ms  server   -> bob      ObjectUpdated        obj:a1b2c3d4 branch:main commits:[e5f6a7b8]
//!
//! assert!(tracer.from("alice").iter().any(|m| m.is_object_updated()));
//! assert!(tracer.to("bob").iter().any(|m| m.is_object_updated()));
//! ```

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::object::{BranchName, ObjectId};
use crate::row_histories::BatchId;
use crate::sync_manager::{ClientId, Destination, QueryId, Source, SyncPayload};

// ============================================================================
// Types
// ============================================================================

/// Human-readable participant name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Participant(pub String);

impl Participant {
    pub fn new(name: impl Into<String>) -> Self {
        Participant(name.into())
    }

    pub fn name(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Participant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Which side recorded the message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    /// Recorded by the sender (outgoing hook).
    Send,
    /// Recorded by the receiver (incoming hook).
    Recv,
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Side::Send => write!(f, "send"),
            Side::Recv => write!(f, "recv"),
        }
    }
}

impl Side {
    /// `=>` for send, `->` for recv.
    fn arrow(&self) -> &'static str {
        match self {
            Side::Send => "=>",
            Side::Recv => "->",
        }
    }
}

/// A single recorded sync message.
#[derive(Debug, Clone)]
pub struct SyncMessage {
    /// Monotonic sequence number (recording order).
    pub seq: usize,
    /// Milliseconds elapsed since the tracer was created.
    pub elapsed_ms: u64,
    /// Who sent this message.
    pub from: Participant,
    /// Who this message was addressed to.
    pub to: Participant,
    /// Which side recorded this message.
    pub side: Side,
    /// The raw sync payload.
    pub payload: SyncPayload,
}

impl SyncMessage {
    /// True if this is an update payload.
    pub fn is_object_updated(&self) -> bool {
        matches!(
            self.payload,
            SyncPayload::RowBatchCreated { .. } | SyncPayload::RowBatchNeeded { .. }
        )
    }

    /// True if this is a durability-state payload.
    pub fn is_persistence_ack(&self) -> bool {
        matches!(self.payload, SyncPayload::BatchFate { .. })
    }

    /// True if this is a `QuerySubscription` payload.
    pub fn is_query_subscription(&self) -> bool {
        matches!(self.payload, SyncPayload::QuerySubscription { .. })
    }

    /// True if this is a `QuerySettled` payload.
    pub fn is_query_settled(&self) -> bool {
        matches!(self.payload, SyncPayload::QuerySettled { .. })
    }

    /// True if this is an `Error` payload.
    pub fn is_error(&self) -> bool {
        matches!(self.payload, SyncPayload::Error(_))
    }

    /// Extract object_id from payloads that carry one.
    pub fn object_id(&self) -> Option<ObjectId> {
        match &self.payload {
            SyncPayload::RowBatchCreated { row, .. } | SyncPayload::RowBatchNeeded { row, .. } => {
                Some(row.row_id)
            }
            SyncPayload::BatchFate { .. } => None,
            _ => None,
        }
    }

    /// Extract query_id from payloads that carry one.
    pub fn query_id(&self) -> Option<QueryId> {
        match &self.payload {
            SyncPayload::QuerySubscription { query_id, .. } => Some(*query_id),
            SyncPayload::QueryUnsubscription { query_id } => Some(*query_id),
            SyncPayload::QuerySettled { query_id, .. } => Some(*query_id),
            _ => None,
        }
    }

    /// Extract batch ids from update or durability payloads.
    pub fn batch_ids(&self) -> Vec<BatchId> {
        match &self.payload {
            SyncPayload::RowBatchCreated { row, .. } | SyncPayload::RowBatchNeeded { row, .. } => {
                vec![row.batch_id()]
            }
            SyncPayload::BatchFate { fate } => vec![fate.batch_id()],
            _ => vec![],
        }
    }
}

// ============================================================================
// SyncTracer
// ============================================================================

struct Inner {
    messages: Vec<SyncMessage>,
    next_seq: usize,
    start: Instant,
    /// ClientId -> human name mapping.
    client_names: HashMap<ClientId, String>,
    /// ObjectId -> human name mapping.
    object_names: HashMap<ObjectId, String>,
    /// BatchId -> human name mapping.
    batch_names: HashMap<BatchId, String>,
}

/// Thread-safe sync message recorder.
///
/// Create one per test, share across all participants. Each participant records
/// messages via `record_outgoing` / `record_incoming`. Query with `messages()`,
/// `from()`, `to()`, `for_object()`, etc.
#[derive(Clone)]
pub struct SyncTracer {
    inner: Arc<Mutex<Inner>>,
}

impl SyncTracer {
    pub fn new() -> Self {
        SyncTracer {
            inner: Arc::new(Mutex::new(Inner {
                messages: Vec::new(),
                next_seq: 1,
                start: Instant::now(),
                client_names: HashMap::new(),
                object_names: HashMap::new(),
                batch_names: HashMap::new(),
            })),
        }
    }

    /// Register a ClientId -> human name mapping.
    ///
    /// Call this when a client connects so the tracer can display "alice"
    /// instead of a UUID.
    pub fn register_client(&self, client_id: ClientId, name: impl Into<String>) {
        let mut inner = self.inner.lock().unwrap();
        inner.client_names.insert(client_id, name.into());
    }

    /// Register an ObjectId -> human name mapping.
    ///
    /// ```ignore
    /// let (todo_id, _) = alice.create("todos", ...).await?;
    /// tracer.register_object(todo_id, "alice-todo");
    /// // Trace now shows "alice-todo" instead of "019d3fc7"
    /// ```
    pub fn register_object(&self, object_id: ObjectId, name: impl Into<String>) {
        let mut inner = self.inner.lock().unwrap();
        inner.object_names.insert(object_id, name.into());
    }

    /// Register a BatchId -> human name mapping.
    ///
    /// ```ignore
    /// tracer.register_batch(batch_id, "B1");
    /// // Trace now shows "B1" instead of "e5f6a7b8"
    /// ```
    pub fn register_batch(&self, batch_id: BatchId, name: impl Into<String>) {
        let mut inner = self.inner.lock().unwrap();
        inner.batch_names.insert(batch_id, name.into());
    }

    // --- Recording ---

    /// Record an outgoing message (from a named participant to a Destination).
    pub fn record_outgoing(
        &self,
        from_name: &str,
        destination: &Destination,
        payload: &SyncPayload,
    ) {
        let mut inner = self.inner.lock().unwrap();
        let to_name = match destination {
            Destination::Server(_) => "server".to_string(),
            Destination::Client(cid) => inner
                .client_names
                .get(cid)
                .cloned()
                .unwrap_or_else(|| short_uuid(&cid.0)),
        };
        let msg = SyncMessage {
            seq: inner.next_seq,
            elapsed_ms: inner.start.elapsed().as_millis() as u64,
            from: Participant::new(from_name),
            to: Participant::new(to_name),
            side: Side::Send,
            payload: payload.clone(),
        };
        inner.next_seq += 1;
        inner.messages.push(msg);
    }

    /// Record an incoming message (from a Source to a named participant).
    pub fn record_incoming(&self, source: &Source, to_name: &str, payload: &SyncPayload) {
        let mut inner = self.inner.lock().unwrap();
        let from_name = match source {
            Source::Server(_) => "server".to_string(),
            Source::Client(cid) => inner
                .client_names
                .get(cid)
                .cloned()
                .unwrap_or_else(|| short_uuid(&cid.0)),
        };
        let msg = SyncMessage {
            seq: inner.next_seq,
            elapsed_ms: inner.start.elapsed().as_millis() as u64,
            from: Participant::new(from_name),
            to: Participant::new(to_name),
            side: Side::Recv,
            payload: payload.clone(),
        };
        inner.next_seq += 1;
        inner.messages.push(msg);
    }

    // --- Query API ---

    /// All recorded messages.
    pub fn messages(&self) -> Vec<SyncMessage> {
        self.inner.lock().unwrap().messages.clone()
    }

    /// Messages sent by a specific participant.
    pub fn from(&self, name: &str) -> Vec<SyncMessage> {
        self.inner
            .lock()
            .unwrap()
            .messages
            .iter()
            .filter(|m| m.from.name() == name)
            .cloned()
            .collect()
    }

    /// Messages received by a specific participant.
    pub fn to(&self, name: &str) -> Vec<SyncMessage> {
        self.inner
            .lock()
            .unwrap()
            .messages
            .iter()
            .filter(|m| m.to.name() == name)
            .cloned()
            .collect()
    }

    /// Messages between two participants (either direction).
    pub fn between(&self, a: &str, b: &str) -> Vec<SyncMessage> {
        self.inner
            .lock()
            .unwrap()
            .messages
            .iter()
            .filter(|m| {
                (m.from.name() == a && m.to.name() == b) || (m.from.name() == b && m.to.name() == a)
            })
            .cloned()
            .collect()
    }

    /// Messages involving a specific object (ObjectUpdated, PersistenceAck, ObjectTruncated).
    pub fn for_object(&self, object_id: ObjectId) -> Vec<SyncMessage> {
        self.inner
            .lock()
            .unwrap()
            .messages
            .iter()
            .filter(|m| m.object_id() == Some(object_id))
            .cloned()
            .collect()
    }

    /// Messages of a specific payload variant (e.g. "ObjectUpdated", "PersistenceAck").
    pub fn of_type(&self, variant: &str) -> Vec<SyncMessage> {
        self.inner
            .lock()
            .unwrap()
            .messages
            .iter()
            .filter(|m| m.payload.variant_name() == variant)
            .cloned()
            .collect()
    }

    /// Number of recorded messages.
    pub fn count(&self) -> usize {
        self.inner.lock().unwrap().messages.len()
    }

    /// Wait until the tally output stops changing (no new messages for
    /// `stable_for` consecutive polls at 50ms intervals).
    ///
    /// Panics with the current tally if `timeout` expires before stabilising.
    #[cfg(feature = "test")]
    pub async fn wait_until_settled(&self, timeout: std::time::Duration) {
        let stable_for_target = 3; // require 3 consecutive identical polls (~150ms quiet)
        let mut stable_count = 0u32;
        let mut last_tally = String::new();
        let start = Instant::now();

        loop {
            let current = self.tally();
            if current == last_tally {
                stable_count += 1;
                if stable_count >= stable_for_target {
                    return;
                }
            } else {
                stable_count = 0;
                last_tally = current;
            }
            if start.elapsed() >= timeout {
                panic!(
                    "SyncTracer: timed out waiting for tally to settle\n\
                     Current tally:\n{}",
                    self.tally()
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// Clear all recorded messages and reset sequence counter.
    pub fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.messages.clear();
        inner.next_seq = 1;
        inner.start = Instant::now();
    }

    // --- Display ---

    /// Deterministic grouped summary: counts messages by (from, to, type).
    ///
    /// Output is sorted alphabetically by direction then type, making it
    /// stable across runs regardless of async message interleaving.
    ///
    /// ```text
    /// alice  -> server : ObjectUpdated (2), QueryUnsubscription (1)
    /// server -> alice  : PersistenceAck (4)
    /// server -> bob    : ObjectUpdated (2), QuerySettled (4)
    /// ```
    pub fn tally(&self) -> String {
        let inner = self.inner.lock().unwrap();
        tally_messages(&inner.messages)
    }

    /// Deterministic grouped summary filtered by participant.
    pub fn tally_for(&self, name: &str) -> String {
        let inner = self.inner.lock().unwrap();
        let filtered: Vec<_> = inner
            .messages
            .iter()
            .filter(|m| m.from.name() == name || m.to.name() == name)
            .cloned()
            .collect();
        tally_messages(&filtered)
    }

    /// Pretty-print all recorded messages with named objects/commits.
    pub fn dump(&self) -> String {
        let inner = self.inner.lock().unwrap();
        let names = Names {
            objects: &inner.object_names,
            batches: &inner.batch_names,
        };
        format_messages(&inner.messages, &names)
    }

    /// Pretty-print messages involving a specific participant.
    pub fn dump_for(&self, name: &str) -> String {
        let inner = self.inner.lock().unwrap();
        let names = Names {
            objects: &inner.object_names,
            batches: &inner.batch_names,
        };
        let filtered: Vec<_> = inner
            .messages
            .iter()
            .filter(|m| m.from.name() == name || m.to.name() == name)
            .cloned()
            .collect();
        format_messages(&filtered, &names)
    }

    /// Detailed trace without timing — stable for `insta` inline snapshots.
    ///
    /// Shows named objects/commits, one line per message:
    /// ```text
    /// alice    -> server   ObjectUpdated        obj:alice-todo branch:main commits:[C1]
    /// server   -> alice    PersistenceAck       obj:alice-todo confirmed:[C1] tier:EdgeServer
    /// ```
    pub fn trace(&self) -> String {
        let inner = self.inner.lock().unwrap();
        let names = Names {
            objects: &inner.object_names,
            batches: &inner.batch_names,
        };
        format_trace(&inner.messages, &names)
    }

    /// Normalized trace for stable inline snapshots.
    ///
    /// Like `trace()` but auto-assigns deterministic names to commits and
    /// branches as they appear: first commit → `C1`, second → `C2`, etc.
    /// Branch names are stripped of the random client-ID prefix.
    /// Registered object names are used; unregistered objects get `obj-1`, `obj-2`.
    ///
    /// ```text
    /// alice    -> server   ObjectUpdated        obj:my-todo branch:main commits:[C1]
    /// server   -> alice    PersistenceAck       obj:my-todo confirmed:[C1] tier:EdgeServer
    /// ```
    pub fn trace_normalized(&self) -> String {
        let inner = self.inner.lock().unwrap();
        let mut normalizer = Normalizer::new(&inner.object_names);

        let mut out = String::from("# => sent, -> received\n");

        // Build lines with sort key (from, to, variant, details)
        let mut lines: Vec<(String, String, String, String, String)> = inner
            .messages
            .iter()
            .map(|msg| {
                let details = normalizer.format_payload(&msg.payload);
                let line = format!(
                    "{:<8} {} {:<8}  {:<20} {}",
                    msg.from.name(),
                    msg.side.arrow(),
                    msg.to.name(),
                    msg.payload.variant_name(),
                    details,
                );
                (
                    msg.from.name().to_string(),
                    msg.to.name().to_string(),
                    msg.payload.variant_name().to_string(),
                    details,
                    line,
                )
            })
            .collect();

        // Stable sort: preserve causal order within the same direction,
        // but sort by (from, to, variant, details) for full determinism.
        lines.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then(a.1.cmp(&b.1))
                .then(a.2.cmp(&b.2))
                .then(a.3.cmp(&b.3))
        });

        for (_, _, _, _, line) in &lines {
            out.push_str(line);
            out.push('\n');
        }
        out
    }

    /// Detailed trace filtered by participant, without timing.
    pub fn trace_for(&self, name: &str) -> String {
        let inner = self.inner.lock().unwrap();
        let names = Names {
            objects: &inner.object_names,
            batches: &inner.batch_names,
        };
        let filtered: Vec<_> = inner
            .messages
            .iter()
            .filter(|m| m.from.name() == name || m.to.name() == name)
            .cloned()
            .collect();
        format_trace(&filtered, &names)
    }

    // --- Snapshot assertions ---

    /// Compact shape summary: one line per message, showing only
    /// `from -> to  MessageType`.
    ///
    /// ```text
    /// alice  -> server  ObjectUpdated
    /// server -> alice   PersistenceAck
    /// server -> bob     ObjectUpdated
    /// ```
    pub fn summary(&self) -> String {
        let inner = self.inner.lock().unwrap();
        summary_lines(&inner.messages)
    }

    /// Summary filtered by participants involved.
    pub fn summary_for(&self, name: &str) -> String {
        let inner = self.inner.lock().unwrap();
        let filtered: Vec<_> = inner
            .messages
            .iter()
            .filter(|m| m.from.name() == name || m.to.name() == name)
            .cloned()
            .collect();
        summary_lines(&filtered)
    }

    /// Assert the message flow matches an expected shape.
    ///
    /// Each line in `expected` should be `from -> to  MessageType`.
    /// Blank lines and leading/trailing whitespace are ignored.
    /// Dynamic values (object IDs, commits, etc.) are not compared.
    ///
    /// # Panics
    ///
    /// Panics with a diff if the actual flow doesn't match.
    ///
    /// # Example
    ///
    /// ```ignore
    /// tracer.expect("
    ///     alice  -> server  ObjectUpdated
    ///     server -> alice   PersistenceAck
    ///     server -> bob     ObjectUpdated
    /// ");
    /// ```
    pub fn expect(&self, expected: &str) {
        let actual = self.summary();
        let expected_lines = normalize_expectation(expected);
        let actual_lines = normalize_expectation(&actual);

        if expected_lines != actual_lines {
            let mut diff = String::new();
            diff.push_str("SyncTracer expectation mismatch!\n\n");
            diff.push_str("=== Expected ===\n");
            for line in &expected_lines {
                diff.push_str(line);
                diff.push('\n');
            }
            diff.push_str("\n=== Actual ===\n");
            for line in &actual_lines {
                diff.push_str(line);
                diff.push('\n');
            }
            diff.push_str("\n=== Full trace (with details) ===\n");
            diff.push_str(&self.dump());
            panic!("{diff}");
        }
    }

    /// Assert that the message flow **contains** the expected subsequence.
    ///
    /// Like `expect`, but the actual trace may have additional messages
    /// before, after, or between the expected lines. Each expected line
    /// must appear in order, but not necessarily consecutively.
    ///
    /// # Example
    ///
    /// ```ignore
    /// tracer.expect_contains("
    ///     alice  -> server  ObjectUpdated
    ///     server -> bob     ObjectUpdated
    /// ");
    /// ```
    pub fn expect_contains(&self, expected: &str) {
        let actual = self.summary();
        let expected_lines = normalize_expectation(expected);
        let actual_lines = normalize_expectation(&actual);

        let mut actual_idx = 0;
        let mut matched = Vec::new();

        for expected_line in &expected_lines {
            let mut found = false;
            while actual_idx < actual_lines.len() {
                if &actual_lines[actual_idx] == expected_line {
                    matched.push(actual_idx);
                    actual_idx += 1;
                    found = true;
                    break;
                }
                actual_idx += 1;
            }
            if !found {
                let mut diff = String::new();
                diff.push_str("SyncTracer expect_contains mismatch!\n\n");
                diff.push_str(&format!(
                    "Could not find expected line: {expected_line}\n\n"
                ));
                diff.push_str("=== Expected subsequence ===\n");
                for line in &expected_lines {
                    diff.push_str(line);
                    diff.push('\n');
                }
                diff.push_str("\n=== Actual (full) ===\n");
                for (i, line) in actual_lines.iter().enumerate() {
                    let marker = if matched.contains(&i) { "✓ " } else { "  " };
                    diff.push_str(marker);
                    diff.push_str(line);
                    diff.push('\n');
                }
                diff.push_str("\n=== Full trace (with details) ===\n");
                diff.push_str(&self.dump());
                panic!("{diff}");
            }
        }
    }
}

impl Default for SyncTracer {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for SyncTracer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SyncTracer")
            .field("message_count", &self.count())
            .finish()
    }
}

// ============================================================================
// Formatting
// ============================================================================

fn tally_messages(messages: &[SyncMessage]) -> String {
    use std::collections::BTreeMap;

    // Group by (from, arrow, to) -> type -> count
    let mut groups: BTreeMap<(String, &str, String), BTreeMap<String, usize>> = BTreeMap::new();

    for msg in messages {
        let key = (
            msg.from.name().to_string(),
            msg.side.arrow(),
            msg.to.name().to_string(),
        );
        let type_counts = groups.entry(key).or_default();
        *type_counts
            .entry(msg.payload.variant_name().to_string())
            .or_default() += 1;
    }

    let mut out = String::new();
    for ((from, arrow, to), type_counts) in &groups {
        let types: Vec<String> = type_counts
            .iter()
            .map(|(t, c)| format!("{t} ({c})"))
            .collect();
        out.push_str(&format!(
            "{from:<8} {arrow} {to:<8}: {}\n",
            types.join(", ")
        ));
    }
    out
}

fn summary_lines(messages: &[SyncMessage]) -> String {
    let mut out = String::new();
    for msg in messages {
        out.push_str(&format!(
            "{:<8} {} {:<8} {}",
            msg.from.name(),
            msg.side.arrow(),
            msg.to.name(),
            msg.payload.variant_name(),
        ));
        out.push('\n');
    }
    out
}

/// Normalize an expectation string: trim, remove blank lines, normalize whitespace.
fn normalize_expectation(s: &str) -> Vec<String> {
    s.lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .map(|l| {
            // Collapse multiple spaces to single space for flexible formatting
            let mut result = String::new();
            let mut prev_space = false;
            for c in l.chars() {
                if c.is_whitespace() {
                    if !prev_space {
                        result.push(' ');
                    }
                    prev_space = true;
                } else {
                    result.push(c);
                    prev_space = false;
                }
            }
            result
        })
        .collect()
}

/// Name maps for resolving IDs to human-readable names in formatted output.
struct Names<'a> {
    objects: &'a HashMap<ObjectId, String>,
    batches: &'a HashMap<BatchId, String>,
}

impl Names<'_> {
    fn object(&self, id: &ObjectId) -> String {
        self.objects
            .get(id)
            .cloned()
            .unwrap_or_else(|| short_object_id(id))
    }

    fn batch(&self, id: &BatchId) -> String {
        self.batches
            .get(id)
            .cloned()
            .unwrap_or_else(|| short_batch_id(id))
    }
}

/// Auto-normalizes dynamic values (commit hashes, branch names, object IDs)
/// into deterministic labels for stable snapshot testing.
struct Normalizer<'a> {
    object_names: &'a HashMap<ObjectId, String>,
    batch_map: HashMap<BatchId, String>,
    next_batch: usize,
    object_map: HashMap<ObjectId, String>,
    next_object: usize,
    branch_map: HashMap<String, String>,
    next_branch: usize,
}

impl<'a> Normalizer<'a> {
    fn new(object_names: &'a HashMap<ObjectId, String>) -> Self {
        Self {
            object_names,
            batch_map: HashMap::new(),
            next_batch: 1,
            object_map: HashMap::new(),
            next_object: 1,
            branch_map: HashMap::new(),
            next_branch: 1,
        }
    }

    fn batch(&mut self, id: &BatchId) -> String {
        self.batch_map
            .entry(*id)
            .or_insert_with(|| {
                let name = format!("B{}", self.next_batch);
                self.next_batch += 1;
                name
            })
            .clone()
    }

    fn object(&mut self, id: &ObjectId) -> String {
        if let Some(name) = self.object_names.get(id) {
            return name.clone();
        }
        self.object_map
            .entry(*id)
            .or_insert_with(|| {
                let name = format!("obj-{}", self.next_object);
                self.next_object += 1;
                name
            })
            .clone()
    }

    fn branch(&mut self, name: &crate::object::BranchName) -> String {
        let raw = name.to_string();
        self.branch_map
            .entry(raw.clone())
            .or_insert_with(|| {
                // Strip "client-<random>-" prefix if present
                if let Some(suffix) = raw.strip_prefix("client-")
                    && let Some((_random, rest)) = suffix.split_once('-')
                {
                    return rest.to_string();
                }
                let label = format!("branch-{}", self.next_branch);
                self.next_branch += 1;
                label
            })
            .clone()
    }

    fn format_payload(&mut self, payload: &SyncPayload) -> String {
        match payload {
            SyncPayload::RowBatchCreated { row, .. } => {
                format!(
                    "created row:{} branch:{} batch:{}",
                    self.object(&row.row_id),
                    self.branch(&BranchName::new(&row.branch)),
                    self.batch(&row.batch_id()),
                )
            }
            SyncPayload::RowBatchNeeded { row, .. } => {
                format!(
                    "needed row:{} branch:{} batch:{}",
                    self.object(&row.row_id),
                    self.branch(&BranchName::new(&row.branch)),
                    self.batch(&row.batch_id()),
                )
            }
            SyncPayload::BatchFate { fate } => self.format_settlement(fate),
            SyncPayload::BatchFateNeeded { batch_ids } => {
                let batches = batch_ids
                    .iter()
                    .map(|batch_id| self.batch(batch_id))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("batch_ids:[{batches}]")
            }
            SyncPayload::SealBatch { submission } => {
                let members = submission
                    .members
                    .iter()
                    .map(|member| format!("row:{}", self.object(&member.object_id)))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "seal batch:{} target:{} members:[{}] frontier:{}",
                    self.batch(&submission.batch_id),
                    self.branch(&submission.target_branch_name),
                    members,
                    submission.captured_frontier.len()
                )
            }
            SyncPayload::CatalogueEntryUpdated { entry } => {
                format!(
                    "catalogue obj:{} type:{}",
                    self.object(&entry.object_id),
                    entry.object_type().unwrap_or("unknown"),
                )
            }
            SyncPayload::QuerySubscription { query_id, .. } => {
                format!("query:{}", query_id.0)
            }
            SyncPayload::QueryUnsubscription { query_id } => {
                format!("query:{}", query_id.0)
            }
            SyncPayload::QuerySettled {
                query_id,
                scope,
                through_seq,
                ..
            } => {
                format!(
                    "query:{} scope:{} through_seq:{}",
                    query_id.0,
                    scope.len(),
                    through_seq
                )
            }
            SyncPayload::SchemaWarning(w) => {
                format!("query:{} table:{}", w.query_id.0, w.table_name)
            }
            SyncPayload::ConnectionSchemaDiagnostics(diagnostics) => {
                format!("client_schema:{}", diagnostics.client_schema_hash.short())
            }
            SyncPayload::Error(e) => {
                format!("{:?}", e)
            }
        }
    }

    fn format_settlement(&mut self, settlement: &crate::batch_fate::BatchFate) -> String {
        match settlement {
            crate::batch_fate::BatchFate::Missing { batch_id } => {
                format!("missing batch:{}", self.batch(batch_id))
            }
            crate::batch_fate::BatchFate::Rejected {
                batch_id,
                code,
                reason,
            } => {
                format!(
                    "rejected batch:{} code:{code} reason:{reason}",
                    self.batch(batch_id)
                )
            }
            crate::batch_fate::BatchFate::DurableDirect {
                batch_id,
                confirmed_tier,
            } => {
                format!(
                    "durable_direct batch:{} tier:{confirmed_tier:?}",
                    self.batch(batch_id)
                )
            }
            crate::batch_fate::BatchFate::AcceptedTransaction {
                batch_id,
                confirmed_tier,
            } => {
                format!(
                    "accepted_transaction batch:{} tier:{confirmed_tier:?}",
                    self.batch(batch_id)
                )
            }
        }
    }
}

fn format_trace(messages: &[SyncMessage], names: &Names<'_>) -> String {
    let mut out = String::new();
    for msg in messages {
        let details = format_payload_details(&msg.payload, names);
        out.push_str(&format!(
            "{:<8} {} {:<8}  {:<20} {}\n",
            msg.from.name(),
            msg.side.arrow(),
            msg.to.name(),
            msg.payload.variant_name(),
            details,
        ));
    }
    out
}

fn format_messages(messages: &[SyncMessage], names: &Names<'_>) -> String {
    let mut out = String::new();
    for msg in messages {
        out.push_str(&format_message(msg, names));
        out.push('\n');
    }
    out
}

fn format_message(msg: &SyncMessage, names: &Names<'_>) -> String {
    let details = format_payload_details(&msg.payload, names);
    format!(
        "[{:03}] +{:>5}ms  {:<8} {} {:<8}  {:<20} {}",
        msg.seq,
        msg.elapsed_ms,
        msg.from.name(),
        msg.side.arrow(),
        msg.to.name(),
        msg.payload.variant_name(),
        details,
    )
}

fn format_payload_details(payload: &SyncPayload, names: &Names<'_>) -> String {
    match payload {
        SyncPayload::RowBatchCreated { row, .. } => {
            format!(
                "created row:{} branch:{} batch:{}",
                names.object(&row.row_id),
                row.branch,
                names.batch(&row.batch_id()),
            )
        }
        SyncPayload::RowBatchNeeded { row, .. } => {
            format!(
                "needed row:{} branch:{} batch:{}",
                names.object(&row.row_id),
                row.branch,
                names.batch(&row.batch_id()),
            )
        }
        SyncPayload::CatalogueEntryUpdated { entry } => {
            format!(
                "catalogue obj:{} type:{}",
                names.object(&entry.object_id),
                entry.object_type().unwrap_or("unknown"),
            )
        }
        SyncPayload::BatchFate { fate } => format_settlement_details(fate, names),
        SyncPayload::BatchFateNeeded { batch_ids } => {
            let batches = batch_ids
                .iter()
                .map(|batch_id| names.batch(batch_id))
                .collect::<Vec<_>>()
                .join(", ");
            format!("batch_ids:[{batches}]")
        }
        SyncPayload::SealBatch { submission } => {
            let members = submission
                .members
                .iter()
                .map(|member| format!("row:{}", names.object(&member.object_id)))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "seal batch:{} target:{} members:[{}] frontier:{}",
                names.batch(&submission.batch_id),
                submission.target_branch_name,
                members,
                submission.captured_frontier.len()
            )
        }
        SyncPayload::QuerySubscription { query_id, .. } => {
            format!("query:{}", query_id.0)
        }
        SyncPayload::QueryUnsubscription { query_id } => {
            format!("query:{}", query_id.0)
        }
        SyncPayload::QuerySettled {
            query_id,
            scope,
            through_seq,
            ..
        } => {
            format!(
                "query:{} scope:{} through_seq:{}",
                query_id.0,
                scope.len(),
                through_seq
            )
        }
        SyncPayload::SchemaWarning(w) => {
            format!("query:{} table:{}", w.query_id.0, w.table_name)
        }
        SyncPayload::ConnectionSchemaDiagnostics(diagnostics) => {
            format!("client_schema:{}", diagnostics.client_schema_hash.short())
        }
        SyncPayload::Error(e) => {
            format!("{:?}", e)
        }
    }
}

fn format_settlement_details(
    settlement: &crate::batch_fate::BatchFate,
    names: &Names<'_>,
) -> String {
    match settlement {
        crate::batch_fate::BatchFate::Missing { batch_id } => {
            format!("missing batch:{}", names.batch(batch_id))
        }
        crate::batch_fate::BatchFate::Rejected {
            batch_id,
            code,
            reason,
        } => {
            format!(
                "rejected batch:{} code:{code} reason:{reason}",
                names.batch(batch_id)
            )
        }
        crate::batch_fate::BatchFate::DurableDirect {
            batch_id,
            confirmed_tier,
        } => {
            format!(
                "durable_direct batch:{} tier:{confirmed_tier:?}",
                names.batch(batch_id)
            )
        }
        crate::batch_fate::BatchFate::AcceptedTransaction {
            batch_id,
            confirmed_tier,
        } => {
            format!(
                "accepted_transaction batch:{} tier:{confirmed_tier:?}",
                names.batch(batch_id)
            )
        }
    }
}

/// First 4 bytes of a BatchId as hex (8 chars).
fn short_batch_id(id: &BatchId) -> String {
    hex::encode(&id.as_bytes()[..4])
}

/// First 8 chars of an ObjectId UUID.
fn short_object_id(id: &ObjectId) -> String {
    let s = id.to_string();
    s[..8.min(s.len())].to_string()
}

/// First 8 chars of a UUID.
fn short_uuid(uuid: &uuid::Uuid) -> String {
    let s = uuid.to_string();
    s[..8.min(s.len())].to_string()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::RowProvenance;
    use crate::row_histories::{RowState, StoredRowBatch};
    use crate::sync_manager::ServerId;

    fn make_row(byte: u8) -> StoredRowBatch {
        let row_id = ObjectId::new();
        StoredRowBatch::new(
            row_id,
            "main",
            Vec::new(),
            vec![byte],
            RowProvenance::for_insert(row_id.to_string(), 1000),
            Default::default(),
            RowState::VisibleDirect,
            None,
        )
    }

    #[test]
    fn record_and_query() {
        let tracer = SyncTracer::new();
        let server_id = ServerId::default();

        let payload = SyncPayload::RowBatchCreated {
            metadata: None,
            row: make_row(1),
        };

        tracer.record_outgoing("alice", &Destination::Server(server_id), &payload);
        tracer.record_outgoing(
            "server",
            &Destination::Client(ClientId::default()),
            &payload,
        );

        assert_eq!(tracer.count(), 2);
        assert_eq!(tracer.from("alice").len(), 1);
        assert_eq!(tracer.from("server").len(), 1);
        assert_eq!(tracer.of_type("RowBatchCreated").len(), 2);
    }

    #[test]
    fn between_filters_both_directions() {
        let tracer = SyncTracer::new();
        let server_id = ServerId::default();

        let row = make_row(1);
        let outgoing = SyncPayload::RowBatchCreated {
            metadata: None,
            row: row.clone(),
        };
        let incoming = SyncPayload::BatchFate {
            fate: crate::batch_fate::BatchFate::DurableDirect {
                batch_id: row.batch_id,
                confirmed_tier: crate::sync_manager::DurabilityTier::EdgeServer,
            },
        };

        tracer.record_outgoing("alice", &Destination::Server(server_id), &outgoing);
        tracer.record_incoming(&Source::Server(server_id), "alice", &incoming);

        let msgs = tracer.between("alice", "server");
        assert_eq!(msgs.len(), 2);
        assert!(msgs[0].is_object_updated());
        assert!(msgs[1].is_persistence_ack());
    }

    #[test]
    fn dump_produces_readable_output() {
        let tracer = SyncTracer::new();
        let server_id = ServerId::default();

        let payload = SyncPayload::RowBatchCreated {
            metadata: None,
            row: make_row(1),
        };

        tracer.record_outgoing("alice", &Destination::Server(server_id), &payload);

        let dump = tracer.dump();
        assert!(dump.contains("alice"));
        assert!(dump.contains("server"));
        assert!(dump.contains("RowBatchCreated"));
        assert!(dump.contains("branch:main"));
    }

    #[test]
    fn clear_resets_state() {
        let tracer = SyncTracer::new();
        let server_id = ServerId::default();

        let payload = SyncPayload::QueryUnsubscription {
            query_id: QueryId(42),
        };
        tracer.record_outgoing("bob", &Destination::Server(server_id), &payload);
        assert_eq!(tracer.count(), 1);

        tracer.clear();
        assert_eq!(tracer.count(), 0);
    }

    #[test]
    fn client_name_resolution() {
        let tracer = SyncTracer::new();
        let cid = ClientId::default();
        tracer.register_client(cid, "bob");

        let payload = SyncPayload::QueryUnsubscription {
            query_id: QueryId(1),
        };
        tracer.record_outgoing("server", &Destination::Client(cid), &payload);

        let msgs = tracer.to("bob");
        assert_eq!(msgs.len(), 1);
    }
}
