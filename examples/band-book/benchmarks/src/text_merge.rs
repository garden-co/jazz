//! BandBook's block text merges: a block's `text` is a Markdown merge column,
//! so concurrent edits to the same block both survive.
//!
//! The fixture is an authority and two devices connected over in-memory
//! transports. Each case puts a block into a state, takes the devices offline,
//! lets them edit, and measures one device reconnecting until the authority
//! has accepted its write (merging it where something was missed) and every
//! replica holds the result.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, RemoteLinkHint, SubscriptionStream, Upstream, block_on,
    duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::test_support::allow_all_policies;
use jazz::tools::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnType, RowDescriptor, Schema, TableName,
    TableSchema, Value as PublicValue,
};
use jazz::tx::DurabilityTier;

const TABLE: &str = "blocks";
const CORE: u8 = 0xc0;
const MAX_TURNS: usize = 10_000;

fn schema() -> JazzSchema {
    JazzSchema::new(&Schema::from([(
        TableName::new(TABLE),
        TableSchema {
            columns: RowDescriptor::new(vec![
                ColumnDescriptor::new("text", ColumnType::Text)
                    .default(PublicValue::Text(String::new()))
                    .merge_strategy(ColumnMergeStrategy::Markdown),
            ]),
            indexed_columns: None,
            composite_indexes: Vec::new(),
            policies: allow_all_policies(),
            branch_by: Vec::new(),
        },
    )]))
    .expect("BandBook block schema compiles")
}

fn open(node: u8, author: AuthorSubject) -> Db {
    block_on(Db::open(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .expect("an in-memory database opens")
}

fn device_author(node: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([node; 16])
}

fn text_cell(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("text".to_owned(), Value::String(text.to_owned()))])
}

struct Device {
    node: u8,
    db: Db,
    upstream: Option<Upstream>,
    _blocks: Option<SubscriptionStream>,
}

/// An authority and two devices, both subscribed to every block.
pub struct Band {
    core: Db,
    devices: Vec<Device>,
}

impl Band {
    pub fn new() -> Self {
        let mut band = Band {
            core: open(CORE, AuthorSubject::SYSTEM),
            devices: Vec::new(),
        };
        for i in 0..2 {
            let node = 0x50 + i as u8;
            let db = open(node, device_author(node));
            db.set_remote_link_hint(RemoteLinkHint::Live);
            band.devices.push(Device {
                node,
                db,
                upstream: None,
                _blocks: None,
            });
            band.connect(i);
            let prepared = band.devices[i]
                .db
                .prepare_query(&Query::from(TABLE))
                .expect("the block query prepares");
            let stream = band.drive(band.devices[i].db.subscribe(&prepared, ReadOpts::default()));
            band.devices[i]._blocks = Some(stream.expect("the block query subscribes"));
            band.pump();
        }
        band
    }

    fn connect(&mut self, i: usize) {
        let (device_side, core_side) = duplex();
        let device = &mut self.devices[i];
        device.upstream = Some(block_on(device.db.connect_upstream(device_side)));
        self.core
            .accept_subscriber(core_side, device_author(device.node));
    }

    pub fn offline(&mut self, i: usize) {
        let device = &mut self.devices[i];
        let upstream = device.upstream.take().expect("the device is online");
        assert!(device.db.detach_connection(&upstream));
        device.db.set_remote_link_hint(RemoteLinkHint::Failed);
    }

    pub fn online(&mut self, i: usize) {
        self.connect(i);
        self.devices[i]
            .db
            .set_remote_link_hint(RemoteLinkHint::Live);
        self.pump();
    }

    fn pump(&self) {
        for _ in 0..3 {
            for device in &self.devices {
                block_on(device.db.tick()).expect("a device ticks");
            }
            block_on(self.core.tick()).expect("the authority ticks");
            for device in &self.devices {
                block_on(device.db.tick()).expect("a device ticks");
            }
        }
    }

    fn drive<T>(&self, future: impl Future<Output = T>) -> T {
        let mut future = pin!(future);
        let mut context = Context::from_waker(Waker::noop());
        for _ in 0..MAX_TURNS {
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                return output;
            }
            self.pump();
        }
        panic!("operation did not complete within {MAX_TURNS} turns");
    }

    /// Wait until the authority has accepted device `i`'s write `tx` and every
    /// replica holds the outcome.
    pub fn settle(&self, i: usize, tx: u64) {
        self.drive(
            self.devices[i]
                .db
                .wait_for_transaction(tx, DurabilityTier::Global),
        )
        .expect("the write settles");
        self.pump();
    }

    /// A block device 0 creates, settled and held by both devices.
    pub fn block(&self, text: &str) -> RowUuid {
        let write = block_on(
            self.devices[0]
                .db
                .insert(TABLE, text_cell(text), Default::default()),
        )
        .expect("the block inserts");
        self.settle(0, write.mergeable_tx_id());
        write.row_uuid()
    }

    /// Device `i` writes the block's whole text; returns the write's tx.
    pub fn write(&self, i: usize, block: RowUuid, text: &str) -> u64 {
        block_on(
            self.devices[i]
                .db
                .update(TABLE, block, text_cell(text), Default::default()),
        )
        .expect("the block updates")
        .mergeable_tx_id()
    }

    /// The block's text as `replica` holds it: the authority, or a device.
    pub fn text(&self, replica: Replica, block: RowUuid) -> String {
        let db = match replica {
            Replica::Authority => &self.core,
            Replica::Device(i) => &self.devices[i].db,
        };
        let prepared = db.prepare_query(&Query::from(TABLE)).expect("prepares");
        match block_on(db.all(&prepared, ReadOpts::default()))
            .expect("the blocks read")
            .into_iter()
            .find(|row| row.row_uuid() == block)
            .and_then(|row| row.cell("text"))
        {
            Some(Value::String(text)) => text,
            other => panic!("the block's text is {other:?}"),
        }
    }

    /// The text every replica holds; panics if they differ.
    pub fn converged_text(&self, block: RowUuid) -> String {
        let text = self.text(Replica::Authority, block);
        for i in 0..self.devices.len() {
            assert_eq!(self.text(Replica::Device(i), block), text, "device {i}");
        }
        text
    }
}

impl Default for Band {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Replica {
    Authority,
    Device(usize),
}

/// A state the timed reconnect starts from: `device` is offline with
/// `pending` unsent writes, in order.
pub struct Reconnect {
    pub band: Band,
    pub block: RowUuid,
    pub device: usize,
    pub pending: Vec<u64>,
}

impl Reconnect {
    /// The timed part: the device reconnects and each pending write settles.
    pub fn run(mut self) -> (Band, RowUuid) {
        self.band.online(self.device);
        for &tx in &self.pending {
            self.band.settle(self.device, tx);
        }
        (self.band, self.block)
    }
}

/// One paragraph of a song page: lyrics with Markdown formatting.
pub const PARAGRAPH: &str = "Salt on the window, the ferry's **late** again. We count the harbour lights like they were friends, and *nobody* minds the rain. Hold on, hold on, the tide will turn around; keep one light burning on the edge of town. Key of D, 92 bpm, capo 2 on the acoustic.";

/// The fast path: device 0 edits the block while offline and nobody else
/// does, so the authority applies the write as is.
pub fn fast_path() -> Reconnect {
    let mut band = Band::new();
    let block = band.block(PARAGRAPH);
    band.offline(0);
    let tx = band.write(0, block, &format!("{PARAGRAPH} Count in on four."));
    Reconnect {
        band,
        block,
        device: 0,
        pending: vec![tx],
    }
}

/// Both devices edit the same paragraph while offline, one near the start
/// and one at the end. Device 0's write is accepted first; the timed part is
/// device 1's write, which the authority merges.
pub fn concurrent_paragraph() -> Reconnect {
    let mut band = Band::new();
    let block = band.block(PARAGRAPH);
    band.offline(0);
    band.offline(1);
    let ada = band.write(0, block, &format!("{PARAGRAPH} Tuning: drop D."));
    let bo = band.write(1, block, &format!("Intro: four bars. {PARAGRAPH}"));
    band.online(0);
    band.settle(0, ada);
    Reconnect {
        band,
        block,
        device: 1,
        pending: vec![bo],
    }
}

/// Device 1 types `writes` sentences into the paragraph while offline, one
/// write per pause in typing, while device 0 keeps editing the start of it
/// online. The timed part is device 1 reconnecting: each write of its chain
/// is merged over what device 1 had seen.
pub fn offline_chain(writes: usize) -> Reconnect {
    let mut band = Band::new();
    let block = band.block(PARAGRAPH);
    band.offline(1);
    let mut theirs = PARAGRAPH.to_owned();
    for i in 0..writes.div_ceil(4) {
        theirs = format!("Take {i}. {theirs}");
        let tx = band.write(0, block, &theirs);
        band.settle(0, tx);
    }
    let mut mine = PARAGRAPH.to_owned();
    let pending = (0..writes)
        .map(|i| {
            mine.push_str(&format!(" Bar {i}."));
            band.write(1, block, &mine)
        })
        .collect();
    Reconnect {
        band,
        block,
        device: 1,
        pending,
    }
}

/// Deterministic prose of about `bytes` bytes, `seed` choosing the words.
pub fn note(bytes: usize, seed: u64) -> String {
    const WORDS: &[&str] = &[
        "the",
        "band",
        "**loads**",
        "in",
        "at",
        "four",
        "through",
        "side",
        "door",
        "*then*",
        "sound",
        "check",
        "runs",
        "late",
        "so",
        "we",
        "cut",
        "encore",
        "drums",
        "bass",
        "`DI`",
        "box",
        "venue",
        "curfew",
        "Porto",
        "tour",
        "van",
        "set",
        "list",
        "chorus",
    ];
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut text = String::with_capacity(bytes + 16);
    let mut words = 0;
    while text.len() < bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        if !text.is_empty() {
            text.push(if words % 40 == 0 { '\n' } else { ' ' });
        }
        text.push_str(WORDS[(state % WORDS.len() as u64) as usize]);
        words += 1;
    }
    text
}

/// `note` with `edits` scattered word insertions, spaced evenly from
/// `offset` (a fraction of the spacing), each inserting `word`.
pub fn scattered(note: &str, edits: usize, offset: f64, word: &str) -> String {
    let spacing = note.len() / (edits + 1);
    let mut out = String::with_capacity(note.len() + edits * (word.len() + 1));
    let mut from = 0;
    for i in 0..edits {
        let mut at = ((i as f64 + offset) * spacing as f64) as usize + spacing / 2;
        // Insert after the next space, so no word or Markdown run is split.
        at = note[at..]
            .find(' ')
            .map_or(note.len(), |space| at + space + 1);
        if at <= from {
            continue;
        }
        out.push_str(&note[from..at]);
        out.push_str(word);
        out.push(' ');
        from = at;
    }
    out.push_str(&note[from..]);
    out
}

/// Scattered edits per side in the long note cases.
pub const LONG_NOTE_EDITS: usize = 200;

/// A long tour note of `bytes` bytes. Both devices make scattered edits while
/// offline, interleaved through the note; the timed part is device 1's
/// write, which the authority merges.
pub fn long_note(bytes: usize) -> Reconnect {
    let mut band = Band::new();
    let base = note(bytes, 1);
    let block = band.block(&base);
    band.offline(0);
    band.offline(1);
    let ada = band.write(0, block, &scattered(&base, LONG_NOTE_EDITS, 0.0, "Ada"));
    let bo = band.write(1, block, &scattered(&base, LONG_NOTE_EDITS, 0.5, "Bo"));
    band.online(0);
    band.settle(0, ada);
    Reconnect {
        band,
        block,
        device: 1,
        pending: vec![bo],
    }
}

/// Past the merge's work budget: while device 1 makes scattered edits to a
/// long note offline, device 0 replaces the whole note. Device 1's write
/// cannot be merged within the budget, so its whole value wins.
pub fn over_budget(bytes: usize) -> (Reconnect, String) {
    let mut band = Band::new();
    let base = note(bytes, 1);
    let block = band.block(&base);
    band.offline(0);
    band.offline(1);
    let ada = band.write(0, block, &note(bytes, 2));
    let bos = scattered(&base, LONG_NOTE_EDITS, 0.5, "Bo");
    let bo = band.write(1, block, &bos);
    band.online(0);
    band.settle(0, ada);
    (
        Reconnect {
            band,
            block,
            device: 1,
            pending: vec![bo],
        },
        bos,
    )
}
