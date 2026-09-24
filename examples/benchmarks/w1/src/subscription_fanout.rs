//! Dashboard pattern: one tenant overview and N independently opened keyed lists.
//! All actors use real Jazz runtimes. Logical in-process transport deliberately
//! excludes serialization, IndexedDB, network latency and browser scheduling.

use super::*;
use jazz::account_registry::AccountId;
use jazz::db::Transport;
use jazz::protocol::SyncMessage;
use jazz::tools::public_schema::Operation;
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

pub const GROUPS: usize = 60;

fn reader(index: u8) -> AuthorSubject {
    let id = row_id(0x61, index as usize).0;
    AuthorSubject::for_test_uuid(id).with_account(AccountId(id))
}

fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("teams")
                    .column("member", ColumnType::Uuid)
                    .policies(TablePolicies::new().with_select(PolicyExpr::eq_session(
                        "member",
                        vec!["user".into(), "account".into()],
                    ))),
            )
            .table(
                TableSchemaBuilder::new("tasks")
                    .fk_column("team", "teams")
                    .column("board", ColumnType::Integer)
                    .column("title", ColumnType::Text)
                    .index_only(["team", "board"])
                    .policies(TablePolicies::new().with_select(PolicyExpr::Inherits {
                        operation: Operation::Select,
                        via_column: "team".into(),
                        max_depth: None,
                    })),
            )
            .build(),
    )
    .expect("dashboard schema")
}

struct Carrier {
    incoming: Rc<RefCell<VecDeque<SyncMessage>>>,
    outgoing: Rc<RefCell<VecDeque<SyncMessage>>>,
}
impl Transport for Carrier {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.outgoing.borrow_mut().push_back(message);
        Ok(())
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.incoming.borrow_mut().pop_front()
    }
}
fn connect(
    upstream: &Db<MemoryStorage>,
    downstream: &Db<MemoryStorage>,
    author: AuthorSubject,
    relay: bool,
) {
    let a = Rc::new(RefCell::new(VecDeque::new()));
    let b = Rc::new(RefCell::new(VecDeque::new()));
    block_on(downstream.connect_upstream(Box::new(Carrier {
        incoming: a.clone(),
        outgoing: b.clone(),
    })));
    let transport = Box::new(Carrier {
        incoming: b,
        outgoing: a,
    });
    if relay {
        upstream.accept_scope_isolated_relay_subscriber_for_test(
            transport,
            author,
            BTreeMap::new(),
            1,
        );
    } else {
        upstream.accept_subscriber(transport, author);
    }
}
fn open(schema: &JazzSchema, tag: u8, author: AuthorSubject, core: bool) -> Db<MemoryStorage> {
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let config = DbConfig::new(
        schema.clone(),
        MemoryStorage::new(&refs).unwrap(),
        DbIdentity {
            node: NodeUuid::from_bytes([tag; 16]),
            author,
        },
    );
    block_on(async {
        if core {
            Db::open_history_complete(config).await
        } else {
            Db::open(config).await
        }
    })
    .expect("open dashboard actor")
}

#[derive(Debug, Default)]
pub struct FanoutReceipt {
    pub rows_per_team: usize,
    pub keyed_lists: usize,
    pub preparation: Duration,
    pub subscribe: Duration,
    pub core_ticks: Duration,
    pub relay_ticks: Duration,
    pub foreground_ticks: Duration,
    pub elapsed: Duration,
    pub turns: usize,
    pub compilations_core_relay_foreground: [usize; 3],
}

/// A fresh Core -> device-local persistence relay -> non-durable foreground topology.
/// Core owns authorization; the scope-isolated relay is not a server Edge.
/// Two teams have equally sized tables; Alice can read only team zero. The
/// overview intentionally has no tenant predicate, so policy is load-bearing.
pub struct FanoutFixture {
    core: Db<MemoryStorage>,
    relay: Db<MemoryStorage>,
    foreground: Db<MemoryStorage>,
    rows_per_team: usize,
    keyed_lists: usize,
    streams: Vec<SubscriptionStream>,
    results: Vec<BTreeMap<RowUuid, String>>,
    settled: Vec<bool>,
    read_opts: ReadOpts,
}

impl FanoutFixture {
    pub fn new(rows_per_team: usize, keyed_lists: usize) -> Self {
        assert!(rows_per_team > 0 && keyed_lists <= GROUPS);
        let schema = schema();
        let core = open(&schema, 0x62, AuthorSubject::SYSTEM, true);
        let tx = block_on(core.mergeable_tx()).unwrap();
        for team in 0..2 {
            block_on(tx.insert(
                "teams",
                BTreeMap::from([("member".into(), Value::Uuid(reader(team as u8).test_uuid()))]),
                InsertOptions {
                    row_id: Some(row_id(0x63, team)),
                    ..Default::default()
                },
            ))
            .unwrap();
            for index in 0..rows_per_team {
                block_on(tx.insert(
                    "tasks",
                    BTreeMap::from([
                        ("team".into(), Value::Uuid(row_id(0x63, team).0)),
                        ("board".into(), Value::I32((index % GROUPS) as i32)),
                        (
                            "title".into(),
                            Value::String(format!("task-{team}-{index}")),
                        ),
                    ]),
                    InsertOptions {
                        row_id: Some(row_id(0x64 + team as u8, index)),
                        ..Default::default()
                    },
                ))
                .unwrap();
            }
        }
        let commit = block_on(tx.commit()).unwrap();
        core.finalize_local_mergeable_commit_for_test(commit)
            .unwrap();
        let relay = open(&schema, 0x66, reader(0), false);
        relay.set_relay_authority_session_owner_for_test();
        let foreground = open(&schema, 0x67, reader(0), false);
        foreground.set_non_durable_client();
        connect(&core, &relay, reader(0), true);
        connect(&relay, &foreground, reader(0), false);
        Self {
            core,
            relay,
            foreground,
            rows_per_team,
            keyed_lists,
            streams: Vec::new(),
            results: Vec::new(),
            settled: Vec::new(),
            read_opts: ReadOpts::default(),
        }
    }

    /// Opens all independent lists before pumping. No app-level query merging.
    /// Seed, runtime creation and connection setup happen outside this timer;
    /// preparation, admission, synchronization and exact-result consumption do not.
    pub fn hydrate(&mut self) -> FanoutReceipt {
        assert!(self.streams.is_empty());
        let before = self.compilation_counts();
        let start = Instant::now();
        let mut receipt = FanoutReceipt {
            rows_per_team: self.rows_per_team,
            keyed_lists: self.keyed_lists,
            ..Default::default()
        };
        for index in 0..=self.keyed_lists {
            let phase = Instant::now();
            let query = Query::from("tasks").select(["id", "title", "board"]);
            let query = if index == 0 {
                query
            } else {
                query.filter(eq(col("board"), lit((index - 1) as i32)))
            };
            let prepared = self.foreground.prepare_query(&query).unwrap();
            receipt.preparation += phase.elapsed();
            let phase = Instant::now();
            self.streams.push(
                block_on(self.foreground.subscribe(&prepared, self.read_opts.clone())).unwrap(),
            );
            receipt.subscribe += phase.elapsed();
            self.results.push(BTreeMap::new());
            self.settled.push(false);
        }
        for turn in 0..1024 {
            self.drain();
            // Local-first may publish a settled empty cache before any remote
            // inputs arrive. The dashboard milestone is complete usable data,
            // not that provisional local result.
            if self.settled.iter().all(|settled| *settled)
                && self.results.iter().enumerate().all(|(query, rows)| {
                    let expected = if query == 0 {
                        self.rows_per_team
                    } else {
                        (self.rows_per_team + GROUPS - query) / GROUPS
                    };
                    rows.len() == expected
                })
            {
                receipt.elapsed = start.elapsed();
                receipt.turns = turn;
                receipt.compilations_core_relay_foreground =
                    std::array::from_fn(|index| self.compilation_counts()[index] - before[index]);
                return receipt;
            }
            self.tick(&mut receipt);
        }
        panic!("dashboard failed to settle within 1024 turns");
    }

    fn compilation_counts(&self) -> [usize; 3] {
        [
            self.core.query_program_compilations_for_test(),
            self.relay.query_program_compilations_for_test(),
            self.foreground.query_program_compilations_for_test(),
        ]
    }

    fn tick(&self, receipt: &mut FanoutReceipt) {
        let phase = Instant::now();
        block_on(self.foreground.tick()).expect("foreground tick");
        receipt.foreground_ticks += phase.elapsed();
        let phase = Instant::now();
        block_on(self.relay.tick()).expect("relay tick");
        receipt.relay_ticks += phase.elapsed();
        let phase = Instant::now();
        block_on(self.core.tick()).expect("core tick");
        receipt.core_ticks += phase.elapsed();
    }

    fn drain(&mut self) {
        for (index, stream) in self.streams.iter_mut().enumerate() {
            while let Some(event) = stream.try_next_event() {
                match event {
                    SubscriptionEvent::Delta {
                        reset,
                        added,
                        updated,
                        removed,
                        settled,
                        ..
                    } => {
                        if reset {
                            self.results[index].clear();
                        }
                        for row in removed {
                            self.results[index].remove(&row.row_uuid);
                        }
                        for row in added.into_iter().chain(updated) {
                            let (descriptor, bytes) = row.row.encoded_record();
                            let record =
                                jazz::groove::records::BorrowedRecord::new(bytes, descriptor);
                            let Value::String(title) = record.get("title").expect("title") else {
                                panic!("title type")
                            };
                            self.results[index].insert(row.row_uuid(), title);
                        }
                        self.settled[index] = settled;
                    }
                    SubscriptionEvent::Rejected { reason } => {
                        panic!("dashboard rejected: {reason:?}")
                    }
                    SubscriptionEvent::Closed => panic!("dashboard closed"),
                }
            }
        }
    }

    /// Outside benchmark timing; checks membership and payloads, not just counts.
    pub fn assert_initial_results(&self) {
        for (query, actual) in self.results.iter().enumerate() {
            let expected = (0..self.rows_per_team)
                .filter(|index| query == 0 || index % GROUPS == query - 1)
                .map(|index| (row_id(0x64, index), format!("task-0-{index}")))
                .collect::<BTreeMap<_, _>>();
            assert_eq!(actual, &expected, "exact policy-filtered list {query}");
        }
    }

    pub fn assert_live_update_and_revocation(&mut self) {
        let write = block_on(self.core.update(
            "tasks",
            row_id(0x64, 0),
            BTreeMap::from([("title".into(), Value::String("edited".into()))]),
            Default::default(),
        ))
        .unwrap();
        self.core
            .finalize_local_mergeable_commit_for_test(write.mergeable_tx_id())
            .unwrap();
        block_on(write.wait(DurabilityTier::Global)).unwrap();
        self.pump_until("edit", |fixture| {
            fixture.results.iter().enumerate().all(|(index, rows)| {
                index > 1
                    || rows
                        .get(&row_id(0x64, 0))
                        .is_some_and(|title| title == "edited")
            })
        });
        for (query, actual) in self.results.iter().enumerate() {
            let expected = (0..self.rows_per_team)
                .filter(|index| query == 0 || index % GROUPS == query - 1)
                .map(|index| row_id(0x64, index))
                .collect::<BTreeSet<_>>();
            assert_eq!(actual.keys().copied().collect::<BTreeSet<_>>(), expected);
            for (id, title) in actual {
                if *id == row_id(0x64, 0) {
                    assert_eq!(title, "edited");
                }
            }
        }
        let revoke = block_on(self.core.update(
            "teams",
            row_id(0x63, 0),
            BTreeMap::from([("member".into(), Value::Uuid(reader(1).test_uuid()))]),
            Default::default(),
        ))
        .unwrap();
        self.core
            .finalize_local_mergeable_commit_for_test(revoke.mergeable_tx_id())
            .unwrap();
        block_on(revoke.wait(DurabilityTier::Global)).unwrap();
        if self.read_opts.tier == DurabilityTier::Local {
            // SPEC 16 §16.1.1: withdrawal is not deletion. Local-first may
            // retain learned rows; strict remote must not.
            for _ in 0..32 {
                self.tick(&mut FanoutReceipt::default());
                self.drain();
            }
            assert_eq!(self.results[0].len(), self.rows_per_team);
            assert_eq!(self.results[0][&row_id(0x64, 0)], "edited");
        } else {
            self.pump_until("revoke", |fixture| {
                fixture.results.iter().all(BTreeMap::is_empty)
            });
        }
    }

    fn pump_until(&mut self, phase: &str, done: impl Fn(&Self) -> bool) {
        for _ in 0..1024 {
            self.tick(&mut FanoutReceipt::default());
            self.drain();
            if done(self) {
                return;
            }
        }
        panic!(
            "dashboard {phase} did not converge: {:?}",
            self.results
                .iter()
                .map(|rows| (rows.len(), rows.get(&row_id(0x64, 0))))
                .collect::<Vec<_>>()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Alice's independent board lists share inherited team access, not Bob's
    /// data. Core -> relay -> Alice propagates edits; revocation removes strict
    /// remote rows but does not delete already learned local-first data.
    #[test]
    fn fanout_preserves_exact_membership_updates_and_revocation() {
        for tier in [DurabilityTier::Local, DurabilityTier::Global] {
            for keyed_lists in [0, 3, GROUPS] {
                let mut fixture = FanoutFixture::new(120, keyed_lists);
                fixture.read_opts.tier = tier;
                if tier == DurabilityTier::Global {
                    fixture.read_opts.local_updates = LocalUpdates::Deferred;
                }
                fixture.hydrate();
                fixture.assert_initial_results();
                fixture.assert_live_update_and_revocation();
            }
        }
    }
}
