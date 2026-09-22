//! Permissioned, batched client uploads to a history-complete authority.
//! Public schema/runtime APIs; synthetic names and payloads only. The logical
//! carrier excludes encoding, network and persistent-backend costs. Unlike a
//! SYSTEM seed, every measured write traverses direct-session authorization.

use super::*;
use jazz::db::Transport;
use jazz::protocol::SyncMessage;
use jazz::tools::public_schema::Operation;
use std::time::{Duration, Instant};

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
    .unwrap()
}

#[derive(Debug, Default)]
pub struct UploadReceipt {
    pub rows: usize,
    pub batches: usize,
    pub publish: Duration,
    pub client_ticks: Duration,
    pub core_ticks: Duration,
    pub elapsed: Duration,
}

pub struct UploadFixture {
    core: Db<MemoryStorage>,
    client: Db<MemoryStorage>,
    rows: usize,
    batch_size: usize,
    payload: String,
}

impl UploadFixture {
    pub fn new(rows: usize, batch_size: usize) -> Self {
        assert!(rows > 0 && batch_size > 0);
        let owner = PolicyExpr::eq_session("owner", vec!["claims".into(), "sub".into()]);
        let inherited = PolicyExpr::Inherits {
            operation: Operation::Select,
            via_column: "folder".into(),
            max_depth: None,
        };
        let schema = JazzSchema::new(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("folders")
                        .column("owner", ColumnType::Uuid)
                        .policies(TablePolicies::new().with_select(owner)),
                )
                .table(
                    TableSchemaBuilder::new("entries")
                        .fk_column("folder", "folders")
                        .column("body", ColumnType::Text)
                        .policies(
                            TablePolicies::new()
                                .with_select(inherited.clone())
                                .with_insert(inherited),
                        ),
                )
                .build(),
        )
        .unwrap();
        let author = AuthorSubject::for_test_bytes([0x71; 16]);
        let core = open(&schema, 0x72, AuthorSubject::SYSTEM, true);
        let tx = block_on(core.mergeable_tx()).unwrap();
        block_on(tx.insert(
            "folders",
            BTreeMap::from([("owner".into(), Value::Uuid(author.test_uuid()))]),
            InsertOptions {
                row_id: Some(row_id(0x73, 0)),
                ..Default::default()
            },
        ))
        .unwrap();
        core.finalize_local_mergeable_commit_for_test(block_on(tx.commit()).unwrap())
            .unwrap();
        let client = open(&schema, 0x74, author, false);
        let a = Rc::new(RefCell::new(VecDeque::new()));
        let b = Rc::new(RefCell::new(VecDeque::new()));
        block_on(client.connect_upstream(Box::new(Carrier {
            incoming: a.clone(),
            outgoing: b.clone(),
        })));
        core.accept_subscriber(
            Box::new(Carrier {
                incoming: b,
                outgoing: a,
            }),
            author,
        );
        Self {
            core,
            client,
            rows,
            batch_size,
            payload: "The small record describes an ordinary change. ".repeat(24),
        }
    }

    /// Includes local publication, authority admission and acknowledged global
    /// settlement for every batch. Setup and final verification are off-clock.
    pub fn upload(&mut self) -> UploadReceipt {
        let start = Instant::now();
        let mut receipt = UploadReceipt {
            rows: self.rows,
            ..Default::default()
        };
        for offset in (0..self.rows).step_by(self.batch_size) {
            let phase = Instant::now();
            let tx = block_on(self.client.mergeable_tx()).unwrap();
            for index in offset..(offset + self.batch_size).min(self.rows) {
                block_on(tx.insert(
                    "entries",
                    BTreeMap::from([
                        ("folder".into(), Value::Uuid(row_id(0x73, 0).0)),
                        (
                            "body".into(),
                            Value::String(format!("{index}: {}", self.payload)),
                        ),
                    ]),
                    InsertOptions {
                        row_id: Some(row_id(0x75, index)),
                        ..Default::default()
                    },
                ))
                .unwrap();
            }
            let tx_id = block_on(tx.commit()).unwrap();
            receipt.publish += phase.elapsed();
            let mut wait = std::pin::pin!(
                self.client
                    .wait_for_transaction(tx_id, DurabilityTier::Global)
            );
            let mut context = Context::from_waker(Waker::noop());
            let mut settled = false;
            for _ in 0..1024 {
                let phase = Instant::now();
                block_on(self.client.tick()).unwrap();
                receipt.client_ticks += phase.elapsed();
                let phase = Instant::now();
                block_on(self.core.tick()).unwrap();
                receipt.core_ticks += phase.elapsed();
                if let Poll::Ready(result) = wait.as_mut().poll(&mut context) {
                    result.expect("permissioned upload accepted at authority");
                    settled = true;
                    break;
                }
            }
            assert!(settled, "upload failed to reach global durability");
            receipt.batches += 1;
        }
        receipt.elapsed = start.elapsed();
        receipt
    }

    pub fn assert_uploaded(&self) {
        let prepared = self
            .core
            .prepare_query(&Query::from("entries").select(["id", "body"]))
            .unwrap();
        let result = block_on(self.core.all(&prepared, ReadOpts::default())).unwrap();
        assert_eq!(result.len(), self.rows);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permissioned_direct_upload_settles_every_batch() {
        let mut fixture = UploadFixture::new(57, 50);
        let receipt = fixture.upload();
        assert_eq!(receipt.batches, 2);
        fixture.assert_uploaded();
    }
}
