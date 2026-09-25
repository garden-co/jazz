//! Hosted initial-SELECT endpoint. Every observation owns a fresh reopened
//! runtime; preparation, verification, encoding and teardown are outside timing.

use super::*;
use jazz::db::PreparedQuery;
use jazz::node::CurrentRow;

type ExpectedSelects = Vec<(String, Vec<RowUuid>)>;

pub struct InitialSelects {
    db: Db<BoxedStorage>,
    _dir: tempfile::TempDir,
    prepared: Vec<PreparedQuery>,
    expected: Rc<ExpectedSelects>,
    identity: AuthorSubject,
    limit: Option<usize>,
    executed: bool,
}

impl Fixture {
    /// Reopen a fresh copy using the seeded node identity. A different identity
    /// would measure copied-store recovery, which is a separate workload.
    pub fn initial_selects(&self, limit: Option<usize>) -> InitialSelects {
        let dir = tempfile::tempdir().expect("initial SELECT store directory");
        copy_dir_contents(self.seeded._core_dir.path(), dir.path()).expect("copy seeded store");
        let identity = self.config.client_author(&self.seeded);
        let reopen_node = node(1);
        let db = block_on(Db::open(DbConfig {
            schema: self.schema.clone(),
            storage: BoxedStorage::new(open_storage(dir.path(), &self.schema)),
            identity: DbIdentity {
                node: reopen_node,
                author: identity,
            },
            id_source: Some(Box::new(SeededRowIdSource::new(node_uuid_seed(
                reopen_node,
            )))),
        }))
        .expect("open authority for initial SELECTs");
        let tables = subscription_tables();
        let expected = tables
            .iter()
            .map(|table| {
                (
                    table.clone(),
                    self.expected_rows_by_table[table]
                        .iter()
                        .copied()
                        .take(limit.unwrap_or(usize::MAX))
                        .collect(),
                )
            })
            .collect();
        let prepared = tables
            .iter()
            .map(|table| {
                let mut query = Query::from(table.as_str());
                if let Some(limit) = limit {
                    query = query.limit(limit);
                }
                db.prepare_query(&query).expect("prepare initial SELECT")
            })
            .collect();
        InitialSelects {
            db,
            _dir: dir,
            prepared,
            expected: Rc::new(expected),
            identity,
            limit,
            executed: false,
        }
    }
}

impl InitialSelects {
    /// Execute each prepared query once. Divan receives the owned results and
    /// drops them after timing, so checks/encoding/destruction are not measured.
    #[inline(never)]
    pub fn read(&mut self) -> CompletedInitialSelects {
        assert!(
            !self.executed,
            "initial SELECTs require a fresh runtime per sample"
        );
        self.executed = true;
        let rows = self
            .prepared
            .iter()
            .map(|query| {
                block_on(self.db.all_for_identity(
                    query,
                    ReadOpts {
                        tier: jazz::tx::DurabilityTier::Global,
                        local_updates: jazz::db::LocalUpdates::Deferred,
                        propagation: jazz::db::Propagation::LocalOnly,
                        include_deleted: false,
                        ..ReadOpts::default()
                    },
                    self.identity,
                ))
                .expect("initial SELECT")
            })
            .collect();
        CompletedInitialSelects {
            rows,
            expected: Rc::clone(&self.expected),
            limit: self.limit,
        }
    }
}

impl Drop for InitialSelects {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            block_on(self.db.close()).expect("close initial SELECT authority");
        }
    }
}

pub struct CompletedInitialSelects {
    rows: Vec<Vec<CurrentRow>>,
    expected: Rc<ExpectedSelects>,
    limit: Option<usize>,
}

impl CompletedInitialSelects {
    /// Exact independent membership/order checks and encoded output receipts.
    /// Hashes are diagnostic comparison values, not storage/wire identities.
    pub fn receipt(&self) -> JsonValue {
        assert_eq!(
            self.rows.len(),
            self.expected.len(),
            "complete SELECT inventory"
        );
        let per_table = self
            .rows
            .iter()
            .zip(self.expected.iter())
            .map(|(rows, (table, expected))| {
                let actual = rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>();
                assert_eq!(
                    &actual, expected,
                    "exact authorized UUID-ordered SELECT: {table}"
                );
                let encoded =
                    jazz::binding_codec::encode_rows(rows).expect("encode result receipt");
                let mut digest = std::collections::hash_map::DefaultHasher::new();
                encoded.hash(&mut digest);
                json!({
                    "table": table,
                    "rows": rows.len(),
                    "encoded_bytes": encoded.len(),
                    "encoded_hash": digest.finish(),
                })
            })
            .collect::<Vec<_>>();
        json!({
            "benchmark": "permissioned_resources_initial_selects",
            "query_limit": self.limit,
            "fresh_runtime": true,
            "queries": self.rows.len(),
            "rows": self.rows.iter().map(Vec::len).sum::<usize>(),
            "per_table": per_table,
        })
    }
}

impl Drop for CompletedInitialSelects {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            // Keep the exact result signatures with the hosted log. This is
            // outside Divan's measured interval, just like CompletedSync checks.
            eprintln!("INITIAL_SELECT_RECEIPT {}", self.receipt());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_selects_preserve_all_tables_and_bounded_uuid_prefixes() {
        let fixture = Fixture::new(0.01);
        let mut full = fixture.initial_selects(None);
        let full = full.read();
        assert_eq!(full.receipt()["queries"], 39);
        assert!(full.rows.iter().any(|rows| rows.len() > 100));
        let mut bounded = fixture.initial_selects(Some(100));
        let bounded = bounded.read();
        assert_eq!(bounded.receipt()["queries"], 39);
        for (full_rows, page) in full.rows.iter().zip(&bounded.rows) {
            assert_eq!(
                page.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
                full_rows
                    .iter()
                    .take(100)
                    .map(|row| row.row_uuid())
                    .collect::<Vec<_>>(),
            );
            let expected = jazz::binding_codec::encode_rows(&full_rows[..full_rows.len().min(100)])
                .expect("encode full-read prefix");
            assert_eq!(jazz::binding_codec::encode_rows(page).unwrap(), expected);
        }
        // A second independently reopened sample must produce identical bytes,
        // including provenance. Preparing a new handle on an old runtime would
        // not exercise the cold compiler path this benchmark is intended to cover.
        let mut repeated = fixture.initial_selects(None);
        let repeated = repeated.read();
        assert_eq!(full.receipt(), repeated.receipt());
    }

    #[test]
    #[should_panic(expected = "initial SELECTs require a fresh runtime per sample")]
    fn initial_select_sample_cannot_silently_become_a_warm_read() {
        let fixture = Fixture::new(0.001);
        let mut sample = fixture.initial_selects(Some(1));
        drop(sample.read());
        let _ = sample.read();
    }
}
