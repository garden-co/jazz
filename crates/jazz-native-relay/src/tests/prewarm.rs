//! The Android prewarm: a launch opens the relay its predecessor admitted,
//! from the record that one left beside the stores, before JavaScript asks.
//! A prewarm is only a guess about this launch: whatever the app then does
//! (signs in as someone else, deletes the store, ships another schema) must
//! end exactly as it would have without one.
use super::*;
use jazz::binding_codec::decode_rows;
use jazz::ids::RowUuid;
use jazz::query::Query;
use jazz::records::{OwnedRecord, RecordDescriptor, ValueType};
use jazz::tools::native_transport_connector::{
    NativeTransportConnector, NativeTransportFuture, NativeTransportRequest,
};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use std::time::Duration;

/// An upstream that never answers: these launches stay offline.
struct Offline;

impl NativeTransportConnector for Offline {
    fn connect(&self, _request: NativeTransportRequest) -> NativeTransportFuture {
        Box::pin(std::future::pending())
    }
}

fn schema() -> JazzSchema {
    let allow = PolicyExpr::True;
    let policies = TablePolicies::new()
        .with_select(allow.clone())
        .with_insert(allow.clone())
        .with_update(Some(allow.clone()), allow.clone())
        .with_delete(allow);
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .policies(policies),
            )
            .build(),
    )
    .unwrap()
}

fn schema_source() -> String {
    serde_json::to_string(schema().public_schema()).unwrap()
}

fn bearer(subject: &str) -> String {
    use base64::Engine;
    let claims = serde_json::json!({ "iss": "https://issuer.example", "sub": subject });
    format!(
        "x.{}.x",
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&claims).unwrap())
    )
}

fn title_cells(title: &str) -> Vec<u8> {
    let descriptor = RecordDescriptor::new([("title", ValueType::String)]);
    let raw = descriptor
        .create(&[Value::String(title.to_owned())])
        .unwrap();
    jazz::binding_codec::encode_named_cells(&OwnedRecord::new(raw, descriptor)).unwrap()
}

fn titles(rows: &[u8]) -> Vec<(RowUuid, String)> {
    let mut rows: Vec<_> = decode_rows(rows)
        .unwrap()
        .into_iter()
        .flat_map(|batch| batch.rows)
        .map(|row| {
            let title = match row.cells.get("title") {
                Some(Value::String(title)) => title.clone(),
                other => panic!("title cell: {other:?}"),
            };
            (row.row_id, title)
        })
        .collect();
    rows.sort();
    rows
}

/// One app process: its platform host, as the Android bridge creates it.
struct Launch {
    host: *mut JazzNativeRelayHost,
    lease: *mut JazzNativeRelayHostLease,
}

impl Launch {
    fn new() -> Self {
        let host = Box::into_raw(Box::new(JazzNativeRelayHost {
            inner: Arc::new(Mutex::new(NativeRelayHost {
                socket_connector: Some(Arc::new(Offline)),
                ..NativeRelayHost::default()
            })),
        }));
        let lease = unsafe { jazz_native_relay_host_retain(host, 1) };
        assert!(!lease.is_null());
        Self { host, lease }
    }

    /// What the content provider runs at process start.
    fn prewarm(&self, root: &std::path::Path) -> JazzNativeRelayStatus {
        let root = root.display().to_string();
        unsafe { jazz_native_relay_host_prewarm(self.host, root.as_ptr(), root.len()) }
    }

    /// The same prewarm with a short hold for an unclaimed relay.
    fn prewarm_holding(&self, root: &std::path::Path, hold: Duration) {
        let _ = host_prewarm(unsafe { &*self.host }, root, hold);
    }

    fn speculative_identity(&self, scope: &RelayScope) -> Option<jazz::db::DbIdentity> {
        unsafe { &*self.host }
            .lock()
            .unwrap()
            .registry
            .speculative_identity(scope)
    }

    fn close(&self, scope: &RelayScope) {
        unsafe { &*self.host }
            .lock()
            .unwrap()
            .registry
            .close(scope)
            .unwrap();
    }

    fn registry_scopes(&self) -> Vec<(RelayScope, bool)> {
        unsafe { &*self.host }
            .lock()
            .unwrap()
            .registry
            .open_scopes()
    }

    /// Session setup as jazz-rn runs it for a signed-in `subject`, then one
    /// foreground on the admitted scope.
    fn sign_in(&self, root: &std::path::Path, subject: &str, schema_json: &str) -> u64 {
        let admitted = self.admit(root, subject, schema_json);
        self.open(&admitted)
    }

    fn admit(&self, root: &std::path::Path, subject: &str, schema_json: &str) -> [u8; 32] {
        let setup = serde_json::to_vec(&serde_json::json!({
            "server_url": "https://upstream.example",
            "app_id": "prewarm-app",
            "jwt": bearer(subject),
            "storage_root": root.display().to_string(),
        }))
        .unwrap();
        let mut output = JazzNativeRelayBytes::EMPTY;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_begin_private_session_json(
                    self.host,
                    setup.as_ptr(),
                    setup.len(),
                    &mut output,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        let pending = capability(&mut output);
        assert_eq!(
            unsafe {
                jazz_native_relay_host_attach_canonical_schema_json(
                    self.host,
                    pending.as_ptr(),
                    pending.len(),
                    schema_json.as_ptr(),
                    schema_json.len(),
                    &mut output,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        capability(&mut output)
    }

    fn open(&self, admitted: &[u8; 32]) -> u64 {
        let mut foreground = 0;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_open_attached_foreground(
                    self.lease,
                    admitted.as_ptr(),
                    admitted.len(),
                    &mut foreground,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        foreground
    }

    fn execute(
        &self,
        foreground: u64,
        command: ForegroundDbCommandRequest,
    ) -> ForegroundDbCommandResponse {
        let request = postcard::to_allocvec(&command).unwrap();
        let mut output = JazzNativeRelayBytes::EMPTY;
        let status = unsafe {
            jazz_native_relay_host_lease_execute_foreground(
                self.lease,
                foreground,
                request.as_ptr(),
                request.len(),
                &mut output,
            )
        };
        assert_eq!(status, JazzNativeRelayStatus::Ok, "{command:?}");
        let response =
            postcard::from_bytes(unsafe { std::slice::from_raw_parts(output.data, output.len) })
                .unwrap();
        unsafe { jazz_native_relay_bytes_free(&mut output) };
        response
    }

    fn tick(&self, foreground: u64) {
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_tick_attached_foreground(self.lease, foreground)
            },
            JazzNativeRelayStatus::Ok
        );
    }

    fn insert(&self, foreground: u64, row: u8, title: &str) {
        let ForegroundDbCommandResponse::TransactionOpened { transaction } = self.execute(
            foreground,
            ForegroundDbCommandRequest::BeginTransaction {
                kind: ForegroundTransactionKind::Mergeable,
            },
        ) else {
            panic!("transaction opens");
        };
        self.execute(
            foreground,
            ForegroundDbCommandRequest::Insert {
                transaction,
                table: "todos".to_owned(),
                cells: title_cells(title),
                row_id: Some([row; 16]),
            },
        );
        let ForegroundDbCommandResponse::TransactionCommitted { .. } = self.execute(
            foreground,
            ForegroundDbCommandRequest::CommitTransaction { transaction },
        ) else {
            panic!("transaction commits");
        };
    }

    fn rows(&self, foreground: u64) -> Vec<(RowUuid, String)> {
        let mut response = self.execute(
            foreground,
            ForegroundDbCommandRequest::All {
                query: postcard::to_allocvec(&Query::from("todos")).unwrap(),
                options_json: "{}".into(),
                transaction: None,
            },
        );
        for _ in 0..400 {
            match response {
                ForegroundDbCommandResponse::Rows { rows } => return titles(&rows),
                ForegroundDbCommandResponse::Pending { operation } => {
                    self.tick(foreground);
                    std::thread::sleep(Duration::from_millis(5));
                    response =
                        self.execute(foreground, ForegroundDbCommandRequest::Poll { operation });
                }
                other => panic!("All: {other:?}"),
            }
        }
        panic!("All did not settle");
    }

    /// Read until `expected` is what the foreground sees.
    fn wait_for_rows(&self, foreground: u64, expected: &[(RowUuid, String)]) {
        for _ in 0..400 {
            if self.rows(foreground) == expected {
                return;
            }
            self.tick(foreground);
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(self.rows(foreground), expected);
    }
}

impl Drop for Launch {
    fn drop(&mut self) {
        unsafe { jazz_native_relay_host_lease_free(self.lease) };
        unsafe { jazz_native_relay_host_free(self.host) };
    }
}

fn capability(output: &mut JazzNativeRelayBytes) -> [u8; 32] {
    let bytes = unsafe { std::slice::from_raw_parts(output.data, output.len) };
    let mut capability = [0; 32];
    capability.copy_from_slice(bytes);
    unsafe { jazz_native_relay_bytes_free(output) };
    capability
}

fn rows(entries: &[(u8, &str)]) -> Vec<(RowUuid, String)> {
    let mut rows: Vec<_> = entries
        .iter()
        .map(|(byte, title)| (RowUuid::from_bytes([*byte; 16]), (*title).to_owned()))
        .collect();
    rows.sort();
    rows
}

/// Launch one as `subject`, write `entries`, and wait until a second
/// foreground (so the relay's store) holds them.
fn first_launch(root: &std::path::Path, subject: &str, entries: &[(u8, &str)]) {
    let launch = Launch::new();
    let admitted = launch.admit(root, subject, &schema_source());
    let foreground = launch.open(&admitted);
    for (byte, title) in entries {
        launch.insert(foreground, *byte, title);
    }
    let other = launch.open(&admitted);
    launch.wait_for_rows(other, &rows(entries));
}

fn store_files(root: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(root)
        .unwrap()
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".sqlite"))
        .collect();
    names.sort();
    names
}

#[test]
fn a_prewarmed_launch_reuses_the_relay_and_reads_what_the_last_one_wrote() {
    let root = tempfile::tempdir().unwrap();
    first_launch(root.path(), "alice", &[(1, "one"), (2, "two")]);

    let launch = Launch::new();
    assert_eq!(launch.prewarm(root.path()), JazzNativeRelayStatus::Ok);
    let prewarmed = launch.registry_scopes();
    assert_eq!(prewarmed.len(), 1, "the record names one relay");
    assert!(prewarmed[0].1, "opened ahead of admission, so speculative");
    let foreground = launch.sign_in(root.path(), "alice", &schema_source());
    assert_eq!(
        launch.registry_scopes(),
        vec![(prewarmed[0].0.clone(), false)],
        "the admission claims the prewarmed relay instead of opening another"
    );
    launch.wait_for_rows(foreground, &rows(&[(1, "one"), (2, "two")]));
    launch.insert(foreground, 3, "three");
    launch.wait_for_rows(foreground, &rows(&[(1, "one"), (2, "two"), (3, "three")]));
}

#[test]
fn a_store_the_app_deleted_is_not_reopened_by_the_next_launch() {
    let root = tempfile::tempdir().unwrap();
    {
        let launch = Launch::new();
        let foreground = launch.sign_in(root.path(), "alice", &schema_source());
        launch.insert(foreground, 1, "one");
        assert!(matches!(
            launch.execute(foreground, ForegroundDbCommandRequest::DeleteStore),
            ForegroundDbCommandResponse::StoreDeleted { .. }
        ));
        assert_eq!(store_files(root.path()), Vec::<String>::new());
        let left: Vec<_> = std::fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .filter(|name| name.starts_with(crate::PREWARM_RECORD))
            .collect();
        assert_eq!(
            left,
            Vec::<String>::new(),
            "the deletion forgets the record"
        );
    }
    let launch = Launch::new();
    launch.prewarm(root.path());
    assert_eq!(
        store_files(root.path()),
        Vec::<String>::new(),
        "a prewarm must not recreate the store of a signed-out account"
    );
    assert_eq!(
        launch.registry_scopes(),
        Vec::new(),
        "nor hold a relay on it"
    );
}

#[test]
fn a_prewarmed_relay_of_another_account_closes_when_this_one_signs_in() {
    let root = tempfile::tempdir().unwrap();
    first_launch(root.path(), "alice", &[(1, "alice's")]);

    let launch = Launch::new();
    assert_eq!(launch.prewarm(root.path()), JazzNativeRelayStatus::Ok);
    let alice = launch.registry_scopes()[0].0.clone();
    let foreground = launch.sign_in(root.path(), "bob", &schema_source());
    let open = launch.registry_scopes();
    assert!(
        open.iter().all(|(scope, _)| *scope != alice),
        "alice's speculative relay must not outlive bob's sign-in: {open:?}"
    );
    assert_eq!(open.len(), 1);
    assert_eq!(
        launch.rows(foreground),
        Vec::new(),
        "bob sees none of alice's rows"
    );
}

/// The guess is disproved as soon as another account is admitted, before any
/// foreground opens on it.
#[test]
fn a_prewarmed_relay_of_another_account_closes_at_its_admission() {
    let root = tempfile::tempdir().unwrap();
    first_launch(root.path(), "alice", &[(1, "alice's")]);

    let launch = Launch::new();
    assert_eq!(launch.prewarm(root.path()), JazzNativeRelayStatus::Ok);
    let alice = launch.registry_scopes()[0].0.clone();
    let _bob = launch.admit(root.path(), "bob", &schema_source());
    let open = launch.registry_scopes();
    assert!(
        open.iter().all(|(scope, _)| *scope != alice),
        "alice's speculative relay outlived bob's admission: {open:?}"
    );
}

/// Revoking is not signing out (a runtime teardown revokes too), so the
/// record survives it; but a launch that then admits nothing (the app's
/// signed-out screen, a headless start) must not keep the store open: the
/// speculative relay closes after its hold.
#[test]
fn a_prewarm_nobody_admits_closes_after_its_hold() {
    let root = tempfile::tempdir().unwrap();
    {
        let launch = Launch::new();
        let admitted = launch.admit(root.path(), "alice", &schema_source());
        let foreground = launch.open(&admitted);
        launch.insert(foreground, 1, "one");
        assert_eq!(
            unsafe {
                jazz_native_relay_host_revoke_scope_capability(
                    launch.host,
                    admitted.as_ptr(),
                    admitted.len(),
                )
            },
            JazzNativeRelayStatus::Ok
        );
    }
    let launch = Launch::new();
    launch.prewarm_holding(root.path(), Duration::from_millis(200));
    assert_eq!(
        launch.registry_scopes().len(),
        1,
        "the record survived the revoke"
    );
    for _ in 0..400 {
        if launch.registry_scopes().is_empty() {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!(
        "the unclaimed prewarm outlived its hold: {:?}",
        launch.registry_scopes()
    );
}

/// A claimed prewarm is the app's relay: the hold running out later does
/// not touch it.
#[test]
fn a_claimed_prewarm_outlives_its_hold() {
    let root = tempfile::tempdir().unwrap();
    first_launch(root.path(), "alice", &[(1, "one")]);
    let launch = Launch::new();
    launch.prewarm_holding(root.path(), Duration::from_millis(100));
    let foreground = launch.sign_in(root.path(), "alice", &schema_source());
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(launch.registry_scopes().len(), 1);
    assert!(!launch.registry_scopes()[0].1);
    launch.wait_for_rows(foreground, &rows(&[(1, "one")]));
}

/// The hold running out races the admission of the same scope: whichever
/// wins, the admission opens the store (the retired relay released it before
/// the host lock let the admission through) and reads what it holds.
#[test]
fn a_hold_running_out_as_the_same_scope_is_admitted_leaves_it_open() {
    let root = tempfile::tempdir().unwrap();
    first_launch(root.path(), "alice", &[(1, "one")]);
    for hold in 0..24u64 {
        let launch = Launch::new();
        launch.prewarm_holding(root.path(), Duration::from_millis(hold));
        let foreground = launch.sign_in(root.path(), "alice", &schema_source());
        launch.wait_for_rows(foreground, &rows(&[(1, "one")]));
        let open = launch.registry_scopes();
        assert!(open.len() == 1 && !open[0].1, "hold {hold}: {open:?}");
    }
}

/// Revoking and admitting the same scope again inside one process is no
/// sign-out: the next launch still prewarms it.
#[test]
fn a_revoke_then_readmission_keeps_the_record() {
    let root = tempfile::tempdir().unwrap();
    {
        let launch = Launch::new();
        let admitted = launch.admit(root.path(), "alice", &schema_source());
        launch.insert(launch.open(&admitted), 1, "one");
        assert_eq!(
            unsafe {
                jazz_native_relay_host_revoke_scope_capability(
                    launch.host,
                    admitted.as_ptr(),
                    admitted.len(),
                )
            },
            JazzNativeRelayStatus::Ok
        );
        let again = launch.admit(root.path(), "alice", &schema_source());
        launch.wait_for_rows(launch.open(&again), &rows(&[(1, "one")]));
    }
    let launch = Launch::new();
    launch.prewarm(root.path());
    let open = launch.registry_scopes();
    assert!(open.len() == 1 && open[0].1, "prewarmed: {open:?}");
    launch.wait_for_rows(
        launch.sign_in(root.path(), "alice", &schema_source()),
        &rows(&[(1, "one")]),
    );
}

/// The store went away without `delete_scope_store` (a crash between its
/// file removals and the record's, app data cleared by hand): the record
/// alone must not bring it back.
#[test]
fn a_record_whose_store_is_gone_opens_nothing_and_is_forgotten() {
    let root = tempfile::tempdir().unwrap();
    first_launch(root.path(), "alice", &[(1, "one")]);
    for name in store_files(root.path()) {
        std::fs::remove_file(root.path().join(name)).unwrap();
    }
    let launch = Launch::new();
    launch.prewarm(root.path());
    assert_eq!(
        store_files(root.path()),
        Vec::<String>::new(),
        "no store recreated"
    );
    assert_eq!(launch.registry_scopes(), Vec::new(), "no relay held");
    assert!(
        !root.path().join(crate::PREWARM_RECORD).exists(),
        "the record is forgotten"
    );
}

/// The provider runs once per process, but a second prewarm (a second host,
/// a retry) must not open the store twice.
#[test]
fn a_second_prewarm_opens_nothing_more() {
    let root = tempfile::tempdir().unwrap();
    first_launch(root.path(), "alice", &[(1, "one")]);
    let launch = Launch::new();
    assert_eq!(launch.prewarm(root.path()), JazzNativeRelayStatus::Ok);
    let first = launch.registry_scopes();
    let identity = launch.speculative_identity(&first[0].0);
    assert_eq!(launch.prewarm(root.path()), JazzNativeRelayStatus::Ok);
    assert_eq!(launch.registry_scopes(), first);
    assert_eq!(
        launch.speculative_identity(&first[0].0),
        identity,
        "the first prewarm's relay is still the one open"
    );
    launch.wait_for_rows(
        launch.sign_in(root.path(), "alice", &schema_source()),
        &rows(&[(1, "one")]),
    );
}

/// A hold retires only the relay its own prewarm opened: once that one was
/// closed and a later prewarm opened the scope again, the first hold running
/// out leaves the second relay alone.
#[test]
fn a_hold_retires_only_its_own_prewarm() {
    let root = tempfile::tempdir().unwrap();
    first_launch(root.path(), "alice", &[(1, "one")]);
    let launch = Launch::new();
    launch.prewarm_holding(root.path(), Duration::from_millis(150));
    let scope = launch.registry_scopes()[0].0.clone();
    launch.close(&scope);
    launch.prewarm_holding(root.path(), Duration::from_secs(30));
    std::thread::sleep(Duration::from_millis(500));
    let open = launch.registry_scopes();
    assert!(
        open.len() == 1 && open[0].1,
        "the second prewarm outlived the first one's hold: {open:?}"
    );
}

/// The hold names its relay without owning it: a host freed while the hold
/// still runs (a reload inside the first 30 s) closes the prewarmed store at
/// once, so the next host opening it is its only owner.
#[test]
fn a_freed_host_closes_its_prewarm_before_the_hold_runs_out() {
    let root = tempfile::tempdir().unwrap();
    first_launch(root.path(), "alice", &[(1, "one")]);
    let launch = Launch::new();
    launch.prewarm_holding(root.path(), Duration::from_secs(30));
    let live = || {
        relay::LIVE_OWNERS
            .lock()
            .unwrap()
            .iter()
            .filter(|path| path.starts_with(root.path()))
            .count()
    };
    assert_eq!(live(), 1, "the prewarm holds the store");
    drop(launch);
    assert_eq!(
        live(),
        0,
        "the freed host's prewarmed relay still holds the store"
    );
    let launch = Launch::new();
    launch.wait_for_rows(
        launch.sign_in(root.path(), "alice", &schema_source()),
        &rows(&[(1, "one")]),
    );
}

/// A prewarm that lands after JavaScript already admitted a scope is late:
/// it opens nothing.
#[test]
fn a_prewarm_after_an_admission_opens_nothing() {
    let root = tempfile::tempdir().unwrap();
    first_launch(root.path(), "alice", &[(1, "one")]);

    let launch = Launch::new();
    let bob = launch.admit(root.path(), "bob", &schema_source());
    // The record still names alice: it is rewritten only when bob's first
    // foreground opens.
    launch.prewarm(root.path());
    let foreground = launch.open(&bob);
    let open = launch.registry_scopes();
    assert_eq!(open.len(), 1, "only bob's relay: {open:?}");
    assert!(!open[0].1, "and it is not speculative");
    assert_eq!(launch.rows(foreground), Vec::new());
}

#[test]
fn a_damaged_record_leaves_the_launch_to_open_as_usual() {
    let root = tempfile::tempdir().unwrap();
    first_launch(root.path(), "alice", &[(1, "one")]);
    std::fs::write(root.path().join(PREWARM_RECORD), b"{ not a record").unwrap();

    let launch = Launch::new();
    launch.prewarm(root.path());
    assert_eq!(launch.registry_scopes(), Vec::new());
    let foreground = launch.sign_in(root.path(), "alice", &schema_source());
    launch.wait_for_rows(foreground, &rows(&[(1, "one")]));
}

// ---- the differential -----------------------------------------------------------------

/// `todos` with one text column `column`, writable by anyone.
fn todos_with(column: &str) -> jazz::tools::Schema {
    let allow = PolicyExpr::True;
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("todos")
                .column(column, ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(allow.clone())
                        .with_insert(allow.clone())
                        .with_update(Some(allow.clone()), allow.clone())
                        .with_delete(allow),
                ),
        )
        .build()
}

/// The app's schema source at `version`: 1 is `todos(label)` alone; 2
/// renamed `label` to `title` and ships the migration from 1.
fn app_source(version: u8) -> String {
    let (v1, v2) = (todos_with("label"), todos_with("title"));
    if version == 1 {
        return serde_json::to_string(&v1).unwrap();
    }
    let hash = |schema: &jazz::tools::Schema| jazz::tools::SchemaHash::compute(schema).to_string();
    let mut app = serde_json::to_value(&v2).unwrap();
    app["migrations"] = serde_json::json!({
        "schemas": [
            {"hash": hash(&v1), "schema": v1},
            {"hash": hash(&v2), "schema": v2},
        ],
        "migrations": [{
            "fromHash": hash(&v1),
            "toHash": hash(&v2),
            "forward": [{
                "table": "todos",
                "operations": [{"type": "rename", "column": "label", "value": "title"}],
            }],
        }],
    });
    app.to_string()
}

fn cells(column: &str, text: &str) -> Vec<u8> {
    let descriptor = RecordDescriptor::new([(column, ValueType::String)]);
    let raw = descriptor
        .create(&[Value::String(text.to_owned())])
        .unwrap();
    jazz::binding_codec::encode_named_cells(&OwnedRecord::new(raw, descriptor)).unwrap()
}

/// The `(row, text)` pairs of a row payload, whichever version names the column.
fn texts(rows: &[u8]) -> Vec<(RowUuid, String)> {
    let mut rows: Vec<_> = decode_rows(rows)
        .unwrap()
        .into_iter()
        .flat_map(|batch| batch.rows)
        .map(|row| {
            let text = ["title", "label"]
                .iter()
                .find_map(|column| match row.cells.get(*column) {
                    Some(Value::String(text)) => Some(text.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("row cells: {:?}", row.cells));
            (row.row_id, text)
        })
        .collect();
    rows.sort();
    rows
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum World {
    /// Every foreground opened fresh, no prewarm: the semantics the
    /// shortcuts must keep.
    Reference,
    Prewarm,
}

impl World {
    fn prewarms(self) -> bool {
        self == World::Prewarm
    }
}

/// xorshift64*: the same script in every world.
struct Script(u64);

impl Script {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

impl Launch {
    fn write(&self, foreground: u64, mutation: ForegroundDbCommandRequest) {
        let ForegroundDbCommandResponse::TransactionOpened { transaction } = self.execute(
            foreground,
            ForegroundDbCommandRequest::BeginTransaction {
                kind: ForegroundTransactionKind::Mergeable,
            },
        ) else {
            panic!("transaction opens");
        };
        let mutation = match mutation {
            ForegroundDbCommandRequest::Insert {
                table,
                cells,
                row_id,
                ..
            } => ForegroundDbCommandRequest::Insert {
                transaction,
                table,
                cells,
                row_id,
            },
            ForegroundDbCommandRequest::Upsert {
                table,
                row_id,
                cells,
                ..
            } => ForegroundDbCommandRequest::Upsert {
                transaction,
                table,
                row_id,
                cells,
            },
            ForegroundDbCommandRequest::Delete { table, row_id, .. } => {
                ForegroundDbCommandRequest::Delete {
                    transaction,
                    table,
                    row_id,
                }
            }
            other => panic!("not a mutation: {other:?}"),
        };
        self.execute(foreground, mutation);
        let ForegroundDbCommandResponse::TransactionCommitted { .. } = self.execute(
            foreground,
            ForegroundDbCommandRequest::CommitTransaction { transaction },
        ) else {
            panic!("transaction commits");
        };
    }

    fn read(&self, foreground: u64) -> Vec<(RowUuid, String)> {
        let mut response = self.execute(
            foreground,
            ForegroundDbCommandRequest::All {
                query: postcard::to_allocvec(&Query::from("todos")).unwrap(),
                options_json: "{}".into(),
                transaction: None,
            },
        );
        for _ in 0..400 {
            match response {
                ForegroundDbCommandResponse::Rows { rows } => return texts(&rows),
                ForegroundDbCommandResponse::Pending { operation } => {
                    self.tick(foreground);
                    std::thread::sleep(Duration::from_millis(2));
                    response =
                        self.execute(foreground, ForegroundDbCommandRequest::Poll { operation });
                }
                other => panic!("All: {other:?}"),
            }
        }
        panic!("All did not settle");
    }

    fn pending_rows(&self, foreground: u64) -> Vec<[u8; 16]> {
        match self.execute(
            foreground,
            ForegroundDbCommandRequest::PendingUploadRows {
                table: "todos".to_owned(),
            },
        ) {
            ForegroundDbCommandResponse::PendingUploadRows { rows } => rows,
            other => panic!("PendingUploadRows: {other:?}"),
        }
    }
}

/// One world's run of the script: what each observation saw. A world other
/// than the reference waits (bounded) for each read to reach what the
/// reference saw there, since rows reach a sibling foreground through the
/// relay, not synchronously; it fails when it never does.
fn run(world: World, seed: u64, reference: Option<&[String]>) -> Vec<String> {
    let root = tempfile::tempdir().unwrap();
    let mut script = Script(seed | 1);
    let mut seen = Vec::new();
    let mut version = 1;
    let mut next_row = 0u8;
    let launches = 4 + script.below(5);
    for launch_index in 0..launches {
        if version == 1 && script.below(3) == 0 {
            version = 2;
        }
        let subject = if script.below(5) == 0 { "bob" } else { "alice" };
        let column = if version == 1 { "label" } else { "title" };
        let launch = Launch::new();
        // Some launches' JavaScript admits before the provider's prewarm runs.
        let late = script.below(5) == 0;
        if world.prewarms() && !late {
            launch.prewarm(root.path());
        }
        let admitted = launch.admit(root.path(), subject, &app_source(version));
        if world.prewarms() {
            // Whatever the guess was, the admission settled it: no other
            // scope's relay may still be speculating.
            let scopes = launch.registry_scopes();
            assert!(
                scopes
                    .iter()
                    .filter(|(_, speculative)| *speculative)
                    .count()
                    <= 1,
                "seed {seed} L{launch_index}: {scopes:?}"
            );
            if late {
                launch.prewarm(root.path());
                assert!(
                    launch
                        .registry_scopes()
                        .iter()
                        .all(|(_, speculative)| !speculative),
                    "seed {seed} L{launch_index}: a late prewarm opened a relay"
                );
            }
        }
        let foregrounds = [launch.open(&admitted), launch.open(&admitted)];
        if world.prewarms() {
            let scopes = launch.registry_scopes();
            assert!(
                scopes.len() == 1 && !scopes[0].1,
                "seed {seed} L{launch_index}: after the open {scopes:?}"
            );
        }
        let observe = |label: String, launch: &Launch, foreground: u64, seen: &mut Vec<String>| {
            let index = seen.len();
            let mut rows = launch.read(foreground);
            if let Some(reference) = reference {
                for _ in 0..500 {
                    if format!("{label} {rows:?}") == reference[index] {
                        break;
                    }
                    launch.tick(foreground);
                    std::thread::sleep(Duration::from_millis(2));
                    rows = launch.read(foreground);
                }
            }
            seen.push(format!("{label} {rows:?}"));
        };
        for (slot, foreground) in foregrounds.iter().enumerate() {
            observe(
                format!("L{launch_index} {subject} v{version} open f{slot}"),
                &launch,
                *foreground,
                &mut seen,
            );
        }
        let steps = 3 + script.below(10);
        let mut known: Vec<u8> = Vec::new();
        for step in 0..steps {
            let slot = script.below(2) as usize;
            let foreground = foregrounds[slot];
            match script.below(6) {
                0..=2 => {
                    next_row = next_row.wrapping_add(1);
                    let row = next_row;
                    known.push(row);
                    launch.write(
                        foreground,
                        ForegroundDbCommandRequest::Insert {
                            transaction: 0,
                            table: "todos".into(),
                            cells: cells(column, &format!("r{row}s{step}")),
                            row_id: Some([row; 16]),
                        },
                    );
                }
                3 if !known.is_empty() => {
                    let row = known[script.below(known.len() as u64) as usize];
                    launch.write(
                        foreground,
                        ForegroundDbCommandRequest::Upsert {
                            transaction: 0,
                            table: "todos".into(),
                            row_id: [row; 16],
                            cells: cells(column, &format!("r{row}u{step}")),
                        },
                    );
                }
                4 if !known.is_empty() => {
                    let row = known.remove(script.below(known.len() as u64) as usize);
                    launch.write(
                        foreground,
                        ForegroundDbCommandRequest::Delete {
                            transaction: 0,
                            table: "todos".into(),
                            row_id: [row; 16],
                        },
                    );
                }
                _ => {}
            }
            observe(
                format!("L{launch_index} step{step} f{slot}"),
                &launch,
                foreground,
                &mut seen,
            );
            seen.push(format!(
                "L{launch_index} step{step} pending {:?}",
                launch.pending_rows(foreground)
            ));
        }
        if script.below(6) == 0 {
            assert!(matches!(
                launch.execute(foregrounds[0], ForegroundDbCommandRequest::DeleteStore),
                ForegroundDbCommandResponse::StoreDeleted { .. }
            ));
            seen.push(format!(
                "L{launch_index} deleted {:?}",
                store_files(root.path())
            ));
        } else if script.below(4) == 0 {
            // A sign-out that keeps the store: the next launch may prewarm it.
            assert_eq!(
                unsafe {
                    jazz_native_relay_host_revoke_scope_capability(
                        launch.host,
                        admitted.as_ptr(),
                        admitted.len(),
                    )
                },
                JazzNativeRelayStatus::Ok
            );
        }
    }
    seen
}

/// Random launch histories (accounts switching, the schema upgrading, a store
/// deleted, revoked, prewarms that land late, writes, updates and deletes
/// from two foregrounds), run without and with the prewarm: every read and
/// every pending-upload answer must be the one the reference gave.
#[test]
fn random_launch_histories_read_the_same_with_and_without_the_prewarm() {
    let seeds: u64 = std::env::var("PREWARM_DIFFERENTIAL_SEEDS")
        .ok()
        .and_then(|seeds| seeds.parse().ok())
        .unwrap_or(12);
    for seed in 1..=seeds {
        let reference = run(World::Reference, seed, None);
        for world in [World::Prewarm] {
            let seen = run(world, seed, Some(&reference));
            for (index, (theirs, ours)) in reference.iter().zip(&seen).enumerate() {
                if ours != theirs {
                    for line in &reference[..=index] {
                        eprintln!("REF  {line}");
                    }
                    eprintln!("OURS {ours}");
                }
                assert_eq!(ours, theirs, "seed {seed} {world:?} observation {index}");
            }
            assert_eq!(seen.len(), reference.len(), "seed {seed} {world:?}");
        }
    }
}
