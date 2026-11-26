//! The foreground store image: a relay's first fresh foreground store is
//! saved beside the relay's file, and later foregrounds (in this process and
//! the next launches) open from it instead of replaying the genesis DDL and
//! the migrations. It is only a shortcut: every launch must read and write
//! exactly as it does when each foreground opens fresh.
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

    /// Session setup as jazz-rn runs it for a signed-in `subject`, then one
    /// foreground on the admitted scope.
    fn sign_in(&self, root: &std::path::Path, subject: &str, schema_json: &str) -> u64 {
        let admitted = self.admit(root, subject, schema_json);
        self.open(&admitted)
    }

    fn admit(&self, root: &std::path::Path, subject: &str, schema_json: &str) -> [u8; 32] {
        let setup = serde_json::to_vec(&serde_json::json!({
            "server_url": "https://upstream.example",
            "app_id": "image-app",
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

fn images(root: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(root)
        .unwrap()
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with("foreground-"))
        .collect();
    names.sort();
    names
}

#[test]
fn the_first_foreground_leaves_an_image_the_next_launch_opens_from() {
    let root = tempfile::tempdir().unwrap();
    first_launch(root.path(), "alice", &[(1, "one")]);
    let saved = images(root.path());
    assert_eq!(saved.len(), 1, "one image for the scope: {saved:?}");
    assert!(saved[0].ends_with(".image"));
    let written = std::fs::metadata(root.path().join(&saved[0]))
        .unwrap()
        .modified()
        .unwrap();

    let launch = Launch::new();
    let foreground = launch.sign_in(root.path(), "alice", &schema_source());
    launch.wait_for_rows(foreground, &rows(&[(1, "one")]));
    launch.insert(foreground, 2, "two");
    launch.wait_for_rows(foreground, &rows(&[(1, "one"), (2, "two")]));
    assert_eq!(
        images(root.path()),
        saved,
        "the same image, not another one"
    );
    assert_eq!(
        std::fs::metadata(root.path().join(&saved[0]))
            .unwrap()
            .modified()
            .unwrap(),
        written,
        "opened from, not written again"
    );
}

#[test]
fn a_damaged_image_is_replaced_by_a_fresh_open() {
    let root = tempfile::tempdir().unwrap();
    first_launch(root.path(), "alice", &[(1, "one")]);
    let saved = images(root.path());
    let path = root.path().join(&saved[0]);
    let mut bytes = std::fs::read(&path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();

    let launch = Launch::new();
    let foreground = launch.sign_in(root.path(), "alice", &schema_source());
    launch.wait_for_rows(foreground, &rows(&[(1, "one")]));
    assert_eq!(images(root.path()), saved);
    assert!(
        relay::foreground_image_body(&std::fs::read(&path).unwrap()).is_some(),
        "the fresh open wrote a sound image back"
    );
}

#[test]
fn another_schema_replaces_the_scopes_image() {
    let root = tempfile::tempdir().unwrap();
    {
        let launch = Launch::new();
        let admitted = launch.admit(root.path(), "alice", &app_source(1));
        launch.open(&admitted);
    }
    let before = images(root.path());
    assert_eq!(before.len(), 1);
    {
        let launch = Launch::new();
        let admitted = launch.admit(root.path(), "alice", &app_source(2));
        launch.open(&admitted);
    }
    let after = images(root.path());
    assert_eq!(
        after.len(),
        1,
        "the older schema's image is gone: {after:?}"
    );
    assert_ne!(after, before);
}

#[test]
fn deleting_the_store_deletes_its_images() {
    let root = tempfile::tempdir().unwrap();
    let launch = Launch::new();
    let foreground = launch.sign_in(root.path(), "alice", &schema_source());
    launch.insert(foreground, 1, "one");
    assert_eq!(images(root.path()).len(), 1);
    assert!(matches!(
        launch.execute(foreground, ForegroundDbCommandRequest::DeleteStore),
        ForegroundDbCommandResponse::StoreDeleted { .. }
    ));
    assert_eq!(store_files(root.path()), Vec::<String>::new());
    assert_eq!(images(root.path()), Vec::<String>::new());
}

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
    /// Every foreground opened fresh: the semantics the
    /// shortcuts must keep.
    Reference,
    Image,
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
    if world == World::Reference {
        relay::IMAGES_OFF_IN
            .lock()
            .unwrap()
            .push(root.path().to_path_buf());
    }
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
        let admitted = launch.admit(root.path(), subject, &app_source(version));
        let foregrounds = [launch.open(&admitted), launch.open(&admitted)];
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
            // A sign-out that keeps the store and its image.
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
    relay::IMAGES_OFF_IN
        .lock()
        .unwrap()
        .retain(|dir| dir != root.path());
    seen
}

/// Random launch histories (accounts switching, the schema upgrading, a store
/// deleted, writes, updates and deletes from two foregrounds), run with every
/// foreground opened fresh and with images: every read and every
/// pending-upload answer must be the one the reference gave.
#[test]
fn random_launch_histories_read_the_same_with_and_without_images() {
    let seeds: u64 = std::env::var("IMAGE_DIFFERENTIAL_SEEDS")
        .ok()
        .and_then(|seeds| seeds.parse().ok())
        .unwrap_or(12);
    for seed in 1..=seeds {
        let reference = run(World::Reference, seed, None);
        for world in [World::Image] {
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
