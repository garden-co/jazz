//! Deploying to a core that holds its store beside it, as a Durable Object
//! does ([`deploy`]): the body a backend publishes from its loaded files,
//! its encryption declarations kept in the store, and the account rules
//! over the registry the store holds.
//!
//! ```text
//! deploy body ──► prepare (kept declarations) ──► one transaction:
//!                    declarations + catalogue rows ──► core serves the app
//! ```
use std::collections::BTreeMap;

use jazz::account_registry::Principal;
use jazz::db::FileOptions;
use jazz::db::{InsertOptions, WriteIdentity, block_on};
use jazz::host::{HostReader, HostTables, StoreFile};
use jazz::ids::{AuthorSubject, RowUuid};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::storage::Storage;
use jazz::tools::OpenTransactionId;
use jazz::tools::deployment::artifact::{self, LoadedApp, system_tables};
use serde_json::{Value as Json, json};

use super::accounts;
use super::deployment::{CoreHost, DECLARATIONS, apply_to_core, deploy, keep_first_declared};
use crate::runtime::ServerCore;

const APP_NAME: &str = "core-deploy-tests";

/// Projects are spaces; a task's title is encrypted in its project's space.
const APP: &str = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
    "tables": {
        "projects": { "columns": { "name": "string", "ownerId": "uuid" } },
        "tasks": {
            "columns": { "projectId": "uuid", "title": "bytes", "__e2ee_eq_title": "bytes" },
            "relations": { "project": { "rel": "projects", "column": "projectId" } },
            "encryption": { "space": "projectId", "scope": "projects",
                "columns": { "title": "string" }, "equality": ["title"] }
        }
    } }"#;

const PLAIN_APP: &str = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
    "tables": { "notes": { "columns": { "title": "string" } } } }"#;

const MEMBERS_RULE: &str = "exists __e2ee_members(scopeId = table_id('projects'), identifier = this.id, accountId = session.user)";

fn permissions(tables: Json) -> String {
    json!({
        "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
        "tables": tables,
    })
    .to_string()
}

fn loaded(app: &str) -> LoadedApp {
    let permissions = permissions(json!({
        "projects": {
            "read": MEMBERS_RULE,
            "insert": "$createdBy.account = session.user",
            "update": "ownerId = session.user",
        },
        "tasks": { "read": "allowedTo.read(project)", "insert": "allowedTo.update(project)" },
    }));
    artifact::load(app.as_bytes(), permissions.as_bytes()).unwrap_or_else(|e| panic!("{e}"))
}

fn plain() -> LoadedApp {
    let permissions = permissions(json!({ "notes": { "read": "true", "insert": "true" } }));
    artifact::load(PLAIN_APP.as_bytes(), permissions.as_bytes()).unwrap_or_else(|e| panic!("{e}"))
}

/// A store in a file (WAL, as native keeps one), which each handle the host
/// takes reaches, and a registry reader of its own reads while the core
/// writes.
struct FileHost {
    dir: tempfile::TempDir,
}

impl FileHost {
    fn new() -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
        }
    }

    fn path(&self) -> std::path::PathBuf {
        self.dir.path().join("store.sqlite")
    }
}

impl CoreHost for FileHost {
    fn store(&self) -> Result<StoreFile, String> {
        StoreFile::open(&self.path(), FileOptions::default()).map_err(|error| error.to_string())
    }

    fn install_registry(&self, core: &mut ServerCore) -> Result<(), String> {
        self.store()?
            .host_sql(&mut |conn| accounts::open(conn).map_err(accounts::storage_error))
            .map_err(|error| error.to_string())?;
        let reader = HostReader::open(&self.path(), None).map_err(|error| error.to_string())?;
        assert!(core.install_account_registry(reader));
        Ok(())
    }
}

/// A Durable Object's SQL, emulated: one connection, no transaction of its
/// own beyond the callback scopes, and the registry read on that same
/// connection, from inside the core's own transaction, as the object does.
struct ObjectHost {
    sql: jazz::storage::managed_emulation::EmulatedObjectSql,
}

impl ObjectHost {
    fn new() -> Self {
        Self {
            sql: jazz::storage::managed_emulation::EmulatedObjectSql::open_in_memory().unwrap(),
        }
    }

    fn handle(&self) -> Storage {
        Storage::from_backend(self.sql.clone())
    }
}

impl CoreHost for ObjectHost {
    fn store(&self) -> Result<StoreFile, String> {
        StoreFile::from_storage(self.handle()).map_err(|error| error.to_string())
    }

    fn install_registry(&self, core: &mut ServerCore) -> Result<(), String> {
        accounts::open(&self.handle()).map_err(|error| error.to_string())?;
        assert!(core.install_account_registry(self.handle()));
        Ok(())
    }
}

/// Both stores a core is deployed to: a native file, and a Durable Object's
/// SQL.
fn hosts() -> Vec<(&'static str, Box<dyn CoreHost>)> {
    vec![
        ("native file", Box::new(FileHost::new())),
        ("durable object emulation", Box::new(ObjectHost::new())),
    ]
}

struct Host {
    host: Box<dyn CoreHost>,
    core: Option<ServerCore>,
}

impl Host {
    fn new(host: Box<dyn CoreHost>) -> Self {
        Self { host, core: None }
    }

    fn deploy(&mut self, body: &Json) -> Result<Json, (u16, Json)> {
        deploy(
            &mut self.core,
            self.host.as_ref(),
            APP_NAME,
            body.to_string().as_bytes(),
            1,
        )
        .map(|response| serde_json::to_value(response).unwrap())
        .map_err(|error| (error.status.as_u16(), error.body))
    }

    /// The declarations kept in the store for `version`.
    fn kept(&self, version: &jazz::tools::public_schema::SchemaHash) -> Option<Vec<u8>> {
        self.host
            .store()
            .unwrap()
            .host_get(DECLARATIONS, &version.0)
            .unwrap()
    }
}

/// An app that declares encryption deploys to a core with its own store:
/// the bundle its loader computed, its declarations kept for the version in
/// the table and form the native server uses, and the same deployment again
/// changes nothing.
#[test]
fn an_encrypted_app_deploys_and_keeps_its_declarations() {
    for (name, backing) in hosts() {
        let mut host = Host::new(backing);

        let app = loaded(APP);
        let response = host
            .deploy(&app.deployment_body(false))
            .unwrap_or_else(|error| panic!("{name}: {error:?}"));
        assert_eq!(response["bundle"], app.bundle().to_hex());
        assert!(response.get("warnings").is_none(), "{response}");

        assert_eq!(host.core.as_ref().unwrap().encrypts(), Ok(true));
        assert!(
            host.core.as_ref().unwrap().has_account_registry(),
            "the account rules read the store's registry"
        );
        let kept = host.kept(&app.current_hash()).expect("declarations kept");
        jazz::tools::deployment::artifact::encryption::decode_stored(&kept)
            .expect("in the stored declarations form");

        let again = host.deploy(&app.deployment_body(false)).unwrap();
        assert_eq!(again["changed"], false);
    }
}

/// The declarations of a deployed version are kept from its first
/// deployment: a later deployment that edits an encrypted column's type in
/// place, with the schema unchanged, is refused, and what it names is not
/// kept.
#[test]
fn a_later_deploy_cannot_change_a_versions_declarations() {
    for (_, backing) in hosts() {
        let mut host = Host::new(backing);

        let app = loaded(APP);
        host.deploy(&app.deployment_body(false)).unwrap();
        let kept = host.kept(&app.current_hash());

        let edited = loaded(&APP.replace(r#""title": "string""#, r#""title": "int""#));
        assert_eq!(edited.current_hash(), app.current_hash(), "the same schema");
        let (status, refusal) = host.deploy(&edited.deployment_body(false)).unwrap_err();
        assert_eq!(status, 422, "{refusal}");
        assert!(
            refusal["error"]
                .as_str()
                .unwrap()
                .contains("deployed with other encryption declarations"),
            "{refusal}"
        );
        assert_eq!(host.kept(&app.current_hash()), kept);

        // Leaving them out of a deployment of the version is refused as well.
        let mut omitted = app.deployment_body(false);
        omitted.as_object_mut().unwrap().remove("encryption");
        host.deploy(&omitted).unwrap_err();
    }
}

/// An app without encryption deploys as before: no declarations kept, and
/// its body is read as a plain deploy request.
#[test]
fn an_app_without_encryption_deploys_as_before() {
    for (_, backing) in hosts() {
        let mut host = Host::new(backing);

        let app = plain();
        let response = host.deploy(&app.deployment_body(false)).unwrap();
        assert_eq!(response["bundle"], app.bundle().to_hex());
        assert_eq!(host.kept(&app.current_hash()), None);
        assert_eq!(host.core.as_ref().unwrap().encrypts(), Ok(false));
        assert!(
            host.core
                .as_ref()
                .unwrap()
                .db()
                .host_prefix(DECLARATIONS, &[])
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            host.deploy(&app.deployment_body(false)).unwrap()["changed"],
            false
        );
    }
}

/// The account rules read the registry the store holds: an account the app
/// founded for an external login may not upload a root of the other kind.
///
/// ```text
/// registry: alice's account founded by an external login (app)
/// alice's account, from a device-account session:
///   device request ──► ✓
///   identity + root "device-account" ──► ✗ (founded by the app)
///   identity + root "app"         ──► ✓
/// ```
#[test]
fn an_account_root_of_the_wrong_kind_is_refused() {
    // A Durable Object core accepts only finished transactions, which this
    // test cannot author on it (see the founders view test below, which
    // reads on the object's connection).
    for (_, backing) in hosts().into_iter().take(1) {
        let mut host = Host::new(backing);
        // A core whose own schema carries the app's policies, as a session
        // pinned to the deployed bundle is governed by them.
        let app = loaded(APP);

        let mut tables = system_tables::with_e2ee_fragment(app.current_schema());
        let entries = app
            .encryption()
            .entries(&app.current_hash().to_string())
            .expect("the app declares encryption");
        for (table, policies) in
            system_tables::derive_e2ee_policies(&tables, entries, &Default::default()).unwrap()
        {
            tables.get_mut(&table).unwrap().policies = policies;
        }
        let mut core = ServerCore::open(
            JazzSchema::new(&tables).unwrap(),
            host.host.store().unwrap(),
            false,
            Some(APP_NAME.to_owned()),
        )
        .unwrap();
        host.host.install_registry(&mut core).unwrap();
        apply_to_core(
            &mut core,
            app.deployment_body(false).to_string().as_bytes(),
            1,
        )
        .unwrap_or_else(|error| panic!("{}", error.body));
        host.core = Some(core);
        let alice = {
            let mut account = None;
            host.host
                .store()
                .unwrap()
                .host_sql(&mut |conn| {
                    account = Some(
                        accounts::login_or_register(
                            conn,
                            &Principal {
                                issuer: "https://issuer.example".into(),
                                subject: "alice".into(),
                            },
                            || jazz::account_registry::AccountId(uuid::Uuid::new_v4()),
                        )
                        .map_err(accounts::storage_error)?
                        .assignment
                        .account,
                    );
                    Ok(())
                })
                .unwrap();
            account.unwrap()
        };
        let db = host.core.as_ref().unwrap().db();

        let subject = uuid::Uuid::from_bytes([0x1f; 16]).to_string();
        let session = AuthorSubject::reserved(jazz::identity::DEVICE_ACCOUNT_ISSUER, &subject)
            .unwrap()
            .with_account(alice);
        let bytes = |text: &str| Value::Bytes(text.as_bytes().to_vec());
        let device = RowUuid(uuid::Uuid::new_v4());
        let epoch = uuid::Uuid::new_v4();
        let commit = |writes: Vec<(&'static str, RowUuid, BTreeMap<String, Value>)>| {
            let id = OpenTransactionId::new();
            block_on(db.begin_mergeable(id)).map_err(|error| error.message)?;
            let tx = db.tx_ref(id);
            for (table, row, cells) in writes {
                let staged = block_on(tx.insert(
                    table,
                    cells,
                    InsertOptions {
                        row_id: Some(row),
                        identity: WriteIdentity::Session(session),
                        ..Default::default()
                    },
                ));
                if let Err(error) = staged {
                    let _ = db.abandon_transaction_handle(id);
                    return Err(error.message);
                }
            }
            block_on(db.commit_mergeable_handle(id))
                .map(drop)
                .map_err(|error| error.message)
        };
        let request = BTreeMap::from([
            ("publicKey".to_owned(), bytes("pk")),
            ("mechanism".to_owned(), Value::String("m".to_owned())),
            ("version".to_owned(), Value::I64(1)),
            ("signingPublicKey".to_owned(), bytes("spk")),
            ("signingMechanism".to_owned(), Value::String("s".to_owned())),
            ("signingVersion".to_owned(), Value::I64(1)),
            ("nonceCommitment".to_owned(), bytes("n")),
        ]);
        let identity_and_root = |kind: &'static str| {
            vec![
                (
                    system_tables::ACCOUNT_IDENTITIES,
                    RowUuid(alice.0),
                    BTreeMap::from([
                        ("ledgerVersion".to_owned(), Value::I64(1)),
                        ("generation".to_owned(), Value::I64(0)),
                        ("deviceId".to_owned(), Value::Uuid(device.0)),
                        ("epochId".to_owned(), Value::Uuid(epoch)),
                        ("envelope".to_owned(), bytes("e")),
                        ("verification".to_owned(), bytes("v")),
                    ]),
                ),
                (
                    system_tables::ACCOUNT_ROOTS,
                    RowUuid(uuid::Uuid::new_v4()),
                    BTreeMap::from([
                        ("ledgerVersion".to_owned(), Value::I64(1)),
                        ("generation".to_owned(), Value::I64(0)),
                        ("accountId".to_owned(), Value::Uuid(alice.0)),
                        ("deviceId".to_owned(), Value::Uuid(device.0)),
                        ("epochId".to_owned(), Value::Uuid(epoch)),
                        ("kind".to_owned(), Value::String(kind.to_owned())),
                    ]),
                ),
            ]
        };

        commit(vec![(system_tables::DEVICE_REQUESTS, device, request)])
            .unwrap_or_else(|error| panic!("the device request: {error}"));
        // Without the registry the session's issuer would decide, and a
        // device-account session's root would have to say
        // "device-account".
        commit(identity_and_root("device-account"))
            .expect_err("the registry says the app founded the account");
        commit(identity_and_root("app"))
            .unwrap_or_else(|error| panic!("the root the registry names: {error}"));
    }
}

/// On a Durable Object's single connection the founders view reads the
/// registry from inside the scope the core has open, and sees what that
/// scope and earlier ones wrote.
#[test]
fn the_founders_view_reads_on_the_objects_connection_inside_a_scope() {
    use jazz::db::AccountRegistryView;
    let object = ObjectHost::new();
    let app = *accounts::device_account_founding_app(APP_NAME).uuid();
    let registry = object.handle();
    accounts::open(&registry).unwrap();
    let view = super::accounts::RegistryFounders::new(object.handle(), app);
    let register = |subject: &str| {
        accounts::login_or_register(
            &registry,
            &Principal {
                issuer: "https://issuer.example".into(),
                subject: subject.into(),
            },
            || jazz::account_registry::AccountId(uuid::Uuid::new_v4()),
        )
        .unwrap()
        .assignment
        .account
    };
    let alice = register("alice");
    let stranger = jazz::account_registry::AccountId(uuid::Uuid::new_v4());
    assert_eq!(view.founded_by_device_account(stranger), Ok(None));
    registry
        .scope("core scope", |_| {
            // Founded by an external login: not by a device-account identity.
            assert_eq!(view.founded_by_device_account(alice), Ok(Some(false)));
            // A device account founded inside the scope is seen at once.
            let device = accounts::execute(
                &registry,
                &jazz::account_registry::AccountCommand::FoundDeviceAccount {
                    principal: Principal {
                        issuer: jazz::identity::DEVICE_ACCOUNT_ISSUER.into(),
                        subject: uuid::Uuid::from_bytes([3; 16]).to_string(),
                    },
                    app,
                },
            )
            .map_err(accounts::storage_error)?;
            let device = accounts::assignment_of(device).account;
            assert_eq!(view.founded_by_device_account(device), Ok(Some(true)));
            // The check core runs inside the write passes the store's own
            // connection, which is the one this scope is open on.
            assert_eq!(
                view.founded_by_device_account_on(&registry, device),
                Ok(Some(true))
            );
            assert_eq!(
                view.founded_by_device_account_on(&registry, alice),
                Ok(Some(false))
            );
            assert_eq!(
                view.founded_for_another_audience_on(&registry, device),
                Ok(false)
            );
            Ok(())
        })
        .unwrap();
}

/// A host whose registry install leaves the core without one.
struct NoRegistryHost(FileHost);

impl CoreHost for NoRegistryHost {
    fn store(&self) -> Result<StoreFile, String> {
        self.0.store()
    }

    fn install_registry(&self, _: &mut ServerCore) -> Result<(), String> {
        Ok(())
    }
}

/// A core that serves an encrypting app without the account registry its
/// checks read is refused, as native refuses it: at open (a host whose
/// install left no registry), and at deploy (a core opened without one).
/// An app without encryption does not need it.
#[test]
fn an_encrypting_app_is_not_served_without_the_account_registry() {
    // At open: the deploy opens the core, and the install left no registry.
    let mut host = Host::new(Box::new(NoRegistryHost(FileHost::new())));
    let error = host
        .deploy(&loaded(APP).deployment_body(false))
        .unwrap_err();
    assert_eq!(error.0, 500, "{error:?}");
    assert!(
        error.1["error"]
            .as_str()
            .unwrap()
            .contains("account registry"),
        "{error:?}"
    );
    assert!(host.core.is_none(), "no core is kept without its registry");
    // Without encryption such a host deploys.
    let mut host = Host::new(Box::new(NoRegistryHost(FileHost::new())));
    host.deploy(&plain().deployment_body(false)).unwrap();

    // At deploy: a core opened for no app has no registry to install.
    let files = FileHost::new();
    let app = loaded(APP);
    let mut core = ServerCore::open(
        JazzSchema::new(app.current_schema()).unwrap(),
        files.store().unwrap(),
        false,
        None,
    )
    .unwrap();
    assert!(!core.has_account_registry());
    let refused = apply_to_core(
        &mut core,
        app.deployment_body(false).to_string().as_bytes(),
        1,
    )
    .unwrap_err();
    assert_eq!(refused.status.as_u16(), 422, "{:?}", refused.body);
    assert!(
        refused.body["error"]
            .as_str()
            .unwrap()
            .contains("needs the account registry"),
        "{:?}",
        refused.body
    );
    assert_eq!(
        files
            .store()
            .unwrap()
            .host_get(DECLARATIONS, &app.current_hash().0)
            .unwrap(),
        None,
        "nothing was kept"
    );
}

/// The deployed body's v1 schema (as JSON), its hash, and a body for a second
/// version `schema` that follows it through `forward` lens tables, declaring
/// what the first declared.
fn follow_up(
    v1: &Json,
    schema: Json,
    forward: Json,
) -> (Json, jazz::tools::public_schema::SchemaHash) {
    let source: jazz::tools::public_schema::Schema =
        serde_json::from_value(schema.clone()).unwrap();
    let hash = jazz::tools::public_schema::SchemaHash::compute(&source);
    let from = v1["targetSchemaHash"].as_str().unwrap().to_owned();
    let mut body = v1.clone();
    body["targetSchemaHash"] = json!(hash.to_string());
    body["schemas"]
        .as_array_mut()
        .unwrap()
        .push(json!({ "hash": hash.to_string(), "schema": schema }));
    body["migrations"] =
        json!([{ "fromHash": from, "toHash": hash.to_string(), "forward": forward }]);
    let declared = body["encryption"]["versions"][&from].clone();
    body["encryption"]["versions"][hash.to_string()] = declared;
    (body, hash)
}

/// The deploy's rules for Jazz's system tables hold on a Durable Object
/// core, which shares them through `prepare`: a version that follows one
/// with the system tables may not drop them, and no lens may reshape one.
/// Neither deploy is applied or kept.
#[test]
fn the_system_table_rules_hold_on_the_durable_object_host() {
    let first = loaded(APP).deployment_body(false);
    let v1 = first["schemas"][0]["schema"].clone();
    let is_system =
        |name: &str| jazz::tools::deployment::artifact::system_tables::is_system_table(name);
    for (name, backing) in hosts() {
        let mut host = Host::new(backing);
        host.deploy(&first)
            .unwrap_or_else(|error| panic!("{name}: {error:?}"));
        let before = host.core.as_ref().unwrap().db().catalogue().unwrap();

        // A later version without the system tables.
        let mut without = v1.clone();
        let system: Vec<String> = without["tables"]
            .as_object()
            .unwrap()
            .keys()
            .filter(|table| is_system(table))
            .cloned()
            .collect();
        assert!(!system.is_empty(), "{name}: the first version has them");
        for table in &system {
            without["tables"].as_object_mut().unwrap().remove(table);
        }
        let removed: Vec<Json> = system
            .iter()
            .map(|table| json!({ "table": table, "removed": true, "operations": [] }))
            .collect();
        let (dropped, _) = follow_up(&first, without, Json::Array(removed));
        let (status, refusal) = host.deploy(&dropped).unwrap_err();
        assert_eq!(status, 422, "{name}: {refusal}");
        assert!(
            refusal["error"]
                .as_str()
                .unwrap()
                .contains("the target lacks them"),
            "{name}: {refusal}"
        );

        // A lens that reshapes a system table, beside a plain change to the
        // app's own table that gives the version its own hash.
        let mut reshaped = v1.clone();
        let tasks = reshaped["tables"]["tasks"]["columns"]
            .as_array_mut()
            .unwrap();
        tasks.push(json!({ "name": "note", "nullable": false, "column_type": { "type": "Text" } }));
        let members = jazz::tools::deployment::artifact::system_tables::MEMBERS;
        let text = || json!({ "type": "Text", "value": "" });
        let (reshaping, _) = follow_up(
            &first,
            reshaped,
            json!([
                { "table": "tasks", "operations": [
                    { "type": "introduce", "column": "note", "column_type": { "type": "Text" }, "value": text() }
                ] },
                { "table": members, "operations": [
                    { "type": "introduce", "column": "extra", "column_type": { "type": "Text" }, "value": text() }
                ] }
            ]),
        );
        let (status, refusal) = host.deploy(&reshaping).unwrap_err();
        assert_eq!(status, 422, "{name}: {refusal}");
        assert!(
            refusal["error"]
                .as_str()
                .unwrap()
                .contains("may not change"),
            "{name}: {refusal}"
        );
        assert_eq!(
            host.core.as_ref().unwrap().db().catalogue().unwrap(),
            before,
            "{name}: nothing was applied"
        );
    }
}

/// A registry that cannot be read refuses the check, never reads as "an
/// account nobody founded", which would let the session's issuer decide: a
/// store whose registry tables were never made, read on the object's
/// connection.
#[test]
fn an_unreadable_registry_refuses_the_account_checks() {
    use jazz::db::AccountRegistryView;
    let object = ObjectHost::new();
    let app = *accounts::device_account_founding_app(APP_NAME).uuid();
    let view = super::accounts::RegistryFounders::new(object.handle(), app);
    let account = jazz::account_registry::AccountId(uuid::Uuid::new_v4());
    let handle = object.handle();
    for result in [
        view.founded_by_device_account(account).map(drop),
        view.founded_by_device_account_on(&handle, account)
            .map(drop),
        view.founded_for_another_audience(account).map(drop),
        view.founded_for_another_audience_on(&handle, account)
            .map(drop),
    ] {
        let error = result.expect_err("the tables do not exist");
        assert!(
            error.contains("the account registry cannot be read"),
            "{error}"
        );
    }
}

/// A deployment that changes no catalogue row still keeps the declarations
/// of a version it names for the first time (one in the catalogue whose
/// declarations the store does not keep, as a store from before they were
/// kept), both on the open core and on a host that opens none for it.
#[test]
fn a_deploy_that_changes_nothing_keeps_first_time_declarations() {
    for (name, backing) in hosts() {
        let mut host = Host::new(backing);
        let app = loaded(APP);
        host.deploy(&app.deployment_body(false)).unwrap();
        let kept = host.kept(&app.current_hash()).expect("kept on deploy");

        let forget = |host: &Host| {
            host.host
                .store()
                .unwrap()
                .host_delete(DECLARATIONS, &app.current_hash().0)
                .unwrap();
            assert_eq!(host.kept(&app.current_hash()), None);
        };

        // The open core.
        forget(&host);
        let again = host.deploy(&app.deployment_body(false)).unwrap();
        assert_eq!(again["changed"], false, "{name}");
        assert_eq!(host.kept(&app.current_hash()), Some(kept.clone()), "{name}");

        // No core open: the store is read and kept on directly.
        forget(&host);
        host.core.take().unwrap().close();
        let again = host.deploy(&app.deployment_body(false)).unwrap();
        assert_eq!(again["changed"], false, "{name}");
        assert!(host.core.is_none(), "{name}: nothing to open a core for");
        assert_eq!(host.kept(&app.current_hash()), Some(kept.clone()), "{name}");
    }
}

/// A deployment that changes no catalogue row refuses declarations that
/// differ from the ones its version keeps (written since the deployment
/// read them), as the transaction of one that does change it refuses
/// them; the kept ones stay. The same ones, or none kept yet, are kept.
#[test]
fn keeping_first_time_declarations_refuses_different_kept_ones() {
    for (name, backing) in hosts() {
        let store = backing.store().unwrap();
        let version = loaded(APP).current_hash();
        let put = |key: &[u8], value: &[u8]| {
            store
                .host_put_if_absent(DECLARATIONS, key, value)
                .map_err(|error| error.to_string())
        };
        let first = vec![(version, b"first".to_vec())];

        keep_first_declared(&first, put).unwrap_or_else(|error| panic!("{name}: {error:?}"));
        keep_first_declared(&first, put).unwrap_or_else(|error| panic!("{name}: {error:?}"));

        let other = vec![(version, b"other".to_vec())];
        let refusal = keep_first_declared(&other, put).unwrap_err();
        assert_eq!(refusal.status.as_u16(), 422, "{name}: {:?}", refusal.body);
        assert!(
            refusal.body["error"]
                .as_str()
                .unwrap()
                .contains("deployed with other encryption declarations"),
            "{name}: {:?}",
            refusal.body
        );
        assert_eq!(
            store.host_get(DECLARATIONS, &version.0).unwrap(),
            Some(b"first".to_vec()),
            "{name}: the kept ones stay"
        );
    }
}

/// Decrypting a column puts its plaintext on the server, so a deploy to a
/// core beside its store (a Durable Object's) asks for the deployer's
/// confirmation naming it, as native does: refused without it (and nothing
/// opened or kept), deployed with it.
#[test]
fn a_decrypt_needs_the_deployers_confirmation_on_a_core_host() {
    let decrypting = APP
        .replace(
            r#""__e2ee_eq_title": "bytes" }"#,
            r#""__e2ee_eq_title": "bytes", "notes": "bytes", "notesText": "string?" }"#,
        )
        .replace(
            r#""columns": { "title": "string" }"#,
            r#""columns": { "title": "string", "notes": { "type": "string", "to": "notesText" } }"#,
        );
    assert_ne!(decrypting, APP);
    for (name, backing) in hosts() {
        let mut host = Host::new(backing);
        let app = loaded(&decrypting);

        for confirmed in [json!([]), json!(["tasks.title"])] {
            let mut body = app.deployment_body(false);
            body["confirmDecrypt"] = confirmed.clone();
            let (status, refusal) = host.deploy(&body).unwrap_err();
            assert_eq!(status, 422, "{name} {confirmed}: {refusal}");
            assert!(
                refusal["error"]
                    .as_str()
                    .unwrap()
                    .contains("confirmDecrypt: [\"tasks.notes\"]"),
                "{name}: {refusal}"
            );
            assert!(host.core.is_none(), "{name}: nothing opened");
            assert_eq!(host.kept(&app.current_hash()), None, "{name}");
        }

        let mut body = app.deployment_body(false);
        body["confirmDecrypt"] = json!(["tasks.title", "tasks.notes"]);
        let response = host
            .deploy(&body)
            .unwrap_or_else(|error| panic!("{name}: {error:?}"));
        assert_eq!(response["bundle"], app.bundle().to_hex(), "{name}");
        assert!(host.kept(&app.current_hash()).is_some(), "{name}");

        // Confirmed once, the version's decrypt is known.
        let again = host.deploy(&app.deployment_body(false)).unwrap();
        assert_eq!(again["changed"], false, "{name}");
    }
}
