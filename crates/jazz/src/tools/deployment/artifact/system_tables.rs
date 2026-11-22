//! Jazz's E2EE system tables (E2EE protocol §1.2–§1.4): catalogue tables
//! whose definitions and policies come from Jazz, not from the app.
//!
//! Their names carry the reserved prefix [`SYSTEM_TABLE_PREFIX`]. An app
//! that declares encryption gets them in every schema version that
//! declares it: the loaders append [`e2ee_system_tables_v1`] before hashing,
//! so a client, a backend and the server compute one schema and one hash.
//! Their policies are never written by the app:
//! [`derive_e2ee_policies`] works them out from the app's encryption
//! declarations, the same way in the loaders that compute a bundle id and
//! on the server, so both reach the same permissions bundle.

use std::collections::{BTreeMap, BTreeSet};

use crate::catalogue::Permissions;
use crate::tools::public_schema::{
    CmpOp, ColumnDescriptor, ColumnType, EncryptionEntry, EncryptionSettings, HistoryRetention,
    Operation, OperationPolicy, PolicyExpr, PolicyValue, RowDescriptor, Schema, TableName,
    TablePolicies, TableSchema, Value,
};

pub use crate::schema::{SYSTEM_TABLE_PREFIX, is_system_table};

/// `accountId` is a member of the space `(scopeId, identifier)`.
pub const MEMBERS: &str = crate::schema::MEMBERS_TABLE;
/// A space root candidate: a space's first key version.
pub const SPACES: &str = "__e2ee_spaces";
/// A space rotation: the next key version.
pub const SPACE_SUCCESSORS: &str = "__e2ee_space_successors";
/// A key version sealed to a member's account.
pub const SPACE_DELIVERIES: &str = "__e2ee_space_deliveries";

pub const DEVICE_REQUESTS: &str = "__e2ee_device_requests";
pub const DEVICE_KEYS: &str = "__e2ee_device_keys";
pub const ACCOUNT_IDENTITIES: &str = crate::schema::ACCOUNT_IDENTITIES_TABLE;
pub const ACCOUNT_ROOTS: &str = crate::schema::ACCOUNT_ROOTS_TABLE;
pub const DEVICE_CHALLENGES: &str = "__e2ee_device_challenges";
pub const DEVICE_PROOFS: &str = "__e2ee_device_proofs";
pub const DEVICE_APPROVALS: &str = "__e2ee_device_approvals";
pub const DEVICE_DELIVERIES: &str = "__e2ee_device_deliveries";
pub const PUBLIC_DEVICE_APPROVALS: &str = "__e2ee_public_device_approvals";
pub const ACCOUNT_SUCCESSORS: &str = "__e2ee_account_successors";
pub const PUBLIC_ACCOUNT_SUCCESSORS: &str = "__e2ee_public_account_successors";
pub const RECOVERY_ROOTS: &str = "__e2ee_recovery_roots";
pub const RECOVERY_DELIVERIES: &str = "__e2ee_recovery_deliveries";
pub const DEVICE_REMOVAL_REQUESTS: &str = "__e2ee_device_removal_requests";
pub const ACCOUNT_KEYS: &str = "__e2ee_account_keys";

/// The only system table an app touches directly: its generated code reads
/// it and inserts and deletes its rows, and an app policy may test it with
/// `exists` (protocol §1.3, R13).
pub fn is_members_table(name: &str) -> bool {
    name == MEMBERS
}

#[derive(Clone, Copy)]
enum Kind {
    Id,
    Text,
    Bytes,
    /// An unsigned 32- or 64-bit integer of the protocol: stored as a
    /// signed 64-bit integer, which holds every value the protocol
    /// writes.
    Number,
}

/// One column: name, kind, optional.
type Col = (&'static str, Kind, bool);

const fn req(name: &'static str, kind: Kind) -> Col {
    (name, kind, false)
}

const fn opt(name: &'static str, kind: Kind) -> Col {
    (name, kind, true)
}

use Kind::{Bytes, Id, Number, Text};

/// The six key columns of a device request, which its public projection
/// repeats.
const DEVICE_KEY_COLUMNS: [Col; 6] = [
    req("publicKey", Bytes),
    req("mechanism", Text),
    req("version", Number),
    req("signingPublicKey", Bytes),
    req("signingMechanism", Text),
    req("signingVersion", Number),
];

/// Columns shared by both account successor tables.
const SUCCESSOR_COLUMNS: [Col; 9] = [
    req("accountId", Id),
    req("predecessor", Id),
    req("epochId", Id),
    req("signerId", Id),
    req("action", Text),
    opt("removedDeviceId", Id),
    req("retiredRecoveryRootIds", Bytes),
    req("membership", Bytes),
    req("revision", Bytes),
];

fn table(columns: &[Col], single: &[&str], composite: &[&[&str]]) -> TableSchema {
    TableSchema {
        columns: RowDescriptor::new(
            columns
                .iter()
                .map(|(name, kind, optional)| {
                    let ty = match kind {
                        Id => ColumnType::Uuid,
                        Text => ColumnType::Text,
                        Bytes => ColumnType::Bytea,
                        Number => ColumnType::BigInt,
                    };
                    let column = ColumnDescriptor::new(*name, ty);
                    if *optional { column.nullable() } else { column }
                })
                .collect(),
        ),
        indexed_columns: (!single.is_empty()).then(|| single.iter().map(|c| (*c).into()).collect()),
        composite_indexes: composite
            .iter()
            .map(|index| index.iter().map(|c| (*c).into()).collect())
            .collect(),
        policies: TablePolicies::default(),
        branch_by: Vec::new(),
        history: HistoryRetention::Full,
    }
}

/// The fixed, versioned schema fragment of Jazz's E2EE system tables: the
/// columns of protocol §1.2 and §1.3 (every ID, account and address column
/// a `uuid`) with the indexes of protocol §1.3, and no policies. No column
/// is a reference: `identifier` names a row of any of several tables.
pub fn e2ee_system_tables_v1() -> Schema {
    let space = |rest: &[Col]| -> Vec<Col> {
        [req("scopeId", Id), req("identifier", Id)]
            .into_iter()
            .chain(rest.iter().copied())
            .collect()
    };
    let by_space: &[&[&str]] = &[&["scopeId", "identifier"]];
    let device_keys: Vec<Col> = std::iter::once(req("deviceId", Id))
        .chain(DEVICE_KEY_COLUMNS)
        .collect();
    let device_requests: Vec<Col> = DEVICE_KEY_COLUMNS
        .into_iter()
        .chain([req("nonceCommitment", Bytes)])
        .collect();
    let private_successors: Vec<Col> = SUCCESSOR_COLUMNS
        .into_iter()
        .chain([
            req("verification", Bytes),
            req("history", Bytes),
            req("deliveries", Bytes),
            req("signature", Bytes),
        ])
        .collect();
    let public_successors: Vec<Col> = SUCCESSOR_COLUMNS
        .into_iter()
        .chain([req("signature", Bytes)])
        .collect();
    let recovery_roots: Vec<Col> = [
        req("accountId", Id),
        req("signerId", Id),
        req("epochId", Id),
    ]
    .into_iter()
    .chain(DEVICE_KEY_COLUMNS)
    .chain([req("signature", Bytes)])
    .collect();
    let tables: Vec<(&str, TableSchema)> = vec![
        (
            MEMBERS,
            table(
                &space(&[req("accountId", Id)]),
                &["accountId"],
                &[&["scopeId", "identifier", "accountId"]],
            ),
        ),
        (
            SPACES,
            table(
                &space(&[
                    req("creatorAccountId", Id),
                    req("deviceId", Id),
                    req("accountEpochId", Id),
                    req("epochId", Id),
                    req("mechanism", Text),
                    req("version", Number),
                    req("verification", Bytes),
                    req("accountEnvelope", Bytes),
                    req("signature", Bytes),
                ]),
                &[],
                by_space,
            ),
        ),
        (
            SPACE_SUCCESSORS,
            table(
                &space(&[
                    req("predecessor", Id),
                    req("historyEpochId", Id),
                    req("epochId", Id),
                    req("authorAccountId", Id),
                    req("authorDeviceId", Id),
                    req("authorEpochId", Id),
                    req("verification", Bytes),
                    req("history", Bytes),
                    req("accountEnvelope", Bytes),
                    req("signature", Bytes),
                ]),
                &[],
                &[&["scopeId", "identifier", "predecessor"]],
            ),
        ),
        (
            SPACE_DELIVERIES,
            table(
                &space(&[
                    req("keyEpochId", Id),
                    req("senderAccountId", Id),
                    req("recipientAccountId", Id),
                    req("recipientAccountEpochId", Id),
                    req("envelope", Bytes),
                ]),
                &["recipientAccountId"],
                &[&["scopeId", "identifier", "keyEpochId"]],
            ),
        ),
        (DEVICE_REQUESTS, table(&device_requests, &[], &[])),
        (DEVICE_KEYS, table(&device_keys, &["deviceId"], &[])),
        (
            ACCOUNT_IDENTITIES,
            table(
                &[
                    req("ledgerVersion", Number),
                    req("generation", Number),
                    req("deviceId", Id),
                    req("epochId", Id),
                    req("envelope", Bytes),
                    req("verification", Bytes),
                ],
                &["deviceId"],
                &[],
            ),
        ),
        (
            ACCOUNT_ROOTS,
            table(
                &[
                    req("ledgerVersion", Number),
                    req("generation", Number),
                    req("accountId", Id),
                    req("deviceId", Id),
                    req("epochId", Id),
                    req("kind", Text),
                    opt("deviceAccountPublicKey", Bytes),
                    opt("deviceAccountSignature", Bytes),
                ],
                &["accountId", "deviceId"],
                &[],
            ),
        ),
        (
            DEVICE_CHALLENGES,
            table(
                &[
                    req("deviceId", Id),
                    req("epochId", Id),
                    req("envelope", Bytes),
                    req("nonce", Bytes),
                    req("signerId", Id),
                    req("signature", Bytes),
                ],
                &["deviceId"],
                &[],
            ),
        ),
        (
            DEVICE_PROOFS,
            table(
                &[
                    req("challengeId", Id),
                    req("proof", Bytes),
                    req("nonce", Bytes),
                    req("signature", Bytes),
                ],
                &[],
                &[],
            ),
        ),
        (
            DEVICE_APPROVALS,
            table(
                &[
                    req("challengeId", Id),
                    req("signerId", Id),
                    req("verification", Bytes),
                    req("signature", Bytes),
                ],
                &[],
                &[],
            ),
        ),
        (
            DEVICE_DELIVERIES,
            table(
                &[
                    req("challengeId", Id),
                    req("envelope", Bytes),
                    req("verification", Bytes),
                ],
                &[],
                &[],
            ),
        ),
        (
            PUBLIC_DEVICE_APPROVALS,
            table(
                &[
                    req("accountId", Id),
                    req("epochId", Id),
                    req("deviceId", Id),
                    req("signerId", Id),
                    req("signature", Bytes),
                    opt("recoveryRootId", Id),
                    opt("recoverySignature", Bytes),
                ],
                &["accountId", "deviceId"],
                &[],
            ),
        ),
        (
            ACCOUNT_SUCCESSORS,
            table(&private_successors, &["accountId"], &[]),
        ),
        (
            PUBLIC_ACCOUNT_SUCCESSORS,
            table(&public_successors, &["accountId"], &[]),
        ),
        (RECOVERY_ROOTS, table(&recovery_roots, &["accountId"], &[])),
        (
            RECOVERY_DELIVERIES,
            table(
                &[
                    req("rootId", Id),
                    req("epochId", Id),
                    req("envelope", Bytes),
                ],
                &[],
                &[],
            ),
        ),
        (
            DEVICE_REMOVAL_REQUESTS,
            table(
                &[
                    req("accountId", Id),
                    req("epochId", Id),
                    req("deviceId", Id),
                    req("signature", Bytes),
                ],
                &["accountId", "deviceId"],
                &[],
            ),
        ),
        (
            ACCOUNT_KEYS,
            table(
                &[
                    req("accountId", Id),
                    req("epochId", Id),
                    req("signerId", Id),
                    req("mechanism", Text),
                    req("version", Number),
                    req("publicKey", Bytes),
                    req("signature", Bytes),
                ],
                &["accountId"],
                &[],
            ),
        ),
    ];
    tables
        .into_iter()
        .map(|(name, schema)| (TableName::from(name), schema))
        .collect()
}

/// `schema` with the system tables added: what every loader does to a
/// schema version that declares encryption, before hashing it.
pub fn with_e2ee_fragment(schema: &Schema) -> Schema {
    let mut schema = schema.clone();
    schema.extend(e2ee_system_tables_v1());
    schema
}

/// Whether `schema` holds system tables, and if it does, that they are
/// exactly [`e2ee_system_tables_v1`] (their policies aside) and that no app
/// table references one. `Ok(true)` when the fragment is there, `Ok(false)`
/// when the schema has no system table.
pub fn check_e2ee_fragment(schema: &Schema) -> Result<bool, String> {
    for (name, table) in schema.iter() {
        if is_system_table(name.as_str()) {
            continue;
        }
        if let Some(column) = table
            .columns
            .columns
            .iter()
            .find(|c| c.references.is_some_and(|t| is_system_table(t.as_str())))
        {
            return Err(format!(
                "column {name}.{} references {}, a Jazz system table; an app table may not \
                 reference one",
                column.name_str(),
                column.references.expect("found above")
            ));
        }
    }
    if !schema.keys().any(|name| is_system_table(name.as_str())) {
        return Ok(false);
    }
    let fragment = e2ee_system_tables_v1();
    for name in schema.keys().filter(|name| is_system_table(name.as_str())) {
        if !fragment.contains_key(name) {
            return Err(format!(
                "table {name} has the reserved prefix `{SYSTEM_TABLE_PREFIX}`, which only Jazz's \
                 own system tables use; rename it"
            ));
        }
    }
    for (name, expected) in fragment.iter() {
        let Some(given) = schema.get(name) else {
            return Err(format!(
                "the schema has Jazz's system tables but lacks {name}; build it with a loader of \
                 this version of Jazz"
            ));
        };
        let mut given = given.clone();
        given.policies = TablePolicies::default();
        if given != *expected {
            return Err(format!(
                "table {name} is not the definition of Jazz's system table; build the schema \
                 with a loader of this version of Jazz"
            ));
        }
    }
    Ok(true)
}

// ---------------------------------------------------------------------------
// Derived policies
// ---------------------------------------------------------------------------

const OUTER_ROW: &str = "__jazz_outer_row";

fn me() -> PolicyValue {
    PolicyValue::SessionRef(vec!["user".to_owned()])
}

fn this(column: &str) -> PolicyValue {
    PolicyValue::SessionRef(vec![OUTER_ROW.to_owned(), column.to_owned()])
}

fn eq(column: &str, value: PolicyValue) -> PolicyExpr {
    PolicyExpr::Cmp {
        column: column.to_owned(),
        op: CmpOp::Eq,
        value,
    }
}

/// `column = this.column`.
fn same(column: &str) -> PolicyExpr {
    eq(column, this(column))
}

fn literal(column: &str, value: Value) -> PolicyExpr {
    eq(column, PolicyValue::Literal(value))
}

fn exists(table: &str, conditions: Vec<PolicyExpr>) -> PolicyExpr {
    PolicyExpr::Exists {
        table: table.to_owned(),
        condition: Box::new(PolicyExpr::And(conditions)),
    }
}

fn and(conditions: Vec<PolicyExpr>) -> PolicyExpr {
    PolicyExpr::And(conditions)
}

fn or(conditions: Vec<PolicyExpr>) -> PolicyExpr {
    PolicyExpr::Or(conditions)
}

/// The row was written by the session's account.
fn created_by_me() -> PolicyExpr {
    eq("$createdBy.account", me())
}

/// A public table's rows: readable by every `device-account` or `external`
/// session (protocol R10).
fn public() -> PolicyExpr {
    PolicyExpr::SessionInList {
        path: vec!["authMode".to_owned()],
        values: vec![
            Value::Text("device-account".to_owned()),
            Value::Text("external".to_owned()),
        ],
    }
}

/// A device request of the session's account whose row id (the device
/// id) is this row's `column`.
fn own_request(column: &str) -> PolicyExpr {
    exists(
        DEVICE_REQUESTS,
        vec![eq("id", this(column)), created_by_me()],
    )
}

/// `member(account)` of protocol §1.4: a member row of the checked row's
/// space for `account`.
fn member(account: PolicyValue) -> PolicyExpr {
    exists(
        MEMBERS,
        vec![
            same("scopeId"),
            same("identifier"),
            eq("accountId", account),
        ],
    )
}

fn inherits_from(operation: Operation, tables: &[String]) -> PolicyExpr {
    PolicyExpr::InheritsFrom {
        operation,
        table_column: "scopeId".to_owned(),
        id_column: "identifier".to_owned(),
        tables: tables.to_vec(),
    }
}

/// `read(row) and update(row)` of protocol §1.4: the session may read the
/// space row and save it unchanged.
fn manages(tables: &[String]) -> PolicyExpr {
    and(vec![
        inherits_from(Operation::Select, tables),
        inherits_from(Operation::Update, tables),
    ])
}

/// `creator(row)` of protocol §1.4: this very transaction inserts the
/// space row, created by the session. It reads the space row as raw
/// evidence, not through the space table's read policy.
fn creator(tables: &[String]) -> PolicyExpr {
    or(tables
        .iter()
        .map(|table| {
            and(vec![
                eq(
                    "scopeId",
                    PolicyValue::TableIdentity {
                        table: table.clone(),
                    },
                ),
                exists(
                    table,
                    vec![
                        eq("id", this("identifier")),
                        created_by_me(),
                        PolicyExpr::InsertedByThisTransaction,
                    ],
                ),
            ])
        })
        .collect())
}

/// Policies of a table whose rows are inserted and read only: no update
/// and no delete (protocol R1).
fn immutable(select: PolicyExpr, insert: PolicyExpr) -> TablePolicies {
    TablePolicies {
        select: OperationPolicy::using(select),
        insert: OperationPolicy::with_check(insert),
        update: OperationPolicy::using_and_check(PolicyExpr::False, PolicyExpr::False),
        delete: OperationPolicy::using(PolicyExpr::False),
    }
}

/// The space tables named as a `scope` by `entries`, sorted and distinct:
/// the candidates every space-table policy reaches through. Refuses none,
/// an unknown table and a system table.
pub fn space_tables(
    schema: &Schema,
    entries: &BTreeMap<String, EncryptionEntry>,
) -> Result<Vec<String>, String> {
    let tables: BTreeSet<&str> = entries.values().map(|e| e.scope.as_str()).collect();
    if tables.is_empty() {
        return Err("encryption is declared but no table names a space table (`scope`)".to_owned());
    }
    for table in &tables {
        if is_system_table(table) {
            return Err(format!(
                "`{table}` is a Jazz system table and can't be a space table (`scope`)"
            ));
        }
        if !schema.contains_key(&TableName::from(*table)) {
            return Err(format!("space table (`scope`) `{table}` is not a table"));
        }
    }
    Ok(tables.into_iter().map(str::to_owned).collect())
}

/// The policies of every system table, derived from the app's encryption
/// declarations for `schema` (one schema version): protocol R1–R17, with
/// `InheritsFrom` reaching the space tables `entries` name as `scope`.
/// The same on every side, so the loaders and the server compute the same
/// permissions bundle. The app writes none of them.
///
/// `settings` decide whether an account identity may be inserted at a
/// generation above 0 (`allowAccountReset`, protocol §2.11). What R5 and
/// R6 read of Jazz's account registry (an account's founder, a root's
/// kind) and an identity's predecessor generation is checked by core
/// beside these policies.
pub fn derive_e2ee_policies(
    schema: &Schema,
    entries: &BTreeMap<String, EncryptionEntry>,
    settings: &EncryptionSettings,
) -> Result<Permissions, String> {
    if !check_e2ee_fragment(schema)? {
        return Err("encryption is declared but the schema lacks Jazz's system tables".to_owned());
    }
    let tables = space_tables(schema, entries)?;
    let manages = manages(&tables);
    let me_member = member(me());
    let mut policies = Permissions::new();
    let mut put = |name: &str, table: TablePolicies| {
        policies.insert(TableName::from(name), table);
    };

    // Space tables (R12–R17).
    put(
        MEMBERS,
        TablePolicies {
            select: OperationPolicy::using(or(vec![eq("accountId", me()), me_member.clone()])),
            insert: OperationPolicy::with_check(or(vec![
                manages.clone(),
                and(vec![eq("accountId", me()), creator(&tables)]),
            ])),
            update: OperationPolicy::using_and_check(PolicyExpr::False, PolicyExpr::False),
            delete: OperationPolicy::using(or(vec![eq("accountId", me()), manages.clone()])),
        },
    );
    let create_or_rotate = |author: &str| {
        let mut conditions = vec![me_member.clone()];
        conditions.extend(match &manages {
            PolicyExpr::And(both) => both.clone(),
            other => vec![other.clone()],
        });
        conditions.push(eq(author, me()));
        conditions.push(created_by_me());
        and(conditions)
    };
    put(
        SPACES,
        immutable(me_member.clone(), create_or_rotate("creatorAccountId")),
    );
    put(
        SPACE_SUCCESSORS,
        immutable(me_member.clone(), create_or_rotate("authorAccountId")),
    );
    put(
        SPACE_DELIVERIES,
        immutable(
            me_member.clone(),
            and(vec![
                me_member.clone(),
                member(this("recipientAccountId")),
                eq("senderAccountId", me()),
                created_by_me(),
            ]),
        ),
    );

    // Account and device tables (R2–R11). They check access only; clients
    // authenticate every record on replay.
    let private = created_by_me;
    put(DEVICE_REQUESTS, immutable(private(), created_by_me()));
    put(
        DEVICE_KEYS,
        immutable(
            public(),
            and(vec![
                created_by_me(),
                exists(
                    DEVICE_REQUESTS,
                    std::iter::once(eq("id", this("deviceId")))
                        .chain(std::iter::once(created_by_me()))
                        .chain(DEVICE_KEY_COLUMNS.iter().map(|(column, _, _)| same(column)))
                        .collect(),
                ),
            ]),
        ),
    );
    put(
        ACCOUNT_IDENTITIES,
        immutable(
            private(),
            and(vec![
                created_by_me(),
                literal("ledgerVersion", Value::BigInt(1)),
                own_request("deviceId"),
                {
                    // The first identity is the account's own row id; a
                    // reset is a later generation, by a session that isn't
                    // a device-account session, when the app allows resets.
                    let first = and(vec![
                        literal("generation", Value::BigInt(0)),
                        eq("id", me()),
                    ]);
                    if settings.allow_account_reset {
                        or(vec![
                            first,
                            and(vec![
                                PolicyExpr::Cmp {
                                    column: "generation".to_owned(),
                                    op: CmpOp::Gt,
                                    value: PolicyValue::Literal(Value::BigInt(0)),
                                },
                                PolicyExpr::SessionCmp {
                                    path: vec!["authMode".to_owned()],
                                    op: CmpOp::Ne,
                                    value: Value::Text("device-account".to_owned()),
                                },
                            ]),
                        ])
                    } else {
                        first
                    }
                },
            ]),
        ),
    );
    put(
        ACCOUNT_ROOTS,
        immutable(
            public(),
            and(vec![
                eq("accountId", me()),
                created_by_me(),
                exists(
                    ACCOUNT_IDENTITIES,
                    vec![
                        same("deviceId"),
                        same("epochId"),
                        same("generation"),
                        same("ledgerVersion"),
                        created_by_me(),
                        PolicyExpr::InsertedByThisTransaction,
                    ],
                ),
            ]),
        ),
    );
    put(
        DEVICE_CHALLENGES,
        immutable(
            private(),
            and(vec![created_by_me(), own_request("deviceId")]),
        ),
    );
    let own_challenge = || {
        exists(
            DEVICE_CHALLENGES,
            vec![eq("id", this("challengeId")), created_by_me()],
        )
    };
    put(
        DEVICE_PROOFS,
        immutable(private(), and(vec![created_by_me(), own_challenge()])),
    );
    put(
        DEVICE_APPROVALS,
        immutable(private(), and(vec![created_by_me(), own_challenge()])),
    );
    put(
        DEVICE_DELIVERIES,
        immutable(private(), and(vec![created_by_me(), own_challenge()])),
    );
    put(
        PUBLIC_DEVICE_APPROVALS,
        immutable(
            public(),
            and(vec![
                eq("accountId", me()),
                created_by_me(),
                // The private approval with the same row id, inserted with
                // it (protocol R8).
                exists(
                    DEVICE_APPROVALS,
                    vec![
                        eq("id", this("id")),
                        created_by_me(),
                        PolicyExpr::InsertedByThisTransaction,
                    ],
                ),
                own_request("signerId"),
                own_request("deviceId"),
                or(vec![
                    PolicyExpr::IsNull {
                        column: "recoveryRootId".to_owned(),
                    },
                    exists(
                        RECOVERY_ROOTS,
                        vec![eq("id", this("recoveryRootId")), eq("accountId", me())],
                    ),
                ]),
            ]),
        ),
    );
    // The target columns match `action`; a removed device's request is
    // the account's own. That a retired recovery root is the account's own
    // is in an ID list, which no policy reads; clients check it on replay.
    let successor = || {
        vec![
            eq("accountId", me()),
            created_by_me(),
            own_request("signerId"),
            or(vec![
                and(vec![
                    literal("action", Value::Text("remove-device".to_owned())),
                    own_request("removedDeviceId"),
                ]),
                and(vec![
                    literal("action", Value::Text("retire-recovery-root".to_owned())),
                    PolicyExpr::IsNull {
                        column: "removedDeviceId".to_owned(),
                    },
                ]),
            ]),
        ]
    };
    put(ACCOUNT_SUCCESSORS, immutable(private(), and(successor())));
    put(
        PUBLIC_ACCOUNT_SUCCESSORS,
        immutable(
            public(),
            and(successor()
                .into_iter()
                // The private successor with the same row id, inserted
                // with it (protocol R8).
                .chain([exists(
                    ACCOUNT_SUCCESSORS,
                    vec![
                        eq("id", this("id")),
                        created_by_me(),
                        PolicyExpr::InsertedByThisTransaction,
                    ],
                )])
                .collect()),
        ),
    );
    put(
        RECOVERY_ROOTS,
        immutable(
            public(),
            and(vec![
                eq("accountId", me()),
                created_by_me(),
                own_request("signerId"),
            ]),
        ),
    );
    put(
        RECOVERY_DELIVERIES,
        immutable(
            private(),
            and(vec![
                created_by_me(),
                exists(
                    RECOVERY_ROOTS,
                    vec![eq("id", this("rootId")), eq("accountId", me())],
                ),
            ]),
        ),
    );
    put(
        DEVICE_REMOVAL_REQUESTS,
        immutable(
            public(),
            and(vec![
                eq("accountId", me()),
                created_by_me(),
                own_request("deviceId"),
            ]),
        ),
    );
    put(
        ACCOUNT_KEYS,
        immutable(
            public(),
            and(vec![
                eq("accountId", me()),
                created_by_me(),
                own_request("signerId"),
            ]),
        ),
    );
    debug_assert_eq!(
        policies.keys().collect::<BTreeSet<_>>(),
        e2ee_system_tables_v1().keys().collect::<BTreeSet<_>>()
    );
    Ok(policies)
}
