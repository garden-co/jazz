use crate::Result;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaDef {
    tables: BTreeMap<String, TableDef>,
}

impl SchemaDef {
    pub fn new() -> Self {
        Self {
            tables: BTreeMap::new(),
        }
    }

    pub fn attempt3_fixture() -> Self {
        Self::new()
            .table("users", |table| {
                table.text("name");
            })
            .table("groups", |table| {
                table.text("name");
            })
            .table("group_members", |table| {
                table.text("member");
                table.ref_("group", "groups");
                table.index("by_member", ["member", "group"]);
            })
            .table("projects", |table| {
                table.text("title");
            })
            .table("project_members", |table| {
                table.ref_("project", "projects");
                table.text("member");
                table.index("by_member", ["member", "project"]);
            })
            .table("todos", |table| {
                table.text("title");
                table.bool("done");
                table.ref_("project", "projects");
                table.index("open_created", ["done", "$createdAt"]);
                table.index("created", ["$createdAt"]);
                table.index("by_title", ["title"]);
                table.index("open_visible", ["done", "project", "$createdAt"]);
            })
            .table("labels", |table| {
                table.text("name");
                table.index("by_name", ["name"]);
            })
            .table("todo_labels", |table| {
                table.ref_("todo", "todos");
                table.ref_("label", "labels");
                table.index("by_todo", ["todo"]);
                table.index("by_label", ["label"]);
            })
    }

    pub fn mini_sqlite_todo_fixture() -> Self {
        Self::new()
            .table("users", |table| {
                table.text("name");
                table.read_if_row_id_equals_user();
            })
            .table("groups", |table| {
                table.text("name");
                table.read_if_inherits_referencing("group_members", "group");
            })
            .table("group_members", |table| {
                table.optional_ref("user", "users");
                table.optional_ref("member_group", "groups");
                table.ref_("group", "groups");
                table.index("by_user", ["user", "group"]);
                table.index("by_member_group", ["member_group", "group"]);
                table.read_if_user_or_ref_readable("user", "member_group");
            })
            .table("projects", |table| {
                table.text("title");
                table.read_if_inherits_referencing("project_members", "project");
            })
            .table("project_members", |table| {
                table.ref_("project", "projects");
                table.optional_ref("user", "users");
                table.optional_ref("group", "groups");
                table.index("by_user", ["user", "project"]);
                table.index("by_group", ["group", "project"]);
                table.index("by_project_user", ["project", "user"]);
                table.index("by_project_group", ["project", "group"]);
                table.read_if_user_or_ref_readable("user", "group");
            })
            .table("todos", |table| {
                table.text("title");
                table.bool("done");
                table.ref_("project", "projects");
                table.index("open_created", ["done", "$createdAt"]);
                table.index("created", ["$createdAt"]);
                table.index("by_title", ["title"]);
                table.index("open_visible", ["done", "project", "$createdAt"]);
                table.read_if_inherits("project");
                table.write_if_ref_readable("project");
                table.update_protected_fields_if_created_by_user(["title", "project"]);
                table.delete_if_created_by_user();
            })
            .table("labels", |table| {
                table.text("name");
                table.index("by_name", ["name"]);
            })
            .table("todo_labels", |table| {
                table.ref_("todo", "todos");
                table.ref_("label", "labels");
                table.index("by_todo", ["todo"]);
                table.index("by_todo_created", ["todo", "$createdAt"]);
                table.index("by_label", ["label"]);
                table.read_if_inherits("todo");
            })
    }

    pub fn table(mut self, name: &str, build: impl FnOnce(&mut TableBuilder)) -> Self {
        let mut builder = TableBuilder::new(name);
        build(&mut builder);
        self.tables.insert(name.to_owned(), builder.finish());
        self
    }

    pub(crate) fn tables(&self) -> impl Iterator<Item = &TableDef> {
        self.tables.values()
    }

    pub(crate) fn table_def(&self, name: &str) -> crate::Result<&TableDef> {
        self.tables
            .get(name)
            .ok_or_else(|| crate::Error::new(format!("unknown table {name}")))
    }

    pub(crate) fn compatibility_fingerprint(&self) -> String {
        let mut parts = Vec::new();
        for table in self.tables.values() {
            parts.push(format!("table:{}", table.name));
            for field in &table.fields {
                parts.push(format!(
                    "field:{}:{}",
                    field.storage_name,
                    field.kind.fingerprint()
                ));
            }
        }
        parts.join("|")
    }

    pub(crate) fn policy_fingerprint(&self) -> String {
        self.policy_fingerprint_for_tables(self.tables.keys())
    }

    pub(crate) fn policy_fingerprint_for_tables<'a>(
        &self,
        table_names: impl IntoIterator<Item = &'a String>,
    ) -> String {
        let mut parts = Vec::new();
        for table_name in table_names {
            let Some(table) = self.tables.get(table_name) else {
                continue;
            };
            parts.push(format!(
                "{}:insert:{}",
                table.name,
                table.insert_policy.fingerprint_for_table(table)
            ));
            parts.push(format!(
                "{}:update:{}",
                table.name,
                table.update_policy.fingerprint_for_table(table)
            ));
            parts.push(format!(
                "{}:delete:{}",
                table.name,
                table.delete_policy.fingerprint_for_table(table)
            ));
        }
        parts.join("|")
    }
}

impl Default for SchemaDef {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct TableDef {
    pub(crate) name: String,
    pub(crate) fields: Vec<FieldDef>,
    pub(crate) indexes: Vec<IndexDef>,
    pub(crate) read_policy: PolicyDef,
    pub(crate) insert_policy: OperationPolicy,
    pub(crate) update_policy: OperationPolicy,
    pub(crate) delete_policy: OperationPolicy,
}

impl TableDef {
    pub(crate) fn effective_delete_using(&self) -> Option<&PolicyDef> {
        self.delete_policy
            .using
            .as_ref()
            .or(self.update_policy.using.as_ref())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct FieldDef {
    pub(crate) name: String,
    pub(crate) storage_name: String,
    pub(crate) kind: FieldKind,
    pub(crate) nullable: bool,
    #[serde(default)]
    pub(crate) default_value: Option<JsonValue>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum FieldKind {
    Text,
    Bool,
    Ref { table: String },
}

impl FieldKind {
    fn fingerprint(&self) -> String {
        match self {
            FieldKind::Text => "text".to_owned(),
            FieldKind::Bool => "bool".to_owned(),
            FieldKind::Ref { table } => format!("ref:{table}"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct IndexDef {
    pub(crate) name: String,
    pub(crate) columns: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum PolicyValue {
    Literal(JsonValue),
    SessionRef(Vec<String>),
}

pub(crate) const OUTER_ROW_SESSION_PREFIX: &str = "__jazz_outer_row";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Operation {
    Select,
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct OperationPolicy {
    pub(crate) using: Option<PolicyDef>,
    pub(crate) with_check: Option<PolicyDef>,
}

impl OperationPolicy {
    fn using(policy: PolicyDef) -> Self {
        Self {
            using: Some(policy),
            with_check: None,
        }
    }

    fn with_check(policy: PolicyDef) -> Self {
        Self {
            using: None,
            with_check: Some(policy),
        }
    }

    fn using_and_check(using: PolicyDef, with_check: PolicyDef) -> Self {
        Self {
            using: Some(using),
            with_check: Some(with_check),
        }
    }

    fn fingerprint_for_table(&self, table: &TableDef) -> String {
        format!(
            "using:{}|check:{}",
            self.using
                .as_ref()
                .map(|policy| policy.fingerprint_for_table(table))
                .unwrap_or_else(|| "none".to_owned()),
            self.with_check
                .as_ref()
                .map(|policy| policy.fingerprint_for_table(table))
                .unwrap_or_else(|| "none".to_owned())
        )
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) enum PolicyExpr {
    Cmp {
        column: String,
        op: CmpOp,
        value: PolicyValue,
    },
    SessionCmp {
        path: Vec<String>,
        op: CmpOp,
        value: JsonValue,
    },
    IsNull {
        column: String,
    },
    SessionIsNull {
        path: Vec<String>,
    },
    IsNotNull {
        column: String,
    },
    SessionIsNotNull {
        path: Vec<String>,
    },
    Contains {
        column: String,
        value: PolicyValue,
    },
    SessionContains {
        path: Vec<String>,
        value: JsonValue,
    },
    In {
        column: String,
        session_path: Vec<String>,
    },
    InList {
        column: String,
        values: Vec<PolicyValue>,
    },
    SessionInList {
        path: Vec<String>,
        values: Vec<JsonValue>,
    },
    Exists {
        table: String,
        condition: Box<PolicyExpr>,
    },
    ExistsRel {
        rel: JsonValue,
    },
    Inherits {
        operation: Operation,
        via_column: String,
        max_depth: Option<usize>,
    },
    InheritsReferencing {
        operation: Operation,
        source_table: String,
        via_column: String,
        max_depth: Option<usize>,
    },
    And(Vec<PolicyExpr>),
    Or(Vec<PolicyExpr>),
    Not(Box<PolicyExpr>),
    #[default]
    True,
    False,
}

pub(crate) type PolicyDef = PolicyExpr;

impl PolicyExpr {
    fn fingerprint_for_table(&self, table: &TableDef) -> String {
        match self {
            PolicyDef::Cmp { column, op, value } => {
                format!(
                    "cmp:{}:{op:?}:{}",
                    policy_column_fingerprint(table, column),
                    policy_value_fingerprint(value)
                )
            }
            PolicyDef::SessionCmp { path, op, value } => {
                format!("session_cmp:{}:{op:?}:{value}", path.join("."))
            }
            PolicyDef::IsNull { column } => {
                format!("is_null:{}", policy_column_fingerprint(table, column))
            }
            PolicyDef::SessionIsNull { path } => format!("session_is_null:{}", path.join(".")),
            PolicyDef::IsNotNull { column } => {
                format!("is_not_null:{}", policy_column_fingerprint(table, column))
            }
            PolicyDef::SessionIsNotNull { path } => {
                format!("session_is_not_null:{}", path.join("."))
            }
            PolicyDef::Contains { column, value } => {
                format!(
                    "contains:{}:{}",
                    policy_column_fingerprint(table, column),
                    policy_value_fingerprint(value)
                )
            }
            PolicyDef::SessionContains { path, value } => {
                format!("session_contains:{}:{value}", path.join("."))
            }
            PolicyDef::In {
                column,
                session_path,
            } => format!(
                "in:{}:{}",
                policy_column_fingerprint(table, column),
                session_path.join(".")
            ),
            PolicyDef::InList { column, values } => format!(
                "in_list:{}:{}",
                policy_column_fingerprint(table, column),
                values
                    .iter()
                    .map(policy_value_fingerprint)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            PolicyDef::SessionInList { path, values } => format!(
                "session_in_list:{}:{}",
                path.join("."),
                values
                    .iter()
                    .map(JsonValue::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            PolicyDef::Exists {
                table: exists_table,
                condition,
            } => {
                format!(
                    "exists:{exists_table}:{}",
                    condition.fingerprint_for_table(table)
                )
            }
            PolicyDef::ExistsRel { rel } => format!("exists_rel:{rel}"),
            PolicyDef::Inherits {
                operation,
                via_column,
                max_depth,
            } => {
                format!(
                    "inherits:{operation:?}:{}:{max_depth:?}",
                    policy_column_fingerprint(table, via_column)
                )
            }
            PolicyDef::InheritsReferencing {
                operation,
                source_table,
                via_column,
                max_depth,
            } => {
                format!(
                    "inherits_referencing:{operation:?}:{source_table}:{via_column}:{max_depth:?}"
                )
            }
            PolicyDef::And(children) => format!(
                "and({})",
                children
                    .iter()
                    .map(|policy| policy.fingerprint_for_table(table))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            PolicyDef::Or(children) => format!(
                "or({})",
                children
                    .iter()
                    .map(|policy| policy.fingerprint_for_table(table))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            PolicyDef::Not(child) => {
                format!("not({})", child.fingerprint_for_table(table))
            }
            PolicyDef::True => "true".to_owned(),
            PolicyDef::False => "false".to_owned(),
        }
    }

    pub(crate) fn is_user_or_ref_readable(&self, user_field: &str, ref_field: &str) -> bool {
        let PolicyDef::Or(children) = self else {
            return false;
        };
        if children.len() != 2 {
            return false;
        }
        let has_session = children.iter().any(|child| {
            matches!(
                child,
                PolicyDef::Cmp {
                    column,
                    op: CmpOp::Eq,
                    value: PolicyValue::SessionRef(path),
                } if column == user_field && path == &["user_id".to_owned()]
            )
        });
        let has_ref = children.iter().any(|child| {
            matches!(
                child,
                PolicyDef::Inherits {
                    operation: Operation::Select,
                    via_column,
                    ..
                } if via_column == ref_field
            )
        });
        has_session && has_ref
    }
}

fn policy_value_fingerprint(value: &PolicyValue) -> String {
    match value {
        PolicyValue::Literal(value) => format!("literal:{value}"),
        PolicyValue::SessionRef(path) => format!("session:{}", path.join(".")),
    }
}

fn policy_column_fingerprint(table: &TableDef, column: &str) -> String {
    if column.starts_with('$') {
        column.to_owned()
    } else {
        storage_field_name(table, column).to_owned()
    }
}

fn storage_field_name<'a>(table: &'a TableDef, field: &'a str) -> &'a str {
    table
        .fields
        .iter()
        .find(|candidate| candidate.name == field)
        .map(|field| field.storage_name.as_str())
        .unwrap_or(field)
}

pub struct TableBuilder {
    table: TableDef,
}

impl TableBuilder {
    fn new(name: &str) -> Self {
        Self {
            table: TableDef {
                name: name.to_owned(),
                fields: Vec::new(),
                indexes: Vec::new(),
                read_policy: PolicyDef::True,
                insert_policy: OperationPolicy::with_check(PolicyDef::True),
                update_policy: OperationPolicy::using_and_check(PolicyDef::True, PolicyDef::True),
                delete_policy: OperationPolicy::default(),
            },
        }
    }

    pub fn text(&mut self, name: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: user_storage_name(name),
            kind: FieldKind::Text,
            nullable: false,
            default_value: None,
        });
    }

    pub fn text_default(&mut self, name: &str, value: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: user_storage_name(name),
            kind: FieldKind::Text,
            nullable: false,
            default_value: Some(JsonValue::String(value.to_owned())),
        });
    }

    pub fn optional_text(&mut self, name: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: user_storage_name(name),
            kind: FieldKind::Text,
            nullable: true,
            default_value: None,
        });
    }

    pub fn text_lens(&mut self, name: &str, stored_as: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: user_storage_name(stored_as),
            kind: FieldKind::Text,
            nullable: false,
            default_value: None,
        });
    }

    pub fn bool(&mut self, name: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: user_storage_name(name),
            kind: FieldKind::Bool,
            nullable: false,
            default_value: None,
        });
    }

    pub fn bool_default(&mut self, name: &str, value: bool) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: user_storage_name(name),
            kind: FieldKind::Bool,
            nullable: false,
            default_value: Some(JsonValue::Bool(value)),
        });
    }

    pub fn ref_(&mut self, name: &str, table: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: user_storage_name(name),
            kind: FieldKind::Ref {
                table: table.to_owned(),
            },
            nullable: false,
            default_value: None,
        });
    }

    pub fn optional_ref(&mut self, name: &str, table: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: user_storage_name(name),
            kind: FieldKind::Ref {
                table: table.to_owned(),
            },
            nullable: true,
            default_value: None,
        });
    }

    pub fn ref_lens(&mut self, name: &str, stored_as: &str, table: &str) {
        self.table.fields.push(FieldDef {
            name: name.to_owned(),
            storage_name: user_storage_name(stored_as),
            kind: FieldKind::Ref {
                table: table.to_owned(),
            },
            nullable: false,
            default_value: None,
        });
    }

    pub fn index<const N: usize>(&mut self, name: &str, columns: [&str; N]) {
        self.table.indexes.push(IndexDef {
            name: name.to_owned(),
            columns: columns.iter().map(|column| (*column).to_owned()).collect(),
        });
    }

    pub fn read_if_created_by_user(&mut self) {
        self.table.read_policy = created_by_user_policy();
    }

    pub fn read_if_row_id_equals_user(&mut self) {
        self.table.read_policy = row_id_equals_user_policy();
    }

    pub fn read_if_user_ref_equals_session(&mut self, field: &str) {
        self.table.read_policy = user_ref_equals_session_policy(field);
    }

    pub fn read_if_inherits(&mut self, field: &str) {
        self.table.read_policy = inherits_policy(field);
    }

    pub fn read_if_inherits_referencing(&mut self, source_table: &str, field: &str) {
        self.table.read_policy = inherits_referencing_policy(source_table, field);
    }

    pub fn read_if_user_or_ref_readable(&mut self, user_field: &str, ref_field: &str) {
        self.table.read_policy = PolicyDef::Or(vec![
            PolicyDef::Cmp {
                column: user_field.to_owned(),
                op: CmpOp::Eq,
                value: PolicyValue::SessionRef(vec!["user_id".to_owned()]),
            },
            PolicyDef::Inherits {
                operation: Operation::Select,
                via_column: ref_field.to_owned(),
                max_depth: None,
            },
        ]);
    }

    pub fn write_if_created_by_user(&mut self) {
        self.insert_if_created_by_user();
        self.update_if_created_by_user();
    }

    pub fn write_if_ref_readable(&mut self, field: &str) {
        self.insert_if_ref_readable(field);
        self.update_if_ref_readable(field);
    }

    pub fn insert_if_created_by_user(&mut self) {
        self.table.insert_policy = OperationPolicy::with_check(created_by_user_policy());
    }

    pub fn insert_if_ref_readable(&mut self, field: &str) {
        self.table.insert_policy = OperationPolicy::with_check(inherits_policy(field));
    }

    pub fn update_if_created_by_user(&mut self) {
        self.table.update_policy =
            OperationPolicy::using_and_check(created_by_user_policy(), created_by_user_policy());
    }

    pub fn update_using_created_by_user(&mut self) {
        self.table.update_policy.using = Some(created_by_user_policy());
    }

    pub fn update_check_created_by_user(&mut self) {
        self.table.update_policy.with_check = Some(created_by_user_policy());
    }

    pub fn update_if_ref_readable(&mut self, field: &str) {
        self.table.update_policy =
            OperationPolicy::using_and_check(inherits_policy(field), inherits_policy(field));
    }

    pub fn update_using_ref_readable(&mut self, field: &str) {
        self.table.update_policy.using = Some(inherits_policy(field));
    }

    pub fn update_check_ref_readable(&mut self, field: &str) {
        self.table.update_policy.with_check = Some(inherits_policy(field));
    }

    pub fn update_protected_fields_if_created_by_user<const N: usize>(
        &mut self,
        fields: [&str; N],
    ) {
        let guard = protected_fields_unchanged_or_created_by_user_policy(&self.table.name, fields);
        self.and_update_check(guard);
    }

    pub fn delete_if_created_by_user(&mut self) {
        self.table.delete_policy = OperationPolicy::using(created_by_user_policy());
    }

    pub fn read_if_ref_readable(&mut self, field: &str) {
        self.table.read_policy = inherits_policy(field);
    }

    fn finish(self) -> TableDef {
        self.table
    }

    fn and_update_check(&mut self, guard: PolicyDef) {
        self.table.update_policy.with_check =
            Some(match self.table.update_policy.with_check.take() {
                None | Some(PolicyDef::True) => guard,
                Some(existing) => PolicyDef::And(vec![existing, guard]),
            });
    }
}

fn created_by_user_policy() -> PolicyDef {
    PolicyDef::Cmp {
        column: "$createdBy".to_owned(),
        op: CmpOp::Eq,
        value: PolicyValue::SessionRef(vec!["user_id".to_owned()]),
    }
}

fn row_id_equals_user_policy() -> PolicyDef {
    PolicyDef::Cmp {
        column: "$id".to_owned(),
        op: CmpOp::Eq,
        value: PolicyValue::SessionRef(vec!["user_id".to_owned()]),
    }
}

fn user_ref_equals_session_policy(field: &str) -> PolicyDef {
    PolicyDef::Cmp {
        column: field.to_owned(),
        op: CmpOp::Eq,
        value: PolicyValue::SessionRef(vec!["user_id".to_owned()]),
    }
}

fn inherits_policy(field: &str) -> PolicyDef {
    PolicyDef::Inherits {
        operation: Operation::Select,
        via_column: field.to_owned(),
        max_depth: None,
    }
}

fn inherits_referencing_policy(source_table: &str, field: &str) -> PolicyDef {
    PolicyDef::InheritsReferencing {
        operation: Operation::Select,
        source_table: source_table.to_owned(),
        via_column: field.to_owned(),
        max_depth: None,
    }
}

fn protected_fields_unchanged_or_created_by_user_policy<const N: usize>(
    table_name: &str,
    fields: [&str; N],
) -> PolicyDef {
    let mut unchanged = vec![PolicyDef::Cmp {
        column: "$id".to_owned(),
        op: CmpOp::Eq,
        value: outer_row_value("$id"),
    }];
    unchanged.extend(fields.into_iter().map(|field| PolicyDef::Cmp {
        column: field.to_owned(),
        op: CmpOp::Eq,
        value: outer_row_value(field),
    }));
    PolicyDef::Or(vec![
        created_by_user_policy(),
        PolicyDef::Exists {
            table: table_name.to_owned(),
            condition: Box::new(PolicyDef::And(unchanged)),
        },
    ])
}

fn outer_row_value(column: &str) -> PolicyValue {
    PolicyValue::SessionRef(vec![OUTER_ROW_SESSION_PREFIX.to_owned(), column.to_owned()])
}

pub(crate) fn install(conn: &Connection, schema: &SchemaDef) -> Result<()> {
    validate_schema_shape(schema)?;
    validate_policy_cycles(schema)?;
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS jazz_node (
          node_num INTEGER PRIMARY KEY,
          node_id TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS jazz_user (
          user_num INTEGER PRIMARY KEY,
          user_id TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS jazz_tx (
          tx_num INTEGER PRIMARY KEY,
          tx_id TEXT NOT NULL UNIQUE,
          node_num INTEGER NOT NULL,
          local_epoch INTEGER NOT NULL,
          global_epoch INTEGER,
          kind INTEGER NOT NULL,
          conflict_mode INTEGER NOT NULL,
          outcome INTEGER NOT NULL,
          created_at INTEGER NOT NULL,
          metadata_json TEXT NOT NULL,
          UNIQUE (node_num, local_epoch)
        );

        CREATE TABLE IF NOT EXISTS jazz_tx_receipt (
          tx_num INTEGER NOT NULL,
          tier INTEGER NOT NULL,
          observed_at INTEGER NOT NULL,
          authority_node_num INTEGER,
          receipt_json TEXT,
          PRIMARY KEY (tx_num, tier)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS jazz_tx_rejection (
          tx_num INTEGER PRIMARY KEY,
          code TEXT NOT NULL,
          detail_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS jazz_tx_awaiting_dependency (
          tx_num INTEGER PRIMARY KEY,
          auth_user TEXT NOT NULL,
          detail_json TEXT NOT NULL,
          updated_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS jazz_table (
          table_num INTEGER PRIMARY KEY,
          table_name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS jazz_tx_write (
          tx_num INTEGER NOT NULL,
          table_num INTEGER NOT NULL,
          row_num INTEGER NOT NULL,
          op INTEGER NOT NULL,
          PRIMARY KEY (tx_num, table_num, row_num)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS jazz_tx_read (
          tx_num INTEGER NOT NULL,
          table_num INTEGER NOT NULL,
          row_num INTEGER NOT NULL,
          reason INTEGER NOT NULL,
          observed_tx_num INTEGER,
          PRIMARY KEY (tx_num, table_num, row_num, reason)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS jazz_query_read (
          branch_id TEXT NOT NULL,
          table_name TEXT NOT NULL,
          field_name TEXT NOT NULL,
          op TEXT NOT NULL,
          value_json TEXT NOT NULL,
          observed_at INTEGER NOT NULL,
          PRIMARY KEY (branch_id, table_name, field_name, op, value_json)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS jazz_row_id (
          row_num INTEGER PRIMARY KEY,
          row_id TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS jazz_branch (
          branch_num INTEGER PRIMARY KEY,
          branch_id TEXT NOT NULL UNIQUE,
          base_global_epoch INTEGER,
          created_at INTEGER NOT NULL,
          source_version INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS jazz_branch_source (
          branch_num INTEGER NOT NULL,
          source_branch_num INTEGER NOT NULL,
          PRIMARY KEY (branch_num, source_branch_num)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS jazz_branch_backing (
          branch_id TEXT PRIMARY KEY,
          base_global_epoch INTEGER,
          source_branch_ids_json TEXT NOT NULL,
          created_at INTEGER NOT NULL
        ) WITHOUT ROWID;

        INSERT OR IGNORE INTO jazz_branch
          (branch_num, branch_id, base_global_epoch, created_at, source_version)
          VALUES (1, 'main', NULL, 0, 0);

        INSERT OR IGNORE INTO jazz_branch_backing
          (branch_id, base_global_epoch, source_branch_ids_json, created_at)
          VALUES ('main', NULL, '[]', 0);
        "#,
    )?;

    for (idx, table) in schema.tables().enumerate() {
        conn.execute(
            "INSERT OR IGNORE INTO jazz_table (table_num, table_name) VALUES (?, ?)",
            rusqlite::params![idx as i64 + 1, table.name],
        )?;
        install_table(conn, table)?;
    }
    Ok(())
}

fn validate_schema_shape(schema: &SchemaDef) -> Result<()> {
    for table in schema.tables() {
        let mut fields = BTreeSet::new();
        let mut storage_fields = BTreeSet::new();
        for field in &table.fields {
            if !fields.insert(field.name.clone()) {
                return Err(crate::Error::new(format!(
                    "duplicate field {}.{}",
                    table.name, field.name
                )));
            }
            if !storage_fields.insert(storage_column(field)) {
                return Err(crate::Error::new(format!(
                    "duplicate storage field {}.{}",
                    table.name, field.storage_name
                )));
            }
            if let FieldKind::Ref { table: ref_table } = &field.kind {
                schema.table_def(ref_table)?;
            }
        }
        for index in &table.indexes {
            for column in &index.columns {
                if column == "$createdAt" || column == "$updatedAt" {
                    continue;
                }
                if !table.fields.iter().any(|field| field.name == *column) {
                    return Err(crate::Error::new(format!(
                        "index {}.{} references unknown field {}",
                        table.name, index.name, column
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_policy_cycles(schema: &SchemaDef) -> Result<()> {
    for table in schema.tables() {
        validate_policy_cycle(schema, table, &table.read_policy, &mut BTreeSet::new())?;
        if let Some(policy) = &table.insert_policy.with_check {
            validate_policy_cycle(schema, table, policy, &mut BTreeSet::new())?;
        }
        if let Some(policy) = &table.update_policy.using {
            validate_policy_cycle(schema, table, policy, &mut BTreeSet::new())?;
        }
        if let Some(policy) = &table.update_policy.with_check {
            validate_policy_cycle(schema, table, policy, &mut BTreeSet::new())?;
        }
        if let Some(delete_policy) = &table.delete_policy.using {
            validate_policy_cycle(schema, table, delete_policy, &mut BTreeSet::new())?;
        }
    }
    Ok(())
}

fn validate_policy_cycle(
    schema: &SchemaDef,
    table: &TableDef,
    policy: &PolicyDef,
    seen: &mut BTreeSet<String>,
) -> Result<()> {
    let field = match policy {
        PolicyDef::Inherits {
            operation,
            via_column,
            ..
        } => {
            validate_select_operation(*operation)?;
            via_column
        }
        PolicyDef::InheritsReferencing {
            source_table,
            via_column,
            operation,
            ..
        } => {
            validate_select_operation(*operation)?;
            validate_inherits_referencing_policy(schema, table, source_table, via_column)?;
            return Ok(());
        }
        PolicyDef::Cmp { column, .. }
        | PolicyDef::IsNull { column }
        | PolicyDef::IsNotNull { column }
        | PolicyDef::Contains { column, .. }
        | PolicyDef::In { column, .. }
        | PolicyDef::InList { column, .. } => {
            validate_policy_column(table, column)?;
            return Ok(());
        }
        PolicyDef::SessionCmp { .. }
        | PolicyDef::SessionIsNull { .. }
        | PolicyDef::SessionIsNotNull { .. }
        | PolicyDef::SessionContains { .. }
        | PolicyDef::SessionInList { .. } => return Ok(()),
        PolicyDef::Exists {
            table: exists_table,
            condition,
        } => {
            let exists_table = schema.table_def(exists_table)?;
            validate_policy_cycle(schema, exists_table, condition, seen)?;
            return Ok(());
        }
        PolicyDef::ExistsRel { .. } => {
            return Ok(());
        }
        PolicyDef::And(children) | PolicyDef::Or(children) => {
            for child in children {
                validate_policy_cycle(schema, table, child, seen)?;
            }
            return Ok(());
        }
        PolicyDef::Not(child) => {
            validate_policy_cycle(schema, table, child, seen)?;
            return Ok(());
        }
        PolicyDef::True | PolicyDef::False => return Ok(()),
    };
    if !seen.insert(table.name.clone()) {
        return Err(crate::Error::new(format!(
            "policy cycle detected at {}",
            table.name
        )));
    }
    let Some(field) = table
        .fields
        .iter()
        .find(|candidate| candidate.name == *field)
    else {
        seen.remove(&table.name);
        return Err(crate::Error::new(format!(
            "policy on {} references unknown field {}",
            table.name, field
        )));
    };
    let FieldKind::Ref { table: parent } = &field.kind else {
        seen.remove(&table.name);
        return Err(crate::Error::new(format!(
            "policy on {} references non-ref field {}",
            table.name, field.name
        )));
    };
    let parent_table = schema.table_def(parent)?;
    let result = validate_policy_cycle(schema, parent_table, &parent_table.read_policy, seen);
    seen.remove(&table.name);
    result
}

fn validate_select_operation(operation: Operation) -> Result<()> {
    if operation != Operation::Select {
        return Err(crate::Error::new(
            "mini-sqlite policies only lower SELECT inheritance today",
        ));
    }
    Ok(())
}

fn validate_policy_column(table: &TableDef, column: &str) -> Result<()> {
    if matches!(column, "$id" | "$createdBy") {
        return Ok(());
    }
    if table.fields.iter().any(|field| field.name == column) {
        return Ok(());
    }
    Err(crate::Error::new(format!(
        "policy on {} references unknown column {}",
        table.name, column
    )))
}

fn validate_inherits_referencing_policy(
    schema: &SchemaDef,
    table: &TableDef,
    source_table_name: &str,
    field_name: &str,
) -> Result<()> {
    let source_table = schema.table_def(source_table_name)?;
    let Some(field) = source_table
        .fields
        .iter()
        .find(|candidate| candidate.name == field_name)
    else {
        return Err(crate::Error::new(format!(
            "policy on {} references unknown field {}.{}",
            table.name, source_table_name, field_name
        )));
    };
    let FieldKind::Ref { table: parent } = &field.kind else {
        return Err(crate::Error::new(format!(
            "policy on {} references non-ref field {}.{}",
            table.name, source_table_name, field.name
        )));
    };
    if parent != &table.name {
        return Err(crate::Error::new(format!(
            "policy on {} expected {}.{} to reference {}",
            table.name, source_table_name, field.name, table.name
        )));
    }
    Ok(())
}

fn install_table(conn: &Connection, table: &TableDef) -> Result<()> {
    let user_columns = table
        .fields
        .iter()
        .map(|field| {
            format!(
                "{} {}",
                quote_ident(&storage_column(field)),
                sql_type(&field.kind)
            )
        })
        .collect::<Vec<_>>()
        .join(",\n          ");
    conn.execute_batch(&format!(
        r#"
        CREATE TABLE IF NOT EXISTS {history} (
          row_num INTEGER NOT NULL,
          tx_num INTEGER NOT NULL,
          j_branch_num INTEGER NOT NULL,
          op INTEGER NOT NULL,
          {user_columns},
          j_created_at INTEGER NOT NULL,
          j_updated_at INTEGER NOT NULL,
          j_created_by INTEGER NOT NULL,
          j_updated_by INTEGER NOT NULL,
          PRIMARY KEY (row_num, tx_num)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS {current} (
          row_num INTEGER NOT NULL,
          j_branch_num INTEGER NOT NULL,
          visible_tx_num INTEGER NOT NULL,
          is_deleted INTEGER NOT NULL,
          {user_columns},
          j_created_at INTEGER NOT NULL,
          j_updated_at INTEGER NOT NULL,
          j_created_by INTEGER NOT NULL,
          j_updated_by INTEGER NOT NULL,
          PRIMARY KEY (row_num, j_branch_num)
        ) WITHOUT ROWID;
        "#,
        history = history_table(&table.name),
        current = current_table(&table.name),
    ))?;

    for index in &table.indexes {
        let mut columns = vec!["j_branch_num".to_owned(), "is_deleted".to_owned()];
        columns.extend(
            index
                .columns
                .iter()
                .map(|column| index_storage_column_name(table, column)),
        );
        columns.push("row_num".to_owned());
        conn.execute_batch(&format!(
            "CREATE INDEX IF NOT EXISTS {} ON {}({});",
            quote_ident(&format!("{}_current_{}_v2", table.name, index.name)),
            current_table(&table.name),
            columns.join(", ")
        ))?;
    }
    Ok(())
}

pub(crate) fn history_table(table: &str) -> String {
    quote_ident(&format!("{table}__schema_v1_history"))
}

pub(crate) fn current_table(table: &str) -> String {
    quote_ident(&format!("{table}__schema_v1_current"))
}

pub(crate) fn table_num(conn: &Connection, table_name: &str) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT table_num FROM jazz_table WHERE table_name = ?",
        rusqlite::params![table_name],
        |row| row.get(0),
    )?)
}

pub(crate) fn table_nums(conn: &Connection) -> Result<BTreeMap<String, i64>> {
    let mut stmt = conn.prepare("SELECT table_name, table_num FROM jazz_table")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    rows.collect::<std::result::Result<BTreeMap<_, _>, _>>()
        .map_err(Into::into)
}

pub(crate) fn storage_column(field: &FieldDef) -> String {
    match field.kind {
        FieldKind::Ref { .. } => format!("{}_row_num", field.storage_name),
        _ => field.storage_name.clone(),
    }
}

pub(crate) fn field_sql_value(
    field: &FieldDef,
    value: &serde_json::Value,
    resolve_ref: impl FnOnce(&str, &str) -> crate::Result<i64>,
) -> crate::Result<rusqlite::types::Value> {
    if value.is_null() {
        if field.nullable {
            return Ok(rusqlite::types::Value::Null);
        }
        return Err(crate::Error::new(format!(
            "expected non-null for {}",
            field.name
        )));
    }
    Ok(match &field.kind {
        FieldKind::Text => rusqlite::types::Value::Text(
            value
                .as_str()
                .ok_or_else(|| crate::Error::new(format!("expected text for {}", field.name)))?
                .to_owned(),
        ),
        FieldKind::Bool => rusqlite::types::Value::Integer(i64::from(
            value
                .as_bool()
                .ok_or_else(|| crate::Error::new(format!("expected bool for {}", field.name)))?,
        )),
        FieldKind::Ref { table } => rusqlite::types::Value::Integer(resolve_ref(
            table,
            value
                .as_str()
                .ok_or_else(|| crate::Error::new(format!("expected ref id for {}", field.name)))?,
        )?),
    })
}

pub(crate) fn storage_column_name(column: &str) -> String {
    let storage = match column {
        "$createdAt" => "j_created_at".to_owned(),
        "$updatedAt" => "j_updated_at".to_owned(),
        other => user_storage_name(other),
    };
    quote_ident(&storage)
}

fn index_storage_column_name(table: &TableDef, column: &str) -> String {
    match column {
        "$createdAt" => format!("{} DESC", quote_ident("j_created_at")),
        "$updatedAt" => quote_ident("j_updated_at"),
        other => table
            .fields
            .iter()
            .find(|field| field.name == other)
            .map(|field| quote_ident(&storage_column(field)))
            .unwrap_or_else(|| storage_column_name(other)),
    }
}

fn user_storage_name(name: &str) -> String {
    if name.starts_with("j_") {
        format!("u_{name}")
    } else {
        name.to_owned()
    }
}

fn sql_type(kind: &FieldKind) -> &'static str {
    match kind {
        FieldKind::Text => "TEXT",
        FieldKind::Ref { table } => {
            let _ = table;
            "INTEGER"
        }
        FieldKind::Bool => "INTEGER",
    }
}

pub(crate) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{storage, Storage};

    #[test]
    fn current_index_for_created_at_page_queries_matches_query_order() -> Result<()> {
        let schema = SchemaDef::attempt3_fixture();
        let conn = storage::open(Storage::Memory)?;
        install(&conn, &schema)?;

        let open_created_sql: String = conn.query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'todos_current_open_created_v2'",
            [],
            |row| row.get(0),
        )?;
        let created_sql: String = conn.query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'todos_current_created_v2'",
            [],
            |row| row.get(0),
        )?;
        let title_sql: String = conn.query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'todos_current_by_title_v2'",
            [],
            |row| row.get(0),
        )?;

        assert_eq!(
            open_created_sql,
            "CREATE INDEX \"todos_current_open_created_v2\" ON \"todos__schema_v1_current\"(j_branch_num, is_deleted, \"done\", \"j_created_at\" DESC, row_num)"
        );
        assert_eq!(
            created_sql,
            "CREATE INDEX \"todos_current_created_v2\" ON \"todos__schema_v1_current\"(j_branch_num, is_deleted, \"j_created_at\" DESC, row_num)"
        );
        assert_eq!(
            title_sql,
            "CREATE INDEX \"todos_current_by_title_v2\" ON \"todos__schema_v1_current\"(j_branch_num, is_deleted, \"title\", row_num)"
        );
        Ok(())
    }
}
