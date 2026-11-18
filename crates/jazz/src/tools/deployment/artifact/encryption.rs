//! What the deploy, `generate` and the loaders check of an app's encryption
//! declarations (E2EE protocol §1.5, migrations §3.1–§3.2): each entry
//! against its schema version, each migration against the declarations of
//! its two versions, and each space table's read rule.
//!
//! The declarations stay outside the schema and its hash. They travel with
//! the files' history (one set per schema version) and in the deploy
//! request as [`DeclaredEncryption`].

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::catalogue::Permissions;
use crate::tools::public_schema::{
    CmpOp, ColumnDescriptor, ColumnType, EncryptionEntry, EncryptionSettings, PolicyExpr,
    PolicyValue, Schema, TableName, TablePolicies,
};

use super::super::published::{PublishLensOp, PublishMigrationRequest, PublishTableLens};
use super::system_tables::{MEMBERS, is_system_table};

/// A schema version's encryption entries, by table.
pub type Entries = BTreeMap<String, EncryptionEntry>;

/// An app's encryption declarations: the app-wide settings and every
/// schema version's entries. What the deploy request carries and generated
/// bindings embed.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeclaredEncryption {
    /// The top-level `encryption` object of the app file.
    #[serde(default)]
    pub settings: EncryptionSettings,
    /// Each schema version's entries, by the version's schema hash (which
    /// covers Jazz's system tables). A version that declares none is
    /// absent.
    #[serde(default)]
    pub versions: BTreeMap<String, Entries>,
}

impl DeclaredEncryption {
    /// The entries of the version whose schema hash is `hash`.
    pub fn entries(&self, hash: &str) -> Option<&Entries> {
        self.versions.get(hash).filter(|e| !e.is_empty())
    }

    /// Whether nothing is declared.
    pub fn is_empty(&self) -> bool {
        self.versions.values().all(BTreeMap::is_empty)
            && self.settings == EncryptionSettings::default()
    }
}

/// A refusal inside an entry: the path below the entry (`.scope`,
/// `.columns.title`) and why.
pub type EntryRefusal = (String, String);

fn column<'a>(schema: &'a Schema, table: &str, column: &str) -> Option<&'a ColumnDescriptor> {
    schema
        .get(&TableName::from(table))?
        .columns
        .columns
        .iter()
        .find(|c| c.name_str() == column)
}

/// The prefix of a table's equality columns, which hold an encrypted
/// column's equality tokens.
pub const EQUALITY_PREFIX: &str = "__e2ee_eq_";

/// The name of a table's equality column for `column`.
pub fn equality_column(column: &str) -> String {
    format!("{EQUALITY_PREFIX}{column}")
}

/// Whether `column` is a stored ciphertext column: non-null `bytes`.
fn is_cipher(column: &ColumnDescriptor) -> bool {
    column.column_type == ColumnType::Bytea && !column.nullable
}

/// Check that `space` names the space row of `scope` from `table`: `id`
/// when `table` is the scope itself, else a `uuid` column referencing it.
fn check_space(
    table: &str,
    space: &str,
    scope: &str,
    schema: &Schema,
    at: &str,
) -> Result<(), EntryRefusal> {
    if is_system_table(scope) {
        return Err((
            format!("{at}.scope"),
            format!("`{scope}` is one of Jazz's system tables and can't be a space table"),
        ));
    }
    if schema.get(&TableName::from(scope)).is_none() {
        return Err((
            format!("{at}.scope"),
            format!(
                "`scope` must be a table with an `id`; this schema version has no table {scope}{}",
                super::expr::did_you_mean(scope, schema.keys().map(|t| t.as_str()))
            ),
        ));
    }
    if space == "id" {
        if scope != table {
            return Err((
                format!("{at}.space"),
                format!(
                    "`space` is `id` only when the table is its own space table; for rows in \
                     spaces of {scope}, name the column referencing {scope}"
                ),
            ));
        }
        return Ok(());
    }
    match column(schema, table, space) {
        Some(c)
            if c.column_type == ColumnType::Uuid
                && c.references.is_some_and(|t| t.as_str() == scope) =>
        {
            Ok(())
        }
        Some(_) => Err((
            format!("{at}.space"),
            format!(
                "`space` must be `id` or a `uuid` column referencing {scope}; column {space} \
                 isn't one"
            ),
        )),
        None => Err((
            format!("{at}.space"),
            format!("table {table} has no column {space}"),
        )),
    }
}

/// Check one table's entry against its schema version (protocol §1.5,
/// migrations §3.2). Deploy, `generate` and the loaders share it.
pub fn check_entry(
    table: &str,
    entry: &EncryptionEntry,
    schema: &Schema,
) -> Result<(), EntryRefusal> {
    check_space(table, &entry.space, &entry.scope, schema, "")?;
    if let Some(rehome) = &entry.rehome {
        check_space(table, &rehome.space, &rehome.scope, schema, ".rehome")?;
    }
    for (name, declared) in &entry.columns {
        let at = format!(".columns.{name}");
        let Some(stored) = column(schema, table, name) else {
            return Err((at, format!("table {table} has no column {name}")));
        };
        if name == "id" || *name == entry.space {
            return Err((
                at,
                "the space column and `id` stay plaintext: the server routes rows by them".into(),
            ));
        }
        if !is_cipher(stored) {
            return Err((
                at,
                format!(
                    "an encrypted column is stored as required `bytes` (its ciphertext); column \
                     {name} isn't"
                ),
            ));
        }
        if stored.merge_strategy.is_some() {
            return Err((
                at,
                "encrypted columns are last-writer-wins: the server can't merge ciphertext, so \
                 a merge strategy is refused"
                    .into(),
            ));
        }
        if declared.from.is_some() && declared.to.is_some() {
            return Err((
                at,
                "a column converts `from` a source or `to` a target, not both".into(),
            ));
        }
        if declared.reverse.is_some() && declared.convert.is_none() {
            return Err((
                format!("{at}.reverse"),
                "`reverse` comes with `convert`".into(),
            ));
        }
        if let Some(to) = &declared.to {
            let Some(target) = column(schema, table, to) else {
                return Err((
                    format!("{at}.to"),
                    format!("table {table} has no column {to}"),
                ));
            };
            if entry.columns.contains_key(to) || !target.nullable {
                return Err((
                    format!("{at}.to"),
                    format!("a decrypt's target is an optional plaintext column; {to} isn't one"),
                ));
            }
            check_convert(
                &declared.plain.column_type,
                &target.column_type,
                declared.convert.is_some(),
                &at,
            )?;
        }
        if let Some(from) = &declared.from {
            let Some(source) = column(schema, table, from) else {
                return Err((
                    format!("{at}.from"),
                    format!("table {table} has no column {from}"),
                ));
            };
            if entry.columns.contains_key(from) {
                return Err((
                    format!("{at}.from"),
                    format!(
                        "a conversion's source is no longer an encrypted column of the entry; \
                         {from} still is"
                    ),
                ));
            }
            // A plaintext source has its own type; an encrypted one is
            // checked against the previous version's declaration.
            if !is_cipher(source) {
                check_convert(
                    &source.column_type,
                    &declared.plain.column_type,
                    declared.convert.is_some(),
                    &at,
                )?;
            }
        } else if entry.rehome.is_some() {
            return Err((
                at,
                "during a re-home every column names the old encrypted column it comes `from`"
                    .into(),
            ));
        }
        if declared.from.is_none() && declared.to.is_none() && declared.convert.is_some() {
            return Err((
                format!("{at}.convert"),
                "`convert` comes with a conversion (`from` or `to`)".into(),
            ));
        }
    }
    let mut seen = BTreeSet::new();
    for name in &entry.equality {
        let at = ".equality".to_string();
        if !seen.insert(name) {
            return Err((at, format!("`{name}` is listed twice")));
        }
        if !entry.columns.contains_key(name) {
            return Err((
                at,
                format!("`{name}` is not an encrypted column of the entry"),
            ));
        }
        let tag = equality_column(name);
        if !column(schema, table, &tag).is_some_and(is_cipher) {
            return Err((
                at,
                format!(
                    "an equality column's tokens are stored in a required `bytes` column {tag}"
                ),
            ));
        }
    }
    for name in &entry.backfill {
        if !entry.equality.contains(name) {
            return Err((
                ".backfill".into(),
                format!("`{name}` is not an equality column of the entry"),
            ));
        }
    }
    Ok(())
}

/// `convert` is required when a conversion changes the type, and refused
/// when it doesn't (migrations §3.2).
fn check_convert(
    from: &ColumnType,
    to: &ColumnType,
    has_convert: bool,
    at: &str,
) -> Result<(), EntryRefusal> {
    match (from == to, has_convert) {
        (false, false) => Err((
            at.to_string(),
            "the conversion changes the type, so it names a `convert` function".into(),
        )),
        (true, true) => Err((
            format!("{at}.convert"),
            "the conversion keeps the type, so it takes no `convert`".into(),
        )),
        _ => Ok(()),
    }
}

// ---------------------------------------------------------------------------
// The transition check
// ---------------------------------------------------------------------------

/// A migration's view of one table: which old column each new one comes
/// from, and where each old one goes.
struct TableMap<'a> {
    lens: Option<&'a PublishTableLens>,
}

impl TableMap<'_> {
    /// The old name of new column `column`, or `None` for one the
    /// migration introduces.
    fn old_name(&self, column: &str) -> Option<String> {
        let Some(lens) = self.lens else {
            return Some(column.to_owned());
        };
        if lens.added {
            return None;
        }
        for op in &lens.operations {
            match op {
                PublishLensOp::Rename { column: was, value } if value == column => {
                    return Some(was.clone());
                }
                PublishLensOp::Introduce { column: c, .. } if c == column => return None,
                _ => {}
            }
        }
        Some(column.to_owned())
    }

    /// The new name of old column `column`, or `None` when the migration
    /// drops it.
    fn new_name(&self, column: &str) -> Option<String> {
        let Some(lens) = self.lens else {
            return Some(column.to_owned());
        };
        for op in &lens.operations {
            match op {
                PublishLensOp::Rename { column: was, value } if was == column => {
                    return Some(value.clone());
                }
                PublishLensOp::Drop { column: c, .. } if c == column => return None,
                _ => {}
            }
        }
        Some(column.to_owned())
    }
}

/// Refuse a migration that changes an encrypted column's type, scope or
/// space without a conversion, or renames an equality column apart from
/// its column (migrations §3.1, §3.2, §4.3). `from`/`to` are the two
/// versions' schemas and entries.
pub fn check_transition(
    from: (&Schema, Option<&Entries>),
    to: (&Schema, Option<&Entries>),
    migration: &PublishMigrationRequest,
) -> Result<(), String> {
    let empty = Entries::new();
    let (old_schema, old_entries) = (from.0, from.1.unwrap_or(&empty));
    let (new_schema, new_entries) = (to.0, to.1.unwrap_or(&empty));
    let lens_of = |table: &str| migration.forward.iter().find(|l| l.table == table);
    // The old name of each new table, and the new name of each old one.
    let old_table = |table: &str| -> Option<String> {
        match lens_of(table) {
            Some(lens) if lens.added => None,
            Some(lens) => Some(
                lens.renamed_from
                    .clone()
                    .unwrap_or_else(|| table.to_owned()),
            ),
            None => Some(table.to_owned()),
        }
    };
    let new_table = |table: &str| -> Option<String> {
        if let Some(lens) = migration
            .forward
            .iter()
            .find(|l| l.renamed_from.as_deref() == Some(table))
        {
            return Some(lens.table.clone());
        }
        match lens_of(table) {
            Some(lens) if lens.removed => None,
            _ => new_schema
                .contains_key(&TableName::from(table))
                .then(|| table.to_owned()),
        }
    };

    // Every old encrypted column the new version keeps stays encrypted,
    // or is a conversion's source.
    for (table, old) in old_entries {
        let Some(renamed) = new_table(table) else {
            continue;
        };
        let map = TableMap {
            lens: lens_of(&renamed),
        };
        let new = new_entries.get(&renamed);
        for name in old.columns.keys() {
            let Some(kept) = map.new_name(name) else {
                continue;
            };
            let declared = new.is_some_and(|e| e.columns.contains_key(&kept));
            let source = new.is_some_and(|e| {
                e.columns
                    .values()
                    .any(|c| c.from.as_deref() == Some(kept.as_str()))
            });
            if !declared && !source {
                return Err(format!(
                    "column {renamed}.{kept} holds ciphertext but is no longer declared \
                     encrypted; decrypt it with a conversion (`\"to\"` naming a new plaintext \
                     column) or drop it"
                ));
            }
        }
    }

    for (table, new) in new_entries {
        let Some(was) = old_table(table) else {
            continue;
        };
        let map = TableMap {
            lens: lens_of(table),
        };
        let old = old_entries.get(&was);
        let old_column = |name: &str| {
            map.old_name(name)
                .and_then(|n| column(old_schema, &was, &n))
        };
        if let Some(old) = old {
            let mapped = |t: &str| new_table(t).unwrap_or_else(|| t.to_owned());
            if new.rehome.is_none() {
                if mapped(&old.scope) != new.scope {
                    return Err(format!(
                        "table {table} moves its space table (`scope`) from {} to {}, which \
                         changes every cell's context; write it as a re-home conversion \
                         (`\"rehome\"`)",
                        old.scope, new.scope
                    ));
                }
                let space = if old.space == "id" {
                    Some("id".to_owned())
                } else {
                    map.new_name(&old.space)
                };
                if space.as_deref() != Some(new.space.as_str()) {
                    return Err(format!(
                        "table {table} changes its space column from {} to {}; write it as a \
                         re-home conversion (`\"rehome\"`)",
                        old.space, new.space
                    ));
                }
            }
        }
        for (name, declared) in &new.columns {
            let path = format!("{table}.{name}");
            if let Some(source) = &declared.from {
                let old_source = map.old_name(source);
                match old_source
                    .as_deref()
                    .and_then(|s| old.and_then(|o| o.columns.get(s)))
                {
                    // A re-encrypt (or re-home) from the old encrypted column.
                    Some(previous) => check_convert(
                        &previous.plain.column_type,
                        &declared.plain.column_type,
                        declared.convert.is_some(),
                        "",
                    )
                    .map_err(|(_, message)| format!("{path}: {message}"))?,
                    // An encrypt from a plaintext column.
                    None => {
                        if old_column(source).is_some_and(|c| c.nullable) {
                            return Err(format!(
                                "{path}: encrypting {table}.{source} in place is refused because \
                                 it was optional: after a conversion clears it, an older \
                                 client's null would be lost. Copy the rows into a new \
                                 encrypted table instead (migrations §5.1)"
                            ));
                        }
                        if let Some(c) = old_column(source) {
                            check_convert(
                                &c.column_type,
                                &declared.plain.column_type,
                                declared.convert.is_some(),
                                "",
                            )
                            .map_err(|(_, message)| format!("{path}: {message}"))?;
                        }
                    }
                }
                continue;
            }
            let previous = map
                .old_name(name)
                .and_then(|n| old.and_then(|o| o.columns.get(&n).map(|c| (n, c))));
            match previous {
                Some((old_name, previous)) => {
                    if declared.cell_type() != previous.cell_type() {
                        return Err(format!(
                            "{path}: the encrypted column's type changes from {:?} to {:?}, \
                             which its cells were sealed under; re-encrypt it into a new column \
                             (`\"from\"`, with `\"convert\"` unless the values carry over), or \
                             pin the old type as `\"sealedType\"` for a widening",
                            previous.cell_type(),
                            declared.cell_type()
                        ));
                    }
                    if declared.plain != *declared.cell_type()
                        && !widens(declared.cell_type(), &declared.plain)
                    {
                        return Err(format!(
                            "{path}: only making a column optional or appending enum variants \
                             keeps its sealed type; re-encrypt it into a new column"
                        ));
                    }
                    let tagged_before = old.is_some_and(|o| o.equality.contains(&old_name));
                    if tagged_before
                        && new.equality.contains(name)
                        && map.old_name(&equality_column(name)) != Some(equality_column(&old_name))
                    {
                        return Err(format!(
                            "{path}: renaming the encrypted column {old_name} also renames \
                             {} to {}",
                            equality_column(&old_name),
                            equality_column(name)
                        ));
                    }
                }
                None => {
                    // A column the old version stored as plaintext.
                    if old_column(name).is_some() {
                        return Err(format!(
                            "{path}: column {name} held plaintext in the earlier version; \
                             encrypt it with a conversion (`\"from\"` naming the plaintext \
                             column)"
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Whether `to` reads every value of `from` unchanged: the same type made
/// optional, or an enum with variants appended.
fn widens(
    from: &crate::tools::public_schema::PlainType,
    to: &crate::tools::public_schema::PlainType,
) -> bool {
    let types = match (&from.column_type, &to.column_type) {
        (ColumnType::Enum { variants: a }, ColumnType::Enum { variants: b }) => b.starts_with(a),
        (a, b) => a == b,
    };
    types && (to.nullable || !from.nullable)
}

// ---------------------------------------------------------------------------
// Space tables' read rules and the deploy's warnings
// ---------------------------------------------------------------------------

const OUTER_ROW: &str = "__jazz_outer_row";

/// Whether `expr` is exactly `M(table)`: `exists __e2ee_members(scopeId =
/// table_id('table'), identifier = this.id, accountId = session.user)`,
/// its conditions in any order (protocol R13).
pub fn is_members_rule(expr: &PolicyExpr, table: &str) -> bool {
    let PolicyExpr::Exists {
        table: members,
        condition,
    } = expr
    else {
        return false;
    };
    let PolicyExpr::And(conditions) = condition.as_ref() else {
        return false;
    };
    let eq = |c: &PolicyExpr, name: &str, value: &PolicyValue| matches!(c, PolicyExpr::Cmp { column, op: CmpOp::Eq, value: v } if column == name && v == value);
    let wanted = [
        (
            "scopeId",
            PolicyValue::TableIdentity {
                table: table.to_owned(),
            },
        ),
        (
            "identifier",
            PolicyValue::SessionRef(vec![OUTER_ROW.to_owned(), "id".to_owned()]),
        ),
        (
            "accountId",
            PolicyValue::SessionRef(vec!["user".to_owned()]),
        ),
    ];
    members == MEMBERS
        && conditions.len() == wanted.len()
        && wanted
            .iter()
            .all(|(name, value)| conditions.iter().any(|c| eq(c, name, value)))
}

/// Whether a read rule comes from the members table: `M(table)`, or an
/// `And` with `M(table)` as one of its direct conjuncts.
fn comes_from_members(rule: Option<&PolicyExpr>, table: &str) -> bool {
    match rule {
        Some(PolicyExpr::And(conjuncts)) => conjuncts.iter().any(|c| is_members_rule(c, table)),
        Some(rule) => is_members_rule(rule, table),
        None => false,
    }
}

/// The space tables of `entries`: each `scope`, and whether some entry
/// naming it encrypts at least one column.
fn scopes(entries: &Entries) -> BTreeMap<&str, bool> {
    let mut scopes = BTreeMap::new();
    for entry in entries.values() {
        *scopes.entry(entry.scope.as_str()).or_insert(false) |= !entry.columns.is_empty();
    }
    scopes
}

/// Refuse a space table whose read rule doesn't come from the members
/// table (protocol R13), naming it. Only space tables of an entry that
/// encrypts a column are checked: an entry without encrypted columns lets
/// an app write member rows first (migrations §4.1).
pub fn check_space_read_rules(permissions: &Permissions, entries: &Entries) -> Result<(), String> {
    for (scope, encrypts) in scopes(entries) {
        let rule = permissions
            .get(&TableName::from(scope))
            .and_then(TablePolicies::select_policy);
        if encrypts && !comes_from_members(rule, scope) {
            return Err(format!(
                "space table {scope}: its read rule must come from the members table, so that \
                 only members read its rows and every member gets their keys. Write it as \
                 `exists __e2ee_members(scopeId = table_id('{scope}'), identifier = this.id, \
                 accountId = session.user)`, alone or conjoined (`allOf`) with further \
                 conditions"
            ));
        }
    }
    Ok(())
}

fn conjuncts(expr: &PolicyExpr) -> Vec<&PolicyExpr> {
    match expr {
        PolicyExpr::And(items) => items.iter().flat_map(conjuncts).collect(),
        other => vec![other],
    }
}

/// Whether `expr` reads a session value or `$createdBy` (which tie an
/// update to who makes it). Inheritance and relation reads may, so they
/// count.
fn reads_session(expr: &PolicyExpr) -> bool {
    let value = |v: &PolicyValue| matches!(v, PolicyValue::SessionRef(path) if path.first().map(String::as_str) != Some(OUTER_ROW));
    match expr {
        PolicyExpr::Cmp {
            column, value: v, ..
        }
        | PolicyExpr::Contains { column, value: v } => value(v) || column.starts_with("$createdBy"),
        PolicyExpr::InList { column, values } => {
            values.iter().any(value) || column.starts_with("$createdBy")
        }
        PolicyExpr::IsNull { column } | PolicyExpr::IsNotNull { column } => {
            column.starts_with("$createdBy")
        }
        PolicyExpr::SessionCmp { .. }
        | PolicyExpr::SessionIsNull { .. }
        | PolicyExpr::SessionIsNotNull { .. }
        | PolicyExpr::SessionContains { .. }
        | PolicyExpr::SessionInList { .. }
        | PolicyExpr::In { .. }
        | PolicyExpr::ExistsRel { .. }
        | PolicyExpr::Inherits { .. }
        | PolicyExpr::InheritsReferencing { .. }
        | PolicyExpr::InheritsFrom { .. } => true,
        PolicyExpr::Exists { condition, .. } => reads_session(condition),
        PolicyExpr::And(items) | PolicyExpr::Or(items) => items.iter().any(reads_session),
        PolicyExpr::Not(inner) => reads_session(inner),
        _ => false,
    }
}

/// Whether `expr` reads a member row of the row being checked: an `exists`
/// over the members table with `identifier = this.id`.
fn reads_own_member_row(expr: &PolicyExpr) -> bool {
    match expr {
        PolicyExpr::Exists { table, condition } if table == MEMBERS => conjuncts(condition)
            .iter()
            .any(|c| {
                matches!(c, PolicyExpr::Cmp { column, op: CmpOp::Eq, value: PolicyValue::SessionRef(path) }
                    if column == "identifier" && path == &[OUTER_ROW.to_owned(), "id".to_owned()])
            }),
        PolicyExpr::Exists { condition, .. } => reads_own_member_row(condition),
        PolicyExpr::And(items) | PolicyExpr::Or(items) => items.iter().any(reads_own_member_row),
        PolicyExpr::Not(inner) => reads_own_member_row(inner),
        _ => false,
    }
}

/// Whether an update policy can only pass on a change: a column compared
/// with one literal in `USING` and with another in `WITH CHECK`, as direct
/// conjuncts.
fn requires_transition(using: &PolicyExpr, check: &PolicyExpr) -> bool {
    let literals = |expr: &PolicyExpr| -> Vec<(String, CmpOp, PolicyValue)> {
        conjuncts(expr)
            .into_iter()
            .filter_map(|c| match c {
                PolicyExpr::Cmp {
                    column,
                    op,
                    value: value @ PolicyValue::Literal(_),
                } => Some((column.clone(), op.clone(), value.clone())),
                _ => None,
            })
            .collect()
    };
    let (old, new) = (literals(using), literals(check));
    old.iter().any(|(column, op, value)| {
        new.iter().any(|(c, o, v)| {
            c == column
                && match (op, o) {
                    (CmpOp::Eq, CmpOp::Eq) => v != value,
                    (CmpOp::Eq, CmpOp::Ne) | (CmpOp::Ne, CmpOp::Eq) => v == value,
                    _ => false,
                }
        })
    })
}

/// Whether `expr` involves the space row of an encrypted table whose
/// space column is `space` in table `scope`: inheritance through the space
/// column, an `exists` over the space table, or the members table.
fn involves_space_row(expr: &PolicyExpr, space: &str, scope: &str) -> bool {
    match expr {
        PolicyExpr::Inherits { via_column, .. } => via_column == space,
        PolicyExpr::Exists { table, condition } => {
            table == scope || table == MEMBERS || involves_space_row(condition, space, scope)
        }
        PolicyExpr::ExistsRel { .. } => true,
        PolicyExpr::And(items) | PolicyExpr::Or(items) => {
            items.iter().any(|i| involves_space_row(i, space, scope))
        }
        PolicyExpr::Not(inner) => involves_space_row(inner, space, scope),
        _ => false,
    }
}

/// The deploy's warnings about what it can see of an encrypted app's
/// policies (protocol §1.5). Each names the table; none claims more than
/// the syntax shows.
pub fn warnings(permissions: &Permissions, entries: &Entries) -> Vec<String> {
    let mut warnings = Vec::new();
    let policies = |table: &str| permissions.get(&TableName::from(table));
    for (scope, encrypts) in scopes(entries) {
        let space_policies = policies(scope);
        let read = space_policies.and_then(TablePolicies::select_policy);
        if !encrypts && !comes_from_members(read, scope) {
            warnings.push(format!(
                "space table {scope}: its read rule doesn't come from the members table yet; \
                 write its member rows, then change it before encrypting columns in its spaces"
            ));
        }
        let Some(space_policies) = space_policies else {
            continue;
        };
        let (using, check) = (
            space_policies.update_using_policy(),
            space_policies.update_check_policy(),
        );
        if let (Some(using), Some(check)) = (using, check)
            && requires_transition(using, check)
        {
            warnings.push(format!(
                "space table {scope}: its update rule needs a column to change, so no save that \
                 changes nothing passes it: nobody can add members, create the space or rotate \
                 its key"
            ));
        }
        let broad = [using, check].into_iter().flatten().any(|clause| {
            matches!(clause, PolicyExpr::True)
                || is_members_rule(clause, scope)
                || !reads_session(clause)
        });
        if broad {
            warnings.push(format!(
                "space table {scope}: its update rule is broad (true, the members rule, or no \
                 session value or $createdBy), and whoever may update a space row manages its \
                 members, creates its space and rotates its key"
            ));
        }
        if space_policies
            .insert_policy()
            .is_some_and(reads_own_member_row)
        {
            warnings.push(format!(
                "space table {scope}: its insert rule reads the members table for the row being \
                 inserted, which no member row can satisfy yet; let the creator insert it, e.g. \
                 `$createdBy.account = session.user`"
            ));
        }
    }
    for (table, entry) in entries {
        if entry.columns.is_empty() || *table == entry.scope {
            continue;
        }
        let (Some(read), Some(space_read)) = (
            policies(table).and_then(TablePolicies::select_policy),
            policies(&entry.scope).and_then(TablePolicies::select_policy),
        ) else {
            continue;
        };
        let space_conjuncts = conjuncts(space_read);
        let own = conjuncts(read);
        if own.len() > space_conjuncts.len() && space_conjuncts.iter().all(|c| own.contains(c)) {
            warnings.push(format!(
                "table {table}: its read rule is its space table {}'s plus further conditions, \
                 so every member of a space can decrypt rows of {table} that Jazz doesn't send \
                 them",
                entry.scope
            ));
        }
        if !involves_space_row(read, &entry.space, &entry.scope) {
            warnings.push(format!(
                "table {table}: its read rule doesn't involve its space row in {}, so a session \
                 that isn't a member may receive its ciphertext and never a key \
                 (`key-unavailable`)",
                entry.scope
            ));
        }
    }
    warnings
}

/// Refuse a policy that reads an encrypted column's value: the server
/// sees only its ciphertext, so no comparison with it can mean what it says
/// (migrations §4.1). Columns are read where a policy names them: its own
/// table's, an `exists` table's, and the checked row's through `this.col`.
/// A relation (`exists (…)`) is judged conservatively: a column it names
/// counts as a column of every table it might belong to.
pub fn check_encrypted_reads(permissions: &Permissions, entries: &Entries) -> Result<(), String> {
    fn encrypted(entries: &Entries, table: &str, column: &str) -> bool {
        entries
            .get(table)
            .is_some_and(|entry| entry.columns.contains_key(column))
    }
    fn reads<'a>(
        expr: &'a PolicyExpr,
        table: &'a str,
        outer: Option<&'a str>,
        entries: &Entries,
    ) -> Option<(String, String)> {
        let column = |name: &str| {
            encrypted(entries, table, name).then(|| (table.to_owned(), name.to_owned()))
        };
        let value = |value: &PolicyValue| match value {
            PolicyValue::SessionRef(path) => match (path.as_slice(), outer) {
                ([row, name], Some(outer))
                    if row == OUTER_ROW && encrypted(entries, outer, name) =>
                {
                    Some((outer.to_owned(), name.clone()))
                }
                _ => None,
            },
            _ => None,
        };
        match expr {
            PolicyExpr::Cmp {
                column: name,
                value: v,
                ..
            }
            | PolicyExpr::Contains {
                column: name,
                value: v,
            } => column(name).or_else(|| value(v)),
            PolicyExpr::InList {
                column: name,
                values,
            } => column(name).or_else(|| values.iter().find_map(value)),
            PolicyExpr::IsNull { column: name }
            | PolicyExpr::IsNotNull { column: name }
            | PolicyExpr::In { column: name, .. } => column(name),
            PolicyExpr::Exists {
                table: inner,
                condition,
            } => reads(condition, inner, Some(table), entries),
            PolicyExpr::And(items) | PolicyExpr::Or(items) => items
                .iter()
                .find_map(|item| reads(item, table, outer, entries)),
            PolicyExpr::Not(inner) => reads(inner, table, outer, entries),
            PolicyExpr::ExistsRel { rel } => relation_reads(rel, table, entries),
            _ => None,
        }
    }
    /// A relation's column references, conservatively: a scoped one names
    /// the table its scope aliases (or, unresolved, any table the relation
    /// or the policy reads); an unscoped one any of them.
    fn relation_reads(
        rel: &crate::tools::public_schema::RelationExpr,
        table: &str,
        entries: &Entries,
    ) -> Option<(String, String)> {
        fn walk(
            value: &serde_json::Value,
            scans: &mut Vec<(String, Option<String>)>,
            columns: &mut Vec<(Option<String>, String)>,
        ) {
            match value {
                serde_json::Value::Object(map) => {
                    if let Some(serde_json::Value::Object(scan)) = map.get("TableScan")
                        && let Some(serde_json::Value::String(t)) = scan.get("table")
                    {
                        let alias = scan
                            .get("alias")
                            .and_then(|a| a.as_str())
                            .map(str::to_owned);
                        scans.push((t.clone(), alias));
                    }
                    if let Some(serde_json::Value::String(column)) = map.get("column") {
                        let scope = map.get("scope").and_then(|s| s.as_str()).map(str::to_owned);
                        columns.push((scope, column.clone()));
                    }
                    map.values().for_each(|item| walk(item, scans, columns));
                }
                serde_json::Value::Array(items) => {
                    items.iter().for_each(|item| walk(item, scans, columns));
                }
                _ => {}
            }
        }
        let (mut scans, mut columns) = (Vec::new(), Vec::new());
        walk(
            &serde_json::to_value(rel).unwrap_or_default(),
            &mut scans,
            &mut columns,
        );
        let every: Vec<&str> = scans
            .iter()
            .map(|(t, _)| t.as_str())
            .chain(std::iter::once(table))
            .collect();
        columns.into_iter().find_map(|(scope, column)| {
            let scoped: Option<&str> = scope.as_deref().and_then(|scope| {
                scans
                    .iter()
                    .find(|(t, alias)| alias.as_deref() == Some(scope) || t == scope)
                    .map(|(t, _)| t.as_str())
            });
            let candidates = match scoped {
                Some(one) => vec![one],
                None => every.clone(),
            };
            candidates
                .into_iter()
                .find(|t| encrypted(entries, t, &column))
                .map(|t| (t.to_owned(), column.clone()))
        })
    }
    for (table, policies) in permissions {
        let clauses = [
            ("read", &policies.select),
            ("insert", &policies.insert),
            ("update", &policies.update),
            ("delete", &policies.delete),
        ];
        for (op, policy) in clauses {
            if let Some((owner, column)) = [&policy.using, &policy.with_check]
                .into_iter()
                .flatten()
                .find_map(|expr| reads(expr, table.as_str(), None, entries))
            {
                return Err(format!(
                    "tables.{table}.{op}: the policy reads {owner}.{column}, an encrypted \
                     column; the server sees only its ciphertext. Write the policy over \
                     plaintext columns"
                ));
            }
        }
    }
    Ok(())
}

/// Refuse an app policy that reaches a system table: `exists` over any but
/// the members table, inheritance from one, or a relation reading one
/// (protocol §1.3). `InheritsFrom` and `InsertedByThisTransaction` are the
/// permissions reader's to refuse.
pub fn reaches_system_table(expr: &PolicyExpr) -> Option<String> {
    fn relation_tables(value: &serde_json::Value, out: &mut Vec<String>) {
        match value {
            serde_json::Value::Object(map) => {
                for (key, item) in map {
                    match (key.as_str(), item) {
                        ("table", serde_json::Value::String(t)) => out.push(t.clone()),
                        _ => relation_tables(item, out),
                    }
                }
            }
            serde_json::Value::Array(items) => {
                items.iter().for_each(|i| relation_tables(i, out));
            }
            _ => {}
        }
    }
    match expr {
        PolicyExpr::Exists { table, condition } => {
            if is_system_table(table) && table != MEMBERS {
                return Some(table.clone());
            }
            reaches_system_table(condition)
        }
        PolicyExpr::InheritsReferencing { source_table, .. } if is_system_table(source_table) => {
            Some(source_table.clone())
        }
        PolicyExpr::ExistsRel { rel } => {
            let mut tables = Vec::new();
            relation_tables(&serde_json::to_value(rel).unwrap_or_default(), &mut tables);
            tables
                .into_iter()
                .find(|t| is_system_table(t) && t != MEMBERS)
        }
        PolicyExpr::And(items) | PolicyExpr::Or(items) => {
            items.iter().find_map(reaches_system_table)
        }
        PolicyExpr::Not(inner) => reaches_system_table(inner),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Declarations the server keeps per schema version
// ---------------------------------------------------------------------------

/// The name and version of the stored form of one schema version's
/// declarations: this line, a newline, then the entries as canonical JSON
/// (keys sorted at every level, no whitespace).
pub const STORED_DECLARATIONS_V1: &str = "jazz-e2ee-declarations-v1";

/// One schema version's entries in their stored form
/// ([`STORED_DECLARATIONS_V1`]).
pub fn encode_stored(entries: &Entries) -> Result<Vec<u8>, String> {
    let json = crate::catalogue::canonical_json(entries).map_err(|error| error.to_string())?;
    Ok(format!("{STORED_DECLARATIONS_V1}\n{json}").into_bytes())
}

/// The entries [`encode_stored`] wrote. Another form is refused, never
/// guessed at.
pub fn decode_stored(bytes: &[u8]) -> Result<Entries, String> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| "stored encryption declarations are not text".to_owned())?;
    let Some(json) = text
        .strip_prefix(STORED_DECLARATIONS_V1)
        .and_then(|rest| rest.strip_prefix('\n'))
    else {
        return Err(format!(
            "stored encryption declarations are not in the `{STORED_DECLARATIONS_V1}` form; a \
             newer Jazz wrote them"
        ));
    };
    serde_json::from_str(json)
        .map_err(|error| format!("stored encryption declarations don't read: {error}"))
}

/// Refuse declarations `given` for a schema version deployed before with
/// `stored`, unless the change is one a migration could make without a
/// conversion (migrations §2, §4.3): the schema is the same, so every
/// encrypted column keeps the type its cells were sealed under, its scope
/// and its space, or pins its old type as `sealedType` for a widening.
pub fn check_redeclaration(
    schema: &Schema,
    stored: &Entries,
    given: &Entries,
) -> Result<(), String> {
    let same_version = PublishMigrationRequest {
        from_hash: String::new(),
        to_hash: String::new(),
        forward: Vec::new(),
    };
    check_transition((schema, Some(stored)), (schema, Some(given)), &same_version).map_err(
        |reason| {
            format!(
                "this schema version was deployed with other encryption declarations, and \
                 its cells are sealed under them: {reason}"
            )
        },
    )
}

/// The decrypt conversions (`to`) in `given` that `stored` (the version's
/// declarations deployed before, if any) doesn't have yet, as
/// `table.column`: each needs the deploy's explicit confirmation, since
/// the server and every replica then hold the plaintext (migrations
/// §4.2).
pub fn new_decrypts(stored: Option<&Entries>, given: &Entries) -> Vec<String> {
    let mut decrypts = Vec::new();
    for (table, entry) in given {
        for (column, declared) in &entry.columns {
            let Some(to) = &declared.to else {
                continue;
            };
            let known = stored
                .and_then(|s| s.get(table))
                .and_then(|e| e.columns.get(column))
                .is_some_and(|c| c.to.as_ref() == Some(to));
            if !known {
                decrypts.push(format!("{table}.{column}"));
            }
        }
    }
    decrypts
}
