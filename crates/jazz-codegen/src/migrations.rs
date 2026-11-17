//! `migrations create`: when `tables` changed since the bindings were
//! generated, move the version they were generated from into `history`
//! (named by its short hash) and write the migration from it, guessing what
//! the diff allows and leaving a `"TODO: …"` string wherever only the author
//! can decide. Loaders refuse a file with a TODO, naming each, so a stub is
//! never deployed half-written.
//!
//! The diff is the one `jazz-tools migrations create` ran in TS
//! (`dev/migrations.ts`), moved here so every language shares it.

use jazz::tools::deployment::artifact::{self, json::Json};
use jazz::tools::public_schema::{ColumnDescriptor, ColumnType, Schema, SchemaHash, TableSchema};

use crate::bindings;
use crate::model::App;
use crate::{CodegenError, Lang};

/// What [`create`] did.
#[derive(Debug, Clone, PartialEq)]
pub struct Created {
    /// The new `jazz.app.json` text (the input, unchanged, if nothing changed).
    pub app: String,
    /// Whether a version and migration were added.
    pub changed: bool,
    /// JSON paths of the TODOs the author must resolve, in file order.
    pub todos: Vec<String>,
    /// Why nothing was written, when it isn't simply that nothing changed.
    pub note: Option<String>,
}

/// Write the migration from the version `bindings` (the committed file in
/// `lang`) were generated from to the file's current `tables`. `name` is
/// recorded as the migration's `name`.
pub fn create(
    lang: Lang,
    app_json: &[u8],
    bindings: Option<&str>,
    name: &str,
) -> Result<Created, CodegenError> {
    let text = std::str::from_utf8(app_json)
        .map_err(|_| CodegenError::new(format!("{}: not UTF-8", artifact::APP_FILE)))?;
    let unchanged = || Created {
        app: text.to_string(),
        changed: false,
        todos: Vec::new(),
        note: None,
    };
    let app = App::load(app_json).map_err(CodegenError::from_artifact)?;
    let Some(bindings) = bindings else {
        // No bindings: the app has no earlier version yet.
        return Ok(unchanged());
    };
    let recorded = bindings::read(lang, bindings)?;
    if app
        .compiled
        .schemas
        .iter()
        .any(|(h, _)| *h == recorded.current)
    {
        return Ok(unchanged());
    }
    let from = &recorded.current_schema;
    let to = app.compiled.current_schema();
    // Single-column indexes change the hash, not the version or the rows
    // (#5494): there is nothing to migrate, as `bindings::staleness` says.
    if bindings::differs_only_in_indexes(from, to) {
        return Ok(Created {
            note: Some(
                "only single-column indexes changed: no migration needed; run `npx jazz-tools generate`".into(),
            ),
            ..unchanged()
        });
    }
    let version = SchemaHash::compute(from).to_string()[..12].to_string();

    let mut root = artifact::json::parse(app_json).map_err(CodegenError::new)?;
    let Json::Object(top) = &mut root else {
        unreachable!("loaded above")
    };
    let history = entry(top, "history", || Json::Object(Vec::new()), "tables");
    let Json::Object(versions) = history else {
        unreachable!("loaded above")
    };
    if versions.iter().any(|(k, _)| *k == version) {
        return Err(CodegenError::new(format!(
            "{}: history already has a version named `{version}`, but it isn't the schema the \
             bindings were generated from; rename that version",
            artifact::APP_FILE
        )));
    }
    let tables = artifact::version_tables(from).map_err(CodegenError::from_artifact)?;
    versions.push((version.clone(), Json::object(vec![("tables", tables)])));

    let mut migration = vec![
        ("name".to_string(), Json::string(name)),
        ("from".to_string(), Json::string(&version)),
    ];
    migration.extend(diff(from, to)?);
    let migrations = entry(top, "migrations", || Json::Array(Vec::new()), "history");
    let Json::Array(items) = migrations else {
        unreachable!("loaded above")
    };
    items.push(Json::Object(migration));

    let out = artifact::json::print(&root);
    let todos: Vec<String> = artifact::todos(&root)
        .into_iter()
        .map(|(path, _)| path)
        .collect();
    if todos.is_empty() {
        // A stub without TODOs must load: what the diff decided passes
        // every lens check a deployment runs.
        artifact::load_app(out.as_bytes()).map_err(CodegenError::from_artifact)?;
    }
    Ok(Created {
        app: out,
        changed: true,
        todos,
        note: None,
    })
}

/// `top[key]`, inserted after `after` (or at the end) when absent.
fn entry<'a>(
    top: &'a mut Vec<(String, Json)>,
    key: &str,
    empty: impl FnOnce() -> Json,
    after: &str,
) -> &'a mut Json {
    let at = match top.iter().position(|(k, _)| k == key) {
        Some(at) => at,
        None => {
            let at = top
                .iter()
                .position(|(k, _)| k == after)
                .map_or(top.len(), |i| i + 1);
            top.insert(at, (key.to_string(), empty()));
            at
        }
    };
    &mut top[at].1
}

// ---------------------------------------------------------------------------
// The diff
// ---------------------------------------------------------------------------

/// A migration can add, drop or rename a column but not convert its
/// values; say how to keep them.
const KEEP_VALUES_ADVICE: &str = "A migration can add, drop or rename a column but not convert \
its values, and a column added in its place starts every existing row at its default. To keep \
the values, add the new column under another name next to this one, copy the values into it \
from your backend, and drop this column in a later release. Clients on the older schema keep \
writing this column until then, so copy again before you drop it.";

fn same_table(a: &TableSchema, b: &TableSchema) -> bool {
    serde_json::to_value(a).ok() == serde_json::to_value(b).ok()
}

/// Whether `b` is `a` under another name: the same table, whatever the
/// order of its columns (as TS's `tableSchemasEqual` compares them).
fn same_table_renamed(a: &TableSchema, b: &TableSchema) -> bool {
    let sorted = |t: &TableSchema| {
        let mut t = t.clone();
        t.columns
            .columns
            .sort_by(|x, y| x.name_str().cmp(y.name_str()));
        t
    };
    same_table(&sorted(a), &sorted(b))
}

fn type_spec(ty: &ColumnType) -> Result<Json, CodegenError> {
    artifact::column_type_spec(ty).map_err(CodegenError::from_artifact)
}

fn default_of(column: &ColumnDescriptor) -> Result<Json, CodegenError> {
    match &column.default {
        Some(value) => {
            artifact::value_spec(value, &column.column_type).map_err(CodegenError::from_artifact)
        }
        None => Ok(Json::Null),
    }
}

/// The variants `to` adds at the end of `from`'s, when that is all that
/// changed (an enum, or an array of enums).
fn added_variants(from: &ColumnType, to: &ColumnType) -> Option<Vec<String>> {
    match (from, to) {
        (ColumnType::Enum { variants: a }, ColumnType::Enum { variants: b }) => {
            (b.len() > a.len() && b.starts_with(a)).then(|| b[a.len()..].to_vec())
        }
        (ColumnType::Array { element: a }, ColumnType::Array { element: b }) => {
            added_variants(a, b)
        }
        _ => None,
    }
}

/// `(dropped, reordered)` between two enums' variants.
fn enum_change(from: &ColumnType, to: &ColumnType) -> Option<(Vec<String>, bool)> {
    match (from, to) {
        (ColumnType::Enum { variants: a }, ColumnType::Enum { variants: b }) => {
            let dropped: Vec<String> = a.iter().filter(|v| !b.contains(v)).cloned().collect();
            Some((dropped, true))
        }
        (ColumnType::Array { element: a }, ColumnType::Array { element: b }) => enum_change(a, b),
        _ => None,
    }
}

/// Refuse what no migration step does: changing a kept column's type or
/// optionality (an enum may gain variants at the end).
fn refuse_shared_column_changes(
    table: &str,
    from: &TableSchema,
    to: &TableSchema,
    renamed: &[(String, String)],
) -> Result<(), CodegenError> {
    // A row's branch columns decide which branch it belongs to, so a
    // deployment refuses any migration that changes them (#5967 review).
    for branch in &from.branch_by {
        if !to.branch_by.contains(branch) {
            return Err(CodegenError::unsupported(format!(
                "Removing `{}` from the branchBy of table `{table}` is not supported: a row's \
                 branch columns decide which branch it belongs to, so a deployment refuses to \
                 change them. Keep `{}` in branchBy.",
                branch.as_str(),
                branch.as_str()
            )));
        }
        let column = |t: &TableSchema| {
            t.columns
                .columns
                .iter()
                .find(|c| &c.name == branch)
                .cloned()
        };
        if let (Some(source), Some(target)) = (column(from), column(to))
            && source.default != target.default
        {
            return Err(CodegenError::unsupported(format!(
                "Changing the default of branch column `{table}.{}` is not supported: it decides \
                 which branch existing rows belong to, so a deployment refuses to change it. Keep \
                 its default as it was.",
                branch.as_str()
            )));
        }
    }
    for column in &to.columns.columns {
        let Some(source) = from.columns.columns.iter().find(|c| c.name == column.name) else {
            continue;
        };
        let name = format!("{table}.{}", column.name_str());
        // A reference to a table the migration renames follows it.
        let followed = |table: &str| {
            renamed
                .iter()
                .find(|(old, _)| old == table)
                .map_or(table.to_string(), |(_, new)| new.clone())
        };
        if let (Some(from), Some(to)) = (&source.references, &column.references)
            && followed(from.as_str()) != to.as_str()
        {
            return Err(CodegenError::unsupported(format!(
                "Moving the reference of column `{name}` from {} to {} is not supported yet: its \
                 values name rows of {}. Add a new column referencing {} next to this one, and \
                 drop this one in a later release.",
                from.as_str(),
                to.as_str(),
                from.as_str(),
                to.as_str()
            )));
        }
        if source.nullable != column.nullable {
            return Err(CodegenError::unsupported(format!(
                "Changing column `{name}` from {} to {} is not supported yet. {KEEP_VALUES_ADVICE}",
                if source.nullable {
                    "optional"
                } else {
                    "required"
                },
                if column.nullable {
                    "optional"
                } else {
                    "required"
                },
            )));
        }
        if source.column_type == column.column_type
            || added_variants(&source.column_type, &column.column_type).is_some()
        {
            continue;
        }
        if let Some((dropped, _)) = enum_change(&source.column_type, &column.column_type) {
            if dropped.is_empty() {
                return Err(CodegenError::unsupported(format!(
                    "Changing the order of the enum variants of column `{name}` is not supported: \
                     add new variants at the end of the list. A variant's position is its sort \
                     order, so moving one would change how existing rows sort and rewrite every \
                     stored value of the column."
                )));
            }
            return Err(CodegenError::unsupported(format!(
                "Changing the enum variants of column `{name}` is not supported yet: it no longer \
                 declares {}. A migration can add enum variants, but renaming or removing one is \
                 planned for a later release. {KEEP_VALUES_ADVICE}",
                dropped
                    .iter()
                    .map(|v| format!("\"{v}\""))
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        // SPEC 20.1: no lens converts to, from or between vector types (a
        // vector's dimensions are its type), so say so rather than "not yet".
        if matches!(source.column_type, ColumnType::Vector { .. })
            || matches!(column.column_type, ColumnType::Vector { .. })
        {
            return Err(CodegenError::unsupported(format!(
                "Changing the type of column `{name}` to or from a vector, or its dimensions, \
                 is not supported: no migration converts vectors (embeddings from another model \
                 are not comparable). {KEEP_VALUES_ADVICE}"
            )));
        }
        let what = if matches!(
            (&source.column_type, &column.column_type),
            (ColumnType::Json { .. }, ColumnType::Json { .. })
        ) {
            "the JSON Schema of"
        } else {
            "the type of"
        };
        return Err(CodegenError::unsupported(format!(
            "Changing {what} column `{name}` is not supported yet. {KEEP_VALUES_ADVICE}"
        )));
    }
    Ok(())
}

fn same_shape(a: &ColumnDescriptor, b: &ColumnDescriptor) -> bool {
    a.nullable == b.nullable && a.references == b.references && a.column_type == b.column_type
}

fn todo(text: String) -> Json {
    Json::String(format!("{} {text}", artifact::TODO_PREFIX))
}

/// What a migration from one schema to another does, as `migrations create`
/// decides it: every language renders its stub from this, so the decisions
/// (which tables are renamed, which columns may be, which steps need a value
/// only the author knows) are made once.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Diff {
    /// Tables only the new schema has (and no rename explains), sorted.
    pub create_tables: Vec<String>,
    /// Tables only the old schema has (and no rename explains), sorted.
    pub drop_tables: Vec<String>,
    /// Tables renamed: a removed table identical, whatever its column
    /// order, to exactly one added table, and the only one it matches.
    pub rename_tables: Vec<TableRename>,
    /// The tables both schemas have that differ, in the new schema's order.
    pub migrate: Vec<TableDiff>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct TableRename {
    pub from: String,
    pub to: String,
}

/// How a table both schemas have changes.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableDiff {
    pub table: String,
    /// The column steps, in the order they are written.
    pub steps: Vec<ColumnStep>,
    /// Kept columns gaining enum variants: they need no step.
    pub added_variants: Vec<AddedVariants>,
    /// When there are neither steps nor added variants, every row passes
    /// unchanged: what changed instead (`"indexes"`, `"branchBy"`, …).
    pub unchanged_rows: Vec<&'static str>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct AddedVariants {
    pub column: String,
    pub variants: Vec<String>,
}

/// One column's step. `column` names it in the new schema (`Add`,
/// `MaybeRenamed`) or the old one (`Drop`, `MaybeRenamedAway`).
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(tag = "step", rename_all = "camelCase")]
pub enum ColumnStep {
    /// Add the column; existing rows take its default (`null` when
    /// optional without one). `needs_default`: required and without a
    /// default, so only the author knows the value existing rows get.
    #[serde(rename_all = "camelCase")]
    Add { column: String, needs_default: bool },
    /// Drop the column; older clients read it back as its default.
    /// `needs_backwards_default`: required and without a default.
    #[serde(rename_all = "camelCase")]
    Drop {
        column: String,
        needs_backwards_default: bool,
    },
    /// An added column shaped like these removed ones: it may be one of
    /// them renamed, which only the author knows.
    MaybeRenamed {
        column: String,
        candidates: Vec<String>,
    },
    /// A removed column an added one may have been renamed from.
    MaybeRenamedAway { column: String },
}

/// The steps for a table both versions have.
fn table_diff(
    table: &str,
    from: &TableSchema,
    to: &TableSchema,
    renamed: &[(String, String)],
) -> Result<TableDiff, CodegenError> {
    refuse_shared_column_changes(table, from, to, renamed)?;
    let has = |schema: &TableSchema, name: &str| {
        schema.columns.columns.iter().any(|c| c.name_str() == name)
    };
    let removed: Vec<&ColumnDescriptor> = from
        .columns
        .columns
        .iter()
        .filter(|c| !has(to, c.name_str()))
        .collect();
    let added: Vec<&ColumnDescriptor> = to
        .columns
        .columns
        .iter()
        .filter(|c| !has(from, c.name_str()))
        .collect();
    let mut steps = Vec::new();
    let mut undecided: Vec<&str> = Vec::new();

    // An added column shaped like a removed one may be it renamed: adding
    // one and dropping the other would start every row empty, so the author
    // says which it is.
    let mut maybe_renamed_away = Vec::new();
    for column in &added {
        let candidates: Vec<&str> = removed
            .iter()
            .filter(|r| same_shape(r, column))
            .map(|r| r.name_str())
            .collect();
        if candidates.is_empty() {
            continue;
        }
        undecided.push(column.name_str());
        steps.push(ColumnStep::MaybeRenamed {
            column: column.name_str().to_string(),
            candidates: candidates.iter().map(|c| c.to_string()).collect(),
        });
        for old in candidates {
            if undecided.contains(&old) {
                continue;
            }
            undecided.push(old);
            maybe_renamed_away.push(ColumnStep::MaybeRenamedAway {
                column: old.to_string(),
            });
        }
    }
    for column in added.iter().filter(|c| !undecided.contains(&c.name_str())) {
        steps.push(ColumnStep::Add {
            column: column.name_str().to_string(),
            needs_default: !column.nullable && column.default.is_none(),
        });
    }
    for column in removed
        .iter()
        .filter(|c| !undecided.contains(&c.name_str()))
    {
        steps.push(ColumnStep::Drop {
            column: column.name_str().to_string(),
            needs_backwards_default: !column.nullable && column.default.is_none(),
        });
    }
    steps.extend(maybe_renamed_away);

    // Kept columns gaining enum variants need no step: their rows pass
    // unchanged.
    let added_variants: Vec<AddedVariants> = to
        .columns
        .columns
        .iter()
        .filter_map(|column| {
            let source = from
                .columns
                .columns
                .iter()
                .find(|c| c.name == column.name)?;
            Some(AddedVariants {
                column: column.name_str().to_string(),
                variants: added_variants(&source.column_type, &column.column_type)?,
            })
        })
        .collect();
    let unchanged_rows = if steps.is_empty() && added_variants.is_empty() {
        unchanged_rows_changes(from, to)
    } else {
        Vec::new()
    };
    Ok(TableDiff {
        table: table.to_string(),
        steps,
        added_variants,
        unchanged_rows,
    })
}

/// A table's steps as `jazz.app.json` writes them, `"TODO: …"` where only
/// the author can decide.
fn table_steps_json(
    from: &TableSchema,
    to: &TableSchema,
    diff: &TableDiff,
) -> Result<Json, CodegenError> {
    let column = |table: &TableSchema, name: &str| -> ColumnDescriptor {
        table
            .columns
            .columns
            .iter()
            .find(|c| c.name_str() == name)
            .cloned()
            .expect("a column the diff names")
    };
    if !diff.unchanged_rows.is_empty() {
        // No column is added, dropped, retyped or made optional: every row
        // passes unchanged, so the table's lens is the identity. Say what
        // changed.
        return Ok(Json::object(vec![(
            "$comment",
            Json::string(format!(
                "Rows pass unchanged: only {} changed.",
                diff.unchanged_rows.join(", ")
            )),
        )]));
    }
    let mut steps = Vec::new();
    for step in &diff.steps {
        steps.push(match step {
            ColumnStep::MaybeRenamed { column: name, candidates } => {
                let column = column(to, name);
                let of = candidates
                    .iter()
                    .map(|c| format!("`{c}`"))
                    .collect::<Vec<_>>()
                    .join(" or ");
                (
                    name.clone(),
                    todo(format!(
                        "added column `{name}` may be a rename of {of}. To keep its values, replace this with \
                         {{ \"renameFrom\": \"{}\" }}; to start it empty, with {{ \"add\": {}, \"default\": {} }}.",
                        candidates[0],
                        artifact::json::print(&type_spec(&column.column_type)?).trim_end(),
                        artifact::json::print(&default_of(&column)?).trim_end(),
                    )),
                )
            }
            ColumnStep::MaybeRenamedAway { column: name } => {
                let source = column(from, name);
                (
                    name.clone(),
                    todo(format!(
                        "removed column `{name}` may have been renamed. If a column is renamed from it, \
                         delete this entry; otherwise replace it with {{ \"drop\": {}, \"backwardsDefault\": {} }}.",
                        artifact::json::print(&type_spec(&source.column_type)?).trim_end(),
                        artifact::json::print(&default_of(&source)?).trim_end(),
                    )),
                )
            }
            ColumnStep::Add {
                column: name,
                needs_default,
            } => {
                let column = column(to, name);
                let default = if *needs_default {
                    todo(format!(
                        "added required column `{name}` needs the value existing rows get; replace this with it."
                    ))
                } else {
                    // Existing rows take the column's declared default.
                    default_of(&column)?
                };
                (
                    name.clone(),
                    Json::Object(vec![
                        ("add".into(), type_spec(&column.column_type)?),
                        ("default".into(), default),
                    ]),
                )
            }
            ColumnStep::Drop {
                column: name,
                needs_backwards_default,
            } => {
                let column = column(from, name);
                let default = if *needs_backwards_default {
                    todo(format!(
                        "removed required column `{name}` needs the value clients on the older schema read; replace this with it."
                    ))
                } else {
                    // Older clients read the dropped column back as its default.
                    default_of(&column)?
                };
                (
                    name.clone(),
                    Json::Object(vec![
                        ("drop".into(), type_spec(&column.column_type)?),
                        ("backwardsDefault".into(), default),
                    ]),
                )
            }
        });
    }
    // Say beside the (possibly empty) lens which columns gain enum variants.
    let notes: Vec<String> = diff
        .added_variants
        .iter()
        .map(|added| {
            format!(
                "`{}` gains enum variant{} {}: existing rows are unchanged, so it needs no step.",
                added.column,
                if added.variants.len() == 1 { "" } else { "s" },
                added
                    .variants
                    .iter()
                    .map(|v| format!("\"{v}\""))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
        .collect();
    let mut out = Vec::new();
    if !notes.is_empty() {
        out.push(("$comment".to_string(), Json::string(notes.join(" "))));
    }
    out.extend(steps);
    Ok(Json::Object(out))
}

/// What changed in a table whose rows pass unchanged.
fn unchanged_rows_changes(from: &TableSchema, to: &TableSchema) -> Vec<&'static str> {
    let mut changes = Vec::new();
    let column = |table: &TableSchema, name: &jazz::tools::public_schema::ColumnName| {
        table
            .columns
            .columns
            .iter()
            .find(|c| &c.name == name)
            .cloned()
    };
    let pairs: Vec<(ColumnDescriptor, ColumnDescriptor)> = to
        .columns
        .columns
        .iter()
        .filter_map(|c| column(from, &c.name).map(|s| (s, c.clone())))
        .collect();
    if from.indexed_columns != to.indexed_columns || from.composite_indexes != to.composite_indexes
    {
        changes.push("indexes");
    }
    if from.history != to.history {
        changes.push("history retention");
    }
    if from.branch_by != to.branch_by {
        changes.push("branchBy");
    }
    if pairs
        .iter()
        .any(|(a, b)| a.merge_strategy != b.merge_strategy)
    {
        changes.push("merge strategies");
    }
    if pairs.iter().any(|(a, b)| a.default != b.default) {
        changes.push("defaults");
    }
    if pairs.iter().any(|(a, b)| a.references != b.references) {
        changes.push("references");
    }
    let order = |t: &TableSchema| t.columns.columns.iter().map(|c| c.name).collect::<Vec<_>>();
    if order(from) != order(to) {
        changes.push("the column order");
    }
    if changes.is_empty() {
        changes.push("the table's options");
    }
    changes
}

/// What the migration from `from` to `to` does; see [`Diff`]. Refuses
/// what no migration step does (a kept column's type or optionality
/// changing, its reference moving to another table).
pub fn diff_steps(from: &Schema, to: &Schema) -> Result<Diff, CodegenError> {
    let names = |schema: &Schema| -> Vec<String> {
        schema.keys().map(|t| t.as_str().to_string()).collect()
    };
    let get = |schema: &'_ Schema, name: &str| -> Option<TableSchema> {
        schema
            .iter()
            .find(|(t, _)| t.as_str() == name)
            .map(|(_, s)| s.clone())
    };
    let (from_names, to_names) = (names(from), names(to));
    let mut removed: Vec<String> = from_names
        .iter()
        .filter(|t| !to_names.contains(t))
        .cloned()
        .collect();
    let mut added: Vec<String> = to_names
        .iter()
        .filter(|t| !from_names.contains(t))
        .cloned()
        .collect();
    removed.sort();
    added.sort();

    // A removed table identical to exactly one added table (and the only
    // one it matches) is that table renamed.
    let matches: Vec<(String, String)> = removed
        .iter()
        .filter_map(|old| {
            let old_table = get(from, old)?;
            let candidates: Vec<&String> = added
                .iter()
                .filter(|new| get(to, new).is_some_and(|t| same_table_renamed(&old_table, &t)))
                .collect();
            (candidates.len() == 1).then(|| (old.clone(), candidates[0].clone()))
        })
        .collect();
    let renames: Vec<(String, String)> = matches
        .iter()
        .filter(|(_, new)| matches.iter().filter(|(_, n)| n == new).count() == 1)
        .cloned()
        .collect();

    let mut migrate = Vec::new();
    for name in &to_names {
        let (Some(old), Some(new)) = (get(from, name), get(to, name)) else {
            continue;
        };
        if same_table(&old, &new) {
            continue;
        }
        migrate.push(table_diff(name, &old, &new, &renames)?);
    }
    Ok(Diff {
        create_tables: added
            .into_iter()
            .filter(|t| !renames.iter().any(|(_, n)| n == t))
            .collect(),
        drop_tables: removed
            .into_iter()
            .filter(|t| !renames.iter().any(|(o, _)| o == t))
            .collect(),
        rename_tables: renames
            .into_iter()
            .map(|(from, to)| TableRename { from, to })
            .collect(),
        migrate,
    })
}

/// [`diff_steps`] of two schemas in the engine's JSON form
/// (`{ "tables": … }`), as JSON: what a language without the Rust types
/// (the TS CLI) renders its stub from.
pub fn diff_steps_json(from_json: &[u8], to_json: &[u8]) -> Result<String, CodegenError> {
    let read = |json: &[u8]| {
        serde_json::from_slice::<Schema>(json)
            .map_err(|error| CodegenError::new(format!("not an engine schema: {error}")))
    };
    let steps = diff_steps(&read(from_json)?, &read(to_json)?)?;
    Ok(serde_json::to_string(&steps).expect("plain data"))
}

/// The fields of the migration from `from` to `to`, after `name` and
/// `from`: `createTables`, `dropTables`, `renameTables`, `migrate`.
pub fn diff(from: &Schema, to: &Schema) -> Result<Vec<(String, Json)>, CodegenError> {
    let steps = diff_steps(from, to)?;
    let table = |schema: &Schema, name: &str| -> TableSchema {
        schema
            .iter()
            .find(|(t, _)| t.as_str() == name)
            .map(|(_, s)| s.clone())
            .expect("a table the diff names")
    };
    let mut fields = Vec::new();
    if !steps.create_tables.is_empty() {
        fields.push((
            "createTables".into(),
            Json::Array(steps.create_tables.iter().map(Json::string).collect()),
        ));
    }
    if !steps.drop_tables.is_empty() {
        fields.push((
            "dropTables".into(),
            Json::Array(steps.drop_tables.iter().map(Json::string).collect()),
        ));
    }
    if !steps.rename_tables.is_empty() {
        fields.push((
            "renameTables".into(),
            Json::Object(
                steps
                    .rename_tables
                    .iter()
                    .map(|rename| (rename.to.clone(), Json::String(rename.from.clone())))
                    .collect(),
            ),
        ));
    }
    if !steps.migrate.is_empty() {
        let mut migrate = Vec::new();
        for diff in &steps.migrate {
            migrate.push((
                diff.table.clone(),
                table_steps_json(&table(from, &diff.table), &table(to, &diff.table), diff)?,
            ));
        }
        fields.push(("migrate".into(), Json::Object(migrate)));
    }
    if fields.is_empty() {
        fields.push((
            "migrate".into(),
            todo("no schema difference was found; delete this migration.".into()),
        ));
    }
    Ok(fields)
}
