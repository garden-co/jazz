//! `jazz.app.json` (SPEC §10.10): tables, columns, relations, history and
//! migrations in the readable form, to and from the engine's forms.

use std::collections::BTreeMap;

use base64::Engine as _;

use crate::tools::ObjectId;
use crate::tools::public_schema::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnType, EnumCaseDescriptor, HistoryRetention,
    RowDescriptor, Schema, SchemaHash, SpecialisedIndex, SpecialisedIndexKind, TableName,
    TablePolicies, TableSchema, TextField, Value, VectorAnn, VectorMetric,
};

use super::super::published::{PublishLensOp, PublishMigrationRequest, PublishTableLens};
use super::json::Json;
use super::{
    Codegen, CodegenRelation, CodegenTable, ComputedField, FieldType, TransformDecl, TsOnlyColumn,
};

/// A refusal at a JSON path of the app file.
pub type Refusal = (String, String);

fn refuse(path: impl Into<String>, message: impl Into<String>) -> Refusal {
    (path.into(), message.into())
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

/// `jazz.app.json`, read.
#[derive(Debug, Clone)]
pub struct AppFile {
    pub current: Schema,
    /// Earlier versions by name, oldest first.
    pub history: Vec<(String, Schema)>,
    /// Migrations, with `from`/`to` still as version names (`None` = current).
    pub migrations: Vec<(String, Option<String>, Vec<PublishTableLens>)>,
    pub codegen: Codegen,
    /// What loads but is ignored (a .63 file's `slices` and `tsOnly`).
    pub warnings: Vec<String>,
}

// The keys each object of the file takes (besides `$comment`), which the
// JSON Schema in docs/public/schemas/ must list too.
pub const TOP_KEYS: &[&str] = &[
    "format",
    "$schema",
    "tables",
    "history",
    "migrations",
    "transforms",
    "slices",
];
pub const TABLE_KEYS: &[&str] = &[
    "columns",
    "relations",
    "indexes",
    "branchBy",
    "history",
    "computed",
    "tsOnly",
];
pub const COLUMN_KEYS: &[&str] = &[
    "type",
    "enum",
    "json",
    "array",
    "vector",
    "optional",
    "default",
    "merge",
    "references",
    "transform",
];
pub const MIGRATION_KEYS: &[&str] = &[
    "name",
    "from",
    "to",
    "createTables",
    "dropTables",
    "renameTables",
    "migrate",
];
pub const TRANSFORM_KEYS: &[&str] = &["stored", "view"];
pub const COMPUTED_KEYS: &[&str] = &["type", "from"];
pub const VERSION_KEYS: &[&str] = &["tables"];
/// A specialised index names its kind by key (SPEC 20.1): exactly one of
/// these, besides `name`.
pub const INDEX_KIND_KEYS: &[&str] = &["text", "vector", "geo"];
pub const TEXT_INDEX_KEYS: &[&str] = &["name", "text", "analyzer", "prefix", "serverOnly"];
pub const VECTOR_INDEX_KEYS: &[&str] = &["name", "vector", "metric", "ann", "serverOnly"];
/// A text index field's object form: `{ "weight": 4, "length": 256 }`.
pub const TEXT_FIELD_KEYS: &[&str] = &["weight", "length"];
pub const GEO_INDEX_KEYS: &[&str] = &["name", "geo", "serverOnly"];
pub const GEO_POINT_KEYS: &[&str] = &["lat", "lng"];

/// Keys of `object` that aren't `allowed` (nor `$comment`) are refused.
fn only_keys(object: &[(String, Json)], allowed: &[&str], path: &str) -> Result<(), Refusal> {
    for (key, value) in object {
        if key == "$comment" {
            if value.as_str().is_none() {
                return Err(refuse(format!("{path}.$comment"), "must be a string"));
            }
            continue;
        }
        if !allowed.contains(&key.as_str()) {
            return Err(refuse(
                path,
                format!(
                    "unknown key `{key}`{}; this object takes {}",
                    super::expr::did_you_mean(key, allowed.iter().copied()),
                    allowed
                        .iter()
                        .map(|k| format!("`{k}`"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ));
        }
    }
    Ok(())
}

fn object<'a>(value: &'a Json, path: &str) -> Result<&'a [(String, Json)], Refusal> {
    value
        .as_object()
        .ok_or_else(|| refuse(path, format!("must be an object, not {}", value.kind())))
}

fn string<'a>(value: &'a Json, path: &str) -> Result<&'a str, Refusal> {
    value
        .as_str()
        .ok_or_else(|| refuse(path, format!("must be a string, not {}", value.kind())))
}

fn names(value: &Json, path: &str) -> Result<Vec<String>, Refusal> {
    match value {
        Json::Array(items) => items
            .iter()
            .enumerate()
            .map(|(i, item)| string(item, &format!("{path}[{i}]")).map(str::to_string))
            .collect(),
        other => Err(refuse(
            path,
            format!("must be an array of names, not {}", other.kind()),
        )),
    }
}

fn identifier(name: &str, path: &str) -> Result<(), Refusal> {
    let mut chars = name.chars();
    let valid = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if valid {
        Ok(())
    } else {
        Err(refuse(
            path,
            format!(
                "`{name}` is not a name: names are letters, digits and `_`, starting with a letter or `_`"
            ),
        ))
    }
}

pub fn read(root: &Json) -> Result<AppFile, Refusal> {
    let top = object(root, "(top)")?;
    only_keys(top, TOP_KEYS, "(top)")?;
    let mut warnings = Vec::new();
    let tables_json = root
        .get("tables")
        .ok_or_else(|| refuse("(top)", "no `tables`"))?;
    let mut codegen = Codegen::default();
    let current = read_tables(tables_json, "tables", Some((&mut codegen, &mut warnings)))?;
    if let Some(transforms) = root.get("transforms") {
        for (name, spec) in object(transforms, "transforms")? {
            let path = format!("transforms.{name}");
            identifier(name, &path)?;
            let fields = object(spec, &path)?;
            only_keys(fields, TRANSFORM_KEYS, &path)?;
            let side = |key: &str| -> Result<ColumnType, Refusal> {
                let value = spec.get(key).ok_or_else(|| {
                    refuse(
                        &path,
                        format!(
                            "a transform needs `{key}`, e.g. {{ \"stored\": \"timestamp\", \"view\": \"string\" }}"
                        ),
                    )
                })?;
                let sub = format!("{path}.{key}");
                let column = read_column(key, value, &sub)?;
                if column.nullable {
                    return Err(refuse(
                        sub,
                        "a transform's types carry no `?`: the column it is named on says whether it is optional",
                    ));
                }
                plain_type(&column, value, &sub)?;
                Ok(column.column_type)
            };
            let decl = TransformDecl {
                stored: side("stored")?,
                view: side("view")?,
            };
            codegen.transforms.insert(name.clone(), decl);
        }
    }

    let mut history = Vec::new();
    if let Some(versions) = root.get("history") {
        for (name, version) in object(versions, "history")? {
            let path = format!("history.{name}");
            // JavaScript moves integer keys below 2^32 - 1 first in every
            // object, so a tool round-tripping the file would reorder such
            // versions. (A 12-hex short hash never is one: twelve digits
            // are either at least 10^11 or start with a zero.)
            if is_array_index(name) {
                return Err(refuse(
                    &path,
                    format!(
                        "`{name}` can't name a version: JavaScript reorders integer keys, so \
                         `history`'s order wouldn't survive a JS tool; use e.g. `v{name}`"
                    ),
                ));
            }
            let fields = object(version, &path)?;
            only_keys(fields, VERSION_KEYS, &path)?;
            let tables = version
                .get("tables")
                .ok_or_else(|| refuse(&path, "no `tables`"))?;
            history.push((
                name.clone(),
                read_tables(tables, &format!("{path}.tables"), None)?,
            ));
        }
    }

    let mut migrations = Vec::new();
    if let Some(list) = root.get("migrations") {
        let Json::Array(items) = list else {
            return Err(refuse("migrations", "must be an array"));
        };
        for (index, item) in items.iter().enumerate() {
            let path = format!("migrations[{index}]");
            migrations.push(read_migration(item, &path, &history, &current)?);
        }
    }

    if root.get("slices").is_some() {
        warnings.push(
            "slices: schema slicing is gone in alpha.65, so `slices` is ignored; \
             `jazz-tools upgrade` removes it"
                .to_string(),
        );
    }
    Ok(AppFile {
        current,
        history,
        migrations,
        codegen,
        warnings,
    })
}

/// Whether JavaScript treats `key` as an array index (a canonical integer
/// below 2^32 - 1), which objects order before every other key.
fn is_array_index(key: &str) -> bool {
    key.parse::<u32>()
        .is_ok_and(|n| n < u32::MAX && n.to_string() == key)
}

fn read_tables(
    value: &Json,
    path: &str,
    mut codegen: Option<(&mut Codegen, &mut Vec<String>)>,
) -> Result<Schema, Refusal> {
    let mut schema = Schema::new();
    let mut forwards: Vec<(String, String, String)> = Vec::new();
    for (name, table) in object(value, path)? {
        let path = format!("{path}.{name}");
        identifier(name, &path)?;
        let fields = object(table, &path)?;
        only_keys(fields, TABLE_KEYS, &path)?;
        let current = codegen.is_some();
        for key in ["computed", "tsOnly"] {
            if !current && table.get(key).is_some() {
                return Err(refuse(
                    format!("{path}.{key}"),
                    "only the current `tables` declare computed fields and transforms",
                ));
            }
        }
        let columns_json = table
            .get("columns")
            .ok_or_else(|| refuse(&path, "no `columns`"))?;
        let mut columns = Vec::new();
        let mut transforms = BTreeMap::new();
        for (column, spec) in object(columns_json, &format!("{path}.columns"))? {
            let column_path = format!("{path}.columns.{column}");
            identifier(column, &column_path)?;
            if let Some(transform) = spec.get("transform") {
                let transform_path = format!("{column_path}.transform");
                if !current {
                    return Err(refuse(
                        transform_path,
                        "only the current `tables` declare computed fields and transforms",
                    ));
                }
                let name = string(transform, &transform_path)?;
                transforms.insert(column.clone(), name.to_string());
            }
            columns.push(read_column(column, spec, &column_path)?);
        }
        let mut computed = Vec::new();
        if let Some(fields) = table.get("computed") {
            for (field, spec) in object(fields, &format!("{path}.computed"))? {
                let field_path = format!("{path}.computed.{field}");
                identifier(field, &field_path)?;
                let entries = object(spec, &field_path)?;
                only_keys(entries, COMPUTED_KEYS, &field_path)?;
                let example = "e.g. { \"type\": \"int\", \"from\": [\"score\"] }";
                let ty = spec.get("type").ok_or_else(|| {
                    refuse(
                        &field_path,
                        format!("a computed field needs `type`, {example}"),
                    )
                })?;
                let type_path = format!("{field_path}.type");
                let column = read_column(field, ty, &type_path)?;
                plain_type(&column, ty, &type_path)?;
                let from = names(
                    spec.get("from").ok_or_else(|| {
                        refuse(
                            &field_path,
                            format!("a computed field needs `from`, {example}"),
                        )
                    })?,
                    &format!("{field_path}.from"),
                )?;
                computed.push(ComputedField {
                    name: field.clone(),
                    field_type: FieldType {
                        column_type: column.column_type,
                        nullable: column.nullable,
                    },
                    from,
                });
            }
        }
        let mut relations = BTreeMap::new();
        if let Some(rels) = table.get("relations") {
            for (relation, spec) in object(rels, &format!("{path}.relations"))? {
                let rel_path = format!("{path}.relations.{relation}");
                identifier(relation, &rel_path)?;
                let fields = object(spec, &rel_path)?;
                let rel = if let Some(target) = spec.get("rel") {
                    only_keys(fields, &["rel", "column"], &rel_path)?;
                    let column = string(
                        spec.get("column").ok_or_else(|| refuse(&rel_path, "a forward relation needs `column`, e.g. { \"rel\": \"chats\", \"column\": \"chatId\" }"))?,
                        &format!("{rel_path}.column"),
                    )?;
                    let target = string(target, &format!("{rel_path}.rel"))?;
                    forwards.push((name.clone(), column.to_string(), target.to_string()));
                    CodegenRelation::Forward {
                        table: target.to_string(),
                        column: column.to_string(),
                    }
                } else if let Some(source) = spec.get("reverse") {
                    only_keys(fields, &["reverse", "relation"], &rel_path)?;
                    let forward = string(
                        spec.get("relation").ok_or_else(|| refuse(&rel_path, "a reverse relation needs `relation`, e.g. { \"reverse\": \"messages\", \"relation\": \"chat\" }"))?,
                        &format!("{rel_path}.relation"),
                    )?;
                    CodegenRelation::Reverse {
                        table: string(source, &format!("{rel_path}.reverse"))?.to_string(),
                        relation: forward.to_string(),
                    }
                } else {
                    return Err(refuse(
                        &rel_path,
                        "a relation is { \"rel\": <table>, \"column\": <column> } or { \"reverse\": <table>, \"relation\": <relation> }",
                    ));
                };
                relations.insert(relation.clone(), rel);
            }
        }
        let mut indexed_columns: Option<Vec<String>> = None;
        let mut composite_indexes = Vec::new();
        let mut specialised_indexes = Vec::new();
        if let Some(indexes) = table.get("indexes") {
            let Json::Array(items) = indexes else {
                return Err(refuse(format!("{path}.indexes"), "must be an array"));
            };
            for (i, item) in items.iter().enumerate() {
                let item_path = format!("{path}.indexes[{i}]");
                match item {
                    Json::String(column) => indexed_columns
                        .get_or_insert_with(Vec::new)
                        .push(column.clone()),
                    Json::Array(_) => composite_indexes.push(names(item, &item_path)?),
                    Json::Object(_) => specialised_indexes
                        .push((item_path.clone(), read_specialised_index(item, &item_path)?)),
                    other => {
                        return Err(refuse(
                            item_path,
                            format!(
                                "an index is a column name, an array of names or a text, vector \
                                 or geo index object, not {}",
                                other.kind()
                            ),
                        ));
                    }
                }
            }
        }
        // An index names stored columns (a composite one also `$createdAt`
        // or `$updatedAt`).
        let column_names: Vec<&str> = columns.iter().map(|c| c.name_str()).collect();
        let check_index =
            |column: &str, item_path: String, composite: bool| -> Result<(), Refusal> {
                if column_names.contains(&column)
                    || (composite && matches!(column, "$createdAt" | "$updatedAt"))
                {
                    return Ok(());
                }
                if computed.iter().any(|f| f.name == column) {
                    return Err(refuse(
                        item_path,
                        format!(
                            "`{column}` is a computed field, which isn't stored, so it can't be \
                         indexed; index the columns it's computed from"
                        ),
                    ));
                }
                Err(refuse(
                    item_path,
                    format!(
                        "table {name} has no column `{column}`{}",
                        super::expr::did_you_mean(column, column_names.iter().copied())
                    ),
                ))
            };
        for (i, column) in indexed_columns.iter().flatten().enumerate() {
            check_index(column, format!("{path}.indexes[{i}]"), false)?;
        }
        for (i, index) in composite_indexes.iter().enumerate() {
            for column in index {
                check_index(column, format!("{path}.indexes"), true)
                    .map_err(|(p, m)| (format!("{p}.composite[{i}]"), m))?;
            }
        }
        let mut seen_index_names = std::collections::BTreeSet::new();
        for (item_path, index) in &specialised_indexes {
            if !seen_index_names.insert(index.name.as_str()) {
                return Err(refuse(
                    format!("{item_path}.name"),
                    format!("two specialised indexes are named `{}`", index.name),
                ));
            }
            let option_of = |i: usize| match &index.kind {
                SpecialisedIndexKind::Text { .. } => "text".to_string(),
                SpecialisedIndexKind::Vector { .. } => "vector".to_string(),
                SpecialisedIndexKind::Geo { .. } => ["geo.lat", "geo.lng"][i].to_string(),
            };
            for (i, column) in index.kind.columns().into_iter().enumerate() {
                let option = match &index.kind {
                    SpecialisedIndexKind::Text { .. } => format!("text.{column}"),
                    _ => option_of(i),
                };
                check_index(column, format!("{item_path}.{option}"), false)?;
            }
        }
        let branch_by = match table.get("branchBy") {
            Some(value) => names(value, &format!("{path}.branchBy"))?,
            None => Vec::new(),
        };
        let history = match table.get("history") {
            None => HistoryRetention::Full,
            Some(Json::String(h)) if h == "latest" => HistoryRetention::Latest,
            Some(_) => {
                return Err(refuse(
                    format!("{path}.history"),
                    "the only history option is \"latest\"",
                ));
            }
        };
        let mut ts_only = Vec::new();
        if let Some(fields) = table.get("tsOnly") {
            if let Some((_, warnings)) = codegen.as_mut() {
                warnings.push(format!(
                    "{path}.tsOnly: `tsOnly` (written before alpha.65) is ignored; \
                     `jazz-tools upgrade` turns it into `computed` and `transforms`"
                ));
            }
            for (field, kind) in object(fields, &format!("{path}.tsOnly"))? {
                ts_only.push(match kind.as_str() {
                    Some("computed") => TsOnlyColumn::Computed {
                        column: field.clone(),
                    },
                    Some("transform") => TsOnlyColumn::Transform {
                        column: field.clone(),
                    },
                    _ => {
                        return Err(refuse(
                            format!("{path}.tsOnly.{field}"),
                            "must be \"computed\" or \"transform\"",
                        ));
                    }
                });
            }
        }
        if let Some((codegen, _)) = codegen.as_mut()
            && (!relations.is_empty()
                || !ts_only.is_empty()
                || !computed.is_empty()
                || !transforms.is_empty())
        {
            codegen.tables.insert(
                name.clone(),
                CodegenTable {
                    relations,
                    computed,
                    transforms,
                    ts_only,
                },
            );
        }
        schema.insert(
            TableName::from(name.as_str()),
            TableSchema {
                columns: RowDescriptor { columns },
                indexed_columns: indexed_columns
                    .map(|c| c.iter().map(|c| c.as_str().into()).collect()),
                composite_indexes: composite_indexes
                    .iter()
                    .map(|index| index.iter().map(|c| c.as_str().into()).collect())
                    .collect(),
                policies: TablePolicies::default(),
                branch_by: branch_by.iter().map(|c| c.as_str().into()).collect(),
                history,
                specialised_indexes: specialised_indexes
                    .into_iter()
                    .map(|(_, index)| index)
                    .collect(),
            },
        );
    }
    // A forward relation's column references the related table, as `s.rel`
    // makes it.
    for (table, column, target) in forwards {
        let path = format!("{path}.{table}.relations");
        let schema_table = schema
            .get_mut(&TableName::from(table.as_str()))
            .expect("read above");
        let Some(descriptor) = schema_table
            .columns
            .columns
            .iter_mut()
            .find(|c| c.name_str() == column)
        else {
            return Err(refuse(
                path,
                format!("a relation names column `{column}`, which {table} doesn't have"),
            ));
        };
        if descriptor.references.is_none() {
            descriptor.references = Some(TableName::from(target.as_str()));
        }
    }
    Ok(schema)
}

/// A specialised index object of a table's `indexes` (SPEC 20.1): `name`
/// and exactly one kind key, with that kind's options.
fn read_specialised_index(item: &Json, path: &str) -> Result<SpecialisedIndex, Refusal> {
    let fields = object(item, path)?;
    let kinds: Vec<&str> = INDEX_KIND_KEYS
        .iter()
        .copied()
        .filter(|kind| item.get(kind).is_some())
        .collect();
    let kind = match kinds.as_slice() {
        [kind] => *kind,
        [] => {
            return Err(refuse(
                path,
                "an index object names its kind by key: exactly one of `text`, `vector` or \
                 `geo`, e.g. { \"name\": \"search\", \"text\": { \"title\": 1 }, \
                 \"analyzer\": \"jazz-text/1:en\" }",
            ));
        }
        _ => {
            return Err(refuse(
                path,
                format!(
                    "an index object has exactly one kind key, not {}",
                    kinds
                        .iter()
                        .map(|k| format!("`{k}`"))
                        .collect::<Vec<_>>()
                        .join(" and ")
                ),
            ));
        }
    };
    only_keys(
        fields,
        match kind {
            "text" => TEXT_INDEX_KEYS,
            "vector" => VECTOR_INDEX_KEYS,
            _ => GEO_INDEX_KEYS,
        },
        path,
    )?;
    let name = string(
        item.get("name")
            .ok_or_else(|| refuse(path, "an index object needs `name`"))?,
        &format!("{path}.name"),
    )?;
    if !crate::tools::public_schema::is_specialised_index_name(name) {
        return Err(refuse(
            format!("{path}.name"),
            format!(
                "`{name}` is not an index name: [a-z][a-z0-9_]*, at most {} bytes",
                crate::tools::public_schema::SPECIALISED_INDEX_NAME_MAX
            ),
        ));
    }
    let flag = |key: &str| -> Result<bool, Refusal> {
        match item.get(key) {
            None => Ok(false),
            Some(Json::Bool(value)) => Ok(*value),
            Some(other) => Err(refuse(
                format!("{path}.{key}"),
                format!("must be true or false, not {}", other.kind()),
            )),
        }
    };
    let server_only = flag("serverOnly")?;
    let value = item.get(kind).expect("kind key present");
    let kind_path = format!("{path}.{kind}");
    let kind = match kind {
        "text" => {
            let mut fields = BTreeMap::new();
            for (column, field) in object(value, &kind_path)? {
                let field_path = format!("{kind_path}.{column}");
                let max = crate::tools::public_schema::TEXT_WEIGHT_MAX;
                let max_length = crate::tools::public_schema::TEXT_FIELD_LENGTH_MAX;
                let weight_of = |value: &Json, path: &str| {
                    match value {
                        Json::Number(n) => n.parse::<u8>().ok().filter(|w| (1..=max).contains(w)),
                        _ => None,
                    }
                    .ok_or_else(|| {
                        refuse(
                            path,
                            format!("a field weight is an integer from 1 to {max}"),
                        )
                    })
                };
                let length_error = || {
                    refuse(
                        format!("{field_path}.length"),
                        format!("a field's reference length is an integer from 1 to {max_length}"),
                    )
                };
                let declared = match field {
                    Json::Object(entries) => {
                        only_keys(entries, TEXT_FIELD_KEYS, &field_path)?;
                        let weight = weight_of(
                            field.get("weight").ok_or_else(|| {
                                refuse(
                                    &field_path,
                                    "a field object needs `weight`, e.g. { \"weight\": 4, \"length\": 256 }",
                                )
                            })?,
                            &format!("{field_path}.weight"),
                        )?;
                        let length = match field.get("length") {
                            None => crate::tools::public_schema::TEXT_FIELD_LENGTH_DEFAULT,
                            Some(Json::Number(n)) => n
                                .parse::<u16>()
                                .ok()
                                .filter(|l| (1..=max_length).contains(l))
                                .ok_or_else(length_error)?,
                            Some(_) => return Err(length_error()),
                        };
                        TextField { weight, length }
                    }
                    other => TextField::weight(weight_of(other, &field_path)?),
                };
                fields.insert(column.clone(), declared);
            }
            if fields.is_empty() {
                return Err(refuse(
                    kind_path,
                    "a text index needs at least one field, e.g. { \"title\": 4, \"body\": 1 }",
                ));
            }
            let analyzer_path = format!("{path}.analyzer");
            let analyzer = string(
                item.get("analyzer").ok_or_else(|| {
                    refuse(
                        path,
                        format!("a text index needs `analyzer`, one of {}", analyzers_list()),
                    )
                })?,
                &analyzer_path,
            )?;
            if !crate::tools::public_schema::TEXT_ANALYZERS.contains(&analyzer) {
                return Err(refuse(
                    analyzer_path,
                    format!(
                        "unknown analyzer `{analyzer}`{}; analyzers are {}",
                        super::expr::did_you_mean(
                            analyzer,
                            crate::tools::public_schema::TEXT_ANALYZERS.iter().copied()
                        ),
                        analyzers_list()
                    ),
                ));
            }
            SpecialisedIndexKind::Text {
                fields,
                analyzer: analyzer.to_string(),
                prefix: flag("prefix")?,
            }
        }
        "vector" => {
            let column = string(value, &kind_path)?;
            let metric_path = format!("{path}.metric");
            let metric = string(
                item.get("metric").ok_or_else(|| {
                    refuse(
                        path,
                        "a vector index needs `metric`: \"cosine\", \"l2\" or \"dot\"",
                    )
                })?,
                &metric_path,
            )?;
            let metric = VectorMetric::parse(metric).ok_or_else(|| {
                refuse(
                    &metric_path,
                    format!("unknown metric `{metric}`; metrics are cosine, l2 and dot"),
                )
            })?;
            let ann = match item.get("ann") {
                None => VectorAnn::Exact,
                Some(Json::String(ann)) if ann == "exact" => VectorAnn::Exact,
                Some(Json::String(ann)) if ann == "ivf" => {
                    return Err(refuse(
                        format!("{path}.ann"),
                        "`ann: \"ivf\"` is not supported yet; the only value is \"exact\"",
                    ));
                }
                Some(_) => {
                    return Err(refuse(
                        format!("{path}.ann"),
                        "the only `ann` value is \"exact\"",
                    ));
                }
            };
            SpecialisedIndexKind::Vector {
                column: column.to_string(),
                metric,
                ann,
            }
        }
        _ => {
            let point = object(value, &kind_path)?;
            only_keys(point, GEO_POINT_KEYS, &kind_path)?;
            let coordinate = |key: &str| -> Result<String, Refusal> {
                let column = value.get(key).ok_or_else(|| {
                    refuse(
                        &kind_path,
                        "a geo index is { \"lat\": <f64 column>, \"lng\": <f64 column> }",
                    )
                })?;
                string(column, &format!("{kind_path}.{key}")).map(str::to_string)
            };
            SpecialisedIndexKind::Geo {
                lat: coordinate("lat")?,
                lng: coordinate("lng")?,
            }
        }
    };
    Ok(SpecialisedIndex {
        name: name.to_string(),
        kind,
        server_only,
    })
}

fn analyzers_list() -> String {
    crate::tools::public_schema::TEXT_ANALYZERS
        .iter()
        .map(|a| format!("\"{a}\""))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A specialised index as `jazz.app.json` writes it: `name`, the kind key,
/// then its options; defaults are omitted.
fn write_specialised_index(index: &SpecialisedIndex) -> Json {
    let mut fields = vec![("name", Json::string(&index.name))];
    match &index.kind {
        SpecialisedIndexKind::Text {
            fields: columns,
            analyzer,
            prefix,
        } => {
            fields.push((
                "text",
                Json::Object(
                    columns
                        .iter()
                        .map(|(column, field)| {
                            // A field at the default length is written as its
                            // bare weight.
                            let value = if field.length
                                == crate::tools::public_schema::TEXT_FIELD_LENGTH_DEFAULT
                            {
                                Json::Number(field.weight.to_string())
                            } else {
                                Json::object(vec![
                                    ("weight", Json::Number(field.weight.to_string())),
                                    ("length", Json::Number(field.length.to_string())),
                                ])
                            };
                            (column.clone(), value)
                        })
                        .collect(),
                ),
            ));
            fields.push(("analyzer", Json::string(analyzer)));
            if *prefix {
                fields.push(("prefix", Json::Bool(true)));
            }
        }
        SpecialisedIndexKind::Vector {
            column,
            metric,
            ann,
        } => {
            fields.push(("vector", Json::string(column)));
            fields.push(("metric", Json::string(metric.as_str())));
            match ann {
                VectorAnn::Exact => {}
            }
        }
        SpecialisedIndexKind::Geo { lat, lng } => {
            fields.push((
                "geo",
                Json::object(vec![("lat", Json::string(lat)), ("lng", Json::string(lng))]),
            ));
        }
    }
    if index.server_only {
        fields.push(("serverOnly", Json::Bool(true)));
    }
    Json::object(fields)
}

/// A computed field's or transform's type: a type and `?`, without the
/// options only a stored column has.
fn plain_type(column: &ColumnDescriptor, spec: &Json, path: &str) -> Result<(), Refusal> {
    let option = if spec.get("transform").is_some() {
        Some("transform")
    } else if column.default.is_some() {
        Some("default")
    } else if column.merge_strategy.is_some() {
        Some("merge")
    } else if column.references.is_some() {
        Some("references")
    } else {
        None
    };
    match option {
        Some(option) => Err(refuse(
            format!("{path}.{option}"),
            format!("`{option}` is for stored columns; this is a type only"),
        )),
        None => Ok(()),
    }
}

fn scalar(name: &str) -> Option<ColumnType> {
    Some(match name {
        "string" => ColumnType::Text,
        "int" => ColumnType::Integer,
        "bigint" => ColumnType::BigInt,
        "float" => ColumnType::Double,
        "boolean" => ColumnType::Boolean,
        "timestamp" => ColumnType::Timestamp,
        "uuid" => ColumnType::Uuid,
        "bytes" => ColumnType::Bytea,
        _ => return None,
    })
}

const SCALARS: &[&str] = &[
    "string",
    "int",
    "bigint",
    "float",
    "boolean",
    "timestamp",
    "uuid",
    "bytes",
];

/// A column type, nullability and options from its spec.
fn read_column(name: &str, spec: &Json, path: &str) -> Result<ColumnDescriptor, Refusal> {
    let (column_type, mut nullable) = read_type(spec, path)?;
    let mut descriptor = ColumnDescriptor::new(name, column_type);
    if let Json::Object(fields) = spec {
        only_keys(fields, COLUMN_KEYS, path)?;
        if let Some(optional) = spec.get("optional") {
            match optional {
                Json::Bool(b) => nullable |= b,
                _ => return Err(refuse(format!("{path}.optional"), "must be true or false")),
            }
        }
        if let Some(default) = spec.get("default") {
            descriptor.default = Some(read_value(
                default,
                &descriptor.column_type,
                &format!("{path}.default"),
            )?);
        }
        if let Some(merge) = spec.get("merge") {
            descriptor.merge_strategy = Some(match merge.as_str() {
                Some("counter") => ColumnMergeStrategy::Counter,
                Some("g-set") => ColumnMergeStrategy::GSet,
                Some("text") => ColumnMergeStrategy::Text,
                Some("markdown") => ColumnMergeStrategy::Markdown,
                Some("html") => ColumnMergeStrategy::Html,
                _ => {
                    return Err(refuse(
                        format!("{path}.merge"),
                        "must be \"counter\", \"g-set\", \"text\", \"markdown\" or \"html\"",
                    ));
                }
            });
        }
        if let Some(target) = spec.get("references") {
            descriptor.references = Some(TableName::from(string(
                target,
                &format!("{path}.references"),
            )?));
        }
    }
    descriptor.nullable = nullable;
    Ok(descriptor)
}

/// A column type from a type string (`"string?"`) or a type object.
fn read_type(spec: &Json, path: &str) -> Result<(ColumnType, bool), Refusal> {
    match spec {
        Json::String(text) => {
            let (base, nullable) = match text.strip_suffix('?') {
                Some(base) => (base, true),
                None => (text.as_str(), false),
            };
            let ty = scalar(base).ok_or_else(|| {
                refuse(
                    path,
                    format!(
                        "unknown type `{base}`{}; a type is one of {} (add `?` for optional), or an object for enum, json, array and vector",
                        super::expr::did_you_mean(base, SCALARS.iter().copied()),
                        SCALARS.join(", ")
                    ),
                )
            })?;
            Ok((ty, nullable))
        }
        Json::Object(_) => {
            let keys: Vec<&str> = ["type", "enum", "json", "array", "vector"]
                .into_iter()
                .filter(|k| spec.get(k).is_some())
                .collect();
            let [key] = keys.as_slice() else {
                return Err(refuse(
                    path,
                    "a column object takes exactly one of `type`, `enum`, `json`, `array`, `vector`",
                ));
            };
            let value = spec.get(key).expect("filtered");
            let sub = format!("{path}.{key}");
            Ok((
                match *key {
                    "type" => return read_type(value, &sub),
                    "enum" => match value {
                        Json::Array(_) => ColumnType::Enum {
                            variants: names(value, &sub)?,
                        },
                        Json::Object(cases) => ColumnType::EnumPayload {
                            cases: cases
                                .iter()
                                .map(|(case, fields)| {
                                    let case_path = format!("{sub}.{case}");
                                    let fields = object(fields, &case_path)?
                                        .iter()
                                        .map(|(field, spec)| {
                                            read_column(
                                                field,
                                                spec,
                                                &format!("{case_path}.{field}"),
                                            )
                                        })
                                        .collect::<Result<_, _>>()?;
                                    Ok(EnumCaseDescriptor {
                                        name: case.clone(),
                                        fields,
                                    })
                                })
                                .collect::<Result<_, Refusal>>()?,
                        },
                        _ => {
                            return Err(refuse(
                                sub,
                                "an enum is an array of variants, or an object of cases and their fields",
                            ));
                        }
                    },
                    "json" => ColumnType::Json {
                        schema: match value {
                            Json::Bool(true) => None,
                            other => Some(to_serde(other)),
                        },
                    },
                    "vector" => ColumnType::Vector {
                        dimensions: match value {
                            Json::Number(n) => n.parse::<u32>().ok(),
                            _ => None,
                        }
                        .filter(|n| (1..=jazz_types::records::VECTOR_MAX_DIMENSIONS).contains(n))
                        .ok_or_else(|| {
                            refuse(&sub, "a vector's dimensions are an integer from 1 to 4096")
                        })?,
                    },
                    _ => {
                        let element = read_type(value, &sub)?.0;
                        if matches!(element, ColumnType::Vector { .. }) {
                            return Err(refuse(sub, "an array of vectors is not supported"));
                        }
                        ColumnType::Array {
                            element: Box::new(element),
                        }
                    }
                },
                false,
            ))
        }
        other => Err(refuse(
            path,
            format!(
                "a column is a type string or an object, not {}",
                other.kind()
            ),
        )),
    }
}

/// A value of `ty` from its JSON.
fn read_value(value: &Json, ty: &ColumnType, path: &str) -> Result<Value, Refusal> {
    let wrong = || {
        refuse(
            path,
            format!("{} doesn't fit this column's type", value.kind()),
        )
    };
    Ok(match (ty, value) {
        (_, Json::Null) => Value::Null,
        (ColumnType::Integer, Json::Number(n)) => Value::Integer(n.parse().map_err(|_| wrong())?),
        (ColumnType::BigInt, Json::String(n)) => Value::BigInt(n.parse().map_err(|_| wrong())?),
        (ColumnType::Double, Json::Number(n)) => Value::Double(n.parse().map_err(|_| wrong())?),
        (ColumnType::Boolean, Json::Bool(b)) => Value::Boolean(*b),
        (ColumnType::Text | ColumnType::Enum { .. }, Json::String(s)) => Value::Text(s.clone()),
        (ColumnType::Timestamp, Json::Number(n)) => {
            Value::Timestamp(n.parse().map_err(|_| wrong())?)
        }
        (ColumnType::Uuid, Json::String(s)) => Value::Uuid(ObjectId::from_uuid(
            uuid::Uuid::parse_str(s).map_err(|_| refuse(path, format!("`{s}` is not a UUID")))?,
        )),
        (ColumnType::Vector { dimensions }, Json::Array(items)) => {
            if items.len() != *dimensions as usize {
                return Err(refuse(
                    path,
                    format!(
                        "a vector default has {dimensions} numbers, not {}",
                        items.len()
                    ),
                ));
            }
            let mut bytes = Vec::with_capacity(items.len() * 4);
            for (i, item) in items.iter().enumerate() {
                let x = match item {
                    Json::Number(n) => n.parse::<f64>().ok().map(|x| x as f32),
                    _ => None,
                }
                .filter(|x| x.is_finite())
                .ok_or_else(|| {
                    refuse(
                        format!("{path}[{i}]"),
                        "a vector holds finite numbers within float32 range",
                    )
                })?;
                bytes.extend_from_slice(&x.to_le_bytes());
            }
            Value::Bytea(bytes)
        }
        (ColumnType::Bytea, Json::String(s)) => Value::Bytea(
            base64::engine::general_purpose::STANDARD
                .decode(s)
                .map_err(|_| refuse(path, "bytes are written as base64"))?,
        ),
        (ColumnType::Json { .. }, other) => {
            Value::Text(serde_json::to_string(&to_serde(other)).expect("JSON"))
        }
        (ColumnType::Array { element }, Json::Array(items)) => Value::Array(
            items
                .iter()
                .enumerate()
                .map(|(i, item)| read_value(item, element, &format!("{path}[{i}]")))
                .collect::<Result<_, _>>()?,
        ),
        (ColumnType::EnumPayload { cases }, Json::Object(entries)) if entries.len() == 1 => {
            let (case, fields) = &entries[0];
            let descriptor = cases
                .iter()
                .find(|c| &c.name == case)
                .ok_or_else(|| refuse(path, format!("unknown case `{case}`")))?;
            let values = descriptor
                .fields
                .iter()
                .map(|field| {
                    let value = fields.get(field.name_str()).unwrap_or(&Json::Null);
                    read_value(
                        value,
                        &field.column_type,
                        &format!("{path}.{case}.{}", field.name_str()),
                    )
                })
                .collect::<Result<_, _>>()?;
            Value::Enum {
                case: case.clone(),
                values,
            }
        }
        _ => return Err(wrong()),
    })
}

fn read_migration(
    item: &Json,
    path: &str,
    history: &[(String, Schema)],
    current: &Schema,
) -> Result<(String, Option<String>, Vec<PublishTableLens>), Refusal> {
    let fields = object(item, path)?;
    only_keys(fields, MIGRATION_KEYS, path)?;
    let version = |key: &str| -> Result<Option<String>, Refusal> {
        let Some(value) = item.get(key) else {
            return Ok(None);
        };
        let name = string(value, &format!("{path}.{key}"))?;
        if !history.iter().any(|(n, _)| n == name) {
            return Err(refuse(
                format!("{path}.{key}"),
                format!(
                    "no version `{name}` in `history`{}",
                    super::expr::did_you_mean(name, history.iter().map(|(n, _)| n.as_str()))
                ),
            ));
        }
        Ok(Some(name.to_string()))
    };
    if let Some(name) = item.get("name") {
        string(name, &format!("{path}.name"))?;
    }
    let from = version("from")?
        .ok_or_else(|| refuse(path, "a migration needs `from`, a version of `history`"))?;
    let to = version("to")?;
    let source = &history.iter().find(|(n, _)| *n == from).expect("checked").1;
    let target = match &to {
        Some(name) => &history.iter().find(|(n, _)| n == name).expect("checked").1,
        None => current,
    };
    let mut lenses: BTreeMap<String, PublishTableLens> = BTreeMap::new();
    let lens = |lenses: &mut BTreeMap<String, PublishTableLens>, table: &str| -> PublishTableLens {
        lenses.remove(table).unwrap_or(PublishTableLens {
            table: table.to_string(),
            added: false,
            removed: false,
            renamed_from: None,
            operations: Vec::new(),
        })
    };
    if let Some(tables) = item.get("createTables") {
        for table in names(tables, &format!("{path}.createTables"))? {
            let mut l = lens(&mut lenses, &table);
            l.added = true;
            lenses.insert(table, l);
        }
    }
    if let Some(tables) = item.get("dropTables") {
        for table in names(tables, &format!("{path}.dropTables"))? {
            let mut l = lens(&mut lenses, &table);
            l.removed = true;
            lenses.insert(table, l);
        }
    }
    if let Some(renames) = item.get("renameTables") {
        for (table, old) in object(renames, &format!("{path}.renameTables"))? {
            let mut l = lens(&mut lenses, table);
            l.renamed_from =
                Some(string(old, &format!("{path}.renameTables.{table}"))?.to_string());
            lenses.insert(table.clone(), l);
        }
    }
    if let Some(migrate) = item.get("migrate") {
        for (table, columns) in object(migrate, &format!("{path}.migrate"))? {
            if table == "$comment" {
                string(columns, &format!("{path}.migrate.$comment"))?;
                continue;
            }
            let mut l = lens(&mut lenses, table);
            for (column, op) in object(columns, &format!("{path}.migrate.{table}"))? {
                if column == "$comment" {
                    string(op, &format!("{path}.migrate.{table}.$comment"))?;
                    continue;
                }
                let op_path = format!("{path}.migrate.{table}.{column}");
                let fields = object(op, &op_path)?;
                let ty_in = |schema: &Schema, key: &str| -> Result<ColumnType, Refusal> {
                    let spec = op.get(key).expect("present");
                    let (ty, _) = read_type(spec, &format!("{op_path}.{key}"))?;
                    let _ = schema;
                    Ok(ty)
                };
                l.operations.push(if op.get("add").is_some() {
                    only_keys(fields, &["add", "default"], &op_path)?;
                    let ty = ty_in(target, "add")?;
                    let value = match op.get("default") {
                        Some(v) => read_value(v, &ty, &format!("{op_path}.default"))?,
                        None => Value::Null,
                    };
                    PublishLensOp::Introduce { column: column.clone(), column_type: ty, value }
                } else if op.get("drop").is_some() {
                    only_keys(fields, &["drop", "backwardsDefault"], &op_path)?;
                    let ty = ty_in(source, "drop")?;
                    let value = match op.get("backwardsDefault") {
                        Some(v) => read_value(v, &ty, &format!("{op_path}.backwardsDefault"))?,
                        None => Value::Null,
                    };
                    PublishLensOp::Drop { column: column.clone(), column_type: ty, value }
                } else if let Some(old) = op.get("renameFrom") {
                    only_keys(fields, &["renameFrom"], &op_path)?;
                    PublishLensOp::Rename {
                        column: string(old, &format!("{op_path}.renameFrom"))?.to_string(),
                        value: column.clone(),
                    }
                } else {
                    return Err(refuse(
                        op_path,
                        "a column change is { \"add\": <type>, \"default\": <value> }, { \"drop\": <type>, \"backwardsDefault\": <value> } or { \"renameFrom\": <old name> }",
                    ));
                });
            }
            lenses.insert(table.clone(), l);
        }
    }
    Ok((from, to, lenses.into_values().collect()))
}

/// Our JSON as serde_json's, for JSON Schemas.
pub fn to_serde(value: &Json) -> serde_json::Value {
    match value {
        Json::Null => serde_json::Value::Null,
        Json::Bool(b) => serde_json::Value::Bool(*b),
        Json::Number(n) => serde_json::from_str(n).unwrap_or(serde_json::Value::Null),
        Json::String(s) => serde_json::Value::String(s.clone()),
        Json::Array(items) => serde_json::Value::Array(items.iter().map(to_serde).collect()),
        Json::Object(entries) => serde_json::Value::Object(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), to_serde(v)))
                .collect(),
        ),
    }
}

pub fn from_serde(value: &serde_json::Value) -> Json {
    match value {
        serde_json::Value::Null => Json::Null,
        serde_json::Value::Bool(b) => Json::Bool(*b),
        serde_json::Value::Number(n) => Json::Number(n.to_string()),
        serde_json::Value::String(s) => Json::String(s.clone()),
        serde_json::Value::Array(items) => Json::Array(items.iter().map(from_serde).collect()),
        serde_json::Value::Object(map) => Json::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), from_serde(v)))
                .collect(),
        ),
    }
}

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

/// A version's name: its short schema hash, as snapshot names end.
pub fn version_name(hash: &SchemaHash) -> String {
    hash.to_string()[..12].to_string()
}

pub fn write(
    current: &Schema,
    history: &[(String, &Schema)],
    migrations: &[(Option<String>, Option<String>, &PublishMigrationRequest)],
    codegen: &Codegen,
) -> Result<Json, String> {
    let mut top = vec![
        (
            "format".to_string(),
            Json::Number(super::FORMAT.to_string()),
        ),
        (
            "tables".to_string(),
            write_tables(current, Some(codegen)).map_err(|e| format!("tables.{e}"))?,
        ),
    ];
    if !history.is_empty() {
        top.push((
            "history".into(),
            Json::Object(
                history
                    .iter()
                    .map(|(name, schema)| {
                        Ok((
                            name.clone(),
                            Json::Object(vec![(
                                "tables".into(),
                                write_tables(schema, None)
                                    .map_err(|e| format!("history.{name}.tables.{e}"))?,
                            )]),
                        ))
                    })
                    .collect::<Result<_, String>>()?,
            ),
        ));
    }
    if !migrations.is_empty() {
        top.push((
            "migrations".into(),
            Json::Array(
                migrations
                    .iter()
                    .enumerate()
                    .map(|(i, (from, to, m))| {
                        write_migration(from.as_deref(), to.as_deref(), m)
                            .map_err(|e| format!("migrations[{i}]: {e}"))
                    })
                    .collect::<Result<_, _>>()?,
            ),
        ));
    }
    if !codegen.transforms.is_empty() {
        top.push((
            "transforms".into(),
            Json::Object(
                codegen
                    .transforms
                    .iter()
                    .map(|(name, decl)| {
                        Ok((
                            name.clone(),
                            Json::Object(vec![
                                ("stored".into(), write_type(&decl.stored)?),
                                ("view".into(), write_type(&decl.view)?),
                            ]),
                        ))
                    })
                    .collect::<Result<_, String>>()
                    .map_err(|e| format!("transforms: {e}"))?,
            ),
        ));
    }
    Ok(Json::Object(top))
}

/// A history version's `tables`: a schema without relations, computed fields
/// or transforms.
pub fn write_version_tables(schema: &Schema) -> Result<Json, String> {
    write_tables(schema, None)
}

fn write_tables(schema: &Schema, codegen: Option<&Codegen>) -> Result<Json, String> {
    let mut tables = Vec::new();
    for (name, table) in schema.iter() {
        let name = name.as_str();
        let info = codegen.and_then(|c| c.tables.get(name));
        let relations = info.map(|i| &i.relations);
        let covered = |column: &str, target: &TableName| {
            relations.is_some_and(|r| {
                r.values().any(|rel| matches!(rel, CodegenRelation::Forward { table, column: c } if c == column && table == target.as_str()))
            })
        };
        let mut fields = vec![(
            "columns".to_string(),
            Json::Object(
                table
                    .columns
                    .columns
                    .iter()
                    .map(|c| {
                        let explicit = c.references.as_ref().filter(|t| !covered(c.name_str(), t));
                        let mut spec = write_column(c, explicit)
                            .map_err(|e| format!("{name}.columns.{}: {e}", c.name_str()))?;
                        if let Some(transform) = info.and_then(|i| i.transforms.get(c.name_str())) {
                            spec = with_transform(spec, transform);
                        }
                        Ok((c.name_str().to_string(), spec))
                    })
                    .collect::<Result<_, String>>()?,
            ),
        )];
        if let Some(relations) = relations.filter(|r| !r.is_empty()) {
            fields.push((
                "relations".into(),
                Json::Object(
                    relations
                        .iter()
                        .map(|(rel, target)| {
                            (
                                rel.clone(),
                                match target {
                                    CodegenRelation::Forward { table, column } => {
                                        Json::object(vec![
                                            ("rel", Json::string(table)),
                                            ("column", Json::string(column)),
                                        ])
                                    }
                                    CodegenRelation::Reverse { table, relation } => {
                                        Json::object(vec![
                                            ("reverse", Json::string(table)),
                                            ("relation", Json::string(relation)),
                                        ])
                                    }
                                },
                            )
                        })
                        .collect(),
                ),
            ));
        }
        let mut indexes = Vec::new();
        if let Some(only) = &table.indexed_columns {
            if only.is_empty() {
                return Err(format!(
                    "{name}: {}",
                    super::expr::unsupported("an empty `indexOnly`")
                ));
            }
            indexes.extend(only.iter().map(|c| Json::string(c.as_str())));
        }
        for index in &table.composite_indexes {
            indexes.push(Json::Array(
                index.iter().map(|c| Json::string(c.as_str())).collect(),
            ));
        }
        let mut specialised: Vec<&SpecialisedIndex> = table.specialised_indexes.iter().collect();
        specialised.sort_by(|left, right| left.name.cmp(&right.name));
        indexes.extend(specialised.into_iter().map(write_specialised_index));
        if !indexes.is_empty() {
            fields.push(("indexes".into(), Json::Array(indexes)));
        }
        if !table.branch_by.is_empty() {
            fields.push((
                "branchBy".into(),
                Json::Array(
                    table
                        .branch_by
                        .iter()
                        .map(|c| Json::string(c.as_str()))
                        .collect(),
                ),
            ));
        }
        if table.history == HistoryRetention::Latest {
            fields.push(("history".into(), Json::string("latest")));
        }
        if let Some(info) = info.filter(|i| !i.computed.is_empty()) {
            fields.push((
                "computed".into(),
                Json::Object(
                    info.computed
                        .iter()
                        .map(|field| {
                            let mut ty = ColumnDescriptor::new(
                                field.name.as_str(),
                                field.field_type.column_type.clone(),
                            );
                            ty.nullable = field.field_type.nullable;
                            Ok((
                                field.name.clone(),
                                Json::Object(vec![
                                    ("type".into(), write_column(&ty, None)?),
                                    (
                                        "from".into(),
                                        Json::Array(field.from.iter().map(Json::string).collect()),
                                    ),
                                ]),
                            ))
                        })
                        .collect::<Result<_, String>>()
                        .map_err(|e| format!("{name}.computed: {e}"))?,
                ),
            ));
        }
        if let Some(info) = info.filter(|i| !i.ts_only.is_empty()) {
            fields.push((
                "tsOnly".into(),
                Json::Object(
                    info.ts_only
                        .iter()
                        .map(|c| match c {
                            TsOnlyColumn::Computed { column } => {
                                (column.clone(), Json::string("computed"))
                            }
                            TsOnlyColumn::Transform { column } => {
                                (column.clone(), Json::string("transform"))
                            }
                        })
                        .collect(),
                ),
            ));
        }
        tables.push((name.to_string(), Json::Object(fields)));
    }
    Ok(Json::Object(tables))
}

/// A column type's spec, without options.
/// A column type's spec, without options, as the file writes it.
pub fn type_spec(ty: &ColumnType) -> Result<Json, String> {
    write_type(ty)
}

fn write_type(ty: &ColumnType) -> Result<Json, String> {
    Ok(match ty {
        ColumnType::Text => Json::string("string"),
        ColumnType::Integer => Json::string("int"),
        ColumnType::BigInt => Json::string("bigint"),
        ColumnType::Double => Json::string("float"),
        ColumnType::Boolean => Json::string("boolean"),
        ColumnType::Timestamp => Json::string("timestamp"),
        ColumnType::Uuid => Json::string("uuid"),
        ColumnType::Bytea => Json::string("bytes"),
        ColumnType::Enum { variants } => Json::object(vec![(
            "enum",
            Json::Array(variants.iter().map(Json::string).collect()),
        )]),
        ColumnType::EnumPayload { cases } => Json::object(vec![(
            "enum",
            Json::Object(
                cases
                    .iter()
                    .map(|case| {
                        Ok((
                            case.name.clone(),
                            Json::Object(
                                case.fields
                                    .iter()
                                    .map(|f| {
                                        Ok((
                                            f.name_str().to_string(),
                                            write_column(f, f.references.as_ref())?,
                                        ))
                                    })
                                    .collect::<Result<_, String>>()?,
                            ),
                        ))
                    })
                    .collect::<Result<_, String>>()?,
            ),
        )]),
        ColumnType::Json { schema } => Json::object(vec![(
            "json",
            schema.as_ref().map_or(Json::Bool(true), from_serde),
        )]),
        ColumnType::Array { element } => Json::object(vec![("array", write_type(element)?)]),
        ColumnType::ScalarEnum { .. } | ColumnType::CatalogueEnumPayload { .. } => {
            return Err(super::expr::unsupported("a catalogue-native enum"));
        }
        ColumnType::TransactionId => {
            return Err(super::expr::unsupported("a transaction-id column"));
        }
        ColumnType::Row { .. } => return Err(super::expr::unsupported("a row column")),
        ColumnType::Vector { dimensions } => {
            Json::object(vec![("vector", Json::Number(dimensions.to_string()))])
        }
    })
}

/// `spec` naming `transform`: in object form, `transform` last.
fn with_transform(spec: Json, transform: &str) -> Json {
    let mut fields = match spec {
        Json::String(text) => match text.strip_suffix('?') {
            Some(base) => vec![
                ("type".to_string(), Json::string(base)),
                ("optional".to_string(), Json::Bool(true)),
            ],
            None => vec![("type".to_string(), Json::String(text))],
        },
        Json::Object(fields) => fields,
        other => return other,
    };
    fields.push(("transform".into(), Json::string(transform)));
    Json::Object(fields)
}

fn write_column(column: &ColumnDescriptor, references: Option<&TableName>) -> Result<Json, String> {
    let ty = write_type(&column.column_type)?;
    let plain = column.default.is_none() && column.merge_strategy.is_none() && references.is_none();
    if plain && let Json::String(name) = &ty {
        return Ok(Json::String(if column.nullable {
            format!("{name}?")
        } else {
            name.clone()
        }));
    }
    let mut fields = match ty {
        Json::String(name) => vec![("type".to_string(), Json::String(name))],
        Json::Object(entries) => entries,
        _ => unreachable!("types are strings or objects"),
    };
    if column.nullable {
        fields.push(("optional".into(), Json::Bool(true)));
    }
    if let Some(default) = &column.default {
        fields.push(("default".into(), write_value(default, &column.column_type)?));
    }
    if let Some(merge) = column.merge_strategy {
        fields.push((
            "merge".into(),
            Json::string(match merge {
                ColumnMergeStrategy::Counter => "counter",
                ColumnMergeStrategy::GSet => "g-set",
                ColumnMergeStrategy::Text => "text",
                ColumnMergeStrategy::Markdown => "markdown",
                ColumnMergeStrategy::Html => "html",
            }),
        ));
    }
    if let Some(target) = references {
        fields.push(("references".into(), Json::string(target.as_str())));
    }
    Ok(Json::Object(fields))
}

/// A value of `ty` as the file writes it.
pub fn value_spec(value: &Value, ty: &ColumnType) -> Result<Json, String> {
    write_value(value, ty)
}

fn write_value(value: &Value, ty: &ColumnType) -> Result<Json, String> {
    Ok(match (value, ty) {
        (Value::Null, _) => Json::Null,
        (Value::Integer(n), _) => Json::Number(n.to_string()),
        (Value::BigInt(n), _) => Json::String(n.to_string()),
        (Value::Double(d), _) if d.is_finite() => Json::Number(
            serde_json::Number::from_f64(*d)
                .expect("finite")
                .to_string(),
        ),
        (Value::Boolean(b), _) => Json::Bool(*b),
        (Value::Text(s), ColumnType::Json { .. }) => from_serde(
            &serde_json::from_str(s)
                .map_err(|_| super::expr::unsupported("a json default that isn't JSON"))?,
        ),
        (Value::Text(s), _) => Json::String(s.clone()),
        (Value::Timestamp(n), _) => Json::Number(n.to_string()),
        (Value::Uuid(id), _) => Json::String(id.to_string()),
        // `f32le/1` bytes as numbers; Rust prints each `f32` in its shortest
        // form that reads back to the same bits.
        (Value::Bytea(bytes), ColumnType::Vector { .. }) if bytes.len() % 4 == 0 => Json::Array(
            bytes
                .chunks_exact(4)
                .map(|c| Json::Number(f32::from_le_bytes([c[0], c[1], c[2], c[3]]).to_string()))
                .collect(),
        ),
        (Value::Bytea(bytes), _) => {
            Json::String(base64::engine::general_purpose::STANDARD.encode(bytes))
        }
        (Value::Array(items), ColumnType::Array { element }) => Json::Array(
            items
                .iter()
                .map(|v| write_value(v, element))
                .collect::<Result<_, _>>()?,
        ),
        (Value::Enum { case, values }, ColumnType::EnumPayload { cases }) => {
            let descriptor = cases
                .iter()
                .find(|c| &c.name == case)
                .ok_or_else(|| super::expr::unsupported("an enum default of an unknown case"))?;
            Json::Object(vec![(
                case.clone(),
                Json::Object(
                    descriptor
                        .fields
                        .iter()
                        .zip(values)
                        .map(|(f, v)| {
                            Ok((f.name_str().to_string(), write_value(v, &f.column_type)?))
                        })
                        .collect::<Result<_, String>>()?,
                ),
            )])
        }
        (other, _) => return Err(super::expr::unsupported(&format!("the default {other:?}"))),
    })
}

fn write_migration(
    from: Option<&str>,
    to: Option<&str>,
    migration: &PublishMigrationRequest,
) -> Result<Json, String> {
    let from =
        from.ok_or_else(|| super::expr::unsupported("a migration from the current version"))?;
    let mut fields = vec![("from".to_string(), Json::string(from))];
    if let Some(to) = to {
        fields.push(("to".into(), Json::string(to)));
    }
    let mut create = Vec::new();
    let mut drop = Vec::new();
    let mut renames = Vec::new();
    let mut migrate = Vec::new();
    for lens in &migration.forward {
        if lens.added && lens.removed {
            return Err(format!(
                "table {} cannot be both added and removed",
                lens.table
            ));
        }
        if lens.added {
            create.push(Json::string(&lens.table));
        }
        if lens.removed {
            drop.push(Json::string(&lens.table));
        }
        if let Some(old) = &lens.renamed_from {
            renames.push((lens.table.clone(), Json::string(old)));
        }
        if lens.operations.is_empty() {
            // A table lens with no column changes (an enum gaining variants,
            // say) is still a lens: keep it as an empty entry.
            if !lens.added && !lens.removed && lens.renamed_from.is_none() {
                migrate.push((lens.table.clone(), Json::Object(Vec::new())));
            }
            continue;
        }
        let ops = lens
            .operations
            .iter()
            .map(|op| {
                Ok(match op {
                    PublishLensOp::Introduce {
                        column,
                        column_type,
                        value,
                    } => (
                        column.clone(),
                        Json::Object(vec![
                            ("add".into(), write_type(column_type)?),
                            ("default".into(), write_value(value, column_type)?),
                        ]),
                    ),
                    PublishLensOp::Drop {
                        column,
                        column_type,
                        value,
                    } => (
                        column.clone(),
                        Json::Object(vec![
                            ("drop".into(), write_type(column_type)?),
                            ("backwardsDefault".into(), write_value(value, column_type)?),
                        ]),
                    ),
                    PublishLensOp::Rename { column, value } => (
                        value.clone(),
                        Json::object(vec![("renameFrom", Json::string(column))]),
                    ),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        migrate.push((lens.table.clone(), Json::Object(ops)));
    }
    if !create.is_empty() {
        fields.push(("createTables".into(), Json::Array(create)));
    }
    if !drop.is_empty() {
        fields.push(("dropTables".into(), Json::Array(drop)));
    }
    if !renames.is_empty() {
        fields.push(("renameTables".into(), Json::Object(renames)));
    }
    if !migrate.is_empty() {
        fields.push(("migrate".into(), Json::Object(migrate)));
    }
    Ok(Json::Object(fields))
}
