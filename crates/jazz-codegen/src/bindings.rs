//! Reading committed bindings back: the `source` and compiled schema a
//! generated module embeds. Bindings are how `jazz validate` knows the
//! schema the app last generated against, so it can tell an edit of
//! `tables` that needs a migration from one that doesn't, and how
//! `migrations create` finds the version to migrate from.

use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use jazz::tools::deployment::artifact::encryption::{
    self, DeclaredEncryption, Entries, MigrationNames,
};
use jazz::tools::deployment::artifact::json::{self, Json};
use jazz::tools::deployment::published::{
    PublishMigrationRequest, PublishedSchema, parse_schema_hash,
};
use jazz::tools::public_schema::{Schema, SchemaHash};

use crate::model::{App, Compiled};
use crate::{CodegenError, Lang, ts};

/// What committed bindings record of the app they were generated from.
#[derive(Debug, Clone)]
pub struct Recorded {
    /// `sha256:…` of the `jazz.app.json` they were generated from.
    pub source: String,
    /// The bindings format (e.g. [`ts::FORMAT`]) they were written in.
    pub format: u32,
    /// That file's current schema.
    pub current: SchemaHash,
    pub current_schema: Schema,
    /// The current version's encryption entries, by table (empty when it
    /// declares none): `migrations create` keeps them on the version it
    /// moves into `history`.
    pub encryption: Entries,
    /// Includes versions that shipped with no encryption declarations.
    history_encryption: BTreeMap<String, Entries>,
}

impl Recorded {
    pub(crate) fn from_app(app: App, format: u32) -> Self {
        let compiled = app.compiled;
        let current_schema = compiled.current_schema().clone();
        let (encryption, history_encryption) = recorded_entries(
            compiled.schemas.iter().map(|(hash, _)| hash.to_string()),
            compiled.current,
            compiled.encryption,
        );
        Self {
            source: app.source,
            format,
            current: compiled.current,
            current_schema,
            encryption,
            history_encryption,
        }
    }
}

/// An encrypted column is stored as bytes whatever its plaintext type.
/// Compare declarations for every shared schema hash before regeneration
/// can overwrite what that version shipped with.
pub(crate) fn check_recorded_declarations(
    app: &App,
    recorded: &Recorded,
) -> Result<(), CodegenError> {
    let versions: Vec<SchemaHash> = app.compiled.schemas.iter().map(|(h, _)| *h).collect();
    check_declarations_of(&versions, app.compiled.encryption.as_ref(), recorded)
}

/// [`check_recorded_declarations`] for `versions` and their declarations
/// as read from the app file.
pub(crate) fn check_declarations_of(
    versions: &[SchemaHash],
    declared: Option<&DeclaredEncryption>,
    recorded: &Recorded,
) -> Result<(), CodegenError> {
    let empty = Entries::new();
    for version in versions {
        let hash = version.to_string();
        let before = if *version == recorded.current {
            &recorded.encryption
        } else if let Some(entries) = recorded.history_encryption.get(&hash) {
            entries
        } else {
            continue;
        };
        let after = declared
            .and_then(|declared| declared.entries(&hash))
            .unwrap_or(&empty);
        check_unchanged(&hash, before, after)?;
    }
    Ok(())
}

/// With no migration, every table and column keeps its identity. A shipped
/// hash keeps its declarations. A new hash is a sibling of the bindings'
/// version to the deploy, since no migration leads to it.
pub(crate) fn check_index_transition(
    recorded: &Recorded,
    to: &Schema,
    entries: Option<&Entries>,
) -> Result<(), CodegenError> {
    if recorded.encryption.is_empty() && entries.is_none() {
        return Ok(());
    }
    let target = SchemaHash::compute(to);
    if target == recorded.current {
        return check_unchanged(
            &target.to_string(),
            &recorded.encryption,
            entries.unwrap_or(&Entries::new()),
        );
    }
    let same = |name: &str| Some(name.to_owned());
    let same_column = |_: &str, name: &str| Some(name.to_owned());
    check_unlinked(
        &recorded.encryption,
        entries,
        same,
        same,
        same_column,
        same_column,
        false,
    )
}

/// Check the current version against the bindings' one, which is in the
/// file's history, as the deploy will. This takes the bindings' version as
/// deployed (codegen can't tell which versions are). A migration from it,
/// written or implied, was checked when the file loaded. Otherwise the
/// deploy compares the declarations of the columns both versions store,
/// as a descendant when migrations lead from it to the current version and
/// as a sibling when they don't. Codegen follows the file's migrations
/// through their nearest common version to find those columns, and checks
/// nothing when no single path connects the two versions.
pub(crate) fn check_recorded_transition(
    recorded: &Recorded,
    compiled: &Compiled,
) -> Result<(), CodegenError> {
    let (from, to) = (recorded.current, compiled.current);
    let entries = compiled
        .encryption
        .as_ref()
        .and_then(|declared| declared.entries(&to.to_string()));
    if from == to || recorded.encryption.is_empty() || entries.is_none() {
        return Ok(());
    }
    if compiled
        .migrations
        .iter()
        .any(|migration| ends(migration) == Some((from, to)))
    {
        return Ok(());
    }
    let Some(path) = only_path(compiled, from, to) else {
        return Ok(());
    };
    let steps: Vec<(MigrationNames, bool)> = path
        .iter()
        .map(|(migration, forward)| {
            let target = parse_schema_hash(&migration.to_hash).expect("loaded");
            let schema = &compiled
                .schemas
                .iter()
                .find(|(hash, _)| *hash == target)
                .expect("loaded")
                .1;
            (
                MigrationNames {
                    migration,
                    to: schema,
                },
                *forward,
            )
        })
        .collect();
    // Each step names a table and column in the next version along the path.
    let step = |(names, forward): &(MigrationNames, bool), (table, column): (String, String)| {
        if *forward {
            Some((names.new_table(&table)?, names.new_name(&table, &column)?))
        } else {
            Some((names.old_table(&table)?, names.old_name(&table, &column)?))
        }
    };
    let back = |(names, forward): &(MigrationNames, bool), (table, column): (String, String)| {
        if *forward {
            Some((names.old_table(&table)?, names.old_name(&table, &column)?))
        } else {
            Some((names.new_table(&table)?, names.new_name(&table, &column)?))
        }
    };
    let table_only = |pair: Option<(String, String)>| pair.map(|(table, _)| table);
    check_unlinked(
        &recorded.encryption,
        entries,
        |table| {
            table_only(
                steps
                    .iter()
                    .rev()
                    .try_fold((table.to_owned(), String::new()), |at, s| back(s, at)),
            )
        },
        |table| {
            table_only(
                steps
                    .iter()
                    .try_fold((table.to_owned(), String::new()), |at, s| step(s, at)),
            )
        },
        |table, name| {
            steps
                .iter()
                .rev()
                .try_fold((table.to_owned(), name.to_owned()), |at, s| back(s, at))
                .map(|(_, name)| name)
        },
        |table, name| {
            steps
                .iter()
                .try_fold((table.to_owned(), name.to_owned()), |at, s| step(s, at))
                .map(|(_, name)| name)
        },
        path.iter().all(|(_, forward)| *forward),
    )
}

/// The migrations from `from` to `to`, each with whether it runs that way,
/// when exactly one path connects the two versions. With more, the
/// deploy's stored identities depend on the order it admitted them in.
fn only_path(
    compiled: &Compiled,
    from: SchemaHash,
    to: SchemaHash,
) -> Option<Vec<(&PublishMigrationRequest, bool)>> {
    let mut reached = vec![from];
    let mut parent = HashMap::new();
    let mut next = 0;
    while let Some(&at) = reached.get(next) {
        next += 1;
        for (index, migration) in compiled.migrations.iter().enumerate() {
            let (other, forward) = match ends(migration) {
                Some((source, target)) if source == at => (target, true),
                Some((source, target)) if target == at => (source, false),
                _ => continue,
            };
            if !reached.contains(&other) {
                reached.push(other);
                parent.insert(other, (at, index, forward));
            }
        }
    }
    // Connected versions with one migration fewer than versions form a tree.
    let edges = compiled
        .migrations
        .iter()
        .filter_map(ends)
        .filter(|(source, _)| reached.contains(source))
        .count();
    if !reached.contains(&to) || edges + 1 != reached.len() {
        return None;
    }
    let mut path = Vec::new();
    let mut at = to;
    while at != from {
        let (previous, index, forward) = parent[&at];
        path.push((&compiled.migrations[index], forward));
        at = previous;
    }
    path.reverse();
    Some(path)
}

fn ends(migration: &PublishMigrationRequest) -> Option<(SchemaHash, SchemaHash)> {
    Some((
        parse_schema_hash(&migration.from_hash).ok()?,
        parse_schema_hash(&migration.to_hash).ok()?,
    ))
}

/// The deploy's check of a version that no migration leads to from the
/// bindings' version, with `precedes` true when migrations lead to it.
fn check_unlinked(
    recorded: &Entries,
    entries: Option<&Entries>,
    old_table: impl Fn(&str) -> Option<String>,
    new_table: impl Fn(&str) -> Option<String>,
    old_name: impl Fn(&str, &str) -> Option<String>,
    new_name: impl Fn(&str, &str) -> Option<String>,
    precedes: bool,
) -> Result<(), CodegenError> {
    let empty = Entries::new();
    let check = encryption::check_unlinked_transition(
        recorded,
        entries.unwrap_or(&empty),
        old_table,
        new_table,
        old_name,
        new_name,
        async || Ok::<_, Infallible>(precedes),
    );
    // Nothing in the check waits: its one callback answers at once.
    let Poll::Ready(Ok(result)) = pin!(check).poll(&mut Context::from_waker(Waker::noop())) else {
        unreachable!("the check runs to completion");
    };
    result.map_err(|reason| {
        CodegenError::new(format!(
            "{}: {reason}",
            jazz::tools::deployment::artifact::APP_FILE
        ))
    })
}

/// Refuse `after` as version `hash`'s declarations when they differ from
/// what it shipped, `before`.
fn check_unchanged(hash: &str, before: &Entries, after: &Entries) -> Result<(), CodegenError> {
    let changed = changed_declarations(before, after);
    if changed.is_empty() {
        return Ok(());
    }
    Err(CodegenError::new(format!(
        "{}: the encryption declarations of version {}, which your committed bindings were \
         generated from, can't change, and its stored columns are the same: {} changed. \
         Restore the declaration as that version shipped it. To change an encrypted column's \
         type, copy the data into a new encrypted table (docs/e2ee/encrypting-existing-data)",
        jazz::tools::deployment::artifact::APP_FILE,
        &hash[..12],
        changed.join(", ")
    )))
}

/// The tables and columns whose declarations differ between `before` and
/// `after`: `table.column` for an encrypted column, `table` for the rest of
/// an entry. The order of `equality` doesn't count.
fn changed_declarations(before: &Entries, after: &Entries) -> Vec<String> {
    let tables: std::collections::BTreeSet<&String> = before.keys().chain(after.keys()).collect();
    let mut changed = Vec::new();
    for table in tables {
        let (Some(old), Some(new)) = (before.get(table), after.get(table)) else {
            changed.push(table.clone());
            continue;
        };
        let columns: std::collections::BTreeSet<&String> =
            old.columns.keys().chain(new.columns.keys()).collect();
        for column in columns {
            if old.columns.get(column) != new.columns.get(column) {
                changed.push(format!("{table}.{column}"));
            }
        }
        let sorted = |list: &[String]| {
            let mut list = list.to_vec();
            list.sort();
            list
        };
        if old.space != new.space
            || old.scope != new.scope
            || sorted(&old.equality) != sorted(&new.equality)
        {
            changed.push(table.clone());
        }
    }
    changed
}

fn recorded_entries(
    versions: impl Iterator<Item = String>,
    current: SchemaHash,
    encryption: Option<DeclaredEncryption>,
) -> (Entries, BTreeMap<String, Entries>) {
    let mut entries = encryption.unwrap_or_default().versions;
    let current = current.to_string();
    let mut history = BTreeMap::new();
    for hash in versions.filter(|hash| hash != &current) {
        history
            .entry(hash)
            .or_insert_with_key(|hash| entries.remove(hash).unwrap_or_default());
    }
    (entries.remove(&current).unwrap_or_default(), history)
}

/// The digest of what the generator wrote that `lang`'s bindings record,
/// if they record one (TS: `Output <hex>` on the format line, which
/// formatters keep).
pub fn output_digest(lang: Lang, bindings: &str) -> Option<String> {
    match lang {
        Lang::TypeScript => bindings.lines().find_map(|line| {
            let rest = line.trim().strip_prefix("// Bindings format ")?;
            let at = rest.find(ts::OUTPUT)? + ts::OUTPUT.len();
            let digest = rest[at..].split('.').next()?;
            (digest.len() == 16 && digest.chars().all(|c| c.is_ascii_hexdigit()))
                .then(|| digest.to_string())
        }),
        _ => None,
    }
}

/// Whether `committed` bindings are other than what the generator writes
/// now (`generated`), formatting aside where the language records a digest
/// of its output: what plain `generate` rewrites, so a generator fix
/// reaches an app whose `jazz.app.json` didn't change.
pub fn output_changed(lang: Lang, committed: &str, generated: &str) -> bool {
    match output_digest(lang, generated) {
        Some(digest) => output_digest(lang, committed).as_deref() != Some(digest.as_str()),
        None => committed != generated,
    }
}

/// Read what bindings generated in `lang` embed. Only a file this
/// generator wrote is read; anything else is refused with what to do.
pub fn read(lang: Lang, bindings: &str) -> Result<Recorded, CodegenError> {
    match lang {
        Lang::TypeScript => read_ts(bindings),
        Lang::Rust => crate::rust::read(bindings),
        Lang::Swift => crate::swift::read(bindings),
        Lang::Go => crate::go::read(bindings),
        Lang::Kotlin => crate::kotlin::read(bindings),
    }
}

fn read_ts(bindings: &str) -> Result<Recorded, CodegenError> {
    let refuse = |what: &str| {
        CodegenError::new(format!(
            "{}: {what}; regenerate it with `npx jazz-tools generate`",
            ts::FILE
        ))
    };
    let bindings = bindings.strip_prefix('\u{feff}').unwrap_or(bindings);
    let first = bindings.trim_start();
    if !first.starts_with(ts::HEADER) && !first.starts_with(ts::LEGACY_HEADER) {
        return Err(refuse(
            "not a module jazz-tools generated (its first line differs)",
        ));
    }
    let format = bindings
        .lines()
        .find_map(|line| line.trim().strip_prefix("// Bindings format "))
        .and_then(|rest| rest.split('.').next())
        .and_then(|n| n.parse::<u32>().ok())
        .ok_or_else(|| refuse("no `// Bindings format <n>.` line"))?;
    let call = bindings
        .find("bindApp<")
        .ok_or_else(|| refuse("no `export const app = bindApp<…>(…)`"))?;
    let open = bindings[call..]
        .find('(')
        .and_then(|at| {
            bindings[call + at + 1..]
                .find('{')
                .map(|brace| call + at + 1 + brace)
        })
        .ok_or_else(|| refuse("no options object in `bindApp<…>(…)`"))?;
    let text = literal_prefix(&bindings[open..])
        .ok_or_else(|| refuse("bindApp's options are cut short"))?;
    let options = json::parse(as_json(text).as_bytes())
        .map_err(|error| refuse(&format!("bindApp's options were edited ({error})")))?;
    let value = |key: &str| -> Result<Json, CodegenError> {
        options
            .get(key)
            .cloned()
            .ok_or_else(|| refuse(&format!("no `{key}` in bindApp's options")))
    };
    let source = match value("source")? {
        Json::String(source) => source,
        _ => return Err(refuse("`source` is not a string")),
    };
    let schema = value("schema")?;
    let current = schema
        .get("current")
        .and_then(Json::as_str)
        .ok_or_else(|| refuse("`schema.current` is missing"))?;
    let current =
        parse_schema_hash(current).map_err(|e| refuse(&format!("schema.current: {e}")))?;
    let schemas: Vec<PublishedSchema> = schema
        .get("schemas")
        .map(|s| serde_json::from_str(&json::print(s)))
        .ok_or_else(|| refuse("`schema.schemas` is missing"))?
        .map_err(|e| refuse(&format!("schema.schemas: {e}")))?;
    let encryption = schema
        .get("encryption")
        .map(|e| serde_json::from_str::<DeclaredEncryption>(&json::print(e)))
        .transpose()
        .map_err(|e| refuse(&format!("schema.encryption: {e}")))?;
    let (encryption, history_encryption) = recorded_entries(
        schemas.iter().map(|schema| schema.hash.clone()),
        current,
        encryption,
    );
    let current_schema = schemas
        .into_iter()
        .find(|s| parse_schema_hash(&s.hash).is_ok_and(|h| h == current))
        .ok_or_else(|| refuse("`schema.schemas` lacks the current schema"))?
        .schema;
    if SchemaHash::compute(&current_schema) != current {
        return Err(refuse("the current schema doesn't match its hash"));
    }
    Ok(Recorded {
        format,
        source,
        current,
        current_schema,
        encryption,
        history_encryption,
    })
}

/// The text of the literal `text` starts with: up to its matching close.
fn literal_prefix(text: &str) -> Option<&str> {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for (i, c) in text.char_indices() {
        if in_string {
            match (escaped, c) {
                (true, _) => escaped = false,
                (false, '\\') => escaped = true,
                (false, '"') => {
                    in_string = false;
                    if depth == 0 {
                        return Some(&text[..=i]);
                    }
                }
                _ => {}
            }
            continue;
        }
        match c {
            '"' => in_string = true,
            '{' | '[' => depth += 1,
            '}' | ']' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(&text[..=i]);
                }
            }
            _ => {}
        }
    }
    None
}

/// A JS object literal as the generator writes it, or as a formatter
/// leaves it, as JSON: keys quoted or bare, trailing commas, shorthand
/// entries (`computed,`, which are values of jazz.computed.ts and dropped)
/// and the computed key `["__proto__"]` as the plain key it stands for.
/// (Inside a string a `"` is always escaped, so none of these patterns
/// occurs there.)
fn as_json(literal: &str) -> String {
    let literal = literal.replace("[\"__proto__\"]", "\"__proto__\"");
    let chars: Vec<char> = literal.chars().collect();
    let mut out = String::with_capacity(literal.len());
    let mut in_string = false;
    let mut escaped = false;
    let mut i = 0;
    let next_significant = |from: usize| chars[from..].iter().copied().find(|c| !c.is_whitespace());
    while i < chars.len() {
        let c = chars[i];
        if in_string {
            match (escaped, c) {
                (true, _) => escaped = false,
                (false, '\\') => escaped = true,
                (false, '"') => in_string = false,
                _ => {}
            }
            out.push(c);
            i += 1;
            continue;
        }
        if c == '"' {
            in_string = true;
        } else if c.is_ascii_alphabetic() || c == '_' || c == '$' {
            let start = i;
            while i < chars.len()
                && (chars[i].is_ascii_alphanumeric() || chars[i] == '_' || chars[i] == '$')
            {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            let before = out.chars().rev().find(|c| !c.is_whitespace());
            match next_significant(i) {
                Some(':') => out.push_str(&format!("\"{word}\"")),
                Some(',' | '}')
                    if matches!(before, Some('{' | ','))
                        && !["true", "false", "null"].contains(&word.as_str()) =>
                {
                    // A shorthand entry: drop it and its comma.
                    while i < chars.len() && chars[i].is_whitespace() {
                        i += 1;
                    }
                    if chars.get(i) == Some(&',') {
                        i += 1;
                    }
                }
                _ => out.push_str(&word),
            }
            continue;
        }
        if c == ',' && matches!(next_significant(i + 1), Some('}' | ']')) {
            i += 1;
            continue;
        }
        out.push(c);
        i += 1;
    }
    // A shorthand entry last leaves its predecessor's comma before `}`.
    let mut cleaned = String::with_capacity(out.len());
    let chars: Vec<char> = out.chars().collect();
    let mut in_string = false;
    let mut escaped = false;
    for (i, &c) in chars.iter().enumerate() {
        if in_string {
            match (escaped, c) {
                (true, _) => escaped = false,
                (false, '\\') => escaped = true,
                (false, '"') => in_string = false,
                _ => {}
            }
        } else if c == '"' {
            in_string = true;
        } else if c == ','
            && chars[i + 1..]
                .iter()
                .find(|c| !c.is_whitespace())
                .is_some_and(|c| *c == '}' || *c == ']')
        {
            continue;
        }
        cleaned.push(c);
    }
    cleaned
}

/// Why committed bindings can't be used with the app file as it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Staleness {
    /// There are none yet.
    Missing,
    /// `tables` changed since the bindings were generated and no migration
    /// leads from that version: `migrations create` is needed.
    SchemaChanged,
    /// They can't be read (edited, or not this generator's): regenerating
    /// replaces them, but whether `tables` changed since can't be told.
    Unreadable(String),
    /// The file changed otherwise, or another Jazz version wrote them:
    /// regenerating is enough. (Formatting doesn't make bindings stale:
    /// only their `source` and format count.)
    OutOfDate,
}

impl Staleness {
    pub fn message(&self, lang: Lang) -> String {
        match self {
            Staleness::Missing => format!("no {}: run `npx jazz-tools generate`", lang.file()),
            Staleness::SchemaChanged => "the schema changed since the last version: run \
                 `npx jazz-tools migrations create --name <name>`"
                .into(),
            Staleness::OutOfDate => "bindings out of date: run `npx jazz-tools generate`".into(),
            Staleness::Unreadable(why) => why.clone(),
        }
    }
}

/// Whether `bindings` (the committed file in `lang`, if any) fit `app`.
pub fn staleness(
    app: &App,
    lang: Lang,
    bindings: Option<&str>,
) -> Result<Option<Staleness>, CodegenError> {
    let Some(bindings) = bindings else {
        return Ok(Some(Staleness::Missing));
    };
    let recorded = match read(lang, bindings) {
        Ok(recorded) => recorded,
        Err(error) => return Ok(Some(Staleness::Unreadable(error.message().to_string()))),
    };
    check_recorded_declarations(app, &recorded)?;
    if !app
        .compiled
        .schemas
        .iter()
        .any(|(h, _)| *h == recorded.current)
    {
        // A change of single-column indexes changes the schema's hash, not
        // its version or its rows (#5494): regenerating is enough.
        let to = app.compiled.current_schema();
        if differs_only_in_indexes(&recorded.current_schema, to) {
            let entries = app
                .compiled
                .encryption
                .as_ref()
                .and_then(|declared| declared.entries(&app.compiled.current.to_string()));
            check_index_transition(&recorded, to, entries)?;
            return Ok(Some(Staleness::OutOfDate));
        }
        return Ok(Some(Staleness::SchemaChanged));
    }
    check_recorded_transition(&recorded, &app.compiled)?;
    if recorded.source != app.source || recorded.format != lang.format() {
        return Ok(Some(Staleness::OutOfDate));
    }
    Ok(None)
}

/// Whether going from `from` to `to` needs no migration because only
/// indexes changed: their tables differ in nothing but `indexed_columns` /
/// `composite_indexes`, and they are the same runtime version. That second
/// condition is the invariant deploys rely on (they check admitted versions,
/// not hashes). Single-column indexes keep the version; a composite index is
/// part of it (`tests/composite_indexes.rs`), so a composite change still
/// needs its (empty) migration.
pub(crate) fn differs_only_in_indexes(from: &Schema, to: &Schema) -> bool {
    let version = |schema: &Schema| {
        jazz::schema::JazzSchema::new(schema)
            .ok()
            .map(|schema| schema.version_id())
    };
    without_indexes(from) == without_indexes(to)
        && version(from).is_some()
        && version(from) == version(to)
}

/// `schema` with every table's indexes removed.
fn without_indexes(schema: &Schema) -> Schema {
    let mut schema = schema.clone();
    for table in schema.values_mut() {
        table.indexed_columns = Default::default();
        table.composite_indexes = Default::default();
    }
    schema
}
