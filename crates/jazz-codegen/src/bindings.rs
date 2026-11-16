//! Reading committed bindings back: the `source` and compiled schema a
//! generated module embeds. Bindings are how `jazz validate` knows the
//! schema the app last generated against, so it can tell an edit of
//! `tables` that needs a migration from one that doesn't, and how
//! `migrations create` finds the version to migrate from.

use jazz::tools::deployment::artifact::json::{self, Json};
use jazz::tools::deployment::published::{PublishedSchema, parse_schema_hash};
use jazz::tools::public_schema::{Schema, SchemaHash};

use crate::model::App;
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
    if !app
        .compiled
        .schemas
        .iter()
        .any(|(h, _)| *h == recorded.current)
    {
        // A change of single-column indexes changes the schema's hash, not
        // its version or its rows (#5494): regenerating is enough.
        if differs_only_in_indexes(&recorded.current_schema, app.compiled.current_schema()) {
            return Ok(Some(Staleness::OutOfDate));
        }
        return Ok(Some(Staleness::SchemaChanged));
    }
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
