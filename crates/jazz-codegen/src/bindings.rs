//! Reading committed bindings back: the `source` and compiled schema a
//! generated module embeds. Bindings are how `jazz validate` knows the
//! schema the app last generated against, so it can tell an edit of
//! `tables` that needs a migration from one that doesn't, and how
//! `migrations create` finds the version to migrate from.

use jazz::tools::deployment::artifact::json::{self, Json};
use jazz::tools::deployment::published::{PublishedSchema, parse_schema_hash};
use jazz::tools::public_schema::{Schema, SchemaHash};

use crate::model::App;
use crate::{CodegenError, ts};

/// What committed bindings record of the app they were generated from.
#[derive(Debug, Clone)]
pub struct Recorded {
    /// `sha256:…` of the `jazz.app.json` they were generated from.
    pub source: String,
    /// The bindings format ([`ts::FORMAT`]) they were written in.
    pub format: u32,
    /// That file's current schema.
    pub current: SchemaHash,
    pub current_schema: Schema,
}

/// Read what generated bindings embed. Only a module this generator wrote
/// is read; anything else is refused with what to do.
pub fn read(bindings: &str) -> Result<Recorded, CodegenError> {
    let refuse = |what: &str| {
        CodegenError::new(format!(
            "{}: {what}; regenerate it with `jazz-tools validate`",
            ts::FILE
        ))
    };
    if !bindings.starts_with(ts::HEADER) {
        return Err(refuse(
            "not a module jazz-tools generated (its first line differs)",
        ));
    }
    let format = bindings
        .lines()
        .nth(1)
        .and_then(|line| line.strip_prefix("// Bindings format "))
        .and_then(|rest| rest.split('.').next())
        .and_then(|n| n.parse::<u32>().ok())
        .ok_or_else(|| refuse("no `// Bindings format <n>.` second line"))?;
    let call = bindings
        .find(ts::BIND_APP)
        .ok_or_else(|| refuse("no `export const app = bindApp<…>(…)`"))?;
    let options = &bindings[call..];
    let value = |key: &str| -> Result<Json, CodegenError> {
        let marker = format!("\n  \"{key}\": ");
        let at = options
            .find(&marker)
            .ok_or_else(|| refuse(&format!("no `{key}` in bindApp's options")))?
            + marker.len();
        let text = literal_prefix(&options[at..])
            .ok_or_else(|| refuse(&format!("`{key}` is cut short")))?;
        json::parse(as_json(text).as_bytes())
            .map_err(|error| refuse(&format!("`{key}` was edited ({error})")))
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

/// The generator's object literal as JSON: no trailing commas, and the
/// computed key `["__proto__"]` as the plain key it stands for. (Inside a
/// string a `"` is always escaped, so neither pattern occurs there.)
fn as_json(literal: &str) -> String {
    let literal = literal.replace("[\"__proto__\"]:", "\"__proto__\":");
    let mut out = String::with_capacity(literal.len());
    let mut in_string = false;
    let mut escaped = false;
    let chars: Vec<char> = literal.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        if in_string {
            match (escaped, c) {
                (true, _) => escaped = false,
                (false, '\\') => escaped = true,
                (false, '"') => in_string = false,
                _ => {}
            }
            out.push(c);
            continue;
        }
        if c == '"' {
            in_string = true;
        }
        if c == ','
            && chars[i + 1..]
                .iter()
                .find(|c| !c.is_whitespace())
                .is_some_and(|c| *c == '}' || *c == ']')
        {
            continue;
        }
        out.push(c);
    }
    out
}

/// Why committed bindings can't be used with the app file as it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Staleness {
    /// There are none yet.
    Missing,
    /// `tables` changed since the bindings were generated and no migration
    /// leads from that version: `migrations create` is needed.
    SchemaChanged,
    /// The file changed otherwise, or another Jazz version wrote them:
    /// regenerating is enough. (Formatting doesn't make bindings stale:
    /// only their `source` and format count.)
    OutOfDate,
}

impl Staleness {
    pub fn message(&self) -> String {
        match self {
            Staleness::Missing => format!("no {}: run `jazz-tools validate`", ts::FILE),
            Staleness::SchemaChanged => "the schema changed since the last version: run \
                 `jazz-tools migrations create --name <name>`"
                .into(),
            Staleness::OutOfDate => "bindings out of date: run `jazz-tools validate`".into(),
        }
    }
}

/// Whether `bindings` (the committed module, if any) fit `app`.
pub fn staleness(app: &App, bindings: Option<&str>) -> Result<Option<Staleness>, CodegenError> {
    let Some(bindings) = bindings else {
        return Ok(Some(Staleness::Missing));
    };
    let recorded = read(bindings)?;
    if !app
        .compiled
        .schemas
        .iter()
        .any(|(h, _)| *h == recorded.current)
    {
        return Ok(Some(Staleness::SchemaChanged));
    }
    if recorded.source != app.source || recorded.format != ts::FORMAT {
        return Ok(Some(Staleness::OutOfDate));
    }
    Ok(None)
}
