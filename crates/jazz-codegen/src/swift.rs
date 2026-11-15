//! The Swift back end: `JazzApp.swift`, typed bindings over the `Jazz`
//! Swift package (`packages/jazz-swift`, which wraps `crates/jazz-ffi`).
//!
//! Per table: a `Codable` row struct (`Todo`), an insert struct (`NewTodo`)
//! and a patch struct (`TodoPatch`), with lowerCamelCase properties mapped
//! to the authored names by `CodingKeys`, and a namespace (`Todos`) with the
//! table's query and typed columns, which build the query JSON jazz-ffi
//! reads. `JazzApp` embeds `jazz.app.json` itself and opens it.
//!
//! Not supported yet, and refused with what to do: computed fields and
//! transforms (no Swift implementations can be passed to jazz-ffi yet).
//! Relations are left out of the bindings, as includes are not supported
//! by jazz-ffi yet.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use jazz::tools::public_schema::ColumnType;

use crate::CodegenError;
use crate::model::{App, Column, FieldType, Table};
use crate::names::{pascal, singular};

/// Where the bindings go, relative to `jazz.app.json`.
pub const FILE: &str = "JazzApp.swift";

/// The version of the bindings' shape this generator writes: the second
/// line records it.
pub const FORMAT: u32 = 1;

/// The first line, which marks a file as this generator's.
pub const HEADER: &str =
    "// JazzApp.swift: generated from jazz.app.json by `jazz-tools generate`. Do not edit.";

const FORMAT_PREFIX: &str = "// Bindings format ";

/// How the embedded `jazz.app.json` starts: a raw multi-line string follows.
const APP_PREFIX: &str = "    public static let app = ";

/// The enum that holds the recorded source and the embedded app.
const JAZZ_APP_OPEN: &str = "\npublic enum JazzApp {\n";

/// How the recorded source starts.
const SOURCE_PREFIX: &str = "    public static let source = ";

/// The runtime module.
const RT: &str = "Jazz";

/// Type names the bindings or their imports declare.
const RESERVED: &[&str] = &[
    "Jazz",
    "JazzApp",
    "Swift",
    "Foundation",
    "Self",
    "Type",
    "Protocol",
];

pub fn generate(app: &App) -> Result<String, CodegenError> {
    Writer::new(app)?.write()
}

/// Whether `manifest` (a `Package.swift`) is a package that uses the Jazz
/// Swift runtime: it names the `jazz-swift` package, as a quoted name or the
/// last part of a quoted path or URL (`"jazz-swift"`, `".../jazz-swift"`,
/// `".../jazz-swift.git"`), not merely mentions it.
pub fn is_app_manifest(manifest: &str) -> bool {
    manifest.match_indices("jazz-swift").any(|(at, word)| {
        let before = manifest[..at].chars().next_back();
        let after = &manifest[at + word.len()..];
        matches!(before, Some('"' | '/')) && (after.starts_with('"') || after.starts_with(".git\""))
    })
}

struct TableNames {
    row: String,
    new: String,
    patch: String,
    namespace: String,
}

struct Writer<'a> {
    app: &'a App,
    tables: BTreeMap<&'a str, TableNames>,
    /// Generated enums by Swift name: cases (Swift name, stored name), and
    /// what declared them.
    enums: BTreeMap<String, (Vec<(String, String)>, String)>,
    /// Every type name taken, with what took it.
    types: BTreeMap<String, String>,
}

impl<'a> Writer<'a> {
    fn new(app: &'a App) -> Result<Self, CodegenError> {
        if let Some(transform) = app.transforms.first() {
            return Err(CodegenError::unsupported(format!(
                "transform `{}`: transforms in Swift bindings are not supported yet",
                transform.name
            )));
        }
        if let Some((table, computed)) = app
            .tables
            .iter()
            .find_map(|t| t.computed.first().map(|c| (t, c)))
        {
            return Err(CodegenError::unsupported(format!(
                "computed field `{}.{}`: computed fields in Swift bindings are not supported yet",
                table.name, computed.name
            )));
        }
        let mut writer = Self {
            app,
            tables: BTreeMap::new(),
            enums: BTreeMap::new(),
            types: RESERVED
                .iter()
                .map(|name| (name.to_string(), "a name the bindings use".to_string()))
                .collect(),
        };
        for table in &app.tables {
            let row = type_ident(&pascal(&singular(&table.name)), &table.name)?;
            let mut namespace = type_ident(&pascal(&table.name), &table.name)?;
            if namespace == row {
                namespace = format!("{row}Table");
            }
            let what = format!("table `{}`", table.name);
            for name in [
                row.clone(),
                format!("New{row}"),
                format!("{row}Patch"),
                namespace.clone(),
            ] {
                writer.take_type(&name, &what)?;
            }
            writer.tables.insert(
                &table.name,
                TableNames {
                    new: format!("New{row}"),
                    patch: format!("{row}Patch"),
                    row,
                    namespace,
                },
            );
        }
        Ok(writer)
    }

    fn take_type(&mut self, name: &str, what: &str) -> Result<(), CodegenError> {
        match self.types.insert(name.to_owned(), what.to_owned()) {
            Some(other) => Err(CodegenError::unsupported(format!(
                "{what} and {other} are both the Swift type `{name}`"
            ))),
            None => Ok(()),
        }
    }

    fn write(mut self) -> Result<String, CodegenError> {
        let mut items = String::new();
        for table in &self.app.tables {
            items.push_str(&self.table(table)?);
        }
        items.push_str(&self.open());

        let mut out = String::new();
        let _ = writeln!(out, "{HEADER}");
        let _ = writeln!(
            out,
            "{FORMAT_PREFIX}{FORMAT}. Regenerate it after changing jazz.app.json."
        );
        out.push_str("// swift-format-ignore-file\n\n");
        out.push_str("import Foundation\nimport Jazz\n\n");
        for (name, (cases, what)) in &self.enums {
            let _ = writeln!(out, "/// The values of {what}.");
            let _ = writeln!(
                out,
                "public enum {name}: Swift.String, Swift.Codable, Swift.Sendable, Swift.Hashable, Swift.CaseIterable {{"
            );
            for (case, stored) in cases {
                let _ = writeln!(out, "    case {case} = {}", swift_string(stored));
            }
            out.push_str("}\n\n");
        }
        out.push_str(&items);
        Ok(out)
    }

    /// The Swift type of a value of `ty`; `name` and `what` name a
    /// generated enum.
    fn swift_type(
        &mut self,
        ty: &ColumnType,
        name: &str,
        what: &str,
    ) -> Result<String, CodegenError> {
        Ok(match ty {
            ColumnType::Integer => "Swift.Int".into(),
            ColumnType::BigInt => format!("{RT}.BigInt"),
            ColumnType::Boolean => "Swift.Bool".into(),
            ColumnType::Text => "Swift.String".into(),
            ColumnType::Double => "Swift.Double".into(),
            ColumnType::Timestamp => "Foundation.Date".into(),
            ColumnType::Uuid => format!("{RT}.ID"),
            ColumnType::Bytea => "Foundation.Data".into(),
            ColumnType::Json { .. } => format!("{RT}.JSONValue"),
            ColumnType::Array { element } => {
                format!("[{}]", self.swift_type(element, name, what)?)
            }
            ColumnType::Enum { variants } => {
                let mut cases = Vec::new();
                let mut seen = BTreeMap::new();
                for variant in variants {
                    let ident = value_ident(variant)?;
                    if let Some(other) = seen.insert(ident.clone(), variant.clone()) {
                        return Err(CodegenError::unsupported(format!(
                            "{what}: values `{other}` and `{variant}` are both the Swift case `{ident}`"
                        )));
                    }
                    cases.push((ident, variant.clone()));
                }
                match self.enums.get(name) {
                    Some((existing, _)) if *existing == cases => {}
                    _ => {
                        self.take_type(name, what)?;
                        self.enums.insert(name.to_owned(), (cases, what.to_owned()));
                    }
                }
                name.to_owned()
            }
            _ => {
                return Err(CodegenError::unsupported(format!(
                    "{what}: values of this type in Swift bindings are not supported yet"
                )));
            }
        })
    }

    fn field_type(
        &mut self,
        ty: &FieldType,
        name: &str,
        what: &str,
    ) -> Result<String, CodegenError> {
        let inner = self.swift_type(&ty.ty, name, what)?;
        Ok(if ty.nullable {
            format!("{inner}?")
        } else {
            inner
        })
    }

    fn table(&mut self, table: &'a Table) -> Result<String, CodegenError> {
        let names = &self.tables[table.name.as_str()];
        let (row, new, patch, namespace) = (
            names.row.clone(),
            names.new.clone(),
            names.patch.clone(),
            names.namespace.clone(),
        );
        // `self` is no property name even in backticks.
        let mut idents = BTreeSet::from(["id".to_owned(), "query".to_owned(), "self".to_owned()]);
        let mut fields = Vec::new();
        for column in &table.columns {
            let ident = value_ident(&column.name)?;
            if !idents.insert(ident.trim_matches('`').to_owned()) {
                return Err(CodegenError::unsupported(format!(
                    "`{}.{}` is the Swift name `{ident}`, which another column, `id`, `query` or `self` already is",
                    table.name, column.name
                )));
            }
            let what = format!("column `{}.{}`", table.name, column.name);
            let ty =
                self.field_type(&column.ty, &format!("{row}{}", pascal(&column.name)), &what)?;
            fields.push(Field { column, ident, ty });
        }
        let table_lit = swift_string(&table.name);

        let mut out = String::new();
        // The row.
        let _ = writeln!(out, "/// A row of `{}`.", table.name);
        let _ = writeln!(out, "public struct {row}: {RT}.Row {{");
        let _ = writeln!(out, "    public static let table = {table_lit}");
        let _ = writeln!(out, "    public let id: {RT}.ID");
        for f in &fields {
            f.doc(&mut out);
            let _ = writeln!(out, "    public var {}: {}", f.ident, f.ty);
        }
        out.push('\n');
        let _ = writeln!(
            out,
            "    public init(id: {RT}.ID{}) {{",
            params(&fields, false)
        );
        out.push_str("        self.id = id\n");
        for f in &fields {
            let _ = writeln!(out, "        self.{0} = {0}", f.ident);
        }
        out.push_str("    }\n\n");
        coding_keys(&mut out, &fields, true);
        out.push_str("}\n\n");

        // New rows.
        let _ = writeln!(
            out,
            "/// A new row of `{}`: `nil` leaves a column out, which stores its default (or `null`).",
            table.name
        );
        let _ = writeln!(out, "public struct {new}: {RT}.Insert {{");
        let _ = writeln!(out, "    public typealias Row = {row}");
        for f in &fields {
            f.doc(&mut out);
            let _ = writeln!(out, "    public var {}: {}", f.ident, f.insert_type());
        }
        out.push('\n');
        let p = params(&fields, true);
        let _ = writeln!(out, "    public init({}) {{", p.trim_start_matches(", "));
        for f in &fields {
            let _ = writeln!(out, "        self.{0} = {0}", f.ident);
        }
        out.push_str("    }\n\n");
        coding_keys(&mut out, &fields, false);
        out.push_str("}\n\n");

        // Patches.
        let _ = writeln!(
            out,
            "/// A change to a row of `{}`: only the columns set to `.set(…)` change; `.set(nil)` stores `null`.",
            table.name
        );
        let _ = writeln!(out, "public struct {patch}: {RT}.Patch {{");
        let _ = writeln!(out, "    public typealias Row = {row}");
        for f in &fields {
            f.doc(&mut out);
            let _ = writeln!(out, "    public var {}: {RT}.Change<{}>", f.ident, f.ty);
        }
        out.push('\n');
        let p = fields
            .iter()
            .map(|f| format!("{}: {RT}.Change<{}> = .unchanged", f.ident, f.ty))
            .collect::<Vec<_>>()
            .join(", ");
        let _ = writeln!(out, "    public init({p}) {{");
        for f in &fields {
            let _ = writeln!(out, "        self.{0} = {0}", f.ident);
        }
        out.push_str("    }\n\n");
        coding_keys(&mut out, &fields, false);
        out.push('\n');
        out.push_str("    public func encode(to encoder: any Swift.Encoder) throws {\n");
        out.push_str("        var container = encoder.container(keyedBy: CodingKeys.self)\n");
        for f in &fields {
            let _ = writeln!(
                out,
                "        try container.encodeChange(self.{0}, forKey: .{0})",
                f.ident
            );
        }
        out.push_str("    }\n}\n\n");

        // Queries and columns.
        let _ = writeln!(out, "/// Queries of `{}` and its columns.", table.name);
        if !table.relations.is_empty() {
            out.push_str(
                "/// (Its relations are left out: includes are not supported in Swift yet.)\n",
            );
        }
        let _ = writeln!(out, "public enum {namespace} {{");
        let _ = writeln!(
            out,
            "    /// Every row of `{}`; narrow it with `where`, `orderBy`, `limit` and `offset`.",
            table.name
        );
        let _ = writeln!(
            out,
            "    public static func query() -> {RT}.Query<{row}> {{ {RT}.Query() }}"
        );
        out.push('\n');
        out.push_str("    /// The row's id.\n");
        let _ = writeln!(
            out,
            "    public static let id = {RT}.Column<{row}, {RT}.ID>(\"id\")"
        );
        for f in &fields {
            let _ = writeln!(out, "    /// Column `{}`.", f.column.name);
            let _ = writeln!(
                out,
                "    public static let {} = {RT}.Column<{row}, {}>({})",
                f.ident,
                f.ty,
                swift_string(&f.column.name)
            );
        }
        out.push_str("}\n\n");
        Ok(out)
    }

    fn open(&self) -> String {
        let mut out = String::new();
        let hashes = "#".repeat(longest_hash_run(&self.app.file) + 1);
        let mut file = self.app.file.clone();
        if !file.ends_with('\n') {
            file.push('\n');
        }
        out.push_str("/// The app: its `jazz.app.json`, and opening it.\n");
        out.push_str(&JAZZ_APP_OPEN[1..]);
        out.push_str(
            "    /// `sha256:…` of the `jazz.app.json` these bindings were generated from.\n",
        );
        let _ = writeln!(out, "{SOURCE_PREFIX}{}", swift_string(&self.app.source));
        out.push('\n');
        out.push_str(
            "    /// The app's `jazz.app.json`: its schema, every earlier version and the\n    /// migrations between them.\n",
        );
        let _ = write!(out, "{APP_PREFIX}{hashes}\"\"\"\n{file}\"\"\"{hashes}\n\n");
        out.push_str(
            "    /// Opens the app. A backend (`config.permissions` set to its\n    /// `jazz.permissions.json`) first publishes the app's permissions bundle\n    /// and pins the session it acts in to it.\n",
        );
        let _ = writeln!(
            out,
            "    public static func open(_ config: {RT}.Config) async throws -> {RT}.Client {{\n        try await {RT}.Client.open(app: app, config: config)\n    }}\n}}"
        );
        out
    }
}

struct Field<'a> {
    column: &'a Column,
    ident: String,
    ty: String,
}

impl Field<'_> {
    fn optional_on_insert(&self) -> bool {
        self.column.optional_on_insert()
    }

    /// Inserts take `nil` for a column with a default, or a nullable one.
    fn insert_type(&self) -> String {
        if self.optional_on_insert() && !self.column.ty.nullable {
            format!("{}?", self.ty)
        } else {
            self.ty.clone()
        }
    }

    fn doc(&self, out: &mut String) {
        if self.ident.trim_matches('`') != self.column.name {
            let _ = writeln!(out, "    /// `{}`.", self.column.name);
        }
    }
}

/// `, a: A, b: B? = nil` for an init: inserts default what they may leave out.
fn params(fields: &[Field<'_>], insert: bool) -> String {
    fields
        .iter()
        .map(|f| {
            if insert {
                let ty = f.insert_type();
                if f.optional_on_insert() {
                    format!(", {}: {ty} = nil", f.ident)
                } else {
                    format!(", {}: {ty}", f.ident)
                }
            } else {
                format!(", {}: {}", f.ident, f.ty)
            }
        })
        .collect()
}

fn coding_keys(out: &mut String, fields: &[Field<'_>], with_id: bool) {
    out.push_str("    enum CodingKeys: Swift.String, Swift.CodingKey {\n");
    if with_id {
        out.push_str("        case id\n");
    }
    for f in fields {
        let _ = writeln!(
            out,
            "        case {} = {}",
            f.ident,
            swift_string(&f.column.name)
        );
    }
    out.push_str("    }\n");
}

/// A Swift string literal of `text`.
fn swift_string(text: &str) -> String {
    let mut out = String::from("\"");
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{{{:x}}}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn longest_hash_run(text: &str) -> usize {
    text.split(|c| c != '#').map(str::len).max().unwrap_or(0)
}

/// Parses a string literal [`swift_string`] wrote.
fn parse_swift_string(literal: &str) -> Option<String> {
    let body = literal.strip_prefix('"')?.strip_suffix('"')?;
    let mut out = String::new();
    let mut chars = body.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next()? {
            '"' => out.push('"'),
            '\\' => out.push('\\'),
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            't' => out.push('\t'),
            'u' => {
                let rest: String = chars.by_ref().take_while(|c| *c != '}').collect();
                let hex = rest.strip_prefix('{')?;
                out.push(char::from_u32(u32::from_str_radix(hex, 16).ok()?)?);
            }
            _ => return None,
        }
    }
    Some(out)
}

pub(crate) fn read(bindings: &str) -> Result<crate::bindings::Recorded, CodegenError> {
    let refuse = |what: &str| {
        CodegenError::new(format!(
            "{FILE}: {what}; regenerate it with `jazz-tools generate`"
        ))
    };
    let bindings = bindings.strip_prefix('\u{feff}').unwrap_or(bindings);
    let mut lines = bindings.lines();
    if lines.next() != Some(HEADER) {
        return Err(refuse(
            "not a file jazz-tools generated (its first line differs)",
        ));
    }
    let format = lines
        .next()
        .and_then(|line| line.strip_prefix(FORMAT_PREFIX))
        .and_then(|rest| rest.split('.').next())
        .and_then(|n| n.parse::<u32>().ok())
        .ok_or_else(|| refuse("no `// Bindings format <n>.` second line"))?;
    // Only `JazzApp` holds them: a table's namespace may have columns named
    // `source` or `app`.
    let start = bindings
        .find(JAZZ_APP_OPEN)
        .ok_or_else(|| refuse("no `public enum JazzApp`"))?;
    let bindings = &bindings[start..];
    let source = bindings
        .lines()
        .find_map(|line| line.strip_prefix(SOURCE_PREFIX))
        .and_then(|literal| parse_swift_string(literal.trim_end()))
        .ok_or_else(|| refuse("no `public static let source`"))?;
    let at = bindings
        .find(APP_PREFIX)
        .ok_or_else(|| refuse("no `public static let app`"))?
        + APP_PREFIX.len();
    let rest = &bindings[at..];
    let hashes = rest.chars().take_while(|c| *c == '#').count();
    let open = format!("{}\"\"\"\n", "#".repeat(hashes));
    let close = format!("\n\"\"\"{}", "#".repeat(hashes));
    let file = rest
        .strip_prefix(&open)
        .and_then(|body| body.find(&close).map(|end| &body[..end + 1]))
        .ok_or_else(|| refuse("`app` is cut short"))?;
    let unusable = |what: &str| {
        CodegenError::new(format!(
            "{FILE}: {what}. Restore it from version control; or delete it and run \
             `jazz-tools generate` (an edit of `tables` since it was generated then \
             has no recorded version to migrate from)"
        ))
    };
    if jazz::tools::deployment::artifact::source_hash(file.as_bytes()) != source {
        return Err(unusable(
            "its `app` was edited (it no longer matches `source`)",
        ));
    }
    let app = App::load(file.as_bytes())
        .map_err(|error| unusable(&format!("its `app` no longer loads ({error})")))?;
    Ok(crate::bindings::Recorded {
        source,
        format,
        current: app.compiled.current,
        current_schema: app.compiled.current_schema().clone(),
    })
}

// ---- names ------------------------------------------------------------------

/// Lowercase words of a camelCase, snake_case or kebab-case name.
fn words(name: &str) -> Vec<String> {
    let chars: Vec<char> = name.chars().collect();
    let mut words = Vec::new();
    let mut word = String::new();
    for (i, &c) in chars.iter().enumerate() {
        if !c.is_alphanumeric() {
            if !word.is_empty() {
                words.push(std::mem::take(&mut word));
            }
            continue;
        }
        let boundary = c.is_uppercase()
            && !word.is_empty()
            && (chars[i - 1].is_lowercase()
                || chars[i - 1].is_ascii_digit()
                || chars.get(i + 1).is_some_and(|next| next.is_lowercase()));
        if boundary {
            words.push(std::mem::take(&mut word));
        }
        word.extend(c.to_lowercase());
    }
    if !word.is_empty() {
        words.push(word);
    }
    words
}

const KEYWORDS: &[&str] = &[
    "associatedtype",
    "class",
    "deinit",
    "enum",
    "extension",
    "fileprivate",
    "func",
    "import",
    "init",
    "inout",
    "internal",
    "let",
    "open",
    "operator",
    "private",
    "precedencegroup",
    "protocol",
    "public",
    "rethrows",
    "static",
    "struct",
    "subscript",
    "typealias",
    "var",
    "break",
    "case",
    "catch",
    "continue",
    "default",
    "defer",
    "do",
    "else",
    "fallthrough",
    "for",
    "guard",
    "if",
    "in",
    "repeat",
    "return",
    "throw",
    "switch",
    "where",
    "while",
    "as",
    "false",
    "is",
    "nil",
    "self",
    "super",
    "throws",
    "true",
    "try",
    "Any",
    "Self",
    "await",
    "async",
    "some",
    "any",
];

/// A property, case or function name: lowerCamelCase, in backticks where
/// it's a keyword.
fn value_ident(name: &str) -> Result<String, CodegenError> {
    let words = words(name);
    let mut ident = String::new();
    for (i, word) in words.iter().enumerate() {
        if i == 0 {
            ident.push_str(word);
        } else {
            let mut chars = word.chars();
            if let Some(first) = chars.next() {
                ident.extend(first.to_uppercase());
                ident.push_str(chars.as_str());
            }
        }
    }
    if ident.is_empty()
        || ident.starts_with(|c: char| c.is_ascii_digit())
        || !ident.chars().all(|c| c.is_ascii_alphanumeric())
    {
        return Err(CodegenError::unsupported(format!(
            "`{name}` has no Swift name: it must start with a letter and use only ASCII letters and digits"
        )));
    }
    Ok(if KEYWORDS.contains(&ident.as_str()) {
        format!("`{ident}`")
    } else {
        ident
    })
}

/// A type name derived from `name`.
fn type_ident(ident: &str, name: &str) -> Result<String, CodegenError> {
    if ident.is_empty()
        || ident.starts_with(|c: char| !c.is_ascii_alphabetic())
        || !ident.chars().all(|c| c.is_ascii_alphanumeric())
    {
        return Err(CodegenError::unsupported(format!(
            "`{name}` has no Swift type name: it must start with a letter and use only ASCII letters and digits"
        )));
    }
    Ok(ident.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert_eq!(value_ident("folder_id").unwrap(), "folderId");
        assert_eq!(value_ident("joinCode").unwrap(), "joinCode");
        assert_eq!(value_ident("in-progress").unwrap(), "inProgress");
        assert_eq!(value_ident("default").unwrap(), "`default`");
        assert!(value_ident("1st").is_err());
    }

    #[test]
    fn strings_round_trip() {
        for text in ["plain", "a \"q\" \\ b\n\t\u{1}"] {
            assert_eq!(parse_swift_string(&swift_string(text)).unwrap(), text);
        }
    }

    #[test]
    fn manifests_name_jazz_swift() {
        assert!(is_app_manifest(
            r#".package(path: "../../packages/jazz-swift")"#
        ));
        assert!(is_app_manifest(
            r#".package(url: "https://github.com/garden-co/jazz-swift.git", from: "1.0.0")"#
        ));
        assert!(!is_app_manifest("let package = Package(name: \"x\")"));
        assert!(!is_app_manifest(
            "// not jazz-swift-tools, see jazz-swiftly"
        ));
        assert!(!is_app_manifest(
            r#".package(path: "../my-jazz-swift-fork/x")"#
        ));
    }

    const COLUMNS_NAMED_LIKE_JAZZ_APP: &str = r#"{
  "$schema": "https://jazz.tools/schemas/jazz.app.schema.json",
  "format": 1,
  "tables": {
    "links": { "columns": { "source": "string", "app": "string?", "format": "int" } }
  }
}
"#;

    #[test]
    fn columns_named_source_or_app_read_back() {
        let file = COLUMNS_NAMED_LIKE_JAZZ_APP.as_bytes();
        let generated = crate::generate(crate::Lang::Swift, file).unwrap();
        assert!(generated.contains(
            "    public static let source = Jazz.Column<Link, Swift.String>(\"source\")"
        ));
        let app = App::load(file).unwrap();
        let recorded = read(&generated).unwrap();
        assert_eq!(recorded.source, app.source);
        assert_eq!(
            crate::bindings::staleness(&app, crate::Lang::Swift, Some(&generated)).unwrap(),
            None
        );
    }

    #[test]
    fn a_column_named_self_is_refused() {
        let file =
            br#"{ "format": 1, "tables": { "things": { "columns": { "self": "string" } } } }"#;
        let error = crate::generate(crate::Lang::Swift, file).unwrap_err();
        assert!(error.is_unsupported(), "{error}");
    }
}
