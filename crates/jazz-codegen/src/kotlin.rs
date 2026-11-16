//! The Kotlin back end: `src/main/kotlin/JazzApp.kt`, typed bindings over
//! the `jazz-kotlin` runtime (`tools.jazz`), which wraps `jazz-ffi`.
//!
//! Everything is in the package `jazzapp`. Per table: a `@Serializable` row
//! data class (`jazzapp.Todo`), a new-row data class (`NewTodo`), a patch
//! builder (`TodoPatch`) and a table object (`Todos`) with typed columns,
//! from which queries are built. Values are the plain JSON the FFI contract
//! speaks, so the classes only (de)serialize. `open` embeds `jazz.app.json`
//! itself.
//!
//! The file imports nothing and names every type outside the package fully
//! qualified (`kotlin.String`, `tools.jazz.Column`), so a table may be any
//! name (`Result`, `Unit`, `Column`) that doesn't clash inside `jazzapp`.
//!
//! Computed fields and transforms are not supported in Kotlin yet: an app
//! that declares any is refused.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use jazz::tools::public_schema::ColumnType;

use crate::CodegenError;
use crate::model::{App, Column, FieldType, Table};
use crate::names::{pascal, singular};

/// Where the bindings go, relative to `jazz.app.json`.
pub const FILE: &str = "src/main/kotlin/JazzApp.kt";

/// The version of the bindings' shape this generator writes.
pub const FORMAT: u32 = 1;

/// The first line, which marks a file as this generator's.
pub const HEADER: &str =
    "// JazzApp.kt: generated from jazz.app.json by `jazz-tools generate`. Do not edit.";

/// The marker beside `jazz.app.json` that says the app is written in Kotlin.
pub const MARKER: &str = "build.gradle.kts";

/// The runtime a Kotlin app's `build.gradle.kts` depends on.
pub const RUNTIME: &str = "jazz-kotlin";

/// Whether `build` (a `build.gradle.kts`) is a Kotlin Jazz app's: it depends
/// on the `jazz-kotlin` runtime.
pub fn is_app_build(build: &str) -> bool {
    build
        .lines()
        .map(|line| line.split("//").next().unwrap_or(""))
        .any(|line| {
            line.contains(&format!(":{RUNTIME}\"")) || line.contains(&format!(":{RUNTIME}:"))
        })
}

/// The package the bindings declare.
pub const PACKAGE: &str = "jazzapp";

const FORMAT_PREFIX: &str = "// Bindings format ";
const SOURCE_PREFIX: &str = "public const val SOURCE: kotlin.String = ";
const APP_OPEN: &str = "public val APP: kotlin.String = kotlin.arrayOf(";
const APP_CLOSE: &str = ").joinToString(\"\")";

/// Type names the bindings declare themselves, besides the tables' (every
/// other name the file uses is fully qualified).
const RESERVED: &[&str] = &[];

const KEYWORDS: &[&str] = &[
    "as",
    "break",
    "class",
    "continue",
    "do",
    "else",
    "false",
    "for",
    "fun",
    "if",
    "in",
    "interface",
    "is",
    "null",
    "object",
    "package",
    "return",
    "super",
    "this",
    "throw",
    "true",
    "try",
    "typealias",
    "typeof",
    "val",
    "var",
    "when",
    "while",
];

pub fn generate(app: &App) -> Result<String, CodegenError> {
    if !app.transforms.is_empty() || app.tables.iter().any(|t| !t.computed.is_empty()) {
        return Err(CodegenError::unsupported(
            "computed fields and transforms in Kotlin bindings are not supported yet",
        ));
    }
    if app
        .tables
        .iter()
        .any(|t| t.columns.iter().any(|c| c.transform.is_some()))
    {
        return Err(CodegenError::unsupported(
            "transforms in Kotlin bindings are not supported yet",
        ));
    }
    Writer::new(app)?.write()
}

struct Names {
    row: String,
    insert: String,
    patch: String,
    object: String,
}

struct Writer<'a> {
    app: &'a App,
    tables: BTreeMap<&'a str, Names>,
    /// Generated enums: Kotlin name → (variants (Kotlin, stored), what).
    enums: BTreeMap<String, (Vec<(String, String)>, String)>,
    types: BTreeMap<String, String>,
}

impl<'a> Writer<'a> {
    fn new(app: &'a App) -> Result<Self, CodegenError> {
        let mut writer = Self {
            app,
            tables: BTreeMap::new(),
            enums: BTreeMap::new(),
            types: RESERVED
                .iter()
                .map(|n| (n.to_string(), "a name the bindings use".to_string()))
                .collect(),
        };
        for table in &app.tables {
            let what = format!("table `{}`", table.name);
            let row = type_ident(
                &pascal(&words(&singular(&table.name)).join("_")),
                &table.name,
            )?;
            let mut object = type_ident(&pascal(&words(&table.name).join("_")), &table.name)?;
            if object == row {
                object.push_str("Table");
            }
            let names = Names {
                insert: format!("New{row}"),
                patch: format!("{row}Patch"),
                row,
                object,
            };
            for name in [&names.row, &names.insert, &names.patch, &names.object] {
                writer.take_type(name, &what)?;
            }
            writer.tables.insert(&table.name, names);
        }
        Ok(writer)
    }

    fn take_type(&mut self, name: &str, what: &str) -> Result<(), CodegenError> {
        match self.types.insert(name.to_owned(), what.to_owned()) {
            Some(other) => Err(CodegenError::unsupported(format!(
                "{what} and {other} are both the Kotlin type `{name}`"
            ))),
            None => Ok(()),
        }
    }

    fn write(mut self) -> Result<String, CodegenError> {
        let mut items = String::new();
        for table in &self.app.tables {
            items.push_str(&self.table(table)?);
        }
        let mut out = String::new();
        let _ = writeln!(out, "{HEADER}");
        let _ = writeln!(
            out,
            "{FORMAT_PREFIX}{FORMAT}. Regenerate it after changing jazz.app.json."
        );
        out.push_str("@file:kotlin.Suppress(\"unused\", \"RedundantVisibilityModifier\")\n\n");
        let _ = writeln!(out, "package {PACKAGE}\n");
        out.push_str(
            "/** `sha256:…` of the `jazz.app.json` these bindings were generated from. */\n",
        );
        let _ = writeln!(
            out,
            "{SOURCE_PREFIX}{};\n",
            string_literal(&self.app.source)
        );
        out.push_str(
            "/** The app's `jazz.app.json`: its schema, every earlier version and the migrations between them. */\n",
        );
        let _ = writeln!(out, "{APP_OPEN}");
        for line in self.app.file.split_inclusive('\n') {
            let _ = writeln!(out, "    {},", string_literal(line));
        }
        let _ = writeln!(out, "{APP_CLOSE}\n");
        out.push_str(
            "/**\n * Opens the app. A backend (`JazzConfig.permissions` set to its\n * `jazz.permissions.json`) first publishes the app's permissions bundle\n * and pins the session it acts in to it.\n */\n",
        );
        out.push_str(
            "public suspend fun open(config: tools.jazz.JazzConfig): tools.jazz.Jazz =\n    tools.jazz.Jazz.open(APP, config)\n\n",
        );
        for (name, (variants, what)) in &self.enums {
            let _ = writeln!(out, "/** The values of {what}. */");
            let _ = writeln!(out, "{SERIALIZABLE}\npublic enum class {name} {{");
            for (variant, stored) in variants {
                let _ = writeln!(
                    out,
                    "    {SERIAL_NAME}({}) {variant},",
                    string_literal(stored)
                );
            }
            out.push_str("}\n\n");
        }
        out.push_str(&items);
        while out.ends_with("\n\n") {
            out.pop();
        }
        Ok(out)
    }

    /// The Kotlin type of a non-null value of `ty`, with the serializer
    /// annotation it needs as a property or list element.
    fn kotlin_type(&mut self, ty: &ColumnType, name: &str, what: &str) -> Result<Ty, CodegenError> {
        let plain = |ty: &str, ser: &str| Ty {
            ty: ty.to_owned(),
            ser: ser.to_owned(),
        };
        let prim = |ty: &str| Ty {
            ty: ty.to_owned(),
            ser: format!("kotlinx.serialization.serializer<{ty}>()"),
        };
        Ok(match ty {
            ColumnType::Integer => prim("kotlin.Int"),
            ColumnType::BigInt => plain(
                &format!("@{SERIALIZABLE_WITH}(with = tools.jazz.JazzBigInt::class) kotlin.Long"),
                "tools.jazz.JazzBigInt",
            ),
            ColumnType::Boolean => prim("kotlin.Boolean"),
            ColumnType::Text | ColumnType::Uuid => prim("kotlin.String"),
            ColumnType::Double => prim("kotlin.Double"),
            ColumnType::Timestamp => prim("kotlin.Long"),
            ColumnType::Bytea => plain(
                &format!(
                    "@{SERIALIZABLE_WITH}(with = tools.jazz.JazzBytes::class) kotlin.ByteArray"
                ),
                "tools.jazz.JazzBytes",
            ),
            ColumnType::Json { .. } => prim("kotlinx.serialization.json.JsonElement"),
            ColumnType::Array { element } => {
                let inner = self.kotlin_type(element, name, what)?;
                Ty {
                    ty: format!("kotlin.collections.List<{}>", inner.ty),
                    ser: format!(
                        "kotlinx.serialization.builtins.ListSerializer({})",
                        inner.ser
                    ),
                }
            }
            ColumnType::Enum { variants } => {
                let mut kotlin = Vec::new();
                let mut seen = BTreeMap::new();
                for variant in variants {
                    let ident = type_ident(&pascal(&words(variant).join("_")), variant)?;
                    if let Some(other) = seen.insert(ident.clone(), variant.clone()) {
                        return Err(CodegenError::unsupported(format!(
                            "{what}: values `{other}` and `{variant}` are both the Kotlin constant `{ident}`"
                        )));
                    }
                    kotlin.push((ident, variant.clone()));
                }
                match self.enums.get(name) {
                    Some((existing, _)) if *existing == kotlin => {}
                    _ => {
                        self.take_type(name, what)?;
                        self.enums
                            .insert(name.to_owned(), (kotlin, what.to_owned()));
                    }
                }
                plain(name, &format!("{name}.serializer()"))
            }
            other => {
                return Err(CodegenError::unsupported(format!(
                    "{what}: {} values in Kotlin bindings",
                    type_label(other)
                )));
            }
        })
    }

    fn field_type(&mut self, ty: &FieldType, name: &str, what: &str) -> Result<Ty, CodegenError> {
        let inner = self.kotlin_type(&ty.ty, name, what)?;
        Ok(if ty.nullable {
            Ty {
                ty: format!("{}?", inner.ty),
                ser: format!("tools.jazz.nullable({})", inner.ser),
            }
        } else {
            inner
        })
    }

    fn table(&mut self, table: &'a Table) -> Result<String, CodegenError> {
        let names = &self.tables[table.name.as_str()];
        let (row, insert, patch, object) = (
            names.row.clone(),
            names.insert.clone(),
            names.patch.clone(),
            names.object.clone(),
        );
        let mut idents = BTreeSet::from(["id".to_owned()]);
        let mut fields = Vec::new();
        for column in &table.columns {
            let ident = value_ident(&column.name)?;
            if !idents.insert(ident.clone()) {
                return Err(clash(table, &column.name, &ident));
            }
            let what = format!("column `{}.{}`", table.name, column.name);
            let ty = self.field_type(
                &column.ty,
                &format!("{row}{}", pascal(&words(&column.name).join("_"))),
                &what,
            )?;
            fields.push(Field { column, ident, ty });
        }

        let mut out = String::new();
        // The row.
        let _ = writeln!(out, "/** A row of `{}`. */", doc(&table.name));
        let _ = writeln!(out, "{SERIALIZABLE}\npublic data class {row}(");
        out.push_str("    val id: kotlin.String,\n");
        for f in &fields {
            let default = if f.column.ty.nullable { " = null" } else { "" };
            let _ = writeln!(
                out,
                "    {}val {}: {}{default},",
                f.serial_name(),
                f.ident,
                f.ty.ty
            );
        }
        out.push_str(")\n\n");

        // Inserts: a column that may be left out defaults to `null`, which
        // is left out of the JSON (and so takes the column's default).
        let _ = writeln!(
            out,
            "/** A new row of `{}`: leave out a column to store its default (or `null`). */",
            doc(&table.name)
        );
        let _ = writeln!(out, "{SERIALIZABLE}\npublic data class {insert}(");
        for f in &fields {
            if f.column.optional_on_insert() {
                let ty = if f.ty.ty.ends_with('?') {
                    f.ty.ty.clone()
                } else {
                    format!("{}?", f.ty.ty)
                };
                let _ = writeln!(out, "    {}val {}: {ty} = null,", f.serial_name(), f.ident);
            } else {
                let _ = writeln!(out, "    {}val {}: {},", f.serial_name(), f.ident, f.ty.ty);
            }
        }
        out.push_str(")\n\n");

        // Patches.
        let _ = writeln!(
            out,
            "/** Changes to a row of `{}`: columns not set are left as they are. */",
            doc(&table.name)
        );
        let _ = writeln!(out, "public class {patch} : tools.jazz.Patch() {{");
        let mut members = BTreeSet::new();
        for f in &fields {
            members.insert(f.ident.clone());
            let _ = writeln!(
                out,
                "    /** Sets `{}`. */\n    public fun {}(value: {}): {patch} = apply {{ set({}, value, {}) }}",
                doc(&f.column.name),
                f.ident,
                plain_type(&f.ty.ty),
                string_literal(&f.column.name),
                f.ty.ser
            );
        }
        out.push_str("}\n\n");

        // The table object.
        let _ = writeln!(
            out,
            "/** Table `{}`: its columns, and queries with `query()`. */",
            doc(&table.name)
        );
        let _ = writeln!(
            out,
            "public object {object} : tools.jazz.Table<{row}, {insert}, {patch}>(\n    {},\n    {row}.serializer(),\n    {insert}.serializer(),\n    ::{patch},\n) {{",
            string_literal(&table.name)
        );
        out.push_str("    /** The row's id. */\n    public val id: tools.jazz.Column<kotlin.String> =\n        tools.jazz.Column(\"id\", kotlinx.serialization.serializer<kotlin.String>())\n");
        // The public members of `tools.jazz.Table` (and `Any`): a column of
        // that name gets `Column` after it.
        let mut props: BTreeSet<String> = [
            "id",
            "query",
            "where",
            "orderBy",
            "tableName",
            "rowSerializer",
            "insertSerializer",
            "newPatch",
            "equals",
            "hashCode",
            "toString",
            // Top-level packages the initializers name: a property of that
            // name would hide them there.
            "kotlin",
            "kotlinx",
            "tools",
            "java",
            "javax",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        for f in &fields {
            let mut ident = f.ident.clone();
            if props.contains(ident.trim_matches('`')) {
                ident = format!("{}Column", ident.trim_matches('`'));
            }
            if !props.insert(ident.clone()) {
                return Err(clash(table, &f.column.name, &ident));
            }
            let ty = plain_type(&f.ty.ty);
            let _ = writeln!(
                out,
                "    /** Column `{}`. */\n    public val {ident}: tools.jazz.Column<{ty}> =\n        tools.jazz.Column({}, {})",
                doc(&f.column.name),
                string_literal(&f.column.name),
                f.ty.ser
            );
        }
        out.push_str("}\n\n");
        Ok(out)
    }
}

const SERIALIZABLE_WITH: &str = "kotlinx.serialization.Serializable";
const SERIALIZABLE: &str = "@kotlinx.serialization.Serializable";
const SERIAL_NAME: &str = "@kotlinx.serialization.SerialName";

/// A type without the serializer annotations a property or element needs.
fn plain_type(ty: &str) -> String {
    ty.replace(
        &format!("@{SERIALIZABLE_WITH}(with = tools.jazz.JazzBigInt::class) "),
        "",
    )
    .replace(
        &format!("@{SERIALIZABLE_WITH}(with = tools.jazz.JazzBytes::class) "),
        "",
    )
}

struct Ty {
    /// The Kotlin type, with any serializer annotation it needs.
    ty: String,
    /// An expression for its `KSerializer`.
    ser: String,
}

struct Field<'c> {
    column: &'c Column,
    ident: String,
    ty: Ty,
}

impl Field<'_> {
    fn serial_name(&self) -> String {
        if self.ident.trim_matches('`') == self.column.name {
            String::new()
        } else {
            format!("{SERIAL_NAME}({}) ", string_literal(&self.column.name))
        }
    }
}

/// Read back what bindings this generator wrote record.
pub(crate) fn read(bindings: &str) -> Result<crate::bindings::Recorded, CodegenError> {
    let refuse = |what: &str| {
        CodegenError::new(format!(
            "{FILE}: {what}; regenerate it with `jazz-tools generate`"
        ))
    };
    let bindings = bindings.strip_prefix('\u{feff}').unwrap_or(bindings);
    let mut lines = bindings.lines();
    if lines.next().map(str::trim_end) != Some(HEADER) {
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
    let source = bindings
        .lines()
        .find_map(|line| line.trim().strip_prefix(SOURCE_PREFIX))
        .and_then(|rest| rest.trim_end().strip_suffix(';').or(Some(rest.trim_end())))
        .and_then(unescape)
        .ok_or_else(|| refuse("no `public const val SOURCE`"))?;
    let mut file = String::new();
    let mut in_app = false;
    let mut closed = false;
    for line in bindings.lines() {
        let line = line.trim();
        if !in_app {
            in_app = line == APP_OPEN;
            continue;
        }
        if line == APP_CLOSE {
            closed = true;
            break;
        }
        let literal = line.strip_suffix(',').unwrap_or(line);
        file.push_str(&unescape(literal).ok_or_else(|| refuse("`APP` was edited"))?);
    }
    if !closed {
        return Err(refuse("no `public val APP`, or it is cut short"));
    }
    let unusable = |what: &str| {
        CodegenError::new(format!(
            "{FILE}: {what}. Restore it from version control; or delete it and run \
             `jazz-tools generate` (an edit of `tables` since it was generated then \
             has no recorded version to migrate from)"
        ))
    };
    if jazz::tools::deployment::artifact::source_hash(file.as_bytes()) != source {
        return Err(unusable(
            "its `APP` was edited (it no longer matches `SOURCE`)",
        ));
    }
    let app = App::load(file.as_bytes())
        .map_err(|error| unusable(&format!("its `APP` no longer loads ({error})")))?;
    Ok(crate::bindings::Recorded {
        source,
        format,
        current: app.compiled.current,
        current_schema: app.compiled.current_schema().clone(),
    })
}

/// A Kotlin string literal for `text`.
fn string_literal(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in text.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '$' => out.push_str("\\$"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// The text of a string literal [`string_literal`] wrote.
fn unescape(literal: &str) -> Option<String> {
    let body = literal.strip_prefix('"')?.strip_suffix('"')?;
    let mut out = String::with_capacity(body.len());
    let mut chars = body.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            if c == '"' {
                return None;
            }
            out.push(c);
            continue;
        }
        match chars.next()? {
            '\\' => out.push('\\'),
            '"' => out.push('"'),
            '$' => out.push('$'),
            '\'' => out.push('\''),
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            't' => out.push('\t'),
            'b' => out.push('\u{8}'),
            'u' => {
                let hex: String = chars.by_ref().take(4).collect();
                out.push(char::from_u32(u32::from_str_radix(&hex, 16).ok()?)?);
            }
            _ => return None,
        }
    }
    Some(out)
}

/// Text safe inside a KDoc comment.
fn doc(text: &str) -> String {
    text.replace("*/", "*&#47;")
}

fn clash(table: &Table, name: &str, ident: &str) -> CodegenError {
    CodegenError::unsupported(format!(
        "`{}.{name}` is the Kotlin name `{ident}`, which another column of the table already is",
        table.name
    ))
}

fn type_label(ty: &ColumnType) -> &'static str {
    match ty {
        ColumnType::TransactionId => "transaction id",
        ColumnType::Row { .. } => "nested row",
        ColumnType::ScalarEnum { .. } => "catalogue enum",
        _ => "these",
    }
}

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

/// A property or function name: the authored name when it is a Kotlin
/// identifier, else its camelCase; a keyword in backticks.
fn value_ident(name: &str) -> Result<String, CodegenError> {
    let is_ident = name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    let ident = if is_ident {
        name.to_owned()
    } else {
        let words = words(name);
        let mut ident = String::new();
        for (i, word) in words.iter().enumerate() {
            if i == 0 {
                ident.push_str(word);
            } else {
                ident.push_str(&pascal(word));
            }
        }
        ident
    };
    if ident.is_empty() || !ident.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') {
        return Err(CodegenError::unsupported(format!(
            "`{name}` has no Kotlin name: it must start with a letter"
        )));
    }
    Ok(if KEYWORDS.contains(&ident.as_str()) {
        format!("`{ident}`")
    } else {
        ident
    })
}

fn type_ident(ident: &str, name: &str) -> Result<String, CodegenError> {
    if ident.is_empty()
        || ident.starts_with(|c: char| !c.is_ascii_alphabetic())
        || !ident.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(CodegenError::unsupported(format!(
            "`{name}` has no Kotlin type name: it must start with a letter and use only letters, digits and `_`"
        )));
    }
    Ok(ident.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_literals_round_trip() {
        for text in ["", "a\"b\\c$d\n", "\t\r\u{1}é", "${x}"] {
            assert_eq!(unescape(&string_literal(text)).as_deref(), Some(text));
        }
    }

    #[test]
    fn app_builds_depend_on_jazz_kotlin() {
        assert!(is_app_build(
            "dependencies {\n    implementation(\"tools.jazz:jazz-kotlin\")\n}\n"
        ));
        assert!(is_app_build(
            "implementation(\"tools.jazz:jazz-kotlin:2.0.0-alpha.65\")"
        ));
        assert!(!is_app_build(
            "// implementation(\"tools.jazz:jazz-kotlin\")\nplugins { kotlin(\"jvm\") }"
        ));
        assert!(!is_app_build("rootProject.name = \"jazz-kotlin\""));
    }

    #[test]
    fn names_become_kotlin_identifiers() {
        assert_eq!(value_ident("dueAt").unwrap(), "dueAt");
        assert_eq!(value_ident("due_at").unwrap(), "due_at");
        assert_eq!(value_ident("in-progress").unwrap(), "inProgress");
        assert_eq!(value_ident("object").unwrap(), "`object`");
        assert!(value_ident("1x").is_err());
    }
}
