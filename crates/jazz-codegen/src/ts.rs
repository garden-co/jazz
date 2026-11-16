//! The TypeScript back end: `jazz.generated.ts`.
//!
//! The module gives the app's typed query builder its types (one Row and
//! Insert interface per table and a `Types` map) and `bindApp` the compiled
//! engine schema, so the TS runtime never parses `jazz.app.json`. Computed
//! fields and transforms are implemented in the hand-written
//! `jazz.computed.ts`, which the module imports; `bindApp`'s types make a
//! missing implementation a compile error.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use jazz::tools::deployment::artifact::json::Json;
use jazz::tools::public_schema::{ColumnDescriptor, ColumnType};

use crate::CodegenError;
use crate::model::{App, Column, RelationKind};
use crate::names::{pascal, singular};

/// Where the module goes, beside `jazz.app.json`.
pub const FILE: &str = "jazz.generated.ts";

/// The first line, which marks a file as this generator's.
pub const HEADER: &str =
    "// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.";

/// The version of the module's shape, on its second line. A generator
/// that writes another shape bumps it, so `validate --check` catches
/// bindings an older Jazz wrote for the same file.
pub const FORMAT: u32 = 1;

/// The second line.
pub(crate) fn format_line() -> String {
    format!("// Bindings format {FORMAT}. Formatters may reformat this file, except `app` below.")
}

/// The marker before the embedded `bindApp` options, which
/// [`crate::bindings`] reads back.
pub(crate) const BIND_APP: &str = "export const app = bindApp<";

/// Names the module declares or imports itself, and the globals it refers
/// to, which no table, enum or JSON type may take. Other globals (`Record`,
/// `Partial`, …) the module never uses, so a type may shadow them.
const RESERVED: &[&str] = &[
    "Types",
    "Transforms",
    "AppTypes",
    "AppComputed",
    "AppTransforms",
    "ComputedImpl",
    "TransformImpl",
    "JsonValue",
    "Date",
    "Uint8Array",
    "Float32Array",
];

/// Type names, with what each was derived from, so a clash names both.
struct Names {
    taken: BTreeMap<String, String>,
}

impl Names {
    fn new() -> Self {
        Names {
            taken: RESERVED
                .iter()
                .map(|n| {
                    (
                        n.to_string(),
                        "a name the generated module uses itself".into(),
                    )
                })
                .collect(),
        }
    }

    fn claim(&mut self, name: String, origin: String) -> Result<String, CodegenError> {
        if let Some(other) = self.taken.get(&name) {
            return Err(CodegenError::new(format!(
                "{origin} would be the TypeScript type `{name}`, which is already {other}; \
                 rename one of them in jazz.app.json"
            )));
        }
        self.taken.insert(name.clone(), origin);
        Ok(name)
    }
}

/// The TS names of one table.
struct TableNames {
    row: String,
    insert: String,
    /// Named types of columns and computed fields: (field, row type, init type).
    fields: BTreeMap<String, (String, Option<String>)>,
}

/// `jazz.generated.ts` for `app`.
pub fn generate(app: &App) -> Result<String, CodegenError> {
    let mut names = Names::new();
    let mut decls = String::new();
    let mut table_names: BTreeMap<String, TableNames> = BTreeMap::new();
    for table in &app.tables {
        let base = pascal(&singular(&table.name));
        let origin = format!("table `{}`", table.name);
        let row = names.claim(base.clone(), origin.clone())?;
        let insert = names.claim(format!("{base}Insert"), format!("{origin}'s insert type"))?;
        let mut fields = BTreeMap::new();
        let named = table
            .columns
            .iter()
            .map(|c| (c.name.as_str(), view_type(app, c)))
            .chain(
                table
                    .computed
                    .iter()
                    .map(|f| (f.name.as_str(), f.ty.ty.clone())),
            );
        for (field, ty) in named {
            if !needs_name(&ty) {
                continue;
            }
            let name = format!("{row}{}", pascal(field));
            let origin = format!("`{}.{field}`", table.name);
            let name = names.claim(name, origin.clone())?;
            let init = if has_init_form(&ty) {
                Some(names.claim(format!("{name}Init"), format!("{origin}'s insert form"))?)
            } else {
                None
            };
            let _ = writeln!(decls, "export type {name} = {};", ts_type(&ty, false));
            if let Some(init) = &init {
                let _ = writeln!(decls, "export type {init} = {};", ts_type(&ty, true));
            }
            fields.insert(field.to_string(), (name, init));
        }
        table_names.insert(
            table.name.clone(),
            TableNames {
                row,
                insert,
                fields,
            },
        );
    }

    let has_computed = app.tables.iter().any(|t| !t.computed.is_empty());
    let has_transforms = !app.transforms.is_empty();

    let mut out = String::new();
    let _ = writeln!(out, "{HEADER}");
    let _ = writeln!(out, "{}", format_line());
    let mut imports = vec!["bindApp", "type AppTypes"];
    if has_computed {
        imports.push("type ComputedImpl");
    }
    if uses_json_value(app) {
        imports.push("type JsonValue");
    }
    if has_transforms {
        imports.push("type TransformImpl");
    }
    let _ = writeln!(
        out,
        "import {{ {} }} from \"jazz-tools\";",
        imports.join(", ")
    );
    let local: Vec<&str> = [(has_computed, "computed"), (has_transforms, "transforms")]
        .into_iter()
        .filter_map(|(has, name)| has.then_some(name))
        .collect();
    if !local.is_empty() {
        let _ = writeln!(
            out,
            "import {{ {} }} from \"./jazz.computed.js\";",
            local.join(", ")
        );
    }
    out.push('\n');
    if !decls.is_empty() {
        out.push_str(&decls);
        out.push('\n');
    }

    // Row and Insert interfaces.
    for table in &app.tables {
        let n = &table_names[&table.name];
        let field_type = |field: &str, ty: &ColumnType, init: bool| -> String {
            match n.fields.get(field) {
                Some((row, init_name)) => match (init, init_name) {
                    (true, Some(init)) => init.clone(),
                    _ => row.clone(),
                },
                None => ts_type(ty, init),
            }
        };
        let _ = writeln!(out, "export type {} = {{", n.row);
        out.push_str("  readonly id: string;\n");
        for column in &table.columns {
            let ty = field_type(&column.name, &view_type(app, column), false);
            let _ = writeln!(
                out,
                "  {}: {};",
                prop(&column.name),
                nullable(ty, column.ty.nullable)
            );
        }
        for field in &table.computed {
            let ty = field_type(&field.name, &field.ty.ty, false);
            let _ = writeln!(
                out,
                "  readonly {}: {};",
                prop(&field.name),
                nullable(ty, field.ty.nullable)
            );
        }
        out.push_str("};\n\n");
        let _ = writeln!(out, "export type {} = {{", n.insert);
        for column in &table.columns {
            let ty = field_type(&column.name, &view_type(app, column), true);
            let _ = writeln!(
                out,
                "  {}{}: {};",
                prop(&column.name),
                if column.optional_on_insert() { "?" } else { "" },
                nullable(ty, column.ty.nullable)
            );
        }
        out.push_str("};\n\n");
    }

    // The type map.
    out.push_str("interface Types extends AppTypes {\n");
    for table in &app.tables {
        let n = &table_names[&table.name];
        let _ = writeln!(out, "  {}: {{", prop(&table.name));
        let _ = writeln!(out, "    row: {};", n.row);
        let _ = writeln!(out, "    insert: {};", n.insert);
        let columns: Vec<String> = table
            .columns
            .iter()
            .map(|c| {
                format!(
                    "{}: {{ kind: \"{}\"; nullable: {}; value: {} }};",
                    prop(&c.name),
                    kind(&c.ty.ty),
                    c.ty.nullable,
                    match &c.transform {
                        Some(_) => ts_type(&c.ty.ty, false),
                        None => n
                            .fields
                            .get(&c.name)
                            .map_or_else(|| ts_type(&c.ty.ty, false), |(row, _)| row.clone()),
                    }
                )
            })
            .collect();
        block(&mut out, 4, "columns", &columns);
        let relations: Vec<String> = table
            .relations
            .iter()
            .map(|r| match &r.kind {
                RelationKind::One { column, nullable } => {
                    let many = table
                        .column(column)
                        .is_some_and(|c| matches!(c.ty.ty, ColumnType::Array { .. }));
                    format!(
                        "{}: {{ {}: \"{}\"; nullable: {nullable} }};",
                        prop(&r.name),
                        if many { "many" } else { "one" },
                        r.target
                    )
                }
                RelationKind::Many { .. } => {
                    format!("{}: {{ many: \"{}\" }};", prop(&r.name), r.target)
                }
            })
            .collect();
        block(&mut out, 4, "relations", &relations);
        if !table.computed.is_empty() {
            let computed: Vec<String> = table
                .computed
                .iter()
                .map(|f| {
                    let from: Vec<String> = f
                        .from
                        .iter()
                        .map(|c| match table.column(c) {
                            Some(column) => format!(
                                "{}: {}",
                                prop(c),
                                nullable(
                                    n.fields.get(c).map_or_else(
                                        || ts_type(&view_type(app, column), false),
                                        |(row, _)| row.clone()
                                    ),
                                    column.ty.nullable
                                )
                            ),
                            None => format!("{}: string", prop(c)), // `id`
                        })
                        .collect();
                    let value = n
                        .fields
                        .get(&f.name)
                        .map_or_else(|| ts_type(&f.ty.ty, false), |(row, _)| row.clone());
                    format!(
                        "{}: {{ from: {}; value: {} }};",
                        prop(&f.name),
                        inline_object(&from),
                        nullable(value, f.ty.nullable)
                    )
                })
                .collect();
            block(&mut out, 4, "computed", &computed);
        }
        let transformed: Vec<String> = table
            .columns
            .iter()
            .filter_map(|c| {
                c.transform
                    .as_ref()
                    .map(|t| format!("{}: \"{t}\";", prop(&c.name)))
            })
            .collect();
        if !transformed.is_empty() {
            block(&mut out, 4, "transforms", &transformed);
        }
        out.push_str("  };\n");
    }
    out.push_str("}\n\n");

    if has_transforms {
        out.push_str("interface Transforms {\n");
        for t in &app.transforms {
            let _ = writeln!(
                out,
                "  {}: {{ stored: {}; view: {} }};",
                prop(&t.name),
                ts_type(&t.stored, false),
                ts_type(&t.view, false)
            );
        }
        out.push_str("}\n\n");
    }
    if has_computed {
        out.push_str("export type AppComputed = ComputedImpl<Types>;\n");
    }
    if has_transforms {
        out.push_str("export type AppTransforms = TransformImpl<Transforms, Types>;\n");
    }
    if has_computed || has_transforms {
        out.push('\n');
    }

    let options = bind_options(app);
    // Read back by `jazz-tools generate` and `migrations create`, so
    // formatters must leave it as it is.
    out.push_str("// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.\n");
    out.push_str("// biome-ignore format: generated JSON\n");
    out.push_str("// prettier-ignore\n");
    let _ = write!(
        out,
        "{BIND_APP}{}>(",
        if has_transforms {
            "Types, Transforms"
        } else {
            "Types"
        }
    );
    // Expanded at the top, so each option sits on its own lines; the
    // callbacks are values of jazz.computed.ts, not JSON.
    out.push_str("{\n");
    if let Json::Object(entries) = &options {
        for (k, v) in entries {
            let head = format!("  {}: ", key(k));
            out.push_str(&head);
            out.push_str(&js_literal(v, 2, head.len()));
            out.push_str(",\n");
        }
    }
    for name in &local {
        let _ = writeln!(out, "  {name},");
    }
    out.push('}');
    out.push_str(");\n");
    Ok(out)
}

/// The `bindApp` options as JSON, without the callbacks:
/// `{ source, schema: { current, schemas, migrations, computed?, transforms? }, relations }`.
pub fn bind_options(app: &App) -> Json {
    let mut schema = match app.compiled.to_json() {
        Json::Object(entries) => entries,
        _ => unreachable!("an object"),
    };
    let computed: Vec<(String, Json)> = app
        .tables
        .iter()
        .filter(|t| !t.computed.is_empty())
        .map(|t| {
            (
                t.name.clone(),
                Json::Object(
                    t.computed
                        .iter()
                        .map(|f| {
                            (
                                f.name.clone(),
                                Json::Array(f.from.iter().map(Json::string).collect()),
                            )
                        })
                        .collect(),
                ),
            )
        })
        .collect();
    if !computed.is_empty() {
        schema.push(("computed".into(), Json::Object(computed)));
    }
    let transforms: Vec<(String, Json)> = app
        .tables
        .iter()
        .filter_map(|t| {
            let columns: Vec<(String, Json)> = t
                .columns
                .iter()
                .filter_map(|c| {
                    c.transform
                        .as_ref()
                        .map(|n| (c.name.clone(), Json::string(n)))
                })
                .collect();
            (!columns.is_empty()).then(|| (t.name.clone(), Json::Object(columns)))
        })
        .collect();
    if !transforms.is_empty() {
        schema.push(("transforms".into(), Json::Object(transforms)));
    }
    let relations = app
        .tables
        .iter()
        .map(|t| {
            (
                t.name.clone(),
                Json::Object(
                    t.relations
                        .iter()
                        .map(|r| {
                            (
                                r.name.clone(),
                                match &r.kind {
                                    RelationKind::One { column, .. } => Json::object(vec![
                                        ("one", Json::string(&r.target)),
                                        ("column", Json::string(column)),
                                    ]),
                                    RelationKind::Many { relation } => Json::object(vec![
                                        ("many", Json::string(&r.target)),
                                        ("relation", Json::string(relation)),
                                    ]),
                                },
                            )
                        })
                        .collect(),
                ),
            )
        })
        .collect();
    Json::Object(vec![
        ("source".into(), Json::string(&app.source)),
        ("schema".into(), Json::Object(schema)),
        ("relations".into(), Json::Object(relations)),
    ])
}

fn block(out: &mut String, indent: usize, key: &str, lines: &[String]) {
    let pad = " ".repeat(indent);
    if lines.is_empty() {
        let _ = writeln!(out, "{pad}{key}: {{}};");
        return;
    }
    let _ = writeln!(out, "{pad}{key}: {{");
    for line in lines {
        let _ = writeln!(out, "{pad}  {line}");
    }
    let _ = writeln!(out, "{pad}}};");
}

fn inline_object(fields: &[String]) -> String {
    if fields.is_empty() {
        "{}".into()
    } else {
        format!("{{ {} }}", fields.join("; "))
    }
}

/// The type code sees for `column`: its transform's view type, or its own.
fn view_type(app: &App, column: &Column) -> ColumnType {
    match column.transform.as_ref().and_then(|t| app.transform(t)) {
        Some(transform) => transform.view.clone(),
        None => column.ty.ty.clone(),
    }
}

/// Whether a top-level field's type gets a named export.
fn needs_name(ty: &ColumnType) -> bool {
    matches!(
        ty,
        ColumnType::Enum { .. }
            | ColumnType::EnumPayload { .. }
            | ColumnType::Json { schema: Some(_) }
    )
}

/// Whether the insert form of a type differs from its row form.
fn has_init_form(ty: &ColumnType) -> bool {
    match ty {
        ColumnType::EnumPayload { cases } => cases.iter().any(|c| {
            c.fields
                .iter()
                .any(|f| f.nullable || f.default.is_some() || has_init_form(&f.column_type))
        }),
        ColumnType::Array { element } => has_init_form(element),
        _ => false,
    }
}

fn uses_json_value(app: &App) -> bool {
    fn uses(ty: &ColumnType) -> bool {
        match ty {
            ColumnType::Json { schema: None } => true,
            ColumnType::Array { element } => uses(element),
            ColumnType::EnumPayload { cases } => cases
                .iter()
                .any(|c| c.fields.iter().any(|f| uses(&f.column_type))),
            _ => false,
        }
    }
    app.tables.iter().any(|t| {
        t.columns
            .iter()
            .any(|c| uses(&c.ty.ty) || uses(&view_type(app, c)))
            || t.computed.iter().any(|f| uses(&f.ty.ty))
    }) || app
        .transforms
        .iter()
        .any(|t| uses(&t.stored) || uses(&t.view))
}

/// The `kind` the typed query builder reads.
fn kind(ty: &ColumnType) -> &'static str {
    match ty {
        ColumnType::Text => "string",
        ColumnType::Uuid => "uuid",
        ColumnType::Boolean => "boolean",
        ColumnType::Integer => "int",
        ColumnType::Double => "float",
        ColumnType::BigInt => "bigint",
        ColumnType::Timestamp => "timestamp",
        ColumnType::Bytea => "bytes",
        ColumnType::Json { .. } => "json",
        ColumnType::Enum { .. }
        | ColumnType::EnumPayload { .. }
        | ColumnType::ScalarEnum { .. }
        | ColumnType::CatalogueEnumPayload { .. } => "enum",
        ColumnType::Array { .. } => "array",
        ColumnType::Vector { .. } => "vector",
        // The loader refuses these in a file.
        ColumnType::TransactionId | ColumnType::Row { .. } => "json",
    }
}

fn nullable(ty: String, nullable: bool) -> String {
    if nullable { format!("{ty} | null") } else { ty }
}

/// A TS type for `ty`; `init` is the insert form, where nullable or
/// defaulted payload fields may be left out.
pub(crate) fn ts_type(ty: &ColumnType, init: bool) -> String {
    match ty {
        ColumnType::Text | ColumnType::Uuid => "string".into(),
        ColumnType::Boolean => "boolean".into(),
        ColumnType::Integer | ColumnType::Double => "number".into(),
        ColumnType::BigInt => "bigint".into(),
        ColumnType::Timestamp => "Date".into(),
        ColumnType::Bytea => "Uint8Array".into(),
        ColumnType::Vector { .. } => "Float32Array".into(),
        ColumnType::Json { schema: None } => "JsonValue".into(),
        ColumnType::Json {
            schema: Some(schema),
        } => crate::json_schema::to_ts(schema),
        ColumnType::Enum { variants } | ColumnType::ScalarEnum { variants, .. } => {
            union(variants.iter().map(|v| quote(v)).collect())
        }
        ColumnType::EnumPayload { cases } | ColumnType::CatalogueEnumPayload { cases, .. } => {
            union(
                cases
                    .iter()
                    .map(|case| {
                        let mut fields = vec![format!("type: {}", quote(&case.name))];
                        fields.extend(case.fields.iter().map(|f| payload_field(f, init)));
                        format!("{{ {} }}", fields.join("; "))
                    })
                    .collect(),
            )
        }
        ColumnType::Array { element } => {
            let inner = ts_type(element, init);
            if inner.contains(' ') && !inner.starts_with('{') {
                format!("({inner})[]")
            } else {
                format!("{inner}[]")
            }
        }
        ColumnType::TransactionId | ColumnType::Row { .. } => "unknown".into(),
    }
}

fn payload_field(field: &ColumnDescriptor, init: bool) -> String {
    let optional = init && (field.nullable || field.default.is_some());
    format!(
        "{}{}: {}",
        prop(field.name_str()),
        if optional { "?" } else { "" },
        nullable(ts_type(&field.column_type, init), field.nullable)
    )
}

fn union(items: Vec<String>) -> String {
    if items.is_empty() {
        "never".into()
    } else {
        items.join(" | ")
    }
}

/// A JS string literal (JSON's escapes are JS's).
pub(crate) fn quote(text: &str) -> String {
    serde_json::to_string(text).expect("strings serialize")
}

/// A property name: bare when it is an identifier, quoted otherwise.
pub(crate) fn prop(name: &str) -> String {
    let mut chars = name.chars();
    let identifier = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || c == '$')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$');
    if identifier {
        name.to_string()
    } else {
        quote(name)
    }
}

/// Width a literal may take on one line before it is expanded.
const WIDTH: usize = 100;

/// `value` as a JS object literal laid out as the files are (two-space
/// indent, one line when it fits in 100 columns). Keys are quoted; a key
/// `__proto__` is computed (`["__proto__"]`), so it stays an own property
/// rather than setting the object's prototype.
pub(crate) fn js_literal(value: &Json, indent: usize, prefix: usize) -> String {
    let mut out = String::new();
    write_literal(&mut out, value, indent, prefix);
    out
}

fn key(text: &str) -> String {
    if text == "__proto__" {
        "[\"__proto__\"]".into()
    } else {
        quote(text)
    }
}

fn flat(value: &Json) -> String {
    match value {
        Json::Null => "null".into(),
        Json::Bool(b) => b.to_string(),
        Json::Number(n) => n.clone(),
        Json::String(s) => quote(s),
        Json::Array(items) => {
            format!(
                "[{}]",
                items.iter().map(flat).collect::<Vec<_>>().join(", ")
            )
        }
        Json::Object(entries) if entries.is_empty() => "{}".into(),
        Json::Object(entries) => format!(
            "{{ {} }}",
            entries
                .iter()
                .map(|(k, v)| format!("{}: {}", key(k), flat(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn write_literal(out: &mut String, value: &Json, indent: usize, prefix: usize) {
    let line = flat(value);
    let fits = prefix + line.chars().count() <= WIDTH;
    let pad = |out: &mut String, n: usize| out.extend(std::iter::repeat_n(' ', n));
    match value {
        Json::Array(items) if !fits && !items.is_empty() => {
            out.push_str("[\n");
            for item in items {
                pad(out, indent + 2);
                write_literal(out, item, indent + 2, indent + 2);
                out.push_str(",\n");
            }
            pad(out, indent);
            out.push(']');
        }
        Json::Object(entries) if !fits && !entries.is_empty() => {
            out.push_str("{\n");
            for (k, item) in entries {
                pad(out, indent + 2);
                let head = format!("{}: ", key(k));
                out.push_str(&head);
                write_literal(out, item, indent + 2, indent + 2 + head.chars().count());
                out.push_str(",\n");
            }
            pad(out, indent);
            out.push('}');
        }
        _ => out.push_str(&line),
    }
}
