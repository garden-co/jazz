//! The Rust back end: `src/jazz_app.rs`, typed bindings over the
//! `jazz-app` crate.
//!
//! Per table: a row struct (`Todo`), a new-row struct (`NewTodo`) and an
//! update struct (`TodoUpdate`), with snake_case fields mapped to the
//! authored names, and a module (`todos`) with its query, typed columns and
//! relations. A `Computed` trait declares the app's computed fields and
//! transforms, so a missing implementation is a compile error. `open`
//! embeds `jazz.app.json` itself: the Rust runtime reads it with the same
//! loader `jazz validate` runs, and a backend checks its
//! `jazz.permissions.json` against it.
//!
//! The output is committed by default; [`build`] generates it from a
//! `build.rs` instead.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use jazz::tools::public_schema::ColumnType;

use crate::CodegenError;
use crate::model::{App, Column, Computed, FieldType, Relation, RelationKind, Table};
use crate::names::{pascal, singular};

/// Where the bindings go, relative to `jazz.app.json`.
pub const FILE: &str = "src/jazz_app.rs";

/// The first line, which marks a file as this generator's.
pub const HEADER: &str =
    "// jazz_app.rs: generated from jazz.app.json by `jazz-tools validate`. Do not edit.";

/// The runtime crate, as an absolute path: the bindings' own module is
/// usually named `jazz_app` too.
const RT: &str = "::jazz_app";

/// Type names the bindings declare themselves.
const RESERVED: &[&str] = &[
    "Computed",
    "Implementations",
    "Option",
    "Vec",
    "String",
    "Self",
];

pub fn generate(app: &App) -> Result<String, CodegenError> {
    Writer::new(app)?.write()
}

/// Generates the bindings in a build script, from `jazz.app.json` beside
/// `Cargo.toml` into `$OUT_DIR/jazz_app.rs`:
///
/// ```ignore
/// // build.rs
/// fn main() {
///     jazz_codegen::rust::build().unwrap();
/// }
/// // src/main.rs
/// mod jazz_app {
///     include!(concat!(env!("OUT_DIR"), "/jazz_app.rs"));
/// }
/// ```
///
/// Committing `src/jazz_app.rs`, as `jazz-tools validate` writes it, is the
/// recommended default.
pub fn build() -> Result<(), CodegenError> {
    let var = |name: &str| {
        std::env::var(name)
            .map_err(|_| CodegenError::new(format!("{name} is unset: run this from build.rs")))
    };
    let dir = std::path::PathBuf::from(var("CARGO_MANIFEST_DIR")?);
    let out = std::path::PathBuf::from(var("OUT_DIR")?);
    let path = dir.join("jazz.app.json");
    println!("cargo:rerun-if-changed={}", path.display());
    let file = std::fs::read(&path)
        .map_err(|error| CodegenError::new(format!("{}: {error}", path.display())))?;
    // `include!` takes no inner attributes, and a build's output is never
    // formatted anyway.
    let text = crate::generate(crate::Lang::Rust, &file)?
        .lines()
        .filter(|line| !line.starts_with("#!["))
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(out.join("jazz_app.rs"), text)
        .map_err(|error| CodegenError::new(format!("writing jazz_app.rs: {error}")))
}

struct TableNames {
    row: String,
    new: String,
    update: String,
    module: String,
}

struct Writer<'a> {
    app: &'a App,
    tables: BTreeMap<&'a str, TableNames>,
    /// Generated enums by Rust name: variants (Rust name, stored name), and
    /// what declared them.
    enums: BTreeMap<String, (Vec<(String, String)>, String)>,
    /// Every type name taken, with what took it.
    types: BTreeMap<String, String>,
    has_computed: bool,
}

impl<'a> Writer<'a> {
    fn new(app: &'a App) -> Result<Self, CodegenError> {
        let mut writer = Self {
            app,
            tables: BTreeMap::new(),
            enums: BTreeMap::new(),
            types: RESERVED
                .iter()
                .map(|name| (name.to_string(), "a name the bindings use".to_string()))
                .collect(),
            has_computed: !app.transforms.is_empty()
                || app.tables.iter().any(|table| !table.computed.is_empty()),
        };
        let mut modules = BTreeMap::new();
        for table in &app.tables {
            let row = type_ident(&pascal(&singular(&table.name)), &table.name)?;
            let what = format!("table `{}`", table.name);
            for name in [row.clone(), format!("New{row}"), format!("{row}Update")] {
                writer.take_type(&name, &what)?;
            }
            let module = value_ident(&table.name)?;
            if let Some(other) = modules.insert(module.clone(), table.name.clone()) {
                return Err(CodegenError::unsupported(format!(
                    "tables `{other}` and `{}` are both the Rust module `{module}`",
                    table.name
                )));
            }
            writer.tables.insert(
                &table.name,
                TableNames {
                    new: format!("New{row}"),
                    update: format!("{row}Update"),
                    row,
                    module,
                },
            );
        }
        Ok(writer)
    }

    fn take_type(&mut self, name: &str, what: &str) -> Result<(), CodegenError> {
        match self.types.insert(name.to_owned(), what.to_owned()) {
            Some(other) => Err(CodegenError::unsupported(format!(
                "{what} and {other} are both the Rust type `{name}`"
            ))),
            None => Ok(()),
        }
    }

    fn row(&self, table: &str) -> &str {
        &self.tables[table].row
    }

    fn write(mut self) -> Result<String, CodegenError> {
        let mut items = String::new();
        for table in &self.app.tables {
            items.push_str(&self.table(table)?);
        }
        items.push_str(&self.computed_trait()?);
        items.push_str(&self.open());

        let mut out = String::new();
        let _ = writeln!(out, "{HEADER}");
        out.push_str("// Regenerate it after changing jazz.app.json.\n");
        // Committed as generated: `cargo fmt` leaves it be, so
        // `validate --check` doesn't find it stale. (Stable Rust takes
        // rustfmt's skip only in this form.)
        out.push_str("#![cfg_attr(rustfmt, rustfmt_skip)]\n");
        out.push_str("#![allow(dead_code, clippy::all)]\n\n");
        out.push_str("/// The `jazz.app.json` these bindings were generated from.\n");
        let _ = writeln!(out, "pub const SOURCE: &str = {:?};\n", self.app.source);
        for (name, (variants, what)) in &self.enums {
            let _ = writeln!(out, "/// The values of {what}.");
            out.push_str("#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]\n");
            let _ = writeln!(out, "pub enum {name} {{");
            for (variant, stored) in variants {
                if variant != stored {
                    let _ = writeln!(out, "    /// `{stored}`.");
                }
                let _ = writeln!(out, "    {variant},");
            }
            out.push_str("}\n\n");
            let _ = writeln!(out, "{RT}::text_enum!({name} {{");
            for (variant, stored) in variants {
                let _ = writeln!(out, "    {variant} => {stored:?},");
            }
            out.push_str("});\n\n");
        }
        out.push_str(&items);
        Ok(out)
    }

    /// The Rust type of a value of `ty`; `name` and `what` name a
    /// generated enum.
    fn rust_type(
        &mut self,
        ty: &ColumnType,
        name: &str,
        what: &str,
    ) -> Result<String, CodegenError> {
        Ok(match ty {
            ColumnType::Integer => "i32".into(),
            ColumnType::BigInt => "i64".into(),
            ColumnType::Boolean => "bool".into(),
            ColumnType::Text => "String".into(),
            ColumnType::Double => "f64".into(),
            ColumnType::Timestamp => format!("{RT}::Timestamp"),
            ColumnType::Uuid => format!("{RT}::ObjectId"),
            ColumnType::Bytea => "Vec<u8>".into(),
            ColumnType::Json { .. } => format!("{RT}::Json"),
            ColumnType::Array { element } => {
                format!("Vec<{}>", self.rust_type(element, name, what)?)
            }
            ColumnType::Enum { variants } => {
                let mut rust = Vec::new();
                let mut seen = BTreeMap::new();
                for variant in variants {
                    let ident = type_ident(&pascal(&snake_words(variant).join("_")), variant)?;
                    if let Some(other) = seen.insert(ident.clone(), variant.clone()) {
                        return Err(CodegenError::unsupported(format!(
                            "{what}: values `{other}` and `{variant}` are both the Rust variant `{ident}`"
                        )));
                    }
                    rust.push((ident, variant.clone()));
                }
                match self.enums.get(name) {
                    // The same enum, reached again (a transform used twice).
                    Some((existing, _)) if *existing == rust => {}
                    _ => {
                        self.take_type(name, what)?;
                        self.enums.insert(name.to_owned(), (rust, what.to_owned()));
                    }
                }
                name.to_owned()
            }
            other => {
                return Err(CodegenError::unsupported(format!(
                    "{what}: {} values in Rust bindings",
                    type_label(other)
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
        let inner = self.rust_type(&ty.ty, name, what)?;
        Ok(if ty.nullable {
            format!("Option<{inner}>")
        } else {
            inner
        })
    }

    fn table(&mut self, table: &'a Table) -> Result<String, CodegenError> {
        let names = &self.tables[table.name.as_str()];
        let (row, new, update, module) = (
            names.row.clone(),
            names.new.clone(),
            names.update.clone(),
            names.module.clone(),
        );
        let mut idents = BTreeSet::from(["id".to_owned()]);
        let mut fields = Vec::new();
        for column in &table.columns {
            let ident = value_ident(&column.name)?;
            if !idents.insert(ident.clone()) {
                return Err(clash(table, &column.name, &ident));
            }
            let what = format!("column `{}.{}`", table.name, column.name);
            let field = Field::new(self, &row, column, ident, &what)?;
            fields.push(field);
        }
        let mut computed = Vec::new();
        for c in &table.computed {
            let ident = value_ident(&c.name)?;
            if !idents.insert(ident.clone()) {
                return Err(clash(table, &c.name, &ident));
            }
            let what = format!("computed field `{}.{}`", table.name, c.name);
            let ty = self.field_type(&c.ty, &format!("{row}{}", pascal(&c.name)), &what)?;
            computed.push((c, ident, ty));
        }

        let mut out = String::new();
        // The row.
        let _ = writeln!(out, "/// A row of `{}`.", table.name);
        out.push_str("#[derive(Debug, Clone, PartialEq)]\n");
        let _ = writeln!(out, "pub struct {row} {{");
        let _ = writeln!(out, "    pub id: {RT}::ObjectId,");
        for field in &fields {
            field.doc(&mut out);
            let _ = writeln!(out, "    pub {}: {},", field.ident, field.view);
        }
        for (c, ident, ty) in &computed {
            let _ = writeln!(
                out,
                "    /// Computed from {}.",
                c.from
                    .iter()
                    .map(|f| format!("`{f}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let _ = writeln!(out, "    pub {ident}: {ty},");
        }
        out.push_str("}\n\n");

        // New rows.
        let defaultable = fields.iter().all(|field| field.insert_has_default());
        let _ = writeln!(out, "/// A new row of `{}`.", table.name);
        let _ = writeln!(
            out,
            "#[derive(Debug, Clone, PartialEq{})]",
            if defaultable { ", Default" } else { "" }
        );
        let _ = writeln!(out, "pub struct {new} {{");
        for field in &fields {
            field.doc(&mut out);
            if field.has_default() {
                out.push_str("    /// `None` stores the column's default.\n");
            }
            let _ = writeln!(out, "    pub {}: {},", field.ident, field.insert_type());
        }
        out.push_str("}\n\n");

        // Updates.
        let _ = writeln!(
            out,
            "/// Changes to a row of `{}`: a `None` field leaves its column as it is.",
            table.name
        );
        out.push_str("#[derive(Debug, Clone, PartialEq, Default)]\n");
        let _ = writeln!(out, "pub struct {update} {{");
        for field in &fields {
            field.doc(&mut out);
            let _ = writeln!(out, "    pub {}: Option<{}>,", field.ident, field.view);
        }
        out.push_str("}\n\n");

        let uses_impl = !computed.is_empty() || fields.iter().any(|f| f.transform.is_some());

        // Table.
        let _ = writeln!(out, "impl {RT}::Table for {row} {{");
        let _ = writeln!(out, "    const NAME: &'static str = {:?};", table.name);
        let _ = writeln!(out, "    type New = {new};");
        let _ = writeln!(out, "    type Update = {update};\n");
        let _ = writeln!(
            out,
            "    fn decode(row: &mut {RT}::Fields<'_>) -> {RT}::Result<Self> {{"
        );
        if uses_impl {
            out.push_str(
                "        let computed = &row.implementations().get::<Implementations>()?.0;\n",
            );
        }
        for field in &fields {
            let take = format!("row.take::<{}>({:?})?", field.stored, field.name);
            let value = match &field.transform {
                Some(ident) if field.nullable => {
                    format!("{take}.map(|stored| computed.{ident}_view(stored))")
                }
                Some(ident) => format!("computed.{ident}_view({take})"),
                None => take,
            };
            let _ = writeln!(out, "        let {} = {value};", field.ident);
        }
        for (c, ident, _) in &computed {
            let args = c
                .from
                .iter()
                .map(|from| match from.as_str() {
                    "id" => Ok("row.id()".to_owned()),
                    from => Ok(format!("{}.clone()", value_ident(from)?)),
                })
                .collect::<Result<Vec<_>, CodegenError>>()?;
            let _ = writeln!(
                out,
                "        let {ident} = computed.{}({});",
                computed_fn(table, c)?,
                args.join(", ")
            );
        }
        out.push_str("        Ok(Self {\n            id: row.id(),\n");
        for field in &fields {
            let _ = writeln!(out, "            {},", field.ident);
        }
        for (_, ident, _) in &computed {
            let _ = writeln!(out, "            {ident},");
        }
        out.push_str("        })\n    }\n\n");
        let _ = writeln!(
            out,
            "    fn id(&self) -> {RT}::ObjectId {{\n        self.id\n    }}\n}}\n"
        );

        let value = format!("{RT}::jazz::tools::Value");
        let implementations = if uses_impl {
            "implementations"
        } else {
            "_implementations"
        };

        // Insert.
        let _ = writeln!(out, "impl {RT}::Insert for {new} {{");
        let _ = writeln!(out, "    type Row = {row};\n");
        let _ = writeln!(
            out,
            "    fn into_cells(\n        self,\n        {implementations}: &{RT}::Implementations,\n    ) -> {RT}::Result<::std::collections::HashMap<String, {value}>> {{"
        );
        if uses_impl {
            out.push_str("        let computed = &implementations.get::<Implementations>()?.0;\n");
        }
        out.push_str("        let mut cells = ::std::collections::HashMap::new();\n");
        for field in &fields {
            let stored = field.stored_value("value");
            if field.has_default() {
                let _ = writeln!(
                    out,
                    "        if let Some(value) = self.{} {{\n            cells.insert({:?}.to_owned(), {stored});\n        }}",
                    field.ident, field.name
                );
            } else {
                let _ = writeln!(
                    out,
                    "        let value = self.{};\n        cells.insert({:?}.to_owned(), {stored});",
                    field.ident, field.name
                );
            }
        }
        out.push_str("        Ok(cells)\n    }\n}\n\n");

        // Update.
        let _ = writeln!(out, "impl {RT}::Update for {update} {{");
        let _ = writeln!(out, "    type Row = {row};\n");
        let _ = writeln!(
            out,
            "    fn into_cells(\n        self,\n        {implementations}: &{RT}::Implementations,\n    ) -> {RT}::Result<Vec<(String, {value})>> {{"
        );
        if uses_impl {
            out.push_str("        let computed = &implementations.get::<Implementations>()?.0;\n");
        }
        out.push_str("        let mut cells = Vec::new();\n");
        for field in &fields {
            let _ = writeln!(
                out,
                "        if let Some(value) = self.{} {{\n            cells.push(({:?}.to_owned(), {}));\n        }}",
                field.ident,
                field.name,
                field.stored_value("value")
            );
        }
        out.push_str("        Ok(cells)\n    }\n}\n\n");

        // The table's module.
        let _ = writeln!(
            out,
            "/// Queries, columns and relations of `{}`.",
            table.name
        );
        let _ = writeln!(out, "pub mod {module} {{");
        out.push_str("    #[allow(unused_imports)]\n    use super::*;\n\n");
        let mut fns = BTreeSet::from(["query".to_owned()]);
        let _ = writeln!(
            out,
            "    /// Every row of `{}`; narrow it with `filter`.\n    pub fn query() -> {RT}::Query<{row}> {{\n        {RT}::Query::all()\n    }}",
            table.name
        );
        for field in &fields {
            if !fns.insert(field.ident.clone()) {
                return Err(clash(table, &field.name, &field.ident));
            }
            let note = if field.transform.is_some() {
                " Compared as stored, before its transform."
            } else {
                ""
            };
            let _ = writeln!(
                out,
                "\n    /// Column `{}`.{note}\n    pub fn {}() -> {RT}::Column<{row}, {}> {{\n        {RT}::Column::new({:?})\n    }}",
                field.name, field.ident, field.stored, field.name
            );
        }
        for relation in &table.relations {
            // A relation named as its column (`todo` over `todo`) gets
            // `_relation` after it.
            let mut ident = value_ident(&relation.name)?;
            if fns.contains(&ident) {
                ident = format!("{}_relation", ident.trim_start_matches("r#"));
            }
            if !fns.insert(ident.clone()) {
                return Err(clash(table, &relation.name, &ident));
            }
            out.push_str(&self.relation(table, &row, relation, &ident)?);
        }
        out.push_str("}\n\n");
        Ok(out)
    }

    fn relation(
        &self,
        table: &Table,
        row: &str,
        relation: &Relation,
        ident: &str,
    ) -> Result<String, CodegenError> {
        let target = self
            .tables
            .get(relation.target.as_str())
            .map(|names| names.row.clone())
            .ok_or_else(|| {
                CodegenError::new(format!(
                    "relation `{}.{}` leads to `{}`, which is not a table",
                    table.name, relation.name, relation.target
                ))
            })?;
        Ok(match &relation.kind {
            RelationKind::One { column, .. } => format!(
                "\n    /// Relation `{}`: the `{}` row `{column}` references.\n    pub fn {ident}() -> {RT}::One<{row}, {target}> {{\n        {RT}::One::new({column:?})\n    }}\n",
                relation.name, relation.target
            ),
            RelationKind::Many { relation: forward } => {
                let column = self
                    .app
                    .table(&relation.target)
                    .and_then(|t| t.relations.iter().find(|r| &r.name == forward))
                    .and_then(|r| match &r.kind {
                        RelationKind::One { column, .. } => Some(column.clone()),
                        RelationKind::Many { .. } => None,
                    })
                    .ok_or_else(|| {
                        CodegenError::new(format!(
                            "relation `{}.{}` reverses `{}.{forward}`, which is not a forward relation",
                            table.name, relation.name, relation.target
                        ))
                    })?;
                format!(
                    "\n    /// Relation `{}`: the `{}` rows whose `{column}` references this row.\n    pub fn {ident}() -> {RT}::Many<{row}, {target}> {{\n        {RT}::Many::new({column:?})\n    }}\n",
                    relation.name, relation.target
                )
            }
        })
    }

    fn computed_trait(&mut self) -> Result<String, CodegenError> {
        if !self.has_computed {
            return Ok(String::new());
        }
        let mut out = String::new();
        out.push_str(
            "/// What the app's code computes: each computed field `jazz.app.json`\n\
             /// declares, and each transform both ways. [`open`] takes an implementation.\n",
        );
        out.push_str("pub trait Computed: Send + Sync + 'static {\n");
        let mut fns = BTreeSet::new();
        for table in &self.app.tables {
            let row = self.row(&table.name).to_owned();
            for c in &table.computed {
                let what = format!("computed field `{}.{}`", table.name, c.name);
                let ret = self.field_type(&c.ty, &format!("{row}{}", pascal(&c.name)), &what)?;
                let mut args = Vec::new();
                for from in &c.from {
                    if from == "id" {
                        args.push(format!("id: {RT}::ObjectId"));
                        continue;
                    }
                    let column = table.column(from).ok_or_else(|| {
                        CodegenError::new(format!(
                            "{what} is computed from `{from}`, which is not a column"
                        ))
                    })?;
                    let what = format!("column `{}.{}`", table.name, column.name);
                    let field = Field::new(self, &row, column, value_ident(from)?, &what)?;
                    args.push(format!("{}: {}", field.ident, field.view));
                }
                let name = computed_fn(table, c)?;
                if !fns.insert(name.clone()) {
                    return Err(CodegenError::unsupported(format!(
                        "two `Computed` methods are named `{name}`"
                    )));
                }
                let _ = writeln!(
                    out,
                    "    /// `{}.{}`.\n    fn {name}(&self, {}) -> {ret};\n",
                    table.name,
                    c.name,
                    args.join(", ")
                );
            }
        }
        for transform in &self.app.transforms {
            let (stored, view) = self.transform_types(&transform.name)?;
            let ident = value_ident(&transform.name)?;
            for name in [format!("{ident}_view"), format!("{ident}_stored")] {
                if !fns.insert(name.clone()) {
                    return Err(CodegenError::unsupported(format!(
                        "two `Computed` methods are named `{name}`"
                    )));
                }
            }
            let _ = writeln!(
                out,
                "    /// Transform `{0}`: a stored value, as the app reads it.\n    fn {ident}_view(&self, stored: {stored}) -> {view};\n\n    /// Transform `{0}`: a value the app writes, as stored.\n    fn {ident}_stored(&self, view: {view}) -> {stored};\n",
                transform.name
            );
        }
        let trimmed = out.trim_end().to_owned();
        let mut out = trimmed;
        out.push_str("\n}\n\nstruct Implementations(Box<dyn Computed>);\n\n");
        Ok(out)
    }

    /// The stored and view types of transform `name`.
    fn transform_types(&mut self, name: &str) -> Result<(String, String), CodegenError> {
        let transform = self
            .app
            .transform(name)
            .ok_or_else(|| CodegenError::new(format!("transform `{name}` is not declared")))?;
        let what = format!("transform `{name}`");
        let base = pascal(&snake_words(name).join("_"));
        let stored = self.rust_type(&transform.stored, &format!("{base}Stored"), &what)?;
        let view = self.rust_type(&transform.view, &format!("{base}View"), &what)?;
        Ok((stored, view))
    }

    fn open(&self) -> String {
        let mut out = String::new();
        let hashes = "#".repeat(longest_hash_run(&self.app.file) + 1);
        let _ = writeln!(
            out,
            "/// The app's `jazz.app.json`: its schema, every earlier version and the\n/// migrations between them.\npub const APP: &str = r{hashes}\"{}\"{hashes};\n",
            self.app.file
        );
        out.push_str(
            "/// Opens the app. A backend (`Config::backend`, with its\n\
             /// `jazz.permissions.json`) first publishes the app's permissions bundle\n\
             /// and pins the session it acts in to it.\n",
        );
        if self.has_computed {
            let _ = writeln!(
                out,
                "pub async fn open(config: {RT}::Config, computed: impl Computed) -> {RT}::Result<{RT}::Jazz> {{\n    {RT}::Jazz::open_with(\n        {RT}::AppFiles::new(APP, None),\n        config,\n        {RT}::Implementations::new(Implementations(Box::new(computed))),\n    )\n    .await\n}}"
            );
        } else {
            let _ = writeln!(
                out,
                "pub async fn open(config: {RT}::Config) -> {RT}::Result<{RT}::Jazz> {{\n    {RT}::Jazz::open({RT}::AppFiles::new(APP, None), config).await\n}}"
            );
        }
        out
    }
}

/// A column as the bindings type it.
struct Field {
    name: String,
    ident: String,
    /// What the row holds: the transform's view type, if it has one.
    view: String,
    /// What the engine stores.
    stored: String,
    nullable: bool,
    default: bool,
    /// The transform's method stem.
    transform: Option<String>,
}

impl Field {
    fn new(
        writer: &mut Writer<'_>,
        row: &str,
        column: &Column,
        ident: String,
        what: &str,
    ) -> Result<Self, CodegenError> {
        let nullable = column.ty.nullable;
        let wrap = |ty: String| {
            if nullable {
                format!("Option<{ty}>")
            } else {
                ty
            }
        };
        let (view, stored, transform) = match &column.transform {
            Some(name) => {
                let (stored, view) = writer.transform_types(name)?;
                (wrap(view), wrap(stored), Some(value_ident(name)?))
            }
            None => {
                let ty = writer.field_type(
                    &column.ty,
                    &format!("{row}{}", pascal(&column.name)),
                    what,
                )?;
                (ty.clone(), ty, None)
            }
        };
        Ok(Self {
            name: column.name.clone(),
            ident,
            view,
            stored,
            nullable,
            default: column.default.is_some(),
            transform,
        })
    }

    /// A non-null column with a default may be left out of an insert.
    fn has_default(&self) -> bool {
        self.default && !self.nullable
    }

    fn insert_type(&self) -> String {
        if self.has_default() {
            format!("Option<{}>", self.view)
        } else {
            self.view.clone()
        }
    }

    fn insert_has_default(&self) -> bool {
        self.has_default() || self.nullable || type_has_default(&self.view)
    }

    fn doc(&self, out: &mut String) {
        if self.ident.trim_start_matches("r#") != self.name {
            let _ = writeln!(out, "    /// `{}`.", self.name);
        }
    }

    /// The cell for a field value `value`, through its transform.
    fn stored_value(&self, value: &str) -> String {
        let converted = match &self.transform {
            Some(ident) if self.nullable => {
                format!("{value}.map(|view| computed.{ident}_stored(view))")
            }
            Some(ident) => format!("computed.{ident}_stored({value})"),
            None => value.to_owned(),
        };
        format!("{RT}::ColumnValue::into_value({converted})")
    }
}

fn type_has_default(ty: &str) -> bool {
    ty.starts_with("Option<")
        || ty.starts_with("Vec<")
        || matches!(ty, "i32" | "i64" | "bool" | "String" | "f64")
}

fn computed_fn(table: &Table, computed: &Computed) -> Result<String, CodegenError> {
    let ident = format!(
        "{}_{}",
        snake_words(&table.name).join("_"),
        snake_words(&computed.name).join("_")
    );
    value_ident(&ident)
}

fn clash(table: &Table, name: &str, ident: &str) -> CodegenError {
    CodegenError::unsupported(format!(
        "`{}.{name}` is the Rust name `{ident}`, which another field, relation or `query` of the table already is",
        table.name
    ))
}

fn type_label(ty: &ColumnType) -> &'static str {
    match ty {
        ColumnType::TransactionId => "transaction id",
        ColumnType::Row { .. } => "nested row",
        ColumnType::EnumPayload { .. } | ColumnType::CatalogueEnumPayload { .. } => {
            "enum-with-payload"
        }
        ColumnType::ScalarEnum { .. } => "catalogue enum",
        _ => "these",
    }
}

fn longest_hash_run(text: &str) -> usize {
    text.split(|c| c != '#').map(str::len).max().unwrap_or(0)
}

// ---- names ------------------------------------------------------------------

/// Lowercase words of a camelCase, snake_case or kebab-case name:
/// `userID` → `user`, `id`; `in-progress` → `in`, `progress`.
fn snake_words(name: &str) -> Vec<String> {
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
    "abstract", "as", "async", "await", "become", "box", "break", "const", "continue", "do", "dyn",
    "else", "enum", "extern", "false", "final", "fn", "for", "gen", "if", "impl", "in", "let",
    "loop", "macro", "match", "mod", "move", "mut", "override", "priv", "pub", "ref", "return",
    "static", "struct", "trait", "true", "try", "type", "typeof", "unsafe", "unsized", "use",
    "virtual", "where", "while", "yield",
];

/// A field, function or module name: snake_case, raw where it's a keyword.
fn value_ident(name: &str) -> Result<String, CodegenError> {
    let ident = snake_words(name).join("_");
    if ident.is_empty() || ident.starts_with(|c: char| c.is_ascii_digit()) {
        return Err(CodegenError::unsupported(format!(
            "`{name}` has no Rust name: it must start with a letter"
        )));
    }
    Ok(match ident.as_str() {
        "self" | "super" | "crate" => format!("{ident}_"),
        _ if KEYWORDS.contains(&ident.as_str()) => format!("r#{ident}"),
        _ => ident,
    })
}

/// A type or variant name derived from `name`.
fn type_ident(ident: &str, name: &str) -> Result<String, CodegenError> {
    if ident.is_empty()
        || ident.starts_with(|c: char| !c.is_ascii_alphabetic())
        || !ident.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(CodegenError::unsupported(format!(
            "`{name}` has no Rust type name: it must start with a letter and use only letters, digits and `_`"
        )));
    }
    Ok(ident.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        let snake = |name: &str| value_ident(name).unwrap();
        assert_eq!(snake("isPublic"), "is_public");
        assert_eq!(snake("messagesViaChat"), "messages_via_chat");
        assert_eq!(snake("userID"), "user_id");
        assert_eq!(snake("chat_members"), "chat_members");
        assert_eq!(snake("type"), "r#type");
        assert_eq!(snake("self"), "self_");
        assert!(value_ident("2fa").is_err());
        let variant = |name: &str| pascal(&snake_words(name).join("_"));
        assert_eq!(variant("in-progress"), "InProgress");
        assert_eq!(variant("HIGH"), "High");
        assert_eq!(longest_hash_run("a ## b ### c"), 3);
    }
}
