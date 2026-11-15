//! The Go back end: `jazzapp/jazz_app.go`, typed bindings over the Go
//! runtime module (`github.com/garden-co/jazz/packages/jazz-go`, package
//! `jazz`), which wraps the `jazz-ffi` C ABI through cgo.
//!
//! The bindings are their own package, `jazzapp`, in a directory beside
//! `jazz.app.json` and `go.mod`, so they never depend on the name of the
//! app's own package. Per table: a row struct (`Todo`) with json tags of
//! the authored names, an insert struct (`NewTodo`), a patch struct
//! (`TodoPatch`, whose fields are set one by one) and a table value
//! (`Todos`) holding its typed columns, its query builder and its writes.
//! `Open` embeds `jazz.app.json` itself.
//!
//! Computed fields and transforms are not supported yet: an app declaring
//! them is refused (as "unsupported", so `jazz-tools generate` skips Go
//! with a warning when it only found a `go.mod`).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use jazz::tools::public_schema::ColumnType;

use crate::CodegenError;
use crate::model::{App, Column, RelationKind, Table};
use crate::names::{pascal, singular};

/// Where the bindings go, relative to `jazz.app.json`.
pub const FILE: &str = "jazzapp/jazz_app.go";

/// The version of the bindings' shape this generator writes: the second
/// line records it.
pub const FORMAT: u32 = 1;

/// The first line, which marks a file as this generator's (and as
/// generated, for gofmt, golint and `go vet`).
pub const HEADER: &str =
    "// Code generated from jazz.app.json by `jazz-tools generate`. DO NOT EDIT.";

/// The Go runtime module's import path.
pub const MODULE: &str = "github.com/garden-co/jazz/packages/jazz-go";

/// How the format line starts.
const FORMAT_PREFIX: &str = "// Bindings format ";

/// How the embedded `jazz.app.json` starts: a raw string follows.
const APP_PREFIX: &str = "const App = ";

/// How the recorded source starts.
const SOURCE_PREFIX: &str = "const Source = ";

/// A backquote inside the embedded file, which a raw string can't hold.
const BACKQUOTE: &str = "` + \"`\" + `";

/// Package-level names the bindings declare themselves.
const RESERVED: &[&str] = &["App", "Source", "Open"];

/// Method names of a table value or a patch struct, which no column may
/// take as its Go name.
const TABLE_METHODS: &[&str] = &["Query", "Insert", "Update", "Delete", "MarshalJSON"];

/// Words Go spells in capitals inside a name (`userID`, `avatarURL`).
const INITIALISMS: &[&str] = &[
    "api", "css", "dns", "html", "http", "https", "id", "ip", "json", "sql", "ssh", "tcp", "ui",
    "uri", "url", "utf8", "uuid", "xml",
];

pub fn generate(app: &App) -> Result<String, CodegenError> {
    Writer::new(app)?.write()
}

/// Whether `go_mod` (a `go.mod`) is a module that uses the Go runtime: it
/// requires (or replaces) [`MODULE`].
pub fn is_app_module(go_mod: &str) -> bool {
    go_mod.lines().any(|line| {
        let line = line.split("//").next().unwrap_or("").trim();
        let line = line
            .strip_prefix("require")
            .or_else(|| line.strip_prefix("replace"))
            .map(str::trim)
            .unwrap_or(line)
            .trim_start_matches('(')
            .trim();
        line.split_whitespace()
            .next()
            .is_some_and(|path| path.trim_matches('"') == MODULE)
    })
}

struct TableNames {
    row: String,
    new: String,
    patch: String,
    /// The table value (`Todos`) and its type (`todosTable`).
    value: String,
    ty: String,
}

struct Writer<'a> {
    app: &'a App,
    tables: BTreeMap<&'a str, TableNames>,
    /// Generated enums by Go name: values (Go const name, stored name), and
    /// what declared them.
    enums: BTreeMap<String, (Vec<(String, String)>, String)>,
    /// Every package-level name taken, with what took it.
    names: BTreeMap<String, String>,
    uses_json: bool,
    /// Transforms by name, once their types were generated.
    transforms: BTreeMap<String, (String, String, String)>,
    /// Generated payload enums by Go name: cases (struct name, stored name,
    /// fields as (Go name, stored name, Go type)), and what declared them.
    unions: BTreeMap<String, (Vec<UnionCase>, String)>,
    /// The methods of the `Computed` interface, with their doc lines.
    computed_methods: Vec<(String, String)>,
    has_computed: bool,
}

/// A column as the bindings hold it.
struct Field {
    name: String,
    ident: String,
    /// The row's (and a patch's) type: a pointer when nullable.
    ty: String,
    /// The type a column's conditions take: never a pointer.
    value: String,
    optional_on_insert: bool,
    nullable: bool,
    /// The payload enum (a sealed interface) the column holds, if any:
    /// decoded by the row's JazzDecode, and nil when null.
    union: Option<String>,
    /// The transform the column goes through: `ty` is then its view type,
    /// `value` (and the JSON) its stored one.
    transform: Option<FieldTransform>,
}

struct FieldTransform {
    /// The method stem: `IsoDate` for `IsoDateView` / `IsoDateStored`.
    stem: String,
    /// The stored type, never a pointer.
    stored: String,
    /// The view type, never a pointer.
    view: String,
}

/// A payload enum case: its struct, its stored name, and its fields as
/// (Go name, stored name, Go type).
type UnionCase = (String, String, Vec<(String, String, String)>);

/// A transform's Go names and types.
struct GoTransform {
    stem: String,
    stored: String,
    view: String,
}

impl<'a> Writer<'a> {
    fn new(app: &'a App) -> Result<Self, CodegenError> {
        let mut writer = Self {
            app,
            tables: BTreeMap::new(),
            enums: BTreeMap::new(),
            names: RESERVED
                .iter()
                .map(|name| (name.to_string(), "a name the bindings use".to_string()))
                .collect(),
            uses_json: false,
            transforms: BTreeMap::new(),
            unions: BTreeMap::new(),
            computed_methods: Vec::new(),
            has_computed: !app.transforms.is_empty()
                || app.tables.iter().any(|t| !t.computed.is_empty()),
        };
        if writer.has_computed {
            writer.take("Computed", "the Computed interface")?;
        }
        for table in &app.tables {
            let row = go_ident(&pascal(&singular(&table.name)), &table.name)?;
            let mut value = go_ident(&pascal(&table.name), &table.name)?;
            if value == row {
                value = format!("{row}Table");
            }
            // `todosTable`; `activityTable` for the value `ActivityTable`.
            let ty = if value.ends_with("Table") {
                lower_first(&value)
            } else {
                format!("{}Table", lower_first(&value))
            };
            let what = format!("table `{}`", table.name);
            let names = TableNames {
                new: format!("New{row}"),
                patch: format!("{row}Patch"),
                row,
                value,
                ty,
            };
            for name in [
                &names.row,
                &names.new,
                &names.patch,
                &names.value,
                &names.ty,
            ] {
                writer.take(name, &what)?;
            }
            writer.tables.insert(&table.name, names);
        }
        Ok(writer)
    }

    fn take(&mut self, name: &str, what: &str) -> Result<(), CodegenError> {
        match self.names.insert(name.to_owned(), what.to_owned()) {
            Some(other) => Err(CodegenError::unsupported(format!(
                "{what} and {other} are both the Go name `{name}`"
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
        out.push('\n');
        out.push_str("// Package jazzapp holds the typed bindings of this app's jazz.app.json.\n");
        out.push_str("package jazzapp\n\n");
        out.push_str("import (\n\t\"context\"\n");
        if self.uses_json {
            out.push_str("\t\"encoding/json\"\n");
        }
        let _ = writeln!(out, "\n\tjazz \"{MODULE}\"\n)\n");
        out.push_str(
            "// Source is the hash of the jazz.app.json these bindings were generated from.\n",
        );
        let _ = writeln!(out, "{SOURCE_PREFIX}{:?}\n", self.app.source);
        out.push_str("// App is the jazz.app.json these bindings were generated from.\n");
        let _ = writeln!(
            out,
            "{APP_PREFIX}`{}`\n",
            self.app.file.replace('`', BACKQUOTE)
        );
        out.push_str(
            "// Open opens this app: a client, or a backend when cfg.Permissions is set.\n",
        );
        if self.has_computed {
            out.push_str("// computed is your Computed: what this app's computed fields and transforms run.\n");
            out.push_str("func Open(ctx context.Context, cfg jazz.Config, computed Computed) (*jazz.DB, error) {\n");
            out.push_str("\tif computed == nil {\n\t\treturn nil, jazz.ErrNoComputed\n\t}\n");
            out.push_str(
                "\tcfg.App = App\n\tcfg.Computed = computed\n\treturn jazz.Open(ctx, cfg)\n}\n\n",
            );
            out.push_str("// Computed is what this app computes, which you implement and pass to Open: its\n// computed fields, and its transforms' conversions.\n");
            out.push_str("type Computed interface {\n");
            for (signature, doc) in &self.computed_methods {
                let _ = writeln!(out, "\t// {doc}\n\t{signature}");
            }
            out.push_str("}\n\n");
        } else {
            out.push_str("func Open(ctx context.Context, cfg jazz.Config) (*jazz.DB, error) {\n");
            out.push_str("\tcfg.App = App\n\treturn jazz.Open(ctx, cfg)\n}\n\n");
        }
        for (name, (values, what)) in &self.enums {
            let _ = writeln!(out, "// {name} is a value of {what}.");
            let _ = writeln!(out, "type {name} string\n");
            let _ = writeln!(out, "// The values of {name}.");
            out.push_str("const (\n");
            let width = values.iter().map(|(c, _)| c.len()).max().unwrap_or(0);
            for (constant, stored) in values {
                let _ = writeln!(out, "\t{constant:<width$} {name} = {}", go_string(stored));
            }
            out.push_str(")\n\n");
        }
        for (name, (cases, what)) in &self.unions {
            let _ = writeln!(
                out,
                "// {name} is a value of {what}: one of {}.",
                cases
                    .iter()
                    .map(|(c, _, _)| c.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let _ = writeln!(out, "type {name} interface{{ is{name}() }}\n");
            for (case_ty, case_name, fields) in cases {
                let _ = writeln!(out, "// {case_ty} is the case `{case_name}` of {name}.");
                let lines: Vec<[String; 3]> = fields
                    .iter()
                    .map(|(ident, field, ty)| {
                        [ident.clone(), ty.clone(), format!("`json:\"{field}\"`")]
                    })
                    .collect();
                write_struct(&mut out, case_ty, &lines);
                let _ = writeln!(out, "func ({case_ty}) is{name}() {{}}\n");
                let _ = writeln!(
                    out,
                    "// MarshalJSON writes the case as Jazz carries it: {{case, fields}}."
                );
                let _ = writeln!(
                    out,
                    "func (v {case_ty}) MarshalJSON() ([]byte, error) {{\n\ttype fields {case_ty}\n\treturn jazz.MarshalCase({}, fields(v))\n}}\n",
                    go_string(case_name)
                );
            }
            let _ = writeln!(
                out,
                "func decode{name}(raw json.RawMessage) ({name}, error) {{"
            );
            out.push_str("\tcase_, fields, err := jazz.DecodeCase(raw)\n\tif err != nil || case_ == \"\" {\n\t\treturn nil, err\n\t}\n\tswitch case_ {\n");
            for (case_ty, case_name, _) in cases {
                let _ = writeln!(
                    out,
                    "\tcase {}:\n\t\tvar v {case_ty}\n\t\terr := json.Unmarshal(fields, &v)\n\t\treturn v, err",
                    go_string(case_name)
                );
            }
            let _ = writeln!(
                out,
                "\t}}\n\treturn nil, jazz.UnknownCase({}, case_)\n}}\n",
                go_string(what)
            );
        }
        out.push_str(&items);
        // One trailing newline, as gofmt leaves it.
        while out.ends_with("\n\n") {
            out.pop();
        }
        Ok(out)
    }

    /// The Go type of a value of `ty`; `name` and `what` name a generated
    /// enum.
    fn go_type(&mut self, ty: &ColumnType, name: &str, what: &str) -> Result<String, CodegenError> {
        Ok(match ty {
            ColumnType::Integer => "int32".into(),
            ColumnType::BigInt => "jazz.BigInt".into(),
            ColumnType::Boolean => "bool".into(),
            ColumnType::Text | ColumnType::Uuid => "string".into(),
            ColumnType::Double => "float64".into(),
            ColumnType::Timestamp => "jazz.Timestamp".into(),
            ColumnType::Bytea => "[]byte".into(),
            ColumnType::Json { .. } => {
                self.uses_json = true;
                "json.RawMessage".into()
            }
            ColumnType::Array { element } => format!("[]{}", self.go_type(element, name, what)?),
            ColumnType::Enum { variants } => {
                let mut values = Vec::new();
                let mut seen = BTreeMap::new();
                for variant in variants {
                    // The value's words follow the type's name, so a value may
                    // start with a digit (`"1"` → `NoteLevel1`).
                    let constant = go_ident(&format!("{name}{}", pascal_words(variant)), variant)?;
                    if let Some(other) = seen.insert(constant.clone(), variant.clone()) {
                        return Err(CodegenError::unsupported(format!(
                            "{what}: values `{other}` and `{variant}` are both the Go name `{constant}`"
                        )));
                    }
                    values.push((constant, variant.clone()));
                }
                self.take(name, what)?;
                for (constant, _) in &values {
                    self.take(constant, what)?;
                }
                self.enums
                    .insert(name.to_owned(), (values, what.to_owned()));
                name.to_owned()
            }
            ColumnType::EnumPayload { cases } => {
                self.uses_json = true;
                self.take(name, what)?;
                let mut out = Vec::new();
                for case in cases {
                    let case_ty =
                        go_ident(&format!("{name}{}", pascal_words(&case.name)), &case.name)?;
                    self.take(&case_ty, what)?;
                    let mut fields = Vec::new();
                    let mut seen = BTreeSet::new();
                    for field in &case.fields {
                        let field_name = field.name_str().to_string();
                        let ident = go_ident(&pascal_words(&field_name), &field_name)?;
                        if !seen.insert(ident.clone()) {
                            return Err(CodegenError::unsupported(format!(
                                "{what}: fields of case `{}` are both the Go name `{ident}`",
                                case.name
                            )));
                        }
                        let inner = self.go_type(
                            &field.column_type,
                            &format!("{case_ty}{ident}"),
                            &format!("{what}, case `{}`", case.name),
                        )?;
                        let ty = if field.nullable {
                            format!("*{inner}")
                        } else {
                            inner
                        };
                        fields.push((ident, field_name, ty));
                    }
                    out.push((case_ty, case.name.clone(), fields));
                }
                self.unions.insert(name.to_owned(), (out, what.to_owned()));
                name.to_owned()
            }
            other => {
                return Err(CodegenError::unsupported(format!(
                    "{what}: {} values in Go bindings",
                    type_label(other)
                )));
            }
        })
    }

    fn field(
        &mut self,
        table: &Table,
        row: &str,
        column: &Column,
        ident: String,
    ) -> Result<Field, CodegenError> {
        let what = format!("column `{}.{}`", table.name, column.name);
        let value = self.go_type(&column.ty.ty, &format!("{row}{ident}"), &what)?;
        let nullable = column.ty.nullable;
        let transform = match &column.transform {
            Some(name) => {
                let t = self.transform(name)?;
                Some(FieldTransform {
                    stem: t.stem,
                    stored: t.stored,
                    view: t.view,
                })
            }
            None => None,
        };
        let shown = transform.as_ref().map_or(value.clone(), |t| t.view.clone());
        let union = self.unions.contains_key(&value).then(|| value.clone());
        Ok(Field {
            name: column.name.clone(),
            ident,
            union: union.clone(),
            ty: if nullable && union.is_none() {
                format!("*{shown}")
            } else {
                shown
            },
            value,
            optional_on_insert: column.optional_on_insert(),
            nullable,
            transform,
        })
    }

    /// The Go names and types of transform `name`, declaring its methods
    /// on `Computed` the first time.
    fn transform(&mut self, name: &str) -> Result<GoTransform, CodegenError> {
        if let Some((stem, stored, view)) = self.transforms.get(name) {
            return Ok(GoTransform {
                stem: stem.clone(),
                stored: stored.clone(),
                view: view.clone(),
            });
        }
        let declared = self
            .app
            .transform(name)
            .ok_or_else(|| CodegenError::new(format!("transform `{name}` is not declared")))?;
        let stem = go_ident(&pascal_words(name), name)?;
        let what = format!("transform `{name}`");
        let stored = self.go_type(&declared.stored.clone(), &format!("{stem}Stored"), &what)?;
        let view = self.go_type(&declared.view.clone(), &format!("{stem}View"), &what)?;
        self.computed_methods.push((
            format!("{stem}View(stored {stored}) ({view}, error)"),
            format!("{stem}View reads a value stored through transform `{name}`."),
        ));
        self.computed_methods.push((
            format!("{stem}Stored(view {view}) ({stored}, error)"),
            format!("{stem}Stored is what transform `{name}` stores for view."),
        ));
        self.transforms.insert(
            name.to_owned(),
            (stem.clone(), stored.clone(), view.clone()),
        );
        Ok(GoTransform { stem, stored, view })
    }

    fn table(&mut self, table: &'a Table) -> Result<String, CodegenError> {
        let names = &self.tables[table.name.as_str()];
        let (row, new, patch, value, ty) = (
            names.row.clone(),
            names.new.clone(),
            names.patch.clone(),
            names.value.clone(),
            names.ty.clone(),
        );
        let mut idents: BTreeSet<String> = TABLE_METHODS.iter().map(|m| m.to_string()).collect();
        idents.insert("ID".into());
        let mut fields = Vec::new();
        for column in &table.columns {
            let ident = go_ident(&pascal_words(&column.name), &column.name)?;
            if !idents.insert(ident.clone()) {
                return Err(CodegenError::unsupported(format!(
                    "`{}.{}` is the Go name `{ident}`, which another column or a method of `{value}` already is",
                    table.name, column.name
                )));
            }
            fields.push(self.field(table, &row, column, ident)?);
        }
        // Relations: a field of the row, filled when the query includes it,
        // and a value of the table to include it with.
        let mut relations = Vec::new();
        for relation in &table.relations {
            let ident = go_ident(&pascal_words(&relation.name), &relation.name)?;
            if !idents.insert(ident.clone()) {
                return Err(CodegenError::unsupported(format!(
                    "relation `{}.{}` is the Go name `{ident}`, which a column, another relation or a method of `{value}` already is",
                    table.name, relation.name
                )));
            }
            let target = self.tables[relation.target.as_str()].row.clone();
            // A forward relation through an array of ids leads to many rows.
            let through_array = match &relation.kind {
                RelationKind::One { column, .. } => table
                    .column(column)
                    .is_some_and(|c| matches!(c.ty.ty, ColumnType::Array { .. })),
                RelationKind::Many { .. } => true,
            };
            let ty = if through_array {
                format!("[]{target}")
            } else {
                format!("*{target}")
            };
            relations.push((ident, relation.name.clone(), target, ty));
        }
        // Computed fields: filled from the row by the app's Computed.
        let mut computed = Vec::new();
        for c in &table.computed {
            let ident = go_ident(&pascal_words(&c.name), &c.name)?;
            if !idents.insert(ident.clone()) {
                return Err(CodegenError::unsupported(format!(
                    "computed field `{}.{}` is the Go name `{ident}`, which a column, a relation or a method of `{value}` already is",
                    table.name, c.name
                )));
            }
            let what = format!("computed field `{}.{}`", table.name, c.name);
            let inner = self.go_type(&c.ty.ty, &format!("{row}{ident}"), &what)?;
            let ty = if c.ty.nullable {
                format!("*{inner}")
            } else {
                inner
            };
            let mut params = Vec::new();
            let mut args = Vec::new();
            for from in &c.from {
                let f = fields.iter().find(|f| &f.name == from).ok_or_else(|| {
                    CodegenError::new(format!(
                        "{what} is computed from `{from}`, which isn't a column"
                    ))
                })?;
                let param = go_param(&f.ident);
                params.push(format!("{param} {}", f.ty));
                args.push(format!("r.{}", f.ident));
            }
            let method = format!("{row}{ident}");
            self.computed_methods.push((
                format!("{method}({}) {ty}", params.join(", ")),
                format!(
                    "{method} computes `{}.{}` from {}.",
                    table.name,
                    c.name,
                    c.from
                        .iter()
                        .map(|f| format!("`{f}`"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ));
            computed.push((ident, ty, method, args));
        }
        let needs_computed = !computed.is_empty() || fields.iter().any(|f| f.transform.is_some());
        let hooks = needs_computed || fields.iter().any(|f| f.union.is_some());
        if hooks {
            self.uses_json = true;
        }

        let mut out = String::new();
        let tag = |name: &str, omitempty: bool| {
            let name = go_tag_name(name);
            if omitempty {
                format!("`json:\"{name},omitempty\"`")
            } else {
                format!("`json:\"{name}\"`")
            }
        };

        // The row.
        let _ = writeln!(out, "// {row} is a row of `{}`.", table.name);
        let mut lines = vec![["ID".to_owned(), "string".to_owned(), tag("id", false)]];
        for f in &fields {
            let t = if f.transform.is_some() || f.union.is_some() {
                "`json:\"-\"`".to_owned()
            } else {
                tag(&f.name, false)
            };
            lines.push([f.ident.clone(), f.ty.clone(), t]);
        }
        for (ident, ty, _, _) in &computed {
            lines.push([ident.clone(), ty.clone(), "`json:\"-\"`".to_owned()]);
        }
        for (ident, name, _, ty) in &relations {
            lines.push([ident.clone(), ty.clone(), tag(name, true)]);
        }
        write_struct(&mut out, &row, &lines);
        if hooks {
            let _ = writeln!(
                out,
                "// JazzDecode reads what plain JSON can't: payload enums, the transformed columns' views and\n// the computed fields."
            );
            let _ = writeln!(
                out,
                "func (r *{row}) JazzDecode(app any, fields map[string]json.RawMessage) error {{"
            );
            if needs_computed {
                out.push_str(
                    "\tc, ok := app.(Computed)\n\tif !ok {\n\t\treturn jazz.ErrNoComputed\n\t}\n",
                );
            }
            for f in &fields {
                let Some(union) = &f.union else { continue };
                let _ = write!(
                    out,
                    "\tif raw, ok := fields[{key}]; ok {{\n\t\tv, err := decode{union}(raw)\n\t\tif err != nil {{\n\t\t\treturn err\n\t\t}}\n\t\tr.{id} = v\n\t}}\n",
                    key = go_string(&f.name),
                    id = f.ident
                );
            }
            for f in &fields {
                let Some(t) = &f.transform else { continue };
                let key = go_string(&f.name);
                if f.nullable {
                    let _ = write!(
                        out,
                        "	if raw, ok := fields[{key}]; ok {{
		var stored *{s}
		if err := json.Unmarshal(raw, &stored); err != nil {{
			return err
		}}
		r.{id} = nil
		if stored != nil {{
			view, err := c.{stem}View(*stored)
			if err != nil {{
				return err
			}}
			r.{id} = &view
		}}
	}}
",
                        s = t.stored,
                        id = f.ident,
                        stem = t.stem
                    );
                } else {
                    let _ = write!(
                        out,
                        "	if raw, ok := fields[{key}]; ok {{
		var stored {s}
		if err := json.Unmarshal(raw, &stored); err != nil {{
			return err
		}}
		view, err := c.{stem}View(stored)
		if err != nil {{
			return err
		}}
		r.{id} = view
	}}
",
                        s = t.stored,
                        id = f.ident,
                        stem = t.stem
                    );
                }
            }
            for (ident, _, method, args) in &computed {
                let _ = writeln!(out, "	r.{ident} = c.{method}({})", args.join(", "));
            }
            out.push_str(
                "	return nil
}

",
            );
        }

        // The insert.
        let _ = writeln!(
            out,
            "// {new} is a row to insert into `{}`: nil fields are null, or take their default.",
            table.name
        );
        let lines: Vec<[String; 3]> = fields
            .iter()
            .map(|f| {
                let ty = if f.optional_on_insert && !f.nullable {
                    format!("*{}", f.value)
                } else {
                    f.ty.clone()
                };
                let ty = match &f.transform {
                    Some(t) if f.optional_on_insert => format!("*{}", t.view),
                    Some(t) => t.view.clone(),
                    None => ty,
                };
                let t = if f.transform.is_some() {
                    "`json:\"-\"`".to_owned()
                } else {
                    tag(&f.name, f.optional_on_insert)
                };
                [f.ident.clone(), ty, t]
            })
            .collect();
        write_struct(&mut out, &new, &lines);
        if fields.iter().any(|f| f.transform.is_some()) {
            let _ = writeln!(
                out,
                "// JazzEncode is the row to insert, its transformed columns stored."
            );
            let _ = writeln!(out, "func (r {new}) JazzEncode(app any) (any, error) {{");
            out.push_str(
                "	c, ok := app.(Computed)
	if !ok {
		return nil, jazz.ErrNoComputed
	}
",
            );
            let _ = writeln!(
                out,
                "	type plain {new}
	out, err := jazz.Fields(plain(r))
	if err != nil {{
		return nil, err
	}}"
            );
            for f in &fields {
                let Some(t) = &f.transform else { continue };
                let key = go_string(&f.name);
                if f.optional_on_insert {
                    let _ = write!(
                        out,
                        "	if r.{id} != nil {{
		stored, err := c.{stem}Stored(*r.{id})
		if err != nil {{
			return nil, err
		}}
		out[{key}] = stored
	}}
",
                        id = f.ident,
                        stem = t.stem
                    );
                } else {
                    let _ = write!(
                        out,
                        "	{{
		stored, err := c.{stem}Stored(r.{id})
		if err != nil {{
			return nil, err
		}}
		out[{key}] = stored
	}}
",
                        id = f.ident,
                        stem = t.stem
                    );
                }
            }
            out.push_str(
                "	return out, nil
}

",
            );
        }

        // The patch.
        let _ = writeln!(
            out,
            "// {patch} changes the fields of a row of `{}` that are set (jazz.Set).",
            table.name
        );
        let lines: Vec<[String; 3]> = fields
            .iter()
            .map(|f| {
                [
                    f.ident.clone(),
                    format!("jazz.Patch[{}]", f.ty),
                    String::new(),
                ]
            })
            .collect();
        write_struct(&mut out, &patch, &lines);
        let entries: Vec<String> = fields
            .iter()
            .filter(|f| f.transform.is_none())
            .map(|f| format!("p.{}.Entry({})", f.ident, go_string(&f.name)))
            .collect();
        if fields.iter().any(|f| f.transform.is_some()) {
            let _ = writeln!(
                out,
                "// JazzEncode is the patch, its transformed columns stored."
            );
            let _ = writeln!(out, "func (p {patch}) JazzEncode(app any) (any, error) {{");
            out.push_str(
                "\tc, ok := app.(Computed)\n\tif !ok {\n\t\treturn nil, jazz.ErrNoComputed\n\t}\n",
            );
            if entries.is_empty() {
                out.push_str("\tentries := []jazz.PatchEntry{}\n");
            } else {
                out.push_str("\tentries := []jazz.PatchEntry{\n");
                for e in &entries {
                    let _ = writeln!(out, "\t\t{e},");
                }
                out.push_str("\t}\n");
            }
            for f in &fields {
                let Some(t) = &f.transform else { continue };
                let key = go_string(&f.name);
                if f.nullable {
                    let _ = write!(
                        out,
                        "\tif p.{id}.IsSet() {{\n\t\tvar stored *{s}\n\t\tif view := p.{id}.Value(); view != nil {{\n\t\t\tv, err := c.{stem}Stored(*view)\n\t\t\tif err != nil {{\n\t\t\t\treturn nil, err\n\t\t\t}}\n\t\t\tstored = &v\n\t\t}}\n\t\tentries = append(entries, jazz.Set(stored).Entry({key}))\n\t}}\n",
                        id = f.ident,
                        s = t.stored,
                        stem = t.stem
                    );
                } else {
                    let _ = write!(
                        out,
                        "\tif p.{id}.IsSet() {{\n\t\tstored, err := c.{stem}Stored(p.{id}.Value())\n\t\tif err != nil {{\n\t\t\treturn nil, err\n\t\t}}\n\t\tentries = append(entries, jazz.Set(stored).Entry({key}))\n\t}}\n",
                        id = f.ident,
                        stem = t.stem
                    );
                }
            }
            out.push_str("\treturn jazz.PatchJSON(entries...)\n}\n\n");
        } else {
            let _ = writeln!(out, "// MarshalJSON writes the fields that are set.");
            let _ = writeln!(out, "func (p {patch}) MarshalJSON() ([]byte, error) {{");
            if entries.is_empty() {
                out.push_str("\treturn jazz.MarshalPatch()\n}\n\n");
            } else {
                out.push_str("\treturn jazz.MarshalPatch(\n");
                for e in &entries {
                    let _ = writeln!(out, "\t\t{e},");
                }
                out.push_str("\t)\n}\n\n");
            }
        }

        // The table value: columns, query, writes.
        let _ = writeln!(
            out,
            "// {ty} is the type of {value}: the columns of `{}`, to build conditions and orders from, and its relations, to include.",
            table.name
        );
        let mut lines = vec![[
            "ID".to_owned(),
            format!("jazz.Column[{row}, string]"),
            String::new(),
        ]];
        for f in &fields {
            lines.push([
                f.ident.clone(),
                format!("jazz.Column[{row}, {}]", f.value),
                String::new(),
            ]);
        }
        for (ident, _, target, _) in &relations {
            lines.push([
                ident.clone(),
                format!("jazz.Relation[{row}, {target}]"),
                String::new(),
            ]);
        }
        write_struct(&mut out, &ty, &lines);
        let _ = writeln!(out, "// {value} is the table `{}`.", table.name);
        let _ = writeln!(out, "var {value} = {ty}{{");
        let width = lines.iter().map(|l| l[0].len() + 1).max().unwrap_or(0);
        let _ = writeln!(
            out,
            "\t{:<width$} jazz.NewColumn[{row}, string](\"id\"),",
            "ID:"
        );
        for f in &fields {
            let _ = writeln!(
                out,
                "\t{:<width$} jazz.NewColumn[{row}, {}]({}),",
                format!("{}:", f.ident),
                f.value,
                go_string(&f.name)
            );
        }
        for (ident, name, target, _) in &relations {
            let _ = writeln!(
                out,
                "\t{:<width$} jazz.NewRelation[{row}, {target}]({}),",
                format!("{ident}:"),
                go_string(name)
            );
        }
        out.push_str("}\n\n");
        let table_name = go_string(&table.name);
        let _ = writeln!(out, "// Query starts a query of `{}`.", table.name);
        let _ = writeln!(
            out,
            "func ({ty}) Query() *jazz.Query[{row}] {{\n\treturn jazz.NewQuery[{row}]({table_name})\n}}\n"
        );
        let _ = writeln!(
            out,
            "// Insert inserts a row into `{}` and returns it, with its id.",
            table.name
        );
        let _ = writeln!(
            out,
            "func ({ty}) Insert(ctx context.Context, db jazz.Querier, row {new}, opts ...jazz.WriteOption) ({row}, error) {{\n\treturn jazz.Insert[{new}, {row}](ctx, db, {table_name}, row, opts...)\n}}\n"
        );
        let _ = writeln!(
            out,
            "// Update changes the fields patch sets of the row of `{}` with id.",
            table.name
        );
        let _ = writeln!(
            out,
            "func ({ty}) Update(ctx context.Context, db jazz.Querier, id string, patch {patch}, opts ...jazz.WriteOption) error {{\n\treturn jazz.Update(ctx, db, {table_name}, id, patch, opts...)\n}}\n"
        );
        let _ = writeln!(
            out,
            "// Delete deletes the row of `{}` with id.",
            table.name
        );
        let _ = writeln!(
            out,
            "func ({ty}) Delete(ctx context.Context, db jazz.Querier, id string, opts ...jazz.WriteOption) error {{\n\treturn jazz.Delete(ctx, db, {table_name}, id, opts...)\n}}\n"
        );
        Ok(out)
    }
}

/// A struct whose lines are `[name, type, tag]`, aligned as gofmt aligns
/// them.
fn write_struct(out: &mut String, name: &str, lines: &[[String; 3]]) {
    if lines.is_empty() {
        let _ = writeln!(out, "type {name} struct{{}}\n");
        return;
    }
    let _ = writeln!(out, "type {name} struct {{");
    let names = lines.iter().map(|l| l[0].len()).max().unwrap_or(0);
    let types = lines.iter().map(|l| l[1].len()).max().unwrap_or(0);
    for [field, ty, tag] in lines {
        let line = if tag.is_empty() {
            format!("\t{field:<names$} {ty}")
        } else {
            format!("\t{field:<names$} {ty:<types$} {tag}")
        };
        let _ = writeln!(out, "{}", line.trim_end());
    }
    out.push_str("}\n\n");
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

// ---- reading back ------------------------------------------------------------

/// What committed bindings embed: their format, their source, and the
/// `jazz.app.json` they were generated from, checked again (its source
/// must still be what they record).
pub(crate) fn read(bindings: &str) -> Result<crate::bindings::Recorded, CodegenError> {
    let refuse = |what: &str| {
        CodegenError::new(format!(
            "{FILE}: {what}; regenerate it with `jazz-tools generate`"
        ))
    };
    let bindings = bindings.replace("\r\n", "\n");
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
    let source = bindings
        .lines()
        .find_map(|line| line.strip_prefix(SOURCE_PREFIX))
        .and_then(|literal| serde_json::from_str::<String>(literal.trim()).ok())
        .ok_or_else(|| refuse("no `const Source`"))?;
    let at = bindings
        .find(&format!("{APP_PREFIX}`"))
        .ok_or_else(|| refuse("no `const App`"))?
        + APP_PREFIX.len()
        + 1;
    let mut rest = &bindings[at..];
    let mut file = String::new();
    loop {
        let end = rest.find('`').ok_or_else(|| refuse("`App` is cut short"))?;
        file.push_str(&rest[..end]);
        rest = &rest[end..];
        match rest.strip_prefix(BACKQUOTE) {
            Some(after) => {
                file.push('`');
                rest = after;
            }
            None => break,
        }
    }
    // Regenerating can't repair these: the file is the record of the
    // version the app last generated against.
    let unusable = |what: &str| {
        CodegenError::new(format!(
            "{FILE}: {what}. Restore it from version control; or delete it and run \
             `jazz-tools generate` (an edit of `tables` since it was generated then \
             has no recorded version to migrate from)"
        ))
    };
    if jazz::tools::deployment::artifact::source_hash(file.as_bytes()) != source {
        return Err(unusable(
            "its `App` was edited (it no longer matches `Source`)",
        ));
    }
    let app = App::load(file.as_bytes())
        .map_err(|error| unusable(&format!("its `App` no longer loads ({error})")))?;
    Ok(crate::bindings::Recorded {
        source,
        format,
        current: app.compiled.current,
        current_schema: app.compiled.current_schema().clone(),
    })
}

// ---- names ------------------------------------------------------------------

/// Words of a camelCase, snake_case or kebab-case name, each capitalised
/// (initialisms in capitals): `userId` → `UserID`, `in-progress` →
/// `InProgress`, `HIGH` → `High`.
fn pascal_words(name: &str) -> String {
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
        .iter()
        .map(|w| {
            if INITIALISMS.contains(&w.as_str()) {
                w.to_uppercase()
            } else if let Some(one) = w.strip_suffix('s').filter(|one| INITIALISMS.contains(one)) {
                // `ids` → `IDs`, `urls` → `URLs`.
                format!("{}s", one.to_uppercase())
            } else {
                pascal(w)
            }
        })
        .collect()
}

/// An exported Go name derived from `name`.
fn go_ident(ident: &str, name: &str) -> Result<String, CodegenError> {
    if ident.is_empty()
        || !ident.starts_with(|c: char| c.is_ascii_uppercase())
        || !ident.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(CodegenError::unsupported(format!(
            "`{name}` has no exported Go name: it must start with a letter and use only ASCII letters, digits and `_`"
        )));
    }
    Ok(ident.to_owned())
}

/// A parameter name from a field's: `ShowID` → `showID`, `Type` → `type_`.
fn go_param(ident: &str) -> String {
    let lower = match ident.char_indices().find(|(_, c)| c.is_ascii_lowercase()) {
        // Lower the leading capitals, keeping the last one of a run before
        // a lowercase letter as the next word's start: `URLPath` → `urlPath`.
        Some((0, _)) | None => ident.to_ascii_lowercase(),
        Some((1, _)) => lower_first(ident),
        Some((i, _)) => ident[..i - 1].to_ascii_lowercase() + &ident[i - 1..],
    };
    match lower.as_str() {
        "break" | "case" | "chan" | "const" | "continue" | "default" | "defer" | "else"
        | "fallthrough" | "for" | "func" | "go" | "goto" | "if" | "import" | "interface"
        | "map" | "package" | "range" | "return" | "select" | "struct" | "switch" | "type"
        | "var" | "r" | "c" | "app" | "fields" | "ok" | "err" | "jazz" | "json" | "context" => {
            format!("{lower}_")
        }
        _ => lower,
    }
}

fn lower_first(name: &str) -> String {
    let mut chars = name.chars();
    match chars.next() {
        Some(first) => first.to_ascii_lowercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}

/// A Go string literal (JSON's escapes are Go's).
fn go_string(text: &str) -> String {
    serde_json::to_string(text).expect("strings serialize")
}

/// A name inside a struct tag's backquotes: what `encoding/json` reads
/// back as the key. Authored column names are identifiers
/// (`[A-Za-z_][A-Za-z0-9_]*`), which a tag holds as they are.
fn go_tag_name(name: &str) -> String {
    name.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert_eq!(pascal_words("isPublic"), "IsPublic");
        assert_eq!(pascal_words("userId"), "UserID");
        assert_eq!(pascal_words("user_id"), "UserID");
        assert_eq!(pascal_words("avatarUrl"), "AvatarURL");
        assert_eq!(pascal_words("assigneeIds"), "AssigneeIDs");
        assert_eq!(pascal_words("in-progress"), "InProgress");
        assert_eq!(pascal_words("HIGH"), "High");
        assert!(go_ident(&pascal_words("2fa"), "2fa").is_err());
        assert_eq!(lower_first("ChatMembers"), "chatMembers");
    }

    #[test]
    fn go_modules_require_the_runtime() {
        assert!(is_app_module(&format!(
            "module example.com/todo\n\ngo 1.23\n\nrequire {MODULE} v0.0.0\n"
        )));
        assert!(is_app_module(&format!(
            "module x\n\nrequire (\n\t{MODULE} v0.0.0\n)\n"
        )));
        assert!(is_app_module(&format!(
            "module x\n\nrequire (\n\tgolang.org/x/net v1\n\t{MODULE} v0.0.0 // indirect\n)\n"
        )));
        assert!(!is_app_module("module x\n\ngo 1.23\n"));
        assert!(!is_app_module(&format!(
            "module x\n// require {MODULE} v0\n"
        )));
        assert!(!is_app_module(&format!("module {MODULE}\n")));
    }
}
