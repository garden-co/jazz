//! The language-neutral model every back end generates from: an app's
//! `jazz.app.json` once it passed every check `jazz validate` runs.
//!
//! A back end reads [`App`] and writes text; nothing here knows any target
//! language. Names are the authored ones (the file's keys); each back end
//! maps them to its own idiom and refuses names it can't express.

use jazz::tools::deployment::artifact::{
    self, AppDefinition, ArtifactError, CodegenRelation, json::Json,
};
use jazz::tools::deployment::published::{PublishMigrationRequest, PublishedSchema};
use jazz::tools::public_schema::{
    ColumnMergeStrategy, ColumnType, Schema, SchemaHash, TableSchema, Value,
};

/// An app, as its bindings see it.
#[derive(Debug, Clone)]
pub struct App {
    /// `sha256:<hex>` of the file ([`artifact::source_hash`]): what the
    /// bindings record so a stale copy is caught.
    pub source: String,
    /// The file's text with CRLF read as LF (`source` hashes its canonical
    /// form, so the two agree whatever the formatting). Empty when the app
    /// wasn't loaded from a file.
    pub file: String,
    /// The current tables, by name.
    pub tables: Vec<Table>,
    /// The file's top-level `transforms`, by name.
    pub transforms: Vec<Transform>,
    /// The engine schema, history and lenses a runtime opens along.
    pub compiled: Compiled,
    /// What loaded but is ignored (a .63 file's `slices`, `tsOnly`), and
    /// the stranded versions.
    pub warnings: Vec<String>,
    /// History versions no migration connects to the current one, whose
    /// rows would need transforming: bindings refuse them.
    pub stranded: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    /// In declared order.
    pub columns: Vec<Column>,
    /// By name.
    pub relations: Vec<Relation>,
    /// In declared order.
    pub computed: Vec<Computed>,
}

/// A type and whether `null` is a value of it.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldType {
    pub ty: ColumnType,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    /// The stored type.
    pub ty: FieldType,
    pub default: Option<Value>,
    pub merge: Option<ColumnMergeStrategy>,
    /// The table a `uuid` column references.
    pub references: Option<String>,
    /// The transform (in [`App::transforms`]) the stored value goes through.
    pub transform: Option<String>,
}

impl Column {
    /// Whether an insert may leave the column out.
    pub fn optional_on_insert(&self) -> bool {
        self.ty.nullable || self.default.is_some()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Relation {
    pub name: String,
    /// The table the relation leads to.
    pub target: String,
    pub kind: RelationKind,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RelationKind {
    /// This table's `column` holds the id of one row of the target.
    One { column: String, nullable: bool },
    /// Rows of the target whose forward relation `relation` points here.
    Many { relation: String },
}

#[derive(Debug, Clone)]
pub struct Computed {
    pub name: String,
    pub ty: FieldType,
    /// Stored columns of the same table, in the order the callback takes them.
    pub from: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Transform {
    pub name: String,
    /// The type the engine stores.
    pub stored: ColumnType,
    /// The type code reads and writes.
    pub view: ColumnType,
}

/// What a runtime opens along: the `ClientMigrations` bundle
/// `artifact::load_client` lowers, plus which version is current.
#[derive(Debug, Clone)]
pub struct Compiled {
    pub current: SchemaHash,
    /// Oldest first, the current one last.
    pub schemas: Vec<(SchemaHash, Schema)>,
    pub migrations: Vec<PublishMigrationRequest>,
}

impl Compiled {
    /// `{ current, schemas: [{ hash, schema }], migrations }` in the
    /// engine's serde forms, which `published::client_migrations` reads.
    pub fn to_json(&self) -> Json {
        let schemas = self
            .schemas
            .iter()
            .map(|(hash, schema)| PublishedSchema {
                hash: hash.to_string(),
                schema: schema.clone(),
            })
            .collect::<Vec<_>>();
        Json::Object(vec![
            ("current".into(), Json::string(self.current.to_string())),
            ("schemas".into(), serde(&schemas)),
            ("migrations".into(), serde(&self.migrations)),
        ])
    }

    /// The current schema.
    pub fn current_schema(&self) -> &Schema {
        &self
            .schemas
            .iter()
            .find(|(hash, _)| *hash == self.current)
            .expect("the current schema is among the schemas")
            .1
    }
}

/// A serde value in the file's JSON form (keys in serde's order).
pub(crate) fn serde<T: serde::Serialize>(value: &T) -> Json {
    from_serde(&serde_json::to_value(value).expect("engine types serialize"))
}

pub(crate) fn from_serde(value: &serde_json::Value) -> Json {
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

impl App {
    /// Read and check `jazz.app.json` as `jazz validate` does.
    pub fn load(file: &[u8]) -> Result<App, ArtifactError> {
        let definition = artifact::load_app(file)?;
        let mut app = App::from_definition(artifact::source_hash(file), &definition);
        app.file = String::from_utf8_lossy(file).replace("\r\n", "\n");
        Ok(app)
    }

    fn from_definition(source: String, definition: &AppDefinition) -> App {
        let codegen = definition.codegen();
        let current = definition.current_schema();
        let tables = current
            .iter()
            .map(|(name, schema)| {
                let name = name.as_str();
                let info = codegen.tables.get(name);
                table(name, schema, info)
            })
            .collect();
        App {
            source,
            file: String::new(),
            tables,
            transforms: codegen
                .transforms
                .iter()
                .map(|(name, decl)| Transform {
                    name: name.clone(),
                    stored: decl.stored.clone(),
                    view: decl.view.clone(),
                })
                .collect(),
            compiled: Compiled {
                current: definition.current_hash(),
                schemas: definition.schemas().to_vec(),
                migrations: definition.migrations().to_vec(),
            },
            warnings: definition.warnings().to_vec(),
            stranded: definition.stranded().to_vec(),
        }
    }

    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|t| t.name == name)
    }

    pub fn transform(&self, name: &str) -> Option<&Transform> {
        self.transforms.iter().find(|t| t.name == name)
    }
}

fn table(name: &str, schema: &TableSchema, info: Option<&artifact::CodegenTable>) -> Table {
    let columns: Vec<Column> = schema
        .columns
        .columns
        .iter()
        .map(|c| Column {
            name: c.name_str().to_string(),
            ty: FieldType {
                ty: c.column_type.clone(),
                nullable: c.nullable,
            },
            default: c.default.clone(),
            merge: c.merge_strategy,
            references: c.references.map(|t| t.as_str().to_string()),
            transform: info.and_then(|i| i.transforms.get(c.name_str()).cloned()),
        })
        .collect();
    let relations = info
        .map(|i| {
            i.relations
                .iter()
                .map(|(rel, target)| match target {
                    CodegenRelation::Forward { table, column } => Relation {
                        name: rel.clone(),
                        target: table.clone(),
                        kind: RelationKind::One {
                            column: column.clone(),
                            nullable: columns
                                .iter()
                                .find(|c| &c.name == column)
                                .is_some_and(|c| c.ty.nullable),
                        },
                    },
                    CodegenRelation::Reverse { table, relation } => Relation {
                        name: rel.clone(),
                        target: table.clone(),
                        kind: RelationKind::Many {
                            relation: relation.clone(),
                        },
                    },
                })
                .collect()
        })
        .unwrap_or_default();
    let computed = info
        .map(|i| {
            i.computed
                .iter()
                .map(|f| Computed {
                    name: f.name.clone(),
                    ty: FieldType {
                        ty: f.field_type.column_type.clone(),
                        nullable: f.field_type.nullable,
                    },
                    from: f.from.clone(),
                })
                .collect()
        })
        .unwrap_or_default();
    Table {
        name: name.to_string(),
        columns,
        relations,
        computed,
    }
}

impl Table {
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }
}
