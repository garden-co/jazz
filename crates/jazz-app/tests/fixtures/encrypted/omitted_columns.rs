use jazz_app_support::jazz::tools::deployment::artifact::{self, ArtifactInput, WrittenApp};
use jazz_app_support::jazz::tools::public_schema::PlainType;
use jazz_app_support::jazz::tools::{ColumnDescriptor, ColumnType, SchemaHash, TableName, Value};

/// Add a nullable encrypted column and give the encrypted title a default
/// in its declaration. Add a plain column whose name resembles a token
/// for the unindexed encrypted column.
pub fn app() -> WrittenApp {
    let loaded = artifact::load(
        include_bytes!("jazz.app.json"),
        include_bytes!("jazz.permissions.json"),
    )
    .unwrap();
    let mut schema = loaded.current_schema().clone();
    let tasks = schema.get_mut(&TableName::new("tasks")).unwrap();
    tasks.columns.columns.extend([
        ColumnDescriptor::new("privateNote", ColumnType::Bytea),
        ColumnDescriptor::new("__e2ee_eq_privateNote", ColumnType::Text)
            .default(Value::Text("plain note".into())),
    ]);

    let current = SchemaHash::compute(&schema).to_string();
    let mut encryption = loaded.encryption().clone();
    let mut entries = encryption
        .versions
        .remove(&loaded.current_hash().to_string())
        .unwrap();
    let encrypted = &mut entries.get_mut("tasks").unwrap().columns;
    encrypted.get_mut("title").unwrap().plain.default = Some(Value::Text("untitled".into()));
    let mut private_note = encrypted["title"].clone();
    private_note.plain = PlainType::new(ColumnType::Text, true);
    encrypted.insert("privateNote".into(), private_note);
    encryption.versions.insert(current.clone(), entries);
    let mut codegen = loaded.codegen().clone();
    codegen.encryption = Some(encryption);
    artifact::write(ArtifactInput {
        current,
        schemas: vec![schema],
        migrations: Vec::new(),
        permissions: loaded.permissions().clone(),
        codegen,
    })
    .unwrap()
}
