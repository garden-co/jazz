use mini_jazz_sqlite::SchemaDef;

pub fn tasks_schema() -> SchemaDef {
    SchemaDef::new().table("tasks", |table| {
        table.text("title");
        table.bool("done");
    })
}

pub fn notes_schema() -> SchemaDef {
    SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
    })
}

pub fn folders_schema() -> SchemaDef {
    SchemaDef::new().table("folders", |table| {
        table.text("name");
        table.ref_("parent", "folders");
        table.read_if_created_by_principal();
    })
}
