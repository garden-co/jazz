use mini_jazz_sqlite::{Result, Runtime, SchemaDef};
use serde_json::json;
use std::collections::BTreeMap;

pub trait FixtureRuntimeExt {
    fn create_project(&mut self, id: &str, title: &str) -> Result<String>;
    fn create_todo(
        &mut self,
        id: &str,
        title: &str,
        done: bool,
        project_id: &str,
    ) -> Result<String>;
    fn delete_todo(&mut self, id: &str) -> Result<String>;
}

impl FixtureRuntimeExt for Runtime {
    fn create_project(&mut self, id: &str, title: &str) -> Result<String> {
        self.insert_row(
            "projects",
            id,
            BTreeMap::from([("title".to_owned(), json!(title))]),
        )
    }

    fn create_todo(
        &mut self,
        id: &str,
        title: &str,
        done: bool,
        project_id: &str,
    ) -> Result<String> {
        self.insert_row(
            "todos",
            id,
            BTreeMap::from([
                ("title".to_owned(), json!(title)),
                ("done".to_owned(), json!(done)),
                ("project".to_owned(), json!(project_id)),
            ]),
        )
    }

    fn delete_todo(&mut self, id: &str) -> Result<String> {
        self.delete_row("todos", id)
    }
}

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
