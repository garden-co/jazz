use mini_jazz_sqlite::SchemaDef;

pub fn todo_schema() -> SchemaDef {
    SchemaDef::new()
        .table("projects", |table| {
            table.text("title");
        })
        .table("todos", |table| {
            table.text("title");
            table.bool("done");
            table.ref_("project", "projects");
            table.index("open_created", ["done", "$createdAt"]);
            table.index("created", ["$createdAt"]);
            table.index("by_title", ["title"]);
        })
        .table("labels", |table| {
            table.text("name");
            table.index("by_name", ["name"]);
        })
        .table("todo_labels", |table| {
            table.ref_("todo", "todos");
            table.ref_("label", "labels");
            table.index("by_todo", ["todo"]);
            table.index("by_label", ["label"]);
        })
}
