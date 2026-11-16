// jazz_app.rs: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 4. Regenerate it after changing jazz.app.json.
#![cfg_attr(rustfmt, rustfmt_skip)]
#![allow(dead_code, unused_variables, clippy::all)]

/// The `jazz.app.json` these bindings were generated from.
pub const SOURCE: &str = "sha256:253d694a8edcc4f39371934b17c154b41e4a6b1f9a33c82c60273a5b9b0ad697";

/// A row of `projects`.
#[derive(Debug, Clone, PartialEq)]
pub struct Project {
    pub id: ::jazz_app::ObjectId,
    pub name: ::std::string::String,
}

/// A new row of `projects`: [`NewProject::new`] takes the columns it requires.
#[derive(Debug, Clone, PartialEq)]
pub struct NewProject {
    pub name: ::std::string::String,
}

impl NewProject {
    /// A new row with the columns it requires, in this order: `name`. The others
    /// are left out; set them with `NewProject { column: …, ..NewProject::new(…) }`.
    pub fn new(name: impl ::std::convert::Into<::std::string::String>) -> Self {
        Self {
            name: name.into(),
        }
    }
}

/// Changes to a row of `projects`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ProjectUpdate {
    pub name: ::std::option::Option<::std::string::String>,
}

impl ::jazz_app::Table for Project {
    const NAME: &'static str = "projects";
    const COLUMNS: &'static [&'static str] = &["name"];
    type New = NewProject;
    type Update = ProjectUpdate;

    fn decode(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self> {
        let name = row.take::<::std::string::String>("name")?;
        ::std::result::Result::Ok(Self {
            id: row.id(),
            name,
        })
    }

    fn id(&self) -> ::jazz_app::ObjectId {
        self.id
    }
}

impl ::jazz_app::Insert for NewProject {
    type Row = Project;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::collections::HashMap<::std::string::String, ::jazz_app::jazz::tools::Value>> {
        let mut cells = ::std::collections::HashMap::new();
        let value = self.name;
        cells.insert("name".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        ::std::result::Result::Ok(cells)
    }
}

impl ::jazz_app::Update for ProjectUpdate {
    type Row = Project;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::vec::Vec<(::std::string::String, ::jazz_app::jazz::tools::Value)>> {
        let mut cells = ::std::vec::Vec::new();
        if let ::std::option::Option::Some(value) = self.name {
            cells.push(("name".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        ::std::result::Result::Ok(cells)
    }
}

/// Queries, columns and relations of `projects`.
pub mod projects {
    #[allow(unused_imports)]
    use super::*;

    /// Every row of `projects`; narrow it with `filter`.
    pub fn query() -> ::jazz_app::Query<Project> {
        ::jazz_app::Query::all()
    }

    /// Every accepted version of `projects`'s rows; bound it to rows with `id` or `id().is_in`.
    pub fn history() -> ::jazz_app::Query<::jazz_app::Version<Project>> {
        ::jazz_app::Query::history()
    }

    /// The row's id. (`::jazz_app::created_at()` and `updated_at()` are columns of every table too.)
    pub fn id() -> ::jazz_app::Column<Project, ::jazz_app::ObjectId> {
        ::jazz_app::id()
    }

    /// Column `name`.
    pub fn name() -> ::jazz_app::Column<Project, ::std::string::String> {
        ::jazz_app::Column::new("name")
    }

    /// Relation `todos`: the `todos` rows whose `project` references this row.
    pub fn todos() -> ::jazz_app::Many<Project, Todo> {
        ::jazz_app::Many::new("project")
    }
}

/// A row of `todos`.
#[derive(Debug, Clone, PartialEq)]
pub struct Todo {
    pub id: ::jazz_app::ObjectId,
    pub title: ::std::string::String,
    pub done: bool,
    pub priority: ::std::option::Option<i32>,
    pub description: ::std::option::Option<::std::string::String>,
    pub parent: ::std::option::Option<::jazz_app::ObjectId>,
    pub project: ::std::option::Option<::jazz_app::ObjectId>,
}

/// A new row of `todos`: [`NewTodo::new`] takes the columns it requires.
#[derive(Debug, Clone, PartialEq)]
pub struct NewTodo {
    pub title: ::std::string::String,
    pub done: bool,
    pub priority: ::std::option::Option<i32>,
    pub description: ::std::option::Option<::std::string::String>,
    pub parent: ::std::option::Option<::jazz_app::ObjectId>,
    pub project: ::std::option::Option<::jazz_app::ObjectId>,
}

impl NewTodo {
    /// A new row with the columns it requires, in this order: `title`, `done`. The others
    /// are left out; set them with `NewTodo { column: …, ..NewTodo::new(…) }`.
    pub fn new(title: impl ::std::convert::Into<::std::string::String>, done: impl ::std::convert::Into<bool>) -> Self {
        Self {
            title: title.into(),
            done: done.into(),
            priority: ::std::option::Option::None,
            description: ::std::option::Option::None,
            parent: ::std::option::Option::None,
            project: ::std::option::Option::None,
        }
    }
}

/// Changes to a row of `todos`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TodoUpdate {
    pub title: ::std::option::Option<::std::string::String>,
    pub done: ::std::option::Option<bool>,
    pub priority: ::std::option::Option<::std::option::Option<i32>>,
    pub description: ::std::option::Option<::std::option::Option<::std::string::String>>,
    pub parent: ::std::option::Option<::std::option::Option<::jazz_app::ObjectId>>,
    pub project: ::std::option::Option<::std::option::Option<::jazz_app::ObjectId>>,
}

impl ::jazz_app::Table for Todo {
    const NAME: &'static str = "todos";
    const COLUMNS: &'static [&'static str] = &["title", "done", "priority", "description", "parent", "project"];
    type New = NewTodo;
    type Update = TodoUpdate;

    fn decode(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self> {
        let title = row.take::<::std::string::String>("title")?;
        let done = row.take::<bool>("done")?;
        let priority = row.take::<::std::option::Option<i32>>("priority")?;
        let description = row.take::<::std::option::Option<::std::string::String>>("description")?;
        let parent = row.take::<::std::option::Option<::jazz_app::ObjectId>>("parent")?;
        let project = row.take::<::std::option::Option<::jazz_app::ObjectId>>("project")?;
        ::std::result::Result::Ok(Self {
            id: row.id(),
            title,
            done,
            priority,
            description,
            parent,
            project,
        })
    }

    fn id(&self) -> ::jazz_app::ObjectId {
        self.id
    }
}

impl ::jazz_app::Insert for NewTodo {
    type Row = Todo;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::collections::HashMap<::std::string::String, ::jazz_app::jazz::tools::Value>> {
        let mut cells = ::std::collections::HashMap::new();
        let value = self.title;
        cells.insert("title".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.done;
        cells.insert("done".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.priority;
        cells.insert("priority".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.description;
        cells.insert("description".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.parent;
        cells.insert("parent".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.project;
        cells.insert("project".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        ::std::result::Result::Ok(cells)
    }
}

impl ::jazz_app::Update for TodoUpdate {
    type Row = Todo;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::vec::Vec<(::std::string::String, ::jazz_app::jazz::tools::Value)>> {
        let mut cells = ::std::vec::Vec::new();
        if let ::std::option::Option::Some(value) = self.title {
            cells.push(("title".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.done {
            cells.push(("done".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.priority {
            cells.push(("priority".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.description {
            cells.push(("description".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.parent {
            cells.push(("parent".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.project {
            cells.push(("project".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        ::std::result::Result::Ok(cells)
    }
}

/// Queries, columns and relations of `todos`.
pub mod todos {
    #[allow(unused_imports)]
    use super::*;

    /// Every row of `todos`; narrow it with `filter`.
    pub fn query() -> ::jazz_app::Query<Todo> {
        ::jazz_app::Query::all()
    }

    /// Every accepted version of `todos`'s rows; bound it to rows with `id` or `id().is_in`.
    pub fn history() -> ::jazz_app::Query<::jazz_app::Version<Todo>> {
        ::jazz_app::Query::history()
    }

    /// The row's id. (`::jazz_app::created_at()` and `updated_at()` are columns of every table too.)
    pub fn id() -> ::jazz_app::Column<Todo, ::jazz_app::ObjectId> {
        ::jazz_app::id()
    }

    /// Column `title`.
    pub fn title() -> ::jazz_app::Column<Todo, ::std::string::String> {
        ::jazz_app::Column::new("title")
    }

    /// Column `done`.
    pub fn done() -> ::jazz_app::Column<Todo, bool> {
        ::jazz_app::Column::new("done")
    }

    /// Column `priority`.
    pub fn priority() -> ::jazz_app::Column<Todo, ::std::option::Option<i32>> {
        ::jazz_app::Column::new("priority")
    }

    /// Column `description`.
    pub fn description() -> ::jazz_app::Column<Todo, ::std::option::Option<::std::string::String>> {
        ::jazz_app::Column::new("description")
    }

    /// Column `parent`.
    pub fn parent() -> ::jazz_app::Column<Todo, ::std::option::Option<::jazz_app::ObjectId>> {
        ::jazz_app::Column::new("parent")
    }

    /// Column `project`.
    pub fn project() -> ::jazz_app::Column<Todo, ::std::option::Option<::jazz_app::ObjectId>> {
        ::jazz_app::Column::new("project")
    }

    /// Relation `children`: the `todos` rows whose `parent` references this row.
    pub fn children() -> ::jazz_app::Many<Todo, Todo> {
        ::jazz_app::Many::new("parent")
    }

    /// Relation `parentRelation`: the `todos` row `parent` references.
    pub fn parent_relation() -> ::jazz_app::One<Todo, Todo, ::jazz_app::Nullable> {
        ::jazz_app::One::new("parent")
    }

    /// Relation `projectRelation`: the `projects` row `project` references.
    pub fn project_relation() -> ::jazz_app::One<Todo, Project, ::jazz_app::Nullable> {
        ::jazz_app::One::new("project")
    }
}

/// The app's `jazz.app.json`: its schema, every earlier version and the
/// migrations between them.
pub const APP: &str = r#"{
  "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
  "tables": {
    "projects": {
      "columns": { "name": "string" },
      "relations": { "todos": { "reverse": "todos", "relation": "projectRelation" } }
    },
    "todos": {
      "columns": {
        "title": "string",
        "done": "boolean",
        "priority": "int?",
        "description": "string?",
        "parent": "uuid?",
        "project": "uuid?"
      },
      "relations": {
        "children": { "reverse": "todos", "relation": "parentRelation" },
        "parentRelation": { "rel": "todos", "column": "parent" },
        "projectRelation": { "rel": "projects", "column": "project" }
      }
    }
  }
}
"#;

/// Opens the app. A backend (`Config::backend`, with its
/// `jazz.permissions.json`) first publishes the app's permissions bundle
/// and pins the session it acts in to it.
pub async fn open(config: ::jazz_app::Config) -> ::jazz_app::Result<::jazz_app::Jazz> {
    ::jazz_app::Jazz::open(::jazz_app::AppFiles::new(APP, ::std::option::Option::None), config).await
}
