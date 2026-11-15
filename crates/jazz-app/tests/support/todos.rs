// Bindings for tests/fixtures/jazz.app.json, written as the generator
// writes them.
#![allow(dead_code)]

use jazz_app::{ColumnValue, Fields, ObjectId, Result};

#[derive(Debug, Clone, PartialEq)]
pub struct Project {
    pub id: ObjectId,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct NewProject {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ProjectUpdate {
    pub name: Option<String>,
}

impl jazz_app::Table for Project {
    const NAME: &'static str = "projects";
    type New = NewProject;
    type Update = ProjectUpdate;
    fn decode(row: &mut Fields<'_>) -> Result<Self> {
        Ok(Self {
            id: row.id(),
            name: row.take("name")?,
        })
    }
    fn id(&self) -> ObjectId {
        self.id
    }
}

impl jazz_app::Insert for NewProject {
    type Row = Project;
    fn into_cells(
        self,
        _: &jazz_app::Implementations,
    ) -> Result<std::collections::HashMap<String, jazz_app::jazz::tools::Value>> {
        Ok([("name".to_owned(), self.name.into_value())].into())
    }
}

impl jazz_app::Update for ProjectUpdate {
    type Row = Project;
    fn into_cells(
        self,
        _: &jazz_app::Implementations,
    ) -> Result<Vec<(String, jazz_app::jazz::tools::Value)>> {
        let mut cells = Vec::new();
        if let Some(value) = self.name {
            cells.push(("name".to_owned(), value.into_value()));
        }
        Ok(cells)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Todo {
    pub id: ObjectId,
    pub title: String,
    pub done: bool,
    pub description: Option<String>,
    pub parent: Option<ObjectId>,
    pub project: Option<ObjectId>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct NewTodo {
    pub title: String,
    pub done: bool,
    pub description: Option<String>,
    pub parent: Option<ObjectId>,
    pub project: Option<ObjectId>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct TodoUpdate {
    pub title: Option<String>,
    pub done: Option<bool>,
    pub description: Option<Option<String>>,
    pub parent: Option<Option<ObjectId>>,
    pub project: Option<Option<ObjectId>>,
}

impl jazz_app::Table for Todo {
    const NAME: &'static str = "todos";
    type New = NewTodo;
    type Update = TodoUpdate;
    fn decode(row: &mut Fields<'_>) -> Result<Self> {
        Ok(Self {
            id: row.id(),
            title: row.take("title")?,
            done: row.take("done")?,
            description: row.take("description")?,
            parent: row.take("parent")?,
            project: row.take("project")?,
        })
    }
    fn id(&self) -> ObjectId {
        self.id
    }
}

impl jazz_app::Insert for NewTodo {
    type Row = Todo;
    fn into_cells(
        self,
        _: &jazz_app::Implementations,
    ) -> Result<std::collections::HashMap<String, jazz_app::jazz::tools::Value>> {
        Ok([
            ("title".to_owned(), self.title.into_value()),
            ("done".to_owned(), self.done.into_value()),
            ("description".to_owned(), self.description.into_value()),
            ("parent".to_owned(), self.parent.into_value()),
            ("project".to_owned(), self.project.into_value()),
        ]
        .into())
    }
}

impl jazz_app::Update for TodoUpdate {
    type Row = Todo;
    fn into_cells(
        self,
        _: &jazz_app::Implementations,
    ) -> Result<Vec<(String, jazz_app::jazz::tools::Value)>> {
        let mut cells = Vec::new();
        if let Some(value) = self.title {
            cells.push(("title".to_owned(), value.into_value()));
        }
        if let Some(value) = self.done {
            cells.push(("done".to_owned(), value.into_value()));
        }
        if let Some(value) = self.description {
            cells.push(("description".to_owned(), value.into_value()));
        }
        if let Some(value) = self.parent {
            cells.push(("parent".to_owned(), value.into_value()));
        }
        if let Some(value) = self.project {
            cells.push(("project".to_owned(), value.into_value()));
        }
        Ok(cells)
    }
}

pub mod todos {
    use super::*;
    use jazz_app::{Column, Many, One, Query};

    pub fn query() -> Query<Todo> {
        Query::all()
    }
    pub fn title() -> Column<Todo, String> {
        Column::new("title")
    }
    pub fn done() -> Column<Todo, bool> {
        Column::new("done")
    }
    pub fn description() -> Column<Todo, Option<String>> {
        Column::new("description")
    }
    pub fn parent_relation() -> One<Todo, Todo> {
        One::new("parent")
    }
    pub fn project_relation() -> One<Todo, Project> {
        One::new("project")
    }
    pub fn todos_via_parent() -> Many<Todo, Todo> {
        Many::new("parent")
    }
}

pub mod projects {
    use super::*;
    use jazz_app::{Column, Many, Query};

    pub fn query() -> Query<Project> {
        Query::all()
    }
    pub fn name() -> Column<Project, String> {
        Column::new("name")
    }
    pub fn todos_via_project() -> Many<Project, Todo> {
        Many::new("project")
    }
}
