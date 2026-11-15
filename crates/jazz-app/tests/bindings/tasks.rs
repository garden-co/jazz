// jazz_app.rs: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 2. Regenerate it after changing jazz.app.json.
#![cfg_attr(rustfmt, rustfmt_skip)]
#![allow(dead_code, clippy::all)]

/// The `jazz.app.json` these bindings were generated from.
pub const SOURCE: &str = "sha256:164e71b0383041ee172ed3ba4ed9beb7590ad8fd4737815ece9552c827f9f581";

/// The values of column `tasks.priority`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskPriority {
    /// `low`.
    Low,
    /// `high`.
    High,
    /// `in-progress`.
    InProgress,
}

::jazz_app::text_enum!(TaskPriority {
    Low => "low",
    High => "high",
    InProgress => "in-progress",
});

/// A row of `boxes`.
#[derive(Debug, Clone, PartialEq)]
pub struct Box {
    pub id: ::jazz_app::ObjectId,
    pub label: ::std::string::String,
    pub query: ::std::option::Option<::std::string::String>,
    pub task: ::std::option::Option<::jazz_app::ObjectId>,
}

/// A new row of `boxes`: [`NewBox::new`] takes the columns it requires.
#[derive(Debug, Clone, PartialEq)]
pub struct NewBox {
    pub label: ::std::string::String,
    pub query: ::std::option::Option<::std::string::String>,
    pub task: ::std::option::Option<::jazz_app::ObjectId>,
}

impl NewBox {
    /// A new row with the columns it requires, in this order: `label`. The others
    /// are left out; set them with `NewBox { column: …, ..NewBox::new(…) }`.
    pub fn new(label: impl ::std::convert::Into<::std::string::String>) -> Self {
        Self {
            label: label.into(),
            query: ::std::option::Option::None,
            task: ::std::option::Option::None,
        }
    }
}

/// Changes to a row of `boxes`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BoxUpdate {
    pub label: ::std::option::Option<::std::string::String>,
    pub query: ::std::option::Option<::std::option::Option<::std::string::String>>,
    pub task: ::std::option::Option<::std::option::Option<::jazz_app::ObjectId>>,
}

impl ::jazz_app::Table for Box {
    const NAME: &'static str = "boxes";
    type New = NewBox;
    type Update = BoxUpdate;

    fn decode(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self> {
        let label = row.take::<::std::string::String>("label")?;
        let query = row.take::<::std::option::Option<::std::string::String>>("query")?;
        let task = row.take::<::std::option::Option<::jazz_app::ObjectId>>("task")?;
        ::std::result::Result::Ok(Self {
            id: row.id(),
            label,
            query,
            task,
        })
    }

    fn id(&self) -> ::jazz_app::ObjectId {
        self.id
    }
}

impl ::jazz_app::Insert for NewBox {
    type Row = Box;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::collections::HashMap<::std::string::String, ::jazz_app::jazz::tools::Value>> {
        let mut cells = ::std::collections::HashMap::new();
        let value = self.label;
        cells.insert("label".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.query;
        cells.insert("query".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.task;
        cells.insert("task".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        ::std::result::Result::Ok(cells)
    }
}

impl ::jazz_app::Update for BoxUpdate {
    type Row = Box;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::vec::Vec<(::std::string::String, ::jazz_app::jazz::tools::Value)>> {
        let mut cells = ::std::vec::Vec::new();
        if let ::std::option::Option::Some(value) = self.label {
            cells.push(("label".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.query {
            cells.push(("query".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.task {
            cells.push(("task".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        ::std::result::Result::Ok(cells)
    }
}

/// Queries, columns and relations of `boxes`.
pub mod boxes {
    #[allow(unused_imports)]
    use super::*;

    /// Every row of `boxes`; narrow it with `filter`.
    pub fn query() -> ::jazz_app::Query<Box> {
        ::jazz_app::Query::all()
    }

    /// The row's id. (`::jazz_app::created_at()` and `updated_at()` are columns of every table too.)
    pub fn id() -> ::jazz_app::Column<Box, ::jazz_app::ObjectId> {
        ::jazz_app::id()
    }

    /// Column `label`.
    pub fn label() -> ::jazz_app::Column<Box, ::std::string::String> {
        ::jazz_app::Column::new("label")
    }

    /// Column `query`.
    pub fn query_column() -> ::jazz_app::Column<Box, ::std::option::Option<::std::string::String>> {
        ::jazz_app::Column::new("query")
    }

    /// Column `task`.
    pub fn task() -> ::jazz_app::Column<Box, ::std::option::Option<::jazz_app::ObjectId>> {
        ::jazz_app::Column::new("task")
    }
}

/// A row of `tasks`.
#[derive(Debug, Clone, PartialEq)]
pub struct Task {
    pub id: ::jazz_app::ObjectId,
    pub title: ::std::string::String,
    pub priority: TaskPriority,
    pub score: i32,
    pub r#type: ::std::option::Option<::std::string::String>,
    /// `dueAt`.
    pub due_at: ::std::option::Option<::std::string::String>,
    pub tags: ::std::vec::Vec<::std::string::String>,
    pub meta: ::std::option::Option<::jazz_app::Json>,
    pub notes: ::std::option::Option<::std::string::String>,
    /// Computed from `score`, `priority`.
    pub urgent: bool,
}

/// A new row of `tasks`: [`NewTask::new`] takes the columns it requires.
#[derive(Debug, Clone, PartialEq)]
pub struct NewTask {
    pub title: ::std::string::String,
    /// `None` stores the column's default.
    pub priority: ::std::option::Option<TaskPriority>,
    pub score: i32,
    pub r#type: ::std::option::Option<::std::string::String>,
    /// `dueAt`.
    pub due_at: ::std::option::Option<::std::string::String>,
    pub tags: ::std::vec::Vec<::std::string::String>,
    pub meta: ::std::option::Option<::jazz_app::Json>,
    pub notes: ::std::option::Option<::std::string::String>,
}

impl NewTask {
    /// A new row with the columns it requires, in this order: `title`, `score`, `tags`. The others
    /// are left out; set them with `NewTask { column: …, ..NewTask::new(…) }`.
    pub fn new(title: impl ::std::convert::Into<::std::string::String>, score: impl ::std::convert::Into<i32>, tags: impl ::std::convert::Into<::std::vec::Vec<::std::string::String>>) -> Self {
        Self {
            title: title.into(),
            priority: ::std::option::Option::None,
            score: score.into(),
            r#type: ::std::option::Option::None,
            due_at: ::std::option::Option::None,
            tags: tags.into(),
            meta: ::std::option::Option::None,
            notes: ::std::option::Option::None,
        }
    }
}

/// Changes to a row of `tasks`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TaskUpdate {
    pub title: ::std::option::Option<::std::string::String>,
    pub priority: ::std::option::Option<TaskPriority>,
    pub score: ::std::option::Option<i32>,
    pub r#type: ::std::option::Option<::std::option::Option<::std::string::String>>,
    /// `dueAt`.
    pub due_at: ::std::option::Option<::std::option::Option<::std::string::String>>,
    pub tags: ::std::option::Option<::std::vec::Vec<::std::string::String>>,
    pub meta: ::std::option::Option<::std::option::Option<::jazz_app::Json>>,
    pub notes: ::std::option::Option<::std::option::Option<::std::string::String>>,
}

impl ::jazz_app::Table for Task {
    const NAME: &'static str = "tasks";
    type New = NewTask;
    type Update = TaskUpdate;

    fn decode(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self> {
        let computed = &row.implementations().get::<Implementations>()?.0;
        let title = row.take::<::std::string::String>("title")?;
        let priority = row.take::<TaskPriority>("priority")?;
        let score = row.take::<i32>("score")?;
        let r#type = row.take::<::std::option::Option<::std::string::String>>("type")?;
        let due_at = row.take::<::std::option::Option<::jazz_app::Timestamp>>("dueAt")?.map(|stored| computed.iso_date_view(stored));
        let tags = row.take::<::std::vec::Vec<::std::string::String>>("tags")?;
        let meta = row.take::<::std::option::Option<::jazz_app::Json>>("meta")?;
        let notes = row.take::<::std::option::Option<::std::string::String>>("notes")?;
        let urgent = computed.tasks_urgent(score.clone(), priority.clone());
        ::std::result::Result::Ok(Self {
            id: row.id(),
            title,
            priority,
            score,
            r#type,
            due_at,
            tags,
            meta,
            notes,
            urgent,
        })
    }

    fn id(&self) -> ::jazz_app::ObjectId {
        self.id
    }
}

impl ::jazz_app::Insert for NewTask {
    type Row = Task;

    fn into_cells(
        self,
        implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::collections::HashMap<::std::string::String, ::jazz_app::jazz::tools::Value>> {
        let computed = &implementations.get::<Implementations>()?.0;
        let mut cells = ::std::collections::HashMap::new();
        let value = self.title;
        cells.insert("title".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        if let ::std::option::Option::Some(value) = self.priority {
            cells.insert("priority".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        }
        let value = self.score;
        cells.insert("score".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.r#type;
        cells.insert("type".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.due_at;
        cells.insert("dueAt".to_owned(), ::jazz_app::ColumnValue::into_value(value.map(|view| computed.iso_date_stored(view))));
        let value = self.tags;
        cells.insert("tags".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.meta;
        cells.insert("meta".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.notes;
        cells.insert("notes".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        ::std::result::Result::Ok(cells)
    }
}

impl ::jazz_app::Update for TaskUpdate {
    type Row = Task;

    fn into_cells(
        self,
        implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::vec::Vec<(::std::string::String, ::jazz_app::jazz::tools::Value)>> {
        let computed = &implementations.get::<Implementations>()?.0;
        let mut cells = ::std::vec::Vec::new();
        if let ::std::option::Option::Some(value) = self.title {
            cells.push(("title".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.priority {
            cells.push(("priority".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.score {
            cells.push(("score".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.r#type {
            cells.push(("type".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.due_at {
            cells.push(("dueAt".to_owned(), ::jazz_app::ColumnValue::into_value(value.map(|view| computed.iso_date_stored(view)))));
        }
        if let ::std::option::Option::Some(value) = self.tags {
            cells.push(("tags".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.meta {
            cells.push(("meta".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.notes {
            cells.push(("notes".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        ::std::result::Result::Ok(cells)
    }
}

/// Queries, columns and relations of `tasks`.
pub mod tasks {
    #[allow(unused_imports)]
    use super::*;

    /// Every row of `tasks`; narrow it with `filter`.
    pub fn query() -> ::jazz_app::Query<Task> {
        ::jazz_app::Query::all()
    }

    /// The row's id. (`::jazz_app::created_at()` and `updated_at()` are columns of every table too.)
    pub fn id() -> ::jazz_app::Column<Task, ::jazz_app::ObjectId> {
        ::jazz_app::id()
    }

    /// Column `title`.
    pub fn title() -> ::jazz_app::Column<Task, ::std::string::String> {
        ::jazz_app::Column::new("title")
    }

    /// Column `priority`.
    pub fn priority() -> ::jazz_app::Column<Task, TaskPriority> {
        ::jazz_app::Column::new("priority")
    }

    /// Column `score`.
    pub fn score() -> ::jazz_app::Column<Task, i32> {
        ::jazz_app::Column::new("score")
    }

    /// Column `type`.
    pub fn r#type() -> ::jazz_app::Column<Task, ::std::option::Option<::std::string::String>> {
        ::jazz_app::Column::new("type")
    }

    /// Column `dueAt`. Compared as stored, before its transform.
    pub fn due_at() -> ::jazz_app::Column<Task, ::std::option::Option<::jazz_app::Timestamp>> {
        ::jazz_app::Column::new("dueAt")
    }

    /// Column `tags`.
    pub fn tags() -> ::jazz_app::Column<Task, ::std::vec::Vec<::std::string::String>> {
        ::jazz_app::Column::new("tags")
    }

    /// Column `meta`.
    pub fn meta() -> ::jazz_app::Column<Task, ::std::option::Option<::jazz_app::Json>> {
        ::jazz_app::Column::new("meta")
    }

    /// Column `notes`.
    pub fn notes() -> ::jazz_app::Column<Task, ::std::option::Option<::std::string::String>> {
        ::jazz_app::Column::new("notes")
    }
}

/// What the app's code computes: each computed field `jazz.app.json`
/// declares, and each transform both ways. [`open`] takes an implementation.
pub trait Computed: ::std::marker::Send + ::std::marker::Sync + 'static {
    /// `tasks.urgent`.
    fn tasks_urgent(&self, score: i32, priority: TaskPriority) -> bool;

    /// Transform `isoDate`: a stored value, as the app reads it.
    fn iso_date_view(&self, stored: ::jazz_app::Timestamp) -> ::std::string::String;

    /// Transform `isoDate`: a value the app writes, as stored.
    fn iso_date_stored(&self, view: ::std::string::String) -> ::jazz_app::Timestamp;
}

struct Implementations(::std::boxed::Box<dyn Computed>);

/// The app's `jazz.app.json`: its schema, every earlier version and the
/// migrations between them.
pub const APP: &str = r#"{
  "$schema": "https://jazz.tools/schemas/jazz.app.schema.json",
  "format": 1,
  "tables": {
    "boxes": { "columns": { "label": "string", "query": "string?", "task": { "type": "uuid", "optional": true, "references": "tasks" } } },
    "tasks": {
      "columns": {
        "title": "string",
        "priority": { "enum": ["low", "high", "in-progress"], "default": "low" },
        "score": "int",
        "type": "string?",
        "dueAt": { "type": "timestamp", "optional": true, "transform": "isoDate" },
        "tags": { "array": "string" },
        "meta": { "json": true, "optional": true },
        "notes": { "type": "string", "optional": true, "merge": "text" }
      },
      "computed": { "urgent": { "type": "boolean", "from": ["score", "priority"] } }
    }
  },
  "transforms": { "isoDate": { "stored": "timestamp", "view": "string" } }
}
"#;

/// Opens the app. A backend (`Config::backend`, with its
/// `jazz.permissions.json`) first publishes the app's permissions bundle
/// and pins the session it acts in to it.
pub async fn open(config: ::jazz_app::Config, computed: impl Computed) -> ::jazz_app::Result<::jazz_app::Jazz> {
    ::jazz_app::Jazz::open_with(
        ::jazz_app::AppFiles::new(APP, ::std::option::Option::None),
        config,
        ::jazz_app::Implementations::new(Implementations(::std::boxed::Box::new(computed))),
    )
    .await
}
