// jazz_app.rs: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 4. Regenerate it after changing jazz.app.json.
#![cfg_attr(rustfmt, rustfmt_skip)]
#![allow(dead_code, unused_variables, clippy::all)]

/// The `jazz.app.json` these bindings were generated from.
pub const SOURCE: &str = "sha256:170dac566cb4dcf8c67f7b78562c0e409e2b2704444f9b1ae2d41a680a2a4682";

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
    const COLUMNS: &'static [&'static str] = &["label", "query", "task"];
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

    /// Every accepted version of `boxes`'s rows; bound it to rows with `id` or `id().is_in`.
    pub fn history() -> ::jazz_app::Query<::jazz_app::Version<Box>> {
        ::jazz_app::Query::history()
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

    /// Relation `taskRelation`: the `tasks` row `task` references.
    pub fn task_relation() -> ::jazz_app::One<Box, Task, ::jazz_app::Nullable> {
        ::jazz_app::One::new("task")
    }
}

/// A row of `steps`.
#[derive(Debug, Clone, PartialEq)]
pub struct Step {
    pub id: ::jazz_app::ObjectId,
    pub label: ::std::string::String,
    pub task: ::jazz_app::ObjectId,
    pub blockers: ::std::vec::Vec<::jazz_app::ObjectId>,
}

/// A new row of `steps`: [`NewStep::new`] takes the columns it requires.
#[derive(Debug, Clone, PartialEq)]
pub struct NewStep {
    pub label: ::std::string::String,
    pub task: ::jazz_app::ObjectId,
    pub blockers: ::std::vec::Vec<::jazz_app::ObjectId>,
}

impl NewStep {
    /// A new row with the columns it requires, in this order: `label`, `task`, `blockers`. The others
    /// are left out; set them with `NewStep { column: …, ..NewStep::new(…) }`.
    pub fn new(label: impl ::std::convert::Into<::std::string::String>, task: impl ::std::convert::Into<::jazz_app::ObjectId>, blockers: impl ::std::convert::Into<::std::vec::Vec<::jazz_app::ObjectId>>) -> Self {
        Self {
            label: label.into(),
            task: task.into(),
            blockers: blockers.into(),
        }
    }
}

/// Changes to a row of `steps`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct StepUpdate {
    pub label: ::std::option::Option<::std::string::String>,
    pub task: ::std::option::Option<::jazz_app::ObjectId>,
    pub blockers: ::std::option::Option<::std::vec::Vec<::jazz_app::ObjectId>>,
}

impl ::jazz_app::Table for Step {
    const NAME: &'static str = "steps";
    const COLUMNS: &'static [&'static str] = &["label", "task", "blockers"];
    type New = NewStep;
    type Update = StepUpdate;

    fn decode(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self> {
        let label = row.take::<::std::string::String>("label")?;
        let task = row.take::<::jazz_app::ObjectId>("task")?;
        let blockers = row.take::<::std::vec::Vec<::jazz_app::ObjectId>>("blockers")?;
        ::std::result::Result::Ok(Self {
            id: row.id(),
            label,
            task,
            blockers,
        })
    }

    fn id(&self) -> ::jazz_app::ObjectId {
        self.id
    }
}

impl ::jazz_app::Insert for NewStep {
    type Row = Step;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::collections::HashMap<::std::string::String, ::jazz_app::jazz::tools::Value>> {
        let mut cells = ::std::collections::HashMap::new();
        let value = self.label;
        cells.insert("label".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.task;
        cells.insert("task".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.blockers;
        cells.insert("blockers".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        ::std::result::Result::Ok(cells)
    }
}

impl ::jazz_app::Update for StepUpdate {
    type Row = Step;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::vec::Vec<(::std::string::String, ::jazz_app::jazz::tools::Value)>> {
        let mut cells = ::std::vec::Vec::new();
        if let ::std::option::Option::Some(value) = self.label {
            cells.push(("label".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.task {
            cells.push(("task".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.blockers {
            cells.push(("blockers".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        ::std::result::Result::Ok(cells)
    }
}

/// Queries, columns and relations of `steps`.
pub mod steps {
    #[allow(unused_imports)]
    use super::*;

    /// Every row of `steps`; narrow it with `filter`.
    pub fn query() -> ::jazz_app::Query<Step> {
        ::jazz_app::Query::all()
    }

    /// Every accepted version of `steps`'s rows; bound it to rows with `id` or `id().is_in`.
    pub fn history() -> ::jazz_app::Query<::jazz_app::Version<Step>> {
        ::jazz_app::Query::history()
    }

    /// The row's id. (`::jazz_app::created_at()` and `updated_at()` are columns of every table too.)
    pub fn id() -> ::jazz_app::Column<Step, ::jazz_app::ObjectId> {
        ::jazz_app::id()
    }

    /// Column `label`.
    pub fn label() -> ::jazz_app::Column<Step, ::std::string::String> {
        ::jazz_app::Column::new("label")
    }

    /// Column `task`.
    pub fn task() -> ::jazz_app::Column<Step, ::jazz_app::ObjectId> {
        ::jazz_app::Column::new("task")
    }

    /// Column `blockers`.
    pub fn blockers() -> ::jazz_app::Column<Step, ::std::vec::Vec<::jazz_app::ObjectId>> {
        ::jazz_app::Column::new("blockers")
    }

    /// Relation `blockerRows`: the `tasks` rows `blockers` references.
    pub fn blocker_rows() -> ::jazz_app::Refs<Step, Task> {
        ::jazz_app::Refs::new("blockers")
    }

    /// Relation `taskRelation`: the `tasks` row `task` references.
    pub fn task_relation() -> ::jazz_app::One<Step, Task> {
        ::jazz_app::One::new("task")
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
    pub attachment: ::std::option::Option<::std::vec::Vec<u8>>,
    /// Computed from `score`, `priority`.
    pub urgent: bool,
    /// Computed from `title`.
    pub title_len: i32,
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
    pub attachment: ::std::option::Option<::std::vec::Vec<u8>>,
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
            attachment: ::std::option::Option::None,
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
    pub attachment: ::std::option::Option<::std::option::Option<::std::vec::Vec<u8>>>,
}

impl ::jazz_app::Table for Task {
    const NAME: &'static str = "tasks";
    const COLUMNS: &'static [&'static str] = &["title", "priority", "score", "type", "dueAt", "tags", "meta", "notes", "attachment"];
    const COMPUTED_FROM: &'static [&'static str] = &["priority", "score", "title"];
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
        let attachment = row.take::<::std::option::Option<::std::vec::Vec<u8>>>("attachment")?;
        let urgent = computed.tasks_urgent(score.clone(), priority.clone());
        let title_len = computed.tasks_title_len(title.clone());
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
            attachment,
            urgent,
            title_len,
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
        let value = self.attachment;
        cells.insert("attachment".to_owned(), ::jazz_app::ColumnValue::into_value(value));
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
        if let ::std::option::Option::Some(value) = self.attachment {
            cells.push(("attachment".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
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

    /// Every accepted version of `tasks`'s rows; bound it to rows with `id` or `id().is_in`.
    pub fn history() -> ::jazz_app::Query<::jazz_app::Version<Task>> {
        ::jazz_app::Query::history()
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

    /// Column `dueAt`. Compared as stored, before its transform; selected as viewed, through it.
    pub fn due_at() -> ::jazz_app::Column<Task, ::std::option::Option<::jazz_app::Timestamp>, DueAtView> {
        ::jazz_app::Column::new("dueAt")
    }

    /// How `select` reads column `dueAt`: as viewed, through its transform.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct DueAtView;

    impl ::jazz_app::Pick for DueAtView {
        type Value = ::std::option::Option<::std::string::String>;

        fn pick(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self::Value> {
            let computed = &row.implementations().get::<Implementations>()?.0;
            ::std::result::Result::Ok(row.take_next::<::std::option::Option<::jazz_app::Timestamp>>()?.map(|stored| computed.iso_date_view(stored)))
        }
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

    /// Column `attachment`.
    pub fn attachment() -> ::jazz_app::Column<Task, ::std::option::Option<::std::vec::Vec<u8>>> {
        ::jazz_app::Column::new("attachment")
    }
}

/// What the app's code computes: each computed field `jazz.app.json`
/// declares, and each transform both ways. [`open`] takes an implementation.
pub trait Computed: ::std::marker::Send + ::std::marker::Sync + 'static {
    /// `tasks.urgent`.
    fn tasks_urgent(&self, score: i32, priority: TaskPriority) -> bool;

    /// `tasks.titleLen`.
    fn tasks_title_len(&self, title: ::std::string::String) -> i32;

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
    "boxes": {
      "columns": {
        "label": "string",
        "query": "string?",
        "task": { "type": "uuid", "optional": true, "references": "tasks" }
      },
      "relations": { "taskRelation": { "rel": "tasks", "column": "task" } }
    },
    "steps": {
      "columns": {
        "label": "string",
        "task": { "type": "uuid", "references": "tasks" },
        "blockers": { "array": "uuid" }
      },
      "relations": {
        "taskRelation": { "rel": "tasks", "column": "task" },
        "blockerRows": { "rel": "tasks", "column": "blockers" }
      }
    },
    "tasks": {
      "columns": {
        "title": "string",
        "priority": { "enum": ["low", "high", "in-progress"], "default": "low" },
        "score": "int",
        "type": "string?",
        "dueAt": { "type": "timestamp", "optional": true, "transform": "isoDate" },
        "tags": { "array": "string" },
        "meta": { "json": true, "optional": true },
        "notes": { "type": "string", "optional": true, "merge": "text" },
        "attachment": "bytes?"
      },
      "computed": {
        "urgent": { "type": "boolean", "from": ["score", "priority"] },
        "titleLen": { "type": "int", "from": ["title"] }
      }
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
