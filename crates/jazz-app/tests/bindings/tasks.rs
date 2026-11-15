// jazz_app.rs: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Regenerate it after changing jazz.app.json.
#![cfg_attr(rustfmt, rustfmt_skip)]
#![allow(dead_code, clippy::all)]

/// The `jazz.app.json` these bindings were generated from.
pub const SOURCE: &str = "sha256:ab63679cfebf9fd1670e5572811bc87255791c1f76da3872eed530e5668323b7";

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

/// A row of `tasks`.
#[derive(Debug, Clone, PartialEq)]
pub struct Task {
    pub id: ::jazz_app::ObjectId,
    pub title: String,
    pub priority: TaskPriority,
    pub score: i32,
    pub r#type: Option<String>,
    /// `dueAt`.
    pub due_at: Option<String>,
    pub tags: Vec<String>,
    pub meta: Option<::jazz_app::Json>,
    /// Computed from `score`, `priority`.
    pub urgent: bool,
}

/// A new row of `tasks`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct NewTask {
    pub title: String,
    /// `None` stores the column's default.
    pub priority: Option<TaskPriority>,
    pub score: i32,
    pub r#type: Option<String>,
    /// `dueAt`.
    pub due_at: Option<String>,
    pub tags: Vec<String>,
    pub meta: Option<::jazz_app::Json>,
}

/// Changes to a row of `tasks`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TaskUpdate {
    pub title: Option<String>,
    pub priority: Option<TaskPriority>,
    pub score: Option<i32>,
    pub r#type: Option<Option<String>>,
    /// `dueAt`.
    pub due_at: Option<Option<String>>,
    pub tags: Option<Vec<String>>,
    pub meta: Option<Option<::jazz_app::Json>>,
}

impl ::jazz_app::Table for Task {
    const NAME: &'static str = "tasks";
    type New = NewTask;
    type Update = TaskUpdate;

    fn decode(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self> {
        let computed = &row.implementations().get::<Implementations>()?.0;
        let title = row.take::<String>("title")?;
        let priority = row.take::<TaskPriority>("priority")?;
        let score = row.take::<i32>("score")?;
        let r#type = row.take::<Option<String>>("type")?;
        let due_at = row.take::<Option<::jazz_app::Timestamp>>("dueAt")?.map(|stored| computed.iso_date_view(stored));
        let tags = row.take::<Vec<String>>("tags")?;
        let meta = row.take::<Option<::jazz_app::Json>>("meta")?;
        let urgent = computed.tasks_urgent(score.clone(), priority.clone());
        Ok(Self {
            id: row.id(),
            title,
            priority,
            score,
            r#type,
            due_at,
            tags,
            meta,
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
    ) -> ::jazz_app::Result<::std::collections::HashMap<String, ::jazz_app::jazz::tools::Value>> {
        let computed = &implementations.get::<Implementations>()?.0;
        let mut cells = ::std::collections::HashMap::new();
        let value = self.title;
        cells.insert("title".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        if let Some(value) = self.priority {
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
        Ok(cells)
    }
}

impl ::jazz_app::Update for TaskUpdate {
    type Row = Task;

    fn into_cells(
        self,
        implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<Vec<(String, ::jazz_app::jazz::tools::Value)>> {
        let computed = &implementations.get::<Implementations>()?.0;
        let mut cells = Vec::new();
        if let Some(value) = self.title {
            cells.push(("title".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let Some(value) = self.priority {
            cells.push(("priority".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let Some(value) = self.score {
            cells.push(("score".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let Some(value) = self.r#type {
            cells.push(("type".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let Some(value) = self.due_at {
            cells.push(("dueAt".to_owned(), ::jazz_app::ColumnValue::into_value(value.map(|view| computed.iso_date_stored(view)))));
        }
        if let Some(value) = self.tags {
            cells.push(("tags".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let Some(value) = self.meta {
            cells.push(("meta".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        Ok(cells)
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

    /// Column `title`.
    pub fn title() -> ::jazz_app::Column<Task, String> {
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
    pub fn r#type() -> ::jazz_app::Column<Task, Option<String>> {
        ::jazz_app::Column::new("type")
    }

    /// Column `dueAt`. Compared as stored, before its transform.
    pub fn due_at() -> ::jazz_app::Column<Task, Option<::jazz_app::Timestamp>> {
        ::jazz_app::Column::new("dueAt")
    }

    /// Column `tags`.
    pub fn tags() -> ::jazz_app::Column<Task, Vec<String>> {
        ::jazz_app::Column::new("tags")
    }

    /// Column `meta`.
    pub fn meta() -> ::jazz_app::Column<Task, Option<::jazz_app::Json>> {
        ::jazz_app::Column::new("meta")
    }
}

/// What the app's code computes: each computed field `jazz.app.json`
/// declares, and each transform both ways. [`open`] takes an implementation.
pub trait Computed: Send + Sync + 'static {
    /// `tasks.urgent`.
    fn tasks_urgent(&self, score: i32, priority: TaskPriority) -> bool;

    /// Transform `isoDate`: a stored value, as the app reads it.
    fn iso_date_view(&self, stored: ::jazz_app::Timestamp) -> String;

    /// Transform `isoDate`: a value the app writes, as stored.
    fn iso_date_stored(&self, view: String) -> ::jazz_app::Timestamp;
}

struct Implementations(Box<dyn Computed>);

/// The app's `jazz.app.json`: its schema, every earlier version and the
/// migrations between them.
pub const APP: &str = r#"{
  "$schema": "https://jazz.tools/schemas/jazz.app.schema.json",
  "format": 1,
  "tables": {
    "tasks": {
      "columns": {
        "title": "string",
        "priority": { "enum": ["low", "high", "in-progress"], "default": "low" },
        "score": "int",
        "type": "string?",
        "dueAt": { "type": "timestamp", "optional": true, "transform": "isoDate" },
        "tags": { "array": "string" },
        "meta": { "json": true, "optional": true }
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
        ::jazz_app::AppFiles::new(APP, None),
        config,
        ::jazz_app::Implementations::new(Implementations(Box::new(computed))),
    )
    .await
}
