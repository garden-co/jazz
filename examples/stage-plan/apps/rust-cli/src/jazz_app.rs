// jazz_app.rs: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 4. Regenerate it after changing jazz.app.json.
#![cfg_attr(rustfmt, rustfmt_skip)]
#![allow(dead_code, clippy::all)]

/// The `jazz.app.json` these bindings were generated from.
pub const SOURCE: &str = "sha256:1adfb9c805c18f17655799f7003dd9ac6f75950914995d9c25b0fbcaad7a545f";

/// The values of column `activity.kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActivityKind {
    /// `created`.
    Created,
    /// `moved`.
    Moved,
    /// `assigned`.
    Assigned,
    /// `renamed`.
    Renamed,
    /// `commented`.
    Commented,
    /// `deleted`.
    Deleted,
}

::jazz_app::text_enum!(ActivityKind {
    Created => "created",
    Moved => "moved",
    Assigned => "assigned",
    Renamed => "renamed",
    Commented => "commented",
    Deleted => "deleted",
});

/// The values of column `showCrew.role`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShowCrewRole {
    /// `chief`.
    Chief,
    /// `crew`.
    Crew,
}

::jazz_app::text_enum!(ShowCrewRole {
    Chief => "chief",
    Crew => "crew",
});

/// The values of column `tasks.status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskStatus {
    /// `todo`.
    Todo,
    /// `doing`.
    Doing,
    /// `done`.
    Done,
    /// `blocked`.
    Blocked,
}

::jazz_app::text_enum!(TaskStatus {
    Todo => "todo",
    Doing => "doing",
    Done => "done",
    Blocked => "blocked",
});

/// A row of `activity`.
#[derive(Debug, Clone, PartialEq)]
pub struct Activity {
    pub id: ::jazz_app::ObjectId,
    /// `showId`.
    pub show_id: ::jazz_app::ObjectId,
    /// `taskId`.
    pub task_id: ::jazz_app::ObjectId,
    /// `actorId`.
    pub actor_id: ::jazz_app::ObjectId,
    pub kind: ActivityKind,
    pub detail: ::std::option::Option<::std::string::String>,
}

/// A new row of `activity`: [`NewActivity::new`] takes the columns it requires.
#[derive(Debug, Clone, PartialEq)]
pub struct NewActivity {
    /// `showId`.
    pub show_id: ::jazz_app::ObjectId,
    /// `taskId`.
    pub task_id: ::jazz_app::ObjectId,
    /// `actorId`.
    pub actor_id: ::jazz_app::ObjectId,
    pub kind: ActivityKind,
    pub detail: ::std::option::Option<::std::string::String>,
}

impl NewActivity {
    /// A new row with the columns it requires, in this order: `showId`, `taskId`, `actorId`, `kind`. The others
    /// are left out; set them with `NewActivity { column: …, ..NewActivity::new(…) }`.
    pub fn new(show_id: impl ::std::convert::Into<::jazz_app::ObjectId>, task_id: impl ::std::convert::Into<::jazz_app::ObjectId>, actor_id: impl ::std::convert::Into<::jazz_app::ObjectId>, kind: impl ::std::convert::Into<ActivityKind>) -> Self {
        Self {
            show_id: show_id.into(),
            task_id: task_id.into(),
            actor_id: actor_id.into(),
            kind: kind.into(),
            detail: ::std::option::Option::None,
        }
    }
}

/// Changes to a row of `activity`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ActivityUpdate {
    /// `showId`.
    pub show_id: ::std::option::Option<::jazz_app::ObjectId>,
    /// `taskId`.
    pub task_id: ::std::option::Option<::jazz_app::ObjectId>,
    /// `actorId`.
    pub actor_id: ::std::option::Option<::jazz_app::ObjectId>,
    pub kind: ::std::option::Option<ActivityKind>,
    pub detail: ::std::option::Option<::std::option::Option<::std::string::String>>,
}

impl ::jazz_app::Table for Activity {
    const NAME: &'static str = "activity";
    const COLUMNS: &'static [&'static str] = &["showId", "taskId", "actorId", "kind", "detail"];
    type New = NewActivity;
    type Update = ActivityUpdate;

    fn decode(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self> {
        let show_id = row.take::<::jazz_app::ObjectId>("showId")?;
        let task_id = row.take::<::jazz_app::ObjectId>("taskId")?;
        let actor_id = row.take::<::jazz_app::ObjectId>("actorId")?;
        let kind = row.take::<ActivityKind>("kind")?;
        let detail = row.take::<::std::option::Option<::std::string::String>>("detail")?;
        ::std::result::Result::Ok(Self {
            id: row.id(),
            show_id,
            task_id,
            actor_id,
            kind,
            detail,
        })
    }

    fn id(&self) -> ::jazz_app::ObjectId {
        self.id
    }
}

impl ::jazz_app::Insert for NewActivity {
    type Row = Activity;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::collections::HashMap<::std::string::String, ::jazz_app::jazz::tools::Value>> {
        let mut cells = ::std::collections::HashMap::new();
        let value = self.show_id;
        cells.insert("showId".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.task_id;
        cells.insert("taskId".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.actor_id;
        cells.insert("actorId".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.kind;
        cells.insert("kind".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.detail;
        cells.insert("detail".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        ::std::result::Result::Ok(cells)
    }
}

impl ::jazz_app::Update for ActivityUpdate {
    type Row = Activity;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::vec::Vec<(::std::string::String, ::jazz_app::jazz::tools::Value)>> {
        let mut cells = ::std::vec::Vec::new();
        if let ::std::option::Option::Some(value) = self.show_id {
            cells.push(("showId".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.task_id {
            cells.push(("taskId".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.actor_id {
            cells.push(("actorId".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.kind {
            cells.push(("kind".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.detail {
            cells.push(("detail".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        ::std::result::Result::Ok(cells)
    }
}

/// Queries, columns and relations of `activity`.
pub mod activity {
    #[allow(unused_imports)]
    use super::*;

    /// Every row of `activity`; narrow it with `filter`.
    pub fn query() -> ::jazz_app::Query<Activity> {
        ::jazz_app::Query::all()
    }

    /// Every accepted version of `activity`'s rows; bound it to rows with `id` or `id().is_in`.
    pub fn history() -> ::jazz_app::Query<::jazz_app::Version<Activity>> {
        ::jazz_app::Query::history()
    }

    /// The row's id. (`::jazz_app::created_at()` and `updated_at()` are columns of every table too.)
    pub fn id() -> ::jazz_app::Column<Activity, ::jazz_app::ObjectId> {
        ::jazz_app::id()
    }

    /// Column `showId`.
    pub fn show_id() -> ::jazz_app::Column<Activity, ::jazz_app::ObjectId> {
        ::jazz_app::Column::new("showId")
    }

    /// Column `taskId`.
    pub fn task_id() -> ::jazz_app::Column<Activity, ::jazz_app::ObjectId> {
        ::jazz_app::Column::new("taskId")
    }

    /// Column `actorId`.
    pub fn actor_id() -> ::jazz_app::Column<Activity, ::jazz_app::ObjectId> {
        ::jazz_app::Column::new("actorId")
    }

    /// Column `kind`.
    pub fn kind() -> ::jazz_app::Column<Activity, ActivityKind> {
        ::jazz_app::Column::new("kind")
    }

    /// Column `detail`.
    pub fn detail() -> ::jazz_app::Column<Activity, ::std::option::Option<::std::string::String>> {
        ::jazz_app::Column::new("detail")
    }

    /// Relation `actor`: the `crew` row `actorId` references.
    pub fn actor() -> ::jazz_app::One<Activity, Crew> {
        ::jazz_app::One::new("actorId")
    }

    /// Relation `show`: the `shows` row `showId` references.
    pub fn show() -> ::jazz_app::One<Activity, Show> {
        ::jazz_app::One::new("showId")
    }

    /// Relation `task`: the `tasks` row `taskId` references.
    pub fn task() -> ::jazz_app::One<Activity, Task> {
        ::jazz_app::One::new("taskId")
    }
}

/// A row of `checklistItems`.
#[derive(Debug, Clone, PartialEq)]
pub struct ChecklistItem {
    pub id: ::jazz_app::ObjectId,
    pub title: ::std::string::String,
    pub done: bool,
    /// `ownerAccount`.
    pub owner_account: ::jazz_app::ObjectId,
}

/// A new row of `checklistItems`: [`NewChecklistItem::new`] takes the columns it requires.
#[derive(Debug, Clone, PartialEq)]
pub struct NewChecklistItem {
    pub title: ::std::string::String,
    pub done: bool,
    /// `ownerAccount`.
    pub owner_account: ::jazz_app::ObjectId,
}

impl NewChecklistItem {
    /// A new row with the columns it requires, in this order: `title`, `done`, `ownerAccount`. The others
    /// are left out; set them with `NewChecklistItem { column: …, ..NewChecklistItem::new(…) }`.
    pub fn new(title: impl ::std::convert::Into<::std::string::String>, done: impl ::std::convert::Into<bool>, owner_account: impl ::std::convert::Into<::jazz_app::ObjectId>) -> Self {
        Self {
            title: title.into(),
            done: done.into(),
            owner_account: owner_account.into(),
        }
    }
}

/// Changes to a row of `checklistItems`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChecklistItemUpdate {
    pub title: ::std::option::Option<::std::string::String>,
    pub done: ::std::option::Option<bool>,
    /// `ownerAccount`.
    pub owner_account: ::std::option::Option<::jazz_app::ObjectId>,
}

impl ::jazz_app::Table for ChecklistItem {
    const NAME: &'static str = "checklistItems";
    const COLUMNS: &'static [&'static str] = &["title", "done", "ownerAccount"];
    type New = NewChecklistItem;
    type Update = ChecklistItemUpdate;

    fn decode(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self> {
        let title = row.take::<::std::string::String>("title")?;
        let done = row.take::<bool>("done")?;
        let owner_account = row.take::<::jazz_app::ObjectId>("ownerAccount")?;
        ::std::result::Result::Ok(Self {
            id: row.id(),
            title,
            done,
            owner_account,
        })
    }

    fn id(&self) -> ::jazz_app::ObjectId {
        self.id
    }
}

impl ::jazz_app::Insert for NewChecklistItem {
    type Row = ChecklistItem;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::collections::HashMap<::std::string::String, ::jazz_app::jazz::tools::Value>> {
        let mut cells = ::std::collections::HashMap::new();
        let value = self.title;
        cells.insert("title".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.done;
        cells.insert("done".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.owner_account;
        cells.insert("ownerAccount".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        ::std::result::Result::Ok(cells)
    }
}

impl ::jazz_app::Update for ChecklistItemUpdate {
    type Row = ChecklistItem;

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
        if let ::std::option::Option::Some(value) = self.owner_account {
            cells.push(("ownerAccount".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        ::std::result::Result::Ok(cells)
    }
}

/// Queries, columns and relations of `checklistItems`.
pub mod checklist_items {
    #[allow(unused_imports)]
    use super::*;

    /// Every row of `checklistItems`; narrow it with `filter`.
    pub fn query() -> ::jazz_app::Query<ChecklistItem> {
        ::jazz_app::Query::all()
    }

    /// Every accepted version of `checklistItems`'s rows; bound it to rows with `id` or `id().is_in`.
    pub fn history() -> ::jazz_app::Query<::jazz_app::Version<ChecklistItem>> {
        ::jazz_app::Query::history()
    }

    /// The row's id. (`::jazz_app::created_at()` and `updated_at()` are columns of every table too.)
    pub fn id() -> ::jazz_app::Column<ChecklistItem, ::jazz_app::ObjectId> {
        ::jazz_app::id()
    }

    /// Column `title`.
    pub fn title() -> ::jazz_app::Column<ChecklistItem, ::std::string::String> {
        ::jazz_app::Column::new("title")
    }

    /// Column `done`.
    pub fn done() -> ::jazz_app::Column<ChecklistItem, bool> {
        ::jazz_app::Column::new("done")
    }

    /// Column `ownerAccount`.
    pub fn owner_account() -> ::jazz_app::Column<ChecklistItem, ::jazz_app::ObjectId> {
        ::jazz_app::Column::new("ownerAccount")
    }
}

/// A row of `comments`.
#[derive(Debug, Clone, PartialEq)]
pub struct Comment {
    pub id: ::jazz_app::ObjectId,
    /// `taskId`.
    pub task_id: ::jazz_app::ObjectId,
    /// `authorId`.
    pub author_id: ::jazz_app::ObjectId,
    pub body: ::std::string::String,
}

/// A new row of `comments`: [`NewComment::new`] takes the columns it requires.
#[derive(Debug, Clone, PartialEq)]
pub struct NewComment {
    /// `taskId`.
    pub task_id: ::jazz_app::ObjectId,
    /// `authorId`.
    pub author_id: ::jazz_app::ObjectId,
    pub body: ::std::string::String,
}

impl NewComment {
    /// A new row with the columns it requires, in this order: `taskId`, `authorId`, `body`. The others
    /// are left out; set them with `NewComment { column: …, ..NewComment::new(…) }`.
    pub fn new(task_id: impl ::std::convert::Into<::jazz_app::ObjectId>, author_id: impl ::std::convert::Into<::jazz_app::ObjectId>, body: impl ::std::convert::Into<::std::string::String>) -> Self {
        Self {
            task_id: task_id.into(),
            author_id: author_id.into(),
            body: body.into(),
        }
    }
}

/// Changes to a row of `comments`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CommentUpdate {
    /// `taskId`.
    pub task_id: ::std::option::Option<::jazz_app::ObjectId>,
    /// `authorId`.
    pub author_id: ::std::option::Option<::jazz_app::ObjectId>,
    pub body: ::std::option::Option<::std::string::String>,
}

impl ::jazz_app::Table for Comment {
    const NAME: &'static str = "comments";
    const COLUMNS: &'static [&'static str] = &["taskId", "authorId", "body"];
    type New = NewComment;
    type Update = CommentUpdate;

    fn decode(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self> {
        let task_id = row.take::<::jazz_app::ObjectId>("taskId")?;
        let author_id = row.take::<::jazz_app::ObjectId>("authorId")?;
        let body = row.take::<::std::string::String>("body")?;
        ::std::result::Result::Ok(Self {
            id: row.id(),
            task_id,
            author_id,
            body,
        })
    }

    fn id(&self) -> ::jazz_app::ObjectId {
        self.id
    }
}

impl ::jazz_app::Insert for NewComment {
    type Row = Comment;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::collections::HashMap<::std::string::String, ::jazz_app::jazz::tools::Value>> {
        let mut cells = ::std::collections::HashMap::new();
        let value = self.task_id;
        cells.insert("taskId".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.author_id;
        cells.insert("authorId".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.body;
        cells.insert("body".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        ::std::result::Result::Ok(cells)
    }
}

impl ::jazz_app::Update for CommentUpdate {
    type Row = Comment;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::vec::Vec<(::std::string::String, ::jazz_app::jazz::tools::Value)>> {
        let mut cells = ::std::vec::Vec::new();
        if let ::std::option::Option::Some(value) = self.task_id {
            cells.push(("taskId".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.author_id {
            cells.push(("authorId".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.body {
            cells.push(("body".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        ::std::result::Result::Ok(cells)
    }
}

/// Queries, columns and relations of `comments`.
pub mod comments {
    #[allow(unused_imports)]
    use super::*;

    /// Every row of `comments`; narrow it with `filter`.
    pub fn query() -> ::jazz_app::Query<Comment> {
        ::jazz_app::Query::all()
    }

    /// Every accepted version of `comments`'s rows; bound it to rows with `id` or `id().is_in`.
    pub fn history() -> ::jazz_app::Query<::jazz_app::Version<Comment>> {
        ::jazz_app::Query::history()
    }

    /// The row's id. (`::jazz_app::created_at()` and `updated_at()` are columns of every table too.)
    pub fn id() -> ::jazz_app::Column<Comment, ::jazz_app::ObjectId> {
        ::jazz_app::id()
    }

    /// Column `taskId`.
    pub fn task_id() -> ::jazz_app::Column<Comment, ::jazz_app::ObjectId> {
        ::jazz_app::Column::new("taskId")
    }

    /// Column `authorId`.
    pub fn author_id() -> ::jazz_app::Column<Comment, ::jazz_app::ObjectId> {
        ::jazz_app::Column::new("authorId")
    }

    /// Column `body`.
    pub fn body() -> ::jazz_app::Column<Comment, ::std::string::String> {
        ::jazz_app::Column::new("body")
    }

    /// Relation `author`: the `crew` row `authorId` references.
    pub fn author() -> ::jazz_app::One<Comment, Crew> {
        ::jazz_app::One::new("authorId")
    }

    /// Relation `task`: the `tasks` row `taskId` references.
    pub fn task() -> ::jazz_app::One<Comment, Task> {
        ::jazz_app::One::new("taskId")
    }
}

/// A row of `crew`.
#[derive(Debug, Clone, PartialEq)]
pub struct Crew {
    pub id: ::jazz_app::ObjectId,
    pub account: ::jazz_app::ObjectId,
    pub name: ::std::string::String,
}

/// A new row of `crew`: [`NewCrew::new`] takes the columns it requires.
#[derive(Debug, Clone, PartialEq)]
pub struct NewCrew {
    pub account: ::jazz_app::ObjectId,
    pub name: ::std::string::String,
}

impl NewCrew {
    /// A new row with the columns it requires, in this order: `account`, `name`. The others
    /// are left out; set them with `NewCrew { column: …, ..NewCrew::new(…) }`.
    pub fn new(account: impl ::std::convert::Into<::jazz_app::ObjectId>, name: impl ::std::convert::Into<::std::string::String>) -> Self {
        Self {
            account: account.into(),
            name: name.into(),
        }
    }
}

/// Changes to a row of `crew`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CrewUpdate {
    pub account: ::std::option::Option<::jazz_app::ObjectId>,
    pub name: ::std::option::Option<::std::string::String>,
}

impl ::jazz_app::Table for Crew {
    const NAME: &'static str = "crew";
    const COLUMNS: &'static [&'static str] = &["account", "name"];
    type New = NewCrew;
    type Update = CrewUpdate;

    fn decode(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self> {
        let account = row.take::<::jazz_app::ObjectId>("account")?;
        let name = row.take::<::std::string::String>("name")?;
        ::std::result::Result::Ok(Self {
            id: row.id(),
            account,
            name,
        })
    }

    fn id(&self) -> ::jazz_app::ObjectId {
        self.id
    }
}

impl ::jazz_app::Insert for NewCrew {
    type Row = Crew;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::collections::HashMap<::std::string::String, ::jazz_app::jazz::tools::Value>> {
        let mut cells = ::std::collections::HashMap::new();
        let value = self.account;
        cells.insert("account".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.name;
        cells.insert("name".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        ::std::result::Result::Ok(cells)
    }
}

impl ::jazz_app::Update for CrewUpdate {
    type Row = Crew;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::vec::Vec<(::std::string::String, ::jazz_app::jazz::tools::Value)>> {
        let mut cells = ::std::vec::Vec::new();
        if let ::std::option::Option::Some(value) = self.account {
            cells.push(("account".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.name {
            cells.push(("name".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        ::std::result::Result::Ok(cells)
    }
}

/// Queries, columns and relations of `crew`.
pub mod crew {
    #[allow(unused_imports)]
    use super::*;

    /// Every row of `crew`; narrow it with `filter`.
    pub fn query() -> ::jazz_app::Query<Crew> {
        ::jazz_app::Query::all()
    }

    /// Every accepted version of `crew`'s rows; bound it to rows with `id` or `id().is_in`.
    pub fn history() -> ::jazz_app::Query<::jazz_app::Version<Crew>> {
        ::jazz_app::Query::history()
    }

    /// The row's id. (`::jazz_app::created_at()` and `updated_at()` are columns of every table too.)
    pub fn id() -> ::jazz_app::Column<Crew, ::jazz_app::ObjectId> {
        ::jazz_app::id()
    }

    /// Column `account`.
    pub fn account() -> ::jazz_app::Column<Crew, ::jazz_app::ObjectId> {
        ::jazz_app::Column::new("account")
    }

    /// Column `name`.
    pub fn name() -> ::jazz_app::Column<Crew, ::std::string::String> {
        ::jazz_app::Column::new("name")
    }

    /// Relation `assignedTasks`: the `tasks` rows whose `assigneeId` references this row.
    pub fn assigned_tasks() -> ::jazz_app::Many<Crew, Task> {
        ::jazz_app::Many::new("assigneeId")
    }

    /// Relation `memberships`: the `showCrew` rows whose `crewId` references this row.
    pub fn memberships() -> ::jazz_app::Many<Crew, ShowCrew> {
        ::jazz_app::Many::new("crewId")
    }
}

/// A row of `showCrew`.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowCrew {
    pub id: ::jazz_app::ObjectId,
    /// `showId`.
    pub show_id: ::jazz_app::ObjectId,
    /// `crewId`.
    pub crew_id: ::jazz_app::ObjectId,
    pub account: ::jazz_app::ObjectId,
    pub role: ShowCrewRole,
    /// `inviteCode`.
    pub invite_code: ::std::option::Option<::std::string::String>,
}

/// A new row of `showCrew`: [`NewShowCrew::new`] takes the columns it requires.
#[derive(Debug, Clone, PartialEq)]
pub struct NewShowCrew {
    /// `showId`.
    pub show_id: ::jazz_app::ObjectId,
    /// `crewId`.
    pub crew_id: ::jazz_app::ObjectId,
    pub account: ::jazz_app::ObjectId,
    pub role: ShowCrewRole,
    /// `inviteCode`.
    pub invite_code: ::std::option::Option<::std::string::String>,
}

impl NewShowCrew {
    /// A new row with the columns it requires, in this order: `showId`, `crewId`, `account`, `role`. The others
    /// are left out; set them with `NewShowCrew { column: …, ..NewShowCrew::new(…) }`.
    pub fn new(show_id: impl ::std::convert::Into<::jazz_app::ObjectId>, crew_id: impl ::std::convert::Into<::jazz_app::ObjectId>, account: impl ::std::convert::Into<::jazz_app::ObjectId>, role: impl ::std::convert::Into<ShowCrewRole>) -> Self {
        Self {
            show_id: show_id.into(),
            crew_id: crew_id.into(),
            account: account.into(),
            role: role.into(),
            invite_code: ::std::option::Option::None,
        }
    }
}

/// Changes to a row of `showCrew`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ShowCrewUpdate {
    /// `showId`.
    pub show_id: ::std::option::Option<::jazz_app::ObjectId>,
    /// `crewId`.
    pub crew_id: ::std::option::Option<::jazz_app::ObjectId>,
    pub account: ::std::option::Option<::jazz_app::ObjectId>,
    pub role: ::std::option::Option<ShowCrewRole>,
    /// `inviteCode`.
    pub invite_code: ::std::option::Option<::std::option::Option<::std::string::String>>,
}

impl ::jazz_app::Table for ShowCrew {
    const NAME: &'static str = "showCrew";
    const COLUMNS: &'static [&'static str] = &["showId", "crewId", "account", "role", "inviteCode"];
    type New = NewShowCrew;
    type Update = ShowCrewUpdate;

    fn decode(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self> {
        let show_id = row.take::<::jazz_app::ObjectId>("showId")?;
        let crew_id = row.take::<::jazz_app::ObjectId>("crewId")?;
        let account = row.take::<::jazz_app::ObjectId>("account")?;
        let role = row.take::<ShowCrewRole>("role")?;
        let invite_code = row.take::<::std::option::Option<::std::string::String>>("inviteCode")?;
        ::std::result::Result::Ok(Self {
            id: row.id(),
            show_id,
            crew_id,
            account,
            role,
            invite_code,
        })
    }

    fn id(&self) -> ::jazz_app::ObjectId {
        self.id
    }
}

impl ::jazz_app::Insert for NewShowCrew {
    type Row = ShowCrew;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::collections::HashMap<::std::string::String, ::jazz_app::jazz::tools::Value>> {
        let mut cells = ::std::collections::HashMap::new();
        let value = self.show_id;
        cells.insert("showId".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.crew_id;
        cells.insert("crewId".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.account;
        cells.insert("account".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.role;
        cells.insert("role".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.invite_code;
        cells.insert("inviteCode".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        ::std::result::Result::Ok(cells)
    }
}

impl ::jazz_app::Update for ShowCrewUpdate {
    type Row = ShowCrew;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::vec::Vec<(::std::string::String, ::jazz_app::jazz::tools::Value)>> {
        let mut cells = ::std::vec::Vec::new();
        if let ::std::option::Option::Some(value) = self.show_id {
            cells.push(("showId".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.crew_id {
            cells.push(("crewId".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.account {
            cells.push(("account".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.role {
            cells.push(("role".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.invite_code {
            cells.push(("inviteCode".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        ::std::result::Result::Ok(cells)
    }
}

/// Queries, columns and relations of `showCrew`.
pub mod show_crew {
    #[allow(unused_imports)]
    use super::*;

    /// Every row of `showCrew`; narrow it with `filter`.
    pub fn query() -> ::jazz_app::Query<ShowCrew> {
        ::jazz_app::Query::all()
    }

    /// Every accepted version of `showCrew`'s rows; bound it to rows with `id` or `id().is_in`.
    pub fn history() -> ::jazz_app::Query<::jazz_app::Version<ShowCrew>> {
        ::jazz_app::Query::history()
    }

    /// The row's id. (`::jazz_app::created_at()` and `updated_at()` are columns of every table too.)
    pub fn id() -> ::jazz_app::Column<ShowCrew, ::jazz_app::ObjectId> {
        ::jazz_app::id()
    }

    /// Column `showId`.
    pub fn show_id() -> ::jazz_app::Column<ShowCrew, ::jazz_app::ObjectId> {
        ::jazz_app::Column::new("showId")
    }

    /// Column `crewId`.
    pub fn crew_id() -> ::jazz_app::Column<ShowCrew, ::jazz_app::ObjectId> {
        ::jazz_app::Column::new("crewId")
    }

    /// Column `account`.
    pub fn account() -> ::jazz_app::Column<ShowCrew, ::jazz_app::ObjectId> {
        ::jazz_app::Column::new("account")
    }

    /// Column `role`.
    pub fn role() -> ::jazz_app::Column<ShowCrew, ShowCrewRole> {
        ::jazz_app::Column::new("role")
    }

    /// Column `inviteCode`.
    pub fn invite_code() -> ::jazz_app::Column<ShowCrew, ::std::option::Option<::std::string::String>> {
        ::jazz_app::Column::new("inviteCode")
    }

    /// Relation `crew`: the `crew` row `crewId` references.
    pub fn crew() -> ::jazz_app::One<ShowCrew, Crew> {
        ::jazz_app::One::new("crewId")
    }

    /// Relation `show`: the `shows` row `showId` references.
    pub fn show() -> ::jazz_app::One<ShowCrew, Show> {
        ::jazz_app::One::new("showId")
    }
}

/// A row of `showInvites`.
#[derive(Debug, Clone, PartialEq)]
pub struct ShowInvite {
    pub id: ::jazz_app::ObjectId,
    /// `showId`.
    pub show_id: ::jazz_app::ObjectId,
    pub code: ::std::string::String,
}

/// A new row of `showInvites`: [`NewShowInvite::new`] takes the columns it requires.
#[derive(Debug, Clone, PartialEq)]
pub struct NewShowInvite {
    /// `showId`.
    pub show_id: ::jazz_app::ObjectId,
    pub code: ::std::string::String,
}

impl NewShowInvite {
    /// A new row with the columns it requires, in this order: `showId`, `code`. The others
    /// are left out; set them with `NewShowInvite { column: …, ..NewShowInvite::new(…) }`.
    pub fn new(show_id: impl ::std::convert::Into<::jazz_app::ObjectId>, code: impl ::std::convert::Into<::std::string::String>) -> Self {
        Self {
            show_id: show_id.into(),
            code: code.into(),
        }
    }
}

/// Changes to a row of `showInvites`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ShowInviteUpdate {
    /// `showId`.
    pub show_id: ::std::option::Option<::jazz_app::ObjectId>,
    pub code: ::std::option::Option<::std::string::String>,
}

impl ::jazz_app::Table for ShowInvite {
    const NAME: &'static str = "showInvites";
    const COLUMNS: &'static [&'static str] = &["showId", "code"];
    type New = NewShowInvite;
    type Update = ShowInviteUpdate;

    fn decode(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self> {
        let show_id = row.take::<::jazz_app::ObjectId>("showId")?;
        let code = row.take::<::std::string::String>("code")?;
        ::std::result::Result::Ok(Self {
            id: row.id(),
            show_id,
            code,
        })
    }

    fn id(&self) -> ::jazz_app::ObjectId {
        self.id
    }
}

impl ::jazz_app::Insert for NewShowInvite {
    type Row = ShowInvite;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::collections::HashMap<::std::string::String, ::jazz_app::jazz::tools::Value>> {
        let mut cells = ::std::collections::HashMap::new();
        let value = self.show_id;
        cells.insert("showId".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.code;
        cells.insert("code".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        ::std::result::Result::Ok(cells)
    }
}

impl ::jazz_app::Update for ShowInviteUpdate {
    type Row = ShowInvite;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::vec::Vec<(::std::string::String, ::jazz_app::jazz::tools::Value)>> {
        let mut cells = ::std::vec::Vec::new();
        if let ::std::option::Option::Some(value) = self.show_id {
            cells.push(("showId".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.code {
            cells.push(("code".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        ::std::result::Result::Ok(cells)
    }
}

/// Queries, columns and relations of `showInvites`.
pub mod show_invites {
    #[allow(unused_imports)]
    use super::*;

    /// Every row of `showInvites`; narrow it with `filter`.
    pub fn query() -> ::jazz_app::Query<ShowInvite> {
        ::jazz_app::Query::all()
    }

    /// Every accepted version of `showInvites`'s rows; bound it to rows with `id` or `id().is_in`.
    pub fn history() -> ::jazz_app::Query<::jazz_app::Version<ShowInvite>> {
        ::jazz_app::Query::history()
    }

    /// The row's id. (`::jazz_app::created_at()` and `updated_at()` are columns of every table too.)
    pub fn id() -> ::jazz_app::Column<ShowInvite, ::jazz_app::ObjectId> {
        ::jazz_app::id()
    }

    /// Column `showId`.
    pub fn show_id() -> ::jazz_app::Column<ShowInvite, ::jazz_app::ObjectId> {
        ::jazz_app::Column::new("showId")
    }

    /// Column `code`.
    pub fn code() -> ::jazz_app::Column<ShowInvite, ::std::string::String> {
        ::jazz_app::Column::new("code")
    }

    /// Relation `show`: the `shows` row `showId` references.
    pub fn show() -> ::jazz_app::One<ShowInvite, Show> {
        ::jazz_app::One::new("showId")
    }
}

/// A row of `shows`.
#[derive(Debug, Clone, PartialEq)]
pub struct Show {
    pub id: ::jazz_app::ObjectId,
    pub name: ::std::string::String,
    pub venue: ::std::string::String,
    pub date: ::std::string::String,
    pub doors: ::std::string::String,
    /// `chiefAccount`.
    pub chief_account: ::jazz_app::ObjectId,
}

/// A new row of `shows`: [`NewShow::new`] takes the columns it requires.
#[derive(Debug, Clone, PartialEq)]
pub struct NewShow {
    pub name: ::std::string::String,
    pub venue: ::std::string::String,
    pub date: ::std::string::String,
    pub doors: ::std::string::String,
    /// `chiefAccount`.
    pub chief_account: ::jazz_app::ObjectId,
}

impl NewShow {
    /// A new row with the columns it requires, in this order: `name`, `venue`, `date`, `doors`, `chiefAccount`. The others
    /// are left out; set them with `NewShow { column: …, ..NewShow::new(…) }`.
    pub fn new(name: impl ::std::convert::Into<::std::string::String>, venue: impl ::std::convert::Into<::std::string::String>, date: impl ::std::convert::Into<::std::string::String>, doors: impl ::std::convert::Into<::std::string::String>, chief_account: impl ::std::convert::Into<::jazz_app::ObjectId>) -> Self {
        Self {
            name: name.into(),
            venue: venue.into(),
            date: date.into(),
            doors: doors.into(),
            chief_account: chief_account.into(),
        }
    }
}

/// Changes to a row of `shows`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ShowUpdate {
    pub name: ::std::option::Option<::std::string::String>,
    pub venue: ::std::option::Option<::std::string::String>,
    pub date: ::std::option::Option<::std::string::String>,
    pub doors: ::std::option::Option<::std::string::String>,
    /// `chiefAccount`.
    pub chief_account: ::std::option::Option<::jazz_app::ObjectId>,
}

impl ::jazz_app::Table for Show {
    const NAME: &'static str = "shows";
    const COLUMNS: &'static [&'static str] = &["name", "venue", "date", "doors", "chiefAccount"];
    type New = NewShow;
    type Update = ShowUpdate;

    fn decode(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self> {
        let name = row.take::<::std::string::String>("name")?;
        let venue = row.take::<::std::string::String>("venue")?;
        let date = row.take::<::std::string::String>("date")?;
        let doors = row.take::<::std::string::String>("doors")?;
        let chief_account = row.take::<::jazz_app::ObjectId>("chiefAccount")?;
        ::std::result::Result::Ok(Self {
            id: row.id(),
            name,
            venue,
            date,
            doors,
            chief_account,
        })
    }

    fn id(&self) -> ::jazz_app::ObjectId {
        self.id
    }
}

impl ::jazz_app::Insert for NewShow {
    type Row = Show;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::collections::HashMap<::std::string::String, ::jazz_app::jazz::tools::Value>> {
        let mut cells = ::std::collections::HashMap::new();
        let value = self.name;
        cells.insert("name".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.venue;
        cells.insert("venue".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.date;
        cells.insert("date".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.doors;
        cells.insert("doors".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.chief_account;
        cells.insert("chiefAccount".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        ::std::result::Result::Ok(cells)
    }
}

impl ::jazz_app::Update for ShowUpdate {
    type Row = Show;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::vec::Vec<(::std::string::String, ::jazz_app::jazz::tools::Value)>> {
        let mut cells = ::std::vec::Vec::new();
        if let ::std::option::Option::Some(value) = self.name {
            cells.push(("name".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.venue {
            cells.push(("venue".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.date {
            cells.push(("date".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.doors {
            cells.push(("doors".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.chief_account {
            cells.push(("chiefAccount".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        ::std::result::Result::Ok(cells)
    }
}

/// Queries, columns and relations of `shows`.
pub mod shows {
    #[allow(unused_imports)]
    use super::*;

    /// Every row of `shows`; narrow it with `filter`.
    pub fn query() -> ::jazz_app::Query<Show> {
        ::jazz_app::Query::all()
    }

    /// Every accepted version of `shows`'s rows; bound it to rows with `id` or `id().is_in`.
    pub fn history() -> ::jazz_app::Query<::jazz_app::Version<Show>> {
        ::jazz_app::Query::history()
    }

    /// The row's id. (`::jazz_app::created_at()` and `updated_at()` are columns of every table too.)
    pub fn id() -> ::jazz_app::Column<Show, ::jazz_app::ObjectId> {
        ::jazz_app::id()
    }

    /// Column `name`.
    pub fn name() -> ::jazz_app::Column<Show, ::std::string::String> {
        ::jazz_app::Column::new("name")
    }

    /// Column `venue`.
    pub fn venue() -> ::jazz_app::Column<Show, ::std::string::String> {
        ::jazz_app::Column::new("venue")
    }

    /// Column `date`.
    pub fn date() -> ::jazz_app::Column<Show, ::std::string::String> {
        ::jazz_app::Column::new("date")
    }

    /// Column `doors`.
    pub fn doors() -> ::jazz_app::Column<Show, ::std::string::String> {
        ::jazz_app::Column::new("doors")
    }

    /// Column `chiefAccount`.
    pub fn chief_account() -> ::jazz_app::Column<Show, ::jazz_app::ObjectId> {
        ::jazz_app::Column::new("chiefAccount")
    }

    /// Relation `activity`: the `activity` rows whose `showId` references this row.
    pub fn activity() -> ::jazz_app::Many<Show, Activity> {
        ::jazz_app::Many::new("showId")
    }

    /// Relation `crew`: the `showCrew` rows whose `showId` references this row.
    pub fn crew() -> ::jazz_app::Many<Show, ShowCrew> {
        ::jazz_app::Many::new("showId")
    }

    /// Relation `invites`: the `showInvites` rows whose `showId` references this row.
    pub fn invites() -> ::jazz_app::Many<Show, ShowInvite> {
        ::jazz_app::Many::new("showId")
    }

    /// Relation `tasks`: the `tasks` rows whose `showId` references this row.
    pub fn tasks() -> ::jazz_app::Many<Show, Task> {
        ::jazz_app::Many::new("showId")
    }
}

/// A row of `tasks`.
#[derive(Debug, Clone, PartialEq)]
pub struct Task {
    pub id: ::jazz_app::ObjectId,
    /// `showId`.
    pub show_id: ::jazz_app::ObjectId,
    pub title: ::std::string::String,
    pub status: TaskStatus,
    /// `assigneeId`.
    pub assignee_id: ::std::option::Option<::jazz_app::ObjectId>,
    pub notes: ::std::option::Option<::std::string::String>,
    pub rank: f64,
}

/// A new row of `tasks`: [`NewTask::new`] takes the columns it requires.
#[derive(Debug, Clone, PartialEq)]
pub struct NewTask {
    /// `showId`.
    pub show_id: ::jazz_app::ObjectId,
    pub title: ::std::string::String,
    pub status: TaskStatus,
    /// `assigneeId`.
    pub assignee_id: ::std::option::Option<::jazz_app::ObjectId>,
    pub notes: ::std::option::Option<::std::string::String>,
    pub rank: f64,
}

impl NewTask {
    /// A new row with the columns it requires, in this order: `showId`, `title`, `status`, `rank`. The others
    /// are left out; set them with `NewTask { column: …, ..NewTask::new(…) }`.
    pub fn new(show_id: impl ::std::convert::Into<::jazz_app::ObjectId>, title: impl ::std::convert::Into<::std::string::String>, status: impl ::std::convert::Into<TaskStatus>, rank: impl ::std::convert::Into<f64>) -> Self {
        Self {
            show_id: show_id.into(),
            title: title.into(),
            status: status.into(),
            assignee_id: ::std::option::Option::None,
            notes: ::std::option::Option::None,
            rank: rank.into(),
        }
    }
}

/// Changes to a row of `tasks`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TaskUpdate {
    /// `showId`.
    pub show_id: ::std::option::Option<::jazz_app::ObjectId>,
    pub title: ::std::option::Option<::std::string::String>,
    pub status: ::std::option::Option<TaskStatus>,
    /// `assigneeId`.
    pub assignee_id: ::std::option::Option<::std::option::Option<::jazz_app::ObjectId>>,
    pub notes: ::std::option::Option<::std::option::Option<::std::string::String>>,
    pub rank: ::std::option::Option<f64>,
}

impl ::jazz_app::Table for Task {
    const NAME: &'static str = "tasks";
    const COLUMNS: &'static [&'static str] = &["showId", "title", "status", "assigneeId", "notes", "rank"];
    type New = NewTask;
    type Update = TaskUpdate;

    fn decode(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self> {
        let show_id = row.take::<::jazz_app::ObjectId>("showId")?;
        let title = row.take::<::std::string::String>("title")?;
        let status = row.take::<TaskStatus>("status")?;
        let assignee_id = row.take::<::std::option::Option<::jazz_app::ObjectId>>("assigneeId")?;
        let notes = row.take::<::std::option::Option<::std::string::String>>("notes")?;
        let rank = row.take::<f64>("rank")?;
        ::std::result::Result::Ok(Self {
            id: row.id(),
            show_id,
            title,
            status,
            assignee_id,
            notes,
            rank,
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
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::collections::HashMap<::std::string::String, ::jazz_app::jazz::tools::Value>> {
        let mut cells = ::std::collections::HashMap::new();
        let value = self.show_id;
        cells.insert("showId".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.title;
        cells.insert("title".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.status;
        cells.insert("status".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.assignee_id;
        cells.insert("assigneeId".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.notes;
        cells.insert("notes".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.rank;
        cells.insert("rank".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        ::std::result::Result::Ok(cells)
    }
}

impl ::jazz_app::Update for TaskUpdate {
    type Row = Task;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::vec::Vec<(::std::string::String, ::jazz_app::jazz::tools::Value)>> {
        let mut cells = ::std::vec::Vec::new();
        if let ::std::option::Option::Some(value) = self.show_id {
            cells.push(("showId".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.title {
            cells.push(("title".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.status {
            cells.push(("status".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.assignee_id {
            cells.push(("assigneeId".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.notes {
            cells.push(("notes".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.rank {
            cells.push(("rank".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
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

    /// Column `showId`.
    pub fn show_id() -> ::jazz_app::Column<Task, ::jazz_app::ObjectId> {
        ::jazz_app::Column::new("showId")
    }

    /// Column `title`.
    pub fn title() -> ::jazz_app::Column<Task, ::std::string::String> {
        ::jazz_app::Column::new("title")
    }

    /// Column `status`.
    pub fn status() -> ::jazz_app::Column<Task, TaskStatus> {
        ::jazz_app::Column::new("status")
    }

    /// Column `assigneeId`.
    pub fn assignee_id() -> ::jazz_app::Column<Task, ::std::option::Option<::jazz_app::ObjectId>> {
        ::jazz_app::Column::new("assigneeId")
    }

    /// Column `notes`.
    pub fn notes() -> ::jazz_app::Column<Task, ::std::option::Option<::std::string::String>> {
        ::jazz_app::Column::new("notes")
    }

    /// Column `rank`.
    pub fn rank() -> ::jazz_app::Column<Task, f64> {
        ::jazz_app::Column::new("rank")
    }

    /// Relation `activity`: the `activity` rows whose `taskId` references this row.
    pub fn activity() -> ::jazz_app::Many<Task, Activity> {
        ::jazz_app::Many::new("taskId")
    }

    /// Relation `assignee`: the `crew` row `assigneeId` references.
    pub fn assignee() -> ::jazz_app::One<Task, Crew, ::jazz_app::Nullable> {
        ::jazz_app::One::new("assigneeId")
    }

    /// Relation `comments`: the `comments` rows whose `taskId` references this row.
    pub fn comments() -> ::jazz_app::Many<Task, Comment> {
        ::jazz_app::Many::new("taskId")
    }

    /// Relation `show`: the `shows` row `showId` references.
    pub fn show() -> ::jazz_app::One<Task, Show> {
        ::jazz_app::One::new("showId")
    }
}

/// The app's `jazz.app.json`: its schema, every earlier version and the
/// migrations between them.
pub const APP: &str = r#"{
  "$schema": "https://jazz.tools/schemas/jazz.app.schema.json",
  "format": 1,
  "tables": {
    "activity": {
      "columns": {
        "showId": "uuid",
        "taskId": "uuid",
        "actorId": "uuid",
        "kind": { "enum": ["created", "moved", "assigned", "renamed", "commented", "deleted"] },
        "detail": "string?"
      },
      "relations": {
        "actor": { "rel": "crew", "column": "actorId" },
        "show": { "rel": "shows", "column": "showId" },
        "task": { "rel": "tasks", "column": "taskId" }
      }
    },
    "checklistItems": {
      "columns": { "title": "string", "done": "boolean", "ownerAccount": "uuid" }
    },
    "comments": {
      "columns": { "taskId": "uuid", "authorId": "uuid", "body": "string" },
      "relations": {
        "author": { "rel": "crew", "column": "authorId" },
        "task": { "rel": "tasks", "column": "taskId" }
      }
    },
    "crew": {
      "columns": { "account": "uuid", "name": "string" },
      "relations": {
        "assignedTasks": { "reverse": "tasks", "relation": "assignee" },
        "memberships": { "reverse": "showCrew", "relation": "crew" }
      }
    },
    "showCrew": {
      "columns": {
        "showId": "uuid",
        "crewId": "uuid",
        "account": "uuid",
        "role": { "enum": ["chief", "crew"] },
        "inviteCode": "string?"
      },
      "relations": {
        "crew": { "rel": "crew", "column": "crewId" },
        "show": { "rel": "shows", "column": "showId" }
      }
    },
    "showInvites": {
      "columns": { "showId": "uuid", "code": "string" },
      "relations": { "show": { "rel": "shows", "column": "showId" } }
    },
    "shows": {
      "columns": {
        "name": "string",
        "venue": "string",
        "date": "string",
        "doors": "string",
        "chiefAccount": "uuid"
      },
      "relations": {
        "activity": { "reverse": "activity", "relation": "show" },
        "crew": { "reverse": "showCrew", "relation": "show" },
        "invites": { "reverse": "showInvites", "relation": "show" },
        "tasks": { "reverse": "tasks", "relation": "show" }
      }
    },
    "tasks": {
      "columns": {
        "showId": "uuid",
        "title": "string",
        "status": { "enum": ["todo", "doing", "done", "blocked"] },
        "assigneeId": "uuid?",
        "notes": { "type": "string", "optional": true, "merge": "text" },
        "rank": "float"
      },
      "relations": {
        "activity": { "reverse": "activity", "relation": "task" },
        "assignee": { "rel": "crew", "column": "assigneeId" },
        "comments": { "reverse": "comments", "relation": "task" },
        "show": { "rel": "shows", "column": "showId" }
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
