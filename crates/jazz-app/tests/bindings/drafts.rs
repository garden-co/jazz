// jazz_app.rs: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 4. Regenerate it after changing jazz.app.json.
#![cfg_attr(rustfmt, rustfmt_skip)]
#![allow(dead_code, unused_variables, clippy::all)]

/// The `jazz.app.json` these bindings were generated from.
pub const SOURCE: &str = "sha256:a01d4647f906d3c4c8b239f5bda2e5a83fecb68fb596cde909ff4af0d71a80a2";

/// A row of `documents`.
#[derive(Debug, Clone, PartialEq)]
pub struct Document {
    pub id: ::jazz_app::ObjectId,
    pub branch: ::std::string::String,
    pub title: ::std::string::String,
}

/// A new row of `documents`: [`NewDocument::new`] takes the columns it requires.
#[derive(Debug, Clone, PartialEq)]
pub struct NewDocument {
    pub branch: ::std::string::String,
    pub title: ::std::string::String,
}

impl NewDocument {
    /// A new row with the columns it requires, in this order: `branch`, `title`. The others
    /// are left out; set them with `NewDocument { column: …, ..NewDocument::new(…) }`.
    pub fn new(branch: impl ::std::convert::Into<::std::string::String>, title: impl ::std::convert::Into<::std::string::String>) -> Self {
        Self {
            branch: branch.into(),
            title: title.into(),
        }
    }
}

/// Changes to a row of `documents`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DocumentUpdate {
    pub branch: ::std::option::Option<::std::string::String>,
    pub title: ::std::option::Option<::std::string::String>,
}

impl ::jazz_app::Table for Document {
    const NAME: &'static str = "documents";
    const COLUMNS: &'static [&'static str] = &["branch", "title"];
    type New = NewDocument;
    type Update = DocumentUpdate;

    fn decode(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self> {
        let branch = row.take::<::std::string::String>("branch")?;
        let title = row.take::<::std::string::String>("title")?;
        ::std::result::Result::Ok(Self {
            id: row.id(),
            branch,
            title,
        })
    }

    fn id(&self) -> ::jazz_app::ObjectId {
        self.id
    }
}

impl ::jazz_app::Insert for NewDocument {
    type Row = Document;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::collections::HashMap<::std::string::String, ::jazz_app::jazz::tools::Value>> {
        let mut cells = ::std::collections::HashMap::new();
        let value = self.branch;
        cells.insert("branch".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.title;
        cells.insert("title".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        ::std::result::Result::Ok(cells)
    }
}

impl ::jazz_app::Update for DocumentUpdate {
    type Row = Document;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::vec::Vec<(::std::string::String, ::jazz_app::jazz::tools::Value)>> {
        let mut cells = ::std::vec::Vec::new();
        if let ::std::option::Option::Some(value) = self.branch {
            cells.push(("branch".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.title {
            cells.push(("title".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        ::std::result::Result::Ok(cells)
    }
}

/// Queries, columns and relations of `documents`.
pub mod documents {
    #[allow(unused_imports)]
    use super::*;

    /// Every row of `documents`; narrow it with `filter`.
    pub fn query() -> ::jazz_app::Query<Document> {
        ::jazz_app::Query::all()
    }

    /// Every accepted version of `documents`'s rows; bound it to rows with `id` or `id().is_in`.
    pub fn history() -> ::jazz_app::Query<::jazz_app::Version<Document>> {
        ::jazz_app::Query::history()
    }

    /// The row's id. (`::jazz_app::created_at()` and `updated_at()` are columns of every table too.)
    pub fn id() -> ::jazz_app::Column<Document, ::jazz_app::ObjectId> {
        ::jazz_app::id()
    }

    /// Column `branch`.
    pub fn branch() -> ::jazz_app::Column<Document, ::std::string::String> {
        ::jazz_app::Column::new("branch")
    }

    /// Column `title`.
    pub fn title() -> ::jazz_app::Column<Document, ::std::string::String> {
        ::jazz_app::Column::new("title")
    }
}

/// A row of `drafts`.
#[derive(Debug, Clone, PartialEq)]
pub struct Draft {
    pub id: ::jazz_app::ObjectId,
    pub name: ::std::string::String,
    pub base: ::std::option::Option<::std::string::String>,
    pub base_snapshot_ref: ::std::option::Option<::std::string::String>,
}

/// A new row of `drafts`: [`NewDraft::new`] takes the columns it requires.
#[derive(Debug, Clone, PartialEq)]
pub struct NewDraft {
    pub name: ::std::string::String,
    pub base: ::std::option::Option<::std::string::String>,
    pub base_snapshot_ref: ::std::option::Option<::std::string::String>,
}

impl NewDraft {
    /// A new row with the columns it requires, in this order: `name`. The others
    /// are left out; set them with `NewDraft { column: …, ..NewDraft::new(…) }`.
    pub fn new(name: impl ::std::convert::Into<::std::string::String>) -> Self {
        Self {
            name: name.into(),
            base: ::std::option::Option::None,
            base_snapshot_ref: ::std::option::Option::None,
        }
    }
}

/// Changes to a row of `drafts`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DraftUpdate {
    pub name: ::std::option::Option<::std::string::String>,
    pub base: ::std::option::Option<::std::option::Option<::std::string::String>>,
    pub base_snapshot_ref: ::std::option::Option<::std::option::Option<::std::string::String>>,
}

impl ::jazz_app::Table for Draft {
    const NAME: &'static str = "drafts";
    const COLUMNS: &'static [&'static str] = &["name", "base", "base_snapshot_ref"];
    type New = NewDraft;
    type Update = DraftUpdate;

    fn decode(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self> {
        let name = row.take::<::std::string::String>("name")?;
        let base = row.take::<::std::option::Option<::std::string::String>>("base")?;
        let base_snapshot_ref = row.take::<::std::option::Option<::std::string::String>>("base_snapshot_ref")?;
        ::std::result::Result::Ok(Self {
            id: row.id(),
            name,
            base,
            base_snapshot_ref,
        })
    }

    fn id(&self) -> ::jazz_app::ObjectId {
        self.id
    }
}

impl ::jazz_app::Insert for NewDraft {
    type Row = Draft;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::collections::HashMap<::std::string::String, ::jazz_app::jazz::tools::Value>> {
        let mut cells = ::std::collections::HashMap::new();
        let value = self.name;
        cells.insert("name".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.base;
        cells.insert("base".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.base_snapshot_ref;
        cells.insert("base_snapshot_ref".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        ::std::result::Result::Ok(cells)
    }
}

impl ::jazz_app::Update for DraftUpdate {
    type Row = Draft;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::vec::Vec<(::std::string::String, ::jazz_app::jazz::tools::Value)>> {
        let mut cells = ::std::vec::Vec::new();
        if let ::std::option::Option::Some(value) = self.name {
            cells.push(("name".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.base {
            cells.push(("base".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.base_snapshot_ref {
            cells.push(("base_snapshot_ref".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        ::std::result::Result::Ok(cells)
    }
}

/// Queries, columns and relations of `drafts`.
pub mod drafts {
    #[allow(unused_imports)]
    use super::*;

    /// Every row of `drafts`; narrow it with `filter`.
    pub fn query() -> ::jazz_app::Query<Draft> {
        ::jazz_app::Query::all()
    }

    /// Every accepted version of `drafts`'s rows; bound it to rows with `id` or `id().is_in`.
    pub fn history() -> ::jazz_app::Query<::jazz_app::Version<Draft>> {
        ::jazz_app::Query::history()
    }

    /// The row's id. (`::jazz_app::created_at()` and `updated_at()` are columns of every table too.)
    pub fn id() -> ::jazz_app::Column<Draft, ::jazz_app::ObjectId> {
        ::jazz_app::id()
    }

    /// Column `name`.
    pub fn name() -> ::jazz_app::Column<Draft, ::std::string::String> {
        ::jazz_app::Column::new("name")
    }

    /// Column `base`.
    pub fn base() -> ::jazz_app::Column<Draft, ::std::option::Option<::std::string::String>> {
        ::jazz_app::Column::new("base")
    }

    /// Column `base_snapshot_ref`.
    pub fn base_snapshot_ref() -> ::jazz_app::Column<Draft, ::std::option::Option<::std::string::String>> {
        ::jazz_app::Column::new("base_snapshot_ref")
    }
}

/// The app's `jazz.app.json`: its schema, every earlier version and the
/// migrations between them.
pub const APP: &str = r#"{
  "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
  "tables": {
    "drafts": {
      "columns": { "name": "string", "base": "string?", "base_snapshot_ref": "string?" }
    },
    "documents": {
      "columns": { "branch": "string", "title": "string" },
      "branchBy": ["branch"]
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
