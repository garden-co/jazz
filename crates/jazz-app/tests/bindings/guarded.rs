// jazz_app.rs: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 4. Regenerate it after changing jazz.app.json.
#![cfg_attr(rustfmt, rustfmt_skip)]
#![allow(dead_code, unused_variables, clippy::all)]

/// The `jazz.app.json` these bindings were generated from.
pub const SOURCE: &str = "sha256:2585de3597efe8dff21e520492a7377766afd44796c9f71d84782cf16729a6ff";

/// A row of `notes`.
#[derive(Debug, Clone, PartialEq)]
pub struct Note {
    pub id: ::jazz_app_support::ObjectId,
    pub owner: ::jazz_app_support::ObjectId,
    pub body: ::std::string::String,
}

/// A new row of `notes`: [`NewNote::new`] takes the columns it requires.
#[derive(Debug, Clone, PartialEq)]
pub struct NewNote {
    pub owner: ::jazz_app_support::ObjectId,
    pub body: ::std::string::String,
}

impl NewNote {
    /// A new row with the columns it requires, in this order: `owner`, `body`. The others
    /// are left out; set them with `NewNote { column: …, ..NewNote::new(…) }`.
    pub fn new(owner: impl ::std::convert::Into<::jazz_app_support::ObjectId>, body: impl ::std::convert::Into<::std::string::String>) -> Self {
        Self {
            owner: owner.into(),
            body: body.into(),
        }
    }
}

/// Changes to a row of `notes`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct NoteUpdate {
    pub owner: ::std::option::Option<::jazz_app_support::ObjectId>,
    pub body: ::std::option::Option<::std::string::String>,
}

impl ::jazz_app_support::Table for Note {
    const NAME: &'static str = "notes";
    const COLUMNS: &'static [&'static str] = &["owner", "body"];
    type New = NewNote;
    type Update = NoteUpdate;

    fn decode(row: &mut ::jazz_app_support::Fields<'_>) -> ::jazz_app_support::Result<Self> {
        let owner = row.take::<::jazz_app_support::ObjectId>("owner")?;
        let body = row.take::<::std::string::String>("body")?;
        ::std::result::Result::Ok(Self {
            id: row.id(),
            owner,
            body,
        })
    }

    fn id(&self) -> ::jazz_app_support::ObjectId {
        self.id
    }
}

impl ::jazz_app_support::Insert for NewNote {
    type Row = Note;

    fn into_cells(
        self,
        _implementations: &::jazz_app_support::Implementations,
    ) -> ::jazz_app_support::Result<::std::collections::HashMap<::std::string::String, ::jazz_app_support::jazz::tools::Value>> {
        let mut cells = ::std::collections::HashMap::new();
        let value = self.owner;
        cells.insert("owner".to_owned(), ::jazz_app_support::ColumnValue::into_value(value));
        let value = self.body;
        cells.insert("body".to_owned(), ::jazz_app_support::ColumnValue::into_value(value));
        ::std::result::Result::Ok(cells)
    }
}

impl ::jazz_app_support::Update for NoteUpdate {
    type Row = Note;

    fn into_cells(
        self,
        _implementations: &::jazz_app_support::Implementations,
    ) -> ::jazz_app_support::Result<::std::vec::Vec<(::std::string::String, ::jazz_app_support::jazz::tools::Value)>> {
        let mut cells = ::std::vec::Vec::new();
        if let ::std::option::Option::Some(value) = self.owner {
            cells.push(("owner".to_owned(), ::jazz_app_support::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.body {
            cells.push(("body".to_owned(), ::jazz_app_support::ColumnValue::into_value(value)));
        }
        ::std::result::Result::Ok(cells)
    }
}

/// Queries, columns and relations of `notes`.
pub mod notes {
    #[allow(unused_imports)]
    use super::*;

    /// Every row of `notes`; narrow it with `filter`.
    pub fn query() -> ::jazz_app_support::Query<Note> {
        ::jazz_app_support::Query::all()
    }

    /// Every accepted version of `notes`'s rows; bound it to rows with `id` or `id().is_in`.
    pub fn history() -> ::jazz_app_support::Query<::jazz_app_support::Version<Note>> {
        ::jazz_app_support::Query::history()
    }

    /// The row's id. (`::jazz_app_support::created_at()` and `updated_at()` are columns of every table too.)
    pub fn id() -> ::jazz_app_support::Column<Note, ::jazz_app_support::ObjectId> {
        ::jazz_app_support::id()
    }

    /// Column `owner`.
    pub fn owner() -> ::jazz_app_support::Column<Note, ::jazz_app_support::ObjectId> {
        ::jazz_app_support::Column::new("owner")
    }

    /// Column `body`.
    pub fn body() -> ::jazz_app_support::Column<Note, ::std::string::String> {
        ::jazz_app_support::Column::new("body")
    }
}

/// The app's `jazz.app.json`: its schema, every earlier version and the
/// migrations between them.
pub const APP: &str = r#"{
  "$schema": "https://jazz.tools/schemas/jazz.app.schema.json",
  "format": 1,
  "tables": {
    "notes": { "columns": { "owner": "uuid", "body": "string" } }
  }
}
"#;

/// Opens the app. A backend (`Config::backend`, with its
/// `jazz.permissions.json`) first publishes the app's permissions bundle
/// and pins the session it acts in to it.
pub async fn open(config: ::jazz_app_support::Config) -> ::jazz_app_support::Result<::jazz_app_support::Jazz> {
    ::jazz_app_support::Jazz::open(::jazz_app_support::AppFiles::new(APP, ::std::option::Option::None), config).await
}
