// jazz_app.rs: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Regenerate it after changing jazz.app.json.
#![cfg_attr(rustfmt, rustfmt_skip)]
#![allow(dead_code, clippy::all)]

/// The `jazz.app.json` these bindings were generated from.
pub const SOURCE: &str = "sha256:779810bd3acad3798d6db0a7f6fa9370c41182ced688951eadf37f0ce95d86e3";

/// A row of `notes`.
#[derive(Debug, Clone, PartialEq)]
pub struct Note {
    pub id: ::jazz_app::ObjectId,
    pub text: ::std::string::String,
}

/// A new row of `notes`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct NewNote {
    pub text: ::std::string::String,
}

/// Changes to a row of `notes`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct NoteUpdate {
    pub text: ::std::option::Option<::std::string::String>,
}

impl ::jazz_app::Table for Note {
    const NAME: &'static str = "notes";
    type New = NewNote;
    type Update = NoteUpdate;

    fn decode(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self> {
        let text = row.take::<::std::string::String>("text")?;
        ::std::result::Result::Ok(Self {
            id: row.id(),
            text,
        })
    }

    fn id(&self) -> ::jazz_app::ObjectId {
        self.id
    }
}

impl ::jazz_app::Insert for NewNote {
    type Row = Note;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::collections::HashMap<::std::string::String, ::jazz_app::jazz::tools::Value>> {
        let mut cells = ::std::collections::HashMap::new();
        let value = self.text;
        cells.insert("text".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        ::std::result::Result::Ok(cells)
    }
}

impl ::jazz_app::Update for NoteUpdate {
    type Row = Note;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::vec::Vec<(::std::string::String, ::jazz_app::jazz::tools::Value)>> {
        let mut cells = ::std::vec::Vec::new();
        if let ::std::option::Option::Some(value) = self.text {
            cells.push(("text".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        ::std::result::Result::Ok(cells)
    }
}

/// Queries, columns and relations of `notes`.
pub mod notes {
    #[allow(unused_imports)]
    use super::*;

    /// Every row of `notes`; narrow it with `filter`.
    pub fn query() -> ::jazz_app::Query<Note> {
        ::jazz_app::Query::all()
    }

    /// Column `text`.
    pub fn text() -> ::jazz_app::Column<Note, ::std::string::String> {
        ::jazz_app::Column::new("text")
    }
}

/// The app's `jazz.app.json`: its schema, every earlier version and the
/// migrations between them.
pub const APP: &str = r#"{
  "$schema": "https://jazz.tools/schemas/jazz.app.schema.json",
  "format": 1,
  "tables": { "notes": { "columns": { "text": "string" } } }
}
"#;

/// Opens the app. A backend (`Config::backend`, with its
/// `jazz.permissions.json`) first publishes the app's permissions bundle
/// and pins the session it acts in to it.
pub async fn open(config: ::jazz_app::Config) -> ::jazz_app::Result<::jazz_app::Jazz> {
    ::jazz_app::Jazz::open(::jazz_app::AppFiles::new(APP, ::std::option::Option::None), config).await
}
