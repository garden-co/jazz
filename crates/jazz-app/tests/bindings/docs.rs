// jazz_app.rs: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Regenerate it after changing jazz.app.json.
#![cfg_attr(rustfmt, rustfmt_skip)]
#![allow(dead_code, clippy::all)]

/// The `jazz.app.json` these bindings were generated from.
pub const SOURCE: &str = "sha256:c96729bc448df827ab477792768535d62c0557ff8b31c0c55b23e9f794310e48";

/// A row of `docs`.
#[derive(Debug, Clone, PartialEq)]
pub struct Doc {
    pub id: ::jazz_app::ObjectId,
    pub title: ::std::string::String,
    pub embedding: ::std::vec::Vec<f32>,
    /// `draftEmbedding`.
    pub draft_embedding: ::std::option::Option<::std::vec::Vec<f32>>,
    pub seed: ::std::vec::Vec<f32>,
}

/// A new row of `docs`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct NewDoc {
    pub title: ::std::string::String,
    pub embedding: ::std::vec::Vec<f32>,
    /// `draftEmbedding`.
    pub draft_embedding: ::std::option::Option<::std::vec::Vec<f32>>,
    /// `None` stores the column's default.
    pub seed: ::std::option::Option<::std::vec::Vec<f32>>,
}

/// Changes to a row of `docs`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DocUpdate {
    pub title: ::std::option::Option<::std::string::String>,
    pub embedding: ::std::option::Option<::std::vec::Vec<f32>>,
    /// `draftEmbedding`.
    pub draft_embedding: ::std::option::Option<::std::option::Option<::std::vec::Vec<f32>>>,
    pub seed: ::std::option::Option<::std::vec::Vec<f32>>,
}

impl ::jazz_app::Table for Doc {
    const NAME: &'static str = "docs";
    type New = NewDoc;
    type Update = DocUpdate;

    fn decode(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self> {
        let title = row.take::<::std::string::String>("title")?;
        let embedding = row.take::<::std::vec::Vec<f32>>("embedding")?;
        let draft_embedding = row.take::<::std::option::Option<::std::vec::Vec<f32>>>("draftEmbedding")?;
        let seed = row.take::<::std::vec::Vec<f32>>("seed")?;
        ::std::result::Result::Ok(Self {
            id: row.id(),
            title,
            embedding,
            draft_embedding,
            seed,
        })
    }

    fn id(&self) -> ::jazz_app::ObjectId {
        self.id
    }
}

impl ::jazz_app::Insert for NewDoc {
    type Row = Doc;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::collections::HashMap<::std::string::String, ::jazz_app::jazz::tools::Value>> {
        let mut cells = ::std::collections::HashMap::new();
        let value = self.title;
        cells.insert("title".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.embedding;
        cells.insert("embedding".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.draft_embedding;
        cells.insert("draftEmbedding".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        if let ::std::option::Option::Some(value) = self.seed {
            cells.insert("seed".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        }
        ::std::result::Result::Ok(cells)
    }
}

impl ::jazz_app::Update for DocUpdate {
    type Row = Doc;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::vec::Vec<(::std::string::String, ::jazz_app::jazz::tools::Value)>> {
        let mut cells = ::std::vec::Vec::new();
        if let ::std::option::Option::Some(value) = self.title {
            cells.push(("title".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.embedding {
            cells.push(("embedding".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.draft_embedding {
            cells.push(("draftEmbedding".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.seed {
            cells.push(("seed".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        ::std::result::Result::Ok(cells)
    }
}

/// Queries, columns and relations of `docs`.
pub mod docs {
    #[allow(unused_imports)]
    use super::*;

    /// Every row of `docs`; narrow it with `filter`.
    pub fn query() -> ::jazz_app::Query<Doc> {
        ::jazz_app::Query::all()
    }

    /// Column `title`.
    pub fn title() -> ::jazz_app::Column<Doc, ::std::string::String> {
        ::jazz_app::Column::new("title")
    }

    /// Column `embedding`.
    pub fn embedding() -> ::jazz_app::Column<Doc, ::std::vec::Vec<f32>> {
        ::jazz_app::Column::new("embedding")
    }

    /// Column `draftEmbedding`.
    pub fn draft_embedding() -> ::jazz_app::Column<Doc, ::std::option::Option<::std::vec::Vec<f32>>> {
        ::jazz_app::Column::new("draftEmbedding")
    }

    /// Column `seed`.
    pub fn seed() -> ::jazz_app::Column<Doc, ::std::vec::Vec<f32>> {
        ::jazz_app::Column::new("seed")
    }
}

/// The app's `jazz.app.json`: its schema, every earlier version and the
/// migrations between them.
pub const APP: &str = r#"{
  "$schema": "https://jazz.tools/schemas/jazz.app.schema.json",
  "format": 1,
  "tables": {
    "docs": {
      "columns": {
        "title": "string",
        "embedding": { "vector": 4 },
        "draftEmbedding": { "vector": 2, "optional": true },
        "seed": { "vector": 2, "default": [0.5, -1] }
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
