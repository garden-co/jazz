// jazz_app.rs: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Regenerate it after changing jazz.app.json.
#![cfg_attr(rustfmt, rustfmt_skip)]
#![allow(dead_code, clippy::all)]

/// The `jazz.app.json` these bindings were generated from.
pub const SOURCE: &str = "sha256:8f7a074feb1ed77724a88737165f527151b3702119575bf88728f029cd6c1c46";

/// A row of `places`.
#[derive(Debug, Clone, PartialEq)]
pub struct Place {
    pub id: ::jazz_app::ObjectId,
    pub name: ::std::string::String,
    pub location: ::jazz_app::Point,
    pub pickup: ::std::option::Option<::jazz_app::Point>,
    pub home: ::jazz_app::Point,
}

/// A new row of `places`.
#[derive(Debug, Clone, PartialEq)]
pub struct NewPlace {
    pub name: ::std::string::String,
    pub location: ::jazz_app::Point,
    pub pickup: ::std::option::Option<::jazz_app::Point>,
    /// `None` stores the column's default.
    pub home: ::std::option::Option<::jazz_app::Point>,
}

/// Changes to a row of `places`: a `None` field leaves its column as it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PlaceUpdate {
    pub name: ::std::option::Option<::std::string::String>,
    pub location: ::std::option::Option<::jazz_app::Point>,
    pub pickup: ::std::option::Option<::std::option::Option<::jazz_app::Point>>,
    pub home: ::std::option::Option<::jazz_app::Point>,
}

impl ::jazz_app::Table for Place {
    const NAME: &'static str = "places";
    type New = NewPlace;
    type Update = PlaceUpdate;

    fn decode(row: &mut ::jazz_app::Fields<'_>) -> ::jazz_app::Result<Self> {
        let name = row.take::<::std::string::String>("name")?;
        let location = row.take::<::jazz_app::Point>("location")?;
        let pickup = row.take::<::std::option::Option<::jazz_app::Point>>("pickup")?;
        let home = row.take::<::jazz_app::Point>("home")?;
        ::std::result::Result::Ok(Self {
            id: row.id(),
            name,
            location,
            pickup,
            home,
        })
    }

    fn id(&self) -> ::jazz_app::ObjectId {
        self.id
    }
}

impl ::jazz_app::Insert for NewPlace {
    type Row = Place;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::collections::HashMap<::std::string::String, ::jazz_app::jazz::tools::Value>> {
        let mut cells = ::std::collections::HashMap::new();
        let value = self.name;
        cells.insert("name".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.location;
        cells.insert("location".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        let value = self.pickup;
        cells.insert("pickup".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        if let ::std::option::Option::Some(value) = self.home {
            cells.insert("home".to_owned(), ::jazz_app::ColumnValue::into_value(value));
        }
        ::std::result::Result::Ok(cells)
    }
}

impl ::jazz_app::Update for PlaceUpdate {
    type Row = Place;

    fn into_cells(
        self,
        _implementations: &::jazz_app::Implementations,
    ) -> ::jazz_app::Result<::std::vec::Vec<(::std::string::String, ::jazz_app::jazz::tools::Value)>> {
        let mut cells = ::std::vec::Vec::new();
        if let ::std::option::Option::Some(value) = self.name {
            cells.push(("name".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.location {
            cells.push(("location".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.pickup {
            cells.push(("pickup".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        if let ::std::option::Option::Some(value) = self.home {
            cells.push(("home".to_owned(), ::jazz_app::ColumnValue::into_value(value)));
        }
        ::std::result::Result::Ok(cells)
    }
}

/// Queries, columns and relations of `places`.
pub mod places {
    #[allow(unused_imports)]
    use super::*;

    /// Every row of `places`; narrow it with `filter`.
    pub fn query() -> ::jazz_app::Query<Place> {
        ::jazz_app::Query::all()
    }

    /// Column `name`.
    pub fn name() -> ::jazz_app::Column<Place, ::std::string::String> {
        ::jazz_app::Column::new("name")
    }

    /// Column `location`.
    pub fn location() -> ::jazz_app::Column<Place, ::jazz_app::Point> {
        ::jazz_app::Column::new("location")
    }

    /// Column `pickup`.
    pub fn pickup() -> ::jazz_app::Column<Place, ::std::option::Option<::jazz_app::Point>> {
        ::jazz_app::Column::new("pickup")
    }

    /// Column `home`.
    pub fn home() -> ::jazz_app::Column<Place, ::jazz_app::Point> {
        ::jazz_app::Column::new("home")
    }
}

/// The app's `jazz.app.json`: its schema, every earlier version and the
/// migrations between them.
pub const APP: &str = r#"{
  "$schema": "https://jazz.tools/schemas/jazz.app.schema.json",
  "format": 1,
  "tables": {
    "places": {
      "columns": {
        "name": "string",
        "location": "point",
        "pickup": "point?",
        "home": { "type": "point", "default": [13.405, 52.52] }
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
