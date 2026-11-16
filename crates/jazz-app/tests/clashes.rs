//! The bindings re-export `jazz-app-support` with a glob (`pub use
//! ::jazz_app_support::*`), so app code names everything through
//! `jazz_app`. Tables named like the crate's exports still compile, and the
//! generated items win: `errors` is `jazz_app::Error`, `queries`
//! `jazz_app::Query`, `configs` `jazz_app::Config`, `results`
//! `jazz_app::Result`, `jazz` `jazz_app::Jazz` and the module
//! `jazz_app::jazz`, `text` the module `jazz_app::text`.

#[path = "bindings/clashes.rs"]
mod jazz_app;

use std::any::type_name;

fn generated<T>() -> bool {
    type_name::<T>().starts_with("clashes::jazz_app::")
}

fn same<A: 'static, B: 'static>() -> bool {
    std::any::TypeId::of::<A>() == std::any::TypeId::of::<B>()
}

#[test]
fn generated_items_shadow_the_reexports() {
    assert!(
        generated::<jazz_app::Error>(),
        "{}",
        type_name::<jazz_app::Error>()
    );
    assert!(
        generated::<jazz_app::Query>(),
        "{}",
        type_name::<jazz_app::Query>()
    );
    assert!(
        generated::<jazz_app::Config>(),
        "{}",
        type_name::<jazz_app::Config>()
    );
    assert!(
        generated::<jazz_app::Result>(),
        "{}",
        type_name::<jazz_app::Result>()
    );
    assert!(
        generated::<jazz_app::Jazz>(),
        "{}",
        type_name::<jazz_app::Jazz>()
    );
    assert!(
        generated::<jazz_app::Text>(),
        "{}",
        type_name::<jazz_app::Text>()
    );

    // Their fields are the tables' columns.
    let _ = |error: jazz_app::Error| -> String { error.message };
    let _ = |query: jazz_app::Query| -> String { query.text };
    let _ = |config: jazz_app::Config| -> Option<String> { config.value };
    let _ = |result: jazz_app::Result| -> i32 { result.score };
    let _ = |jazz: jazz_app::Jazz| -> String { jazz.style };
    let _ = |text: jazz_app::Text| -> String { text.body };

    // The table modules shadow the crate's `jazz` and `text` re-exports.
    let _ = jazz_app::jazz::query();
    let _ = jazz_app::text::query();
    let _ = jazz_app::errors::query();
}

#[test]
fn the_rest_of_the_crate_comes_through() {
    assert!(same::<jazz_app::ObjectId, jazz_app_support::ObjectId>());
    assert!(same::<jazz_app::Storage, jazz_app_support::Storage>());
    assert!(same::<jazz_app::Timestamp, jazz_app_support::Timestamp>());
    assert!(same::<jazz_app::Order, jazz_app_support::Order>());
    assert!(same::<jazz_app::AppId, jazz_app_support::AppId>());
    let _ = jazz_app::created_at::<jazz_app::Error>();
    // `open` takes the crate's `Config`, and gives its `Jazz` and `Result`.
    let open = |config: jazz_app_support::Config| jazz_app::open(config);
    let _ = open;
}
