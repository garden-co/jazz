//! `migrations create`: the bindings record the version the app last
//! generated against; editing `tables` and creating a migration moves that
//! version into `history` and writes the step from it, with a TODO wherever
//! only the author can decide.

use jazz::tools::deployment::artifact::{self, json};
use jazz_codegen::bindings::{self, Staleness};
use jazz_codegen::migrations::{Created, create};
use jazz_codegen::{App, Lang, generate};

fn file(tables: &str) -> String {
    format!("{{ \"format\": 1, \"tables\": {tables} }}")
}

/// Bindings generated from `before`, then `migrations create` on `after`.
fn migrate(before: &str, after: &str) -> Created {
    let bindings = generate(Lang::TypeScript, file(before).as_bytes()).unwrap();
    create(
        Lang::TypeScript,
        file(after).as_bytes(),
        Some(&bindings),
        "change",
    )
    .unwrap()
}

/// The migration the stub wrote, as canonical text.
fn migration(created: &Created) -> String {
    let root = json::parse(created.app.as_bytes()).unwrap();
    let Some(json::Json::Array(items)) = root.get("migrations") else {
        panic!("no migrations in {}", created.app)
    };
    json::print(items.last().unwrap())
}

fn refused(before: &str, after: &str) -> String {
    let bindings = generate(Lang::TypeScript, file(before).as_bytes()).unwrap();
    create(
        Lang::TypeScript,
        file(after).as_bytes(),
        Some(&bindings),
        "change",
    )
    .expect_err("must be refused")
    .to_string()
}

const TODOS: &str = r#"{ "todos": { "columns": { "title": "string", "done": "boolean" } } }"#;

#[test]
fn a_new_version_moves_the_old_one_into_history_and_loads() {
    let created = migrate(
        TODOS,
        r#"{ "todos": { "columns": { "title": "string", "done": "boolean", "note": "string?" } } }"#,
    );
    assert!(created.changed);
    assert!(created.todos.is_empty(), "{:?}", created.todos);
    let app = App::load(created.app.as_bytes()).unwrap();
    assert_eq!(app.compiled.schemas.len(), 2);
    let root = json::parse(created.app.as_bytes()).unwrap();
    let history = root.get("history").unwrap().as_object().unwrap();
    let old = App::load(file(TODOS).as_bytes()).unwrap().compiled.current;
    assert_eq!(history[0].0, old.to_string()[..12]);
    assert_eq!(
        migration(&created),
        format!(
            "{{\n  \"name\": \"change\",\n  \"from\": \"{}\",\n  \"migrate\": {{ \"todos\": {{ \"note\": {{ \"add\": \"string\", \"default\": null }} }} }}\n}}\n",
            &old.to_string()[..12]
        )
    );
    // The new bindings fit; the old ones say a migration exists, so only
    // regenerating is needed.
    let old_bindings = generate(Lang::TypeScript, file(TODOS).as_bytes()).unwrap();
    assert_eq!(
        bindings::staleness(&app, Lang::TypeScript, Some(&old_bindings)).unwrap(),
        Some(Staleness::OutOfDate)
    );
    // Running it again changes nothing.
    let again = create(
        Lang::TypeScript,
        created.app.as_bytes(),
        Some(&old_bindings),
        "change",
    )
    .unwrap();
    assert!(!again.changed);
    assert_eq!(again.app, created.app);
}

#[test]
fn an_edit_without_a_migration_is_caught_against_the_bindings() {
    let bindings = generate(Lang::TypeScript, file(TODOS).as_bytes()).unwrap();
    let edited = App::load(
        file(
            r#"{ "todos": { "columns": { "title": "string", "done": "boolean", "n": "int?" } } }"#,
        )
        .as_bytes(),
    )
    .unwrap();
    assert_eq!(
        bindings::staleness(&edited, Lang::TypeScript, Some(&bindings)).unwrap(),
        Some(Staleness::SchemaChanged)
    );
    assert!(
        Staleness::SchemaChanged
            .message(Lang::TypeScript)
            .contains("migrations create --name")
    );
    // Without bindings there is nothing to migrate from.
    let created = create(Lang::TypeScript, file(TODOS).as_bytes(), None, "x").unwrap();
    assert!(!created.changed);
    // A change outside the engine schema (a relation, a computed field)
    // only needs regenerating.
    let relabelled = App::load(
        file(r#"{ "todos": { "columns": { "title": "string", "done": "boolean" }, "computed": { "shout": { "type": "string", "from": ["title"] } } } }"#)
            .as_bytes(),
    )
    .unwrap();
    assert_eq!(
        bindings::staleness(&relabelled, Lang::TypeScript, Some(&bindings)).unwrap(),
        Some(Staleness::OutOfDate)
    );
}

/// #5494: an edit that only adds or changes single-column indexes needs no
/// migration (the runtime version stays the same): the bindings are only out
/// of date. A composite index is part of the version, and any other edit
/// needs a migration too.
#[test]
fn an_index_only_edit_needs_no_migration() {
    let bindings = generate(Lang::TypeScript, file(TODOS).as_bytes()).unwrap();
    for indexes in [r#"["title"]"#, r#"["title", "done"]"#] {
        let indexed = App::load(
            file(&format!(
                r#"{{ "todos": {{ "columns": {{ "title": "string", "done": "boolean" }}, "indexes": {indexes} }} }}"#
            ))
            .as_bytes(),
        )
        .unwrap();
        assert_ne!(
            indexed.compiled.current,
            App::load(file(TODOS).as_bytes()).unwrap().compiled.current,
            "an index changes the hash"
        );
        assert_eq!(
            bindings::staleness(&indexed, Lang::TypeScript, Some(&bindings)).unwrap(),
            Some(Staleness::OutOfDate),
            "{indexes}"
        );
    }
    // `migrations create` agrees: it writes nothing and says why.
    let created = migrate(
        TODOS,
        r#"{ "todos": { "columns": { "title": "string", "done": "boolean" }, "indexes": ["title"] } }"#,
    );
    assert!(!created.changed);
    assert!(created.todos.is_empty());
    assert_eq!(
        created.note.as_deref(),
        Some(
            "only single-column indexes changed: no migration needed; run `npx jazz-tools generate`"
        )
    );
    // A composite index changes the version: still a migration.
    let composite = App::load(
        file(r#"{ "todos": { "columns": { "title": "string", "done": "boolean" }, "indexes": [["done", "title"]] } }"#)
            .as_bytes(),
    )
    .unwrap();
    assert_eq!(
        bindings::staleness(&composite, Lang::TypeScript, Some(&bindings)).unwrap(),
        Some(Staleness::SchemaChanged)
    );
    let indexed_and_more = App::load(
        file(r#"{ "todos": { "columns": { "title": "string", "done": "boolean", "n": "int?" }, "indexes": ["title"] } }"#)
            .as_bytes(),
    )
    .unwrap();
    assert_eq!(
        bindings::staleness(&indexed_and_more, Lang::TypeScript, Some(&bindings)).unwrap(),
        Some(Staleness::SchemaChanged)
    );
}

#[test]
fn a_column_renamed_is_left_to_the_author() {
    let created = migrate(
        TODOS,
        r#"{ "todos": { "columns": { "name": "string", "done": "boolean" } } }"#,
    );
    assert_eq!(
        created.todos,
        vec![
            "migrations[0].migrate.todos.name".to_string(),
            "migrations[0].migrate.todos.title".to_string()
        ]
    );
    let text = migration(&created);
    assert!(text.contains(r#"added column `name` may be a rename of `title`. To keep its values, replace this with { \"renameFrom\": \"title\" }"#), "{text}");
    assert!(
        text.contains("removed column `title` may have been renamed"),
        "{text}"
    );
    // The loader refuses the stub until both are resolved, naming each.
    let error = App::load(created.app.as_bytes()).unwrap_err().to_string();
    assert!(error.contains("2 unresolved TODOs"), "{error}");
    assert!(
        error.contains("migrations[0].migrate.todos.name: TODO: added column"),
        "{error}"
    );
    // Resolved as a rename, it loads.
    let mut root = json::parse(created.app.as_bytes()).unwrap();
    resolve(&mut root, &["migrations", "0", "migrate", "todos"], |ops| {
        ops.retain(|(k, _)| k != "title");
        ops[0].1 = json::Json::object(vec![("renameFrom", json::Json::string("title"))]);
    });
    App::load(json::print(&root).as_bytes()).unwrap();
}

fn resolve(root: &mut json::Json, path: &[&str], f: impl FnOnce(&mut Vec<(String, json::Json)>)) {
    let mut at = root;
    for key in path {
        at = match at {
            json::Json::Object(entries) => {
                &mut entries.iter_mut().find(|(k, _)| k == key).unwrap().1
            }
            json::Json::Array(items) => &mut items[key.parse::<usize>().unwrap()],
            _ => panic!("no {key}"),
        };
    }
    let json::Json::Object(entries) = at else {
        panic!("not an object")
    };
    f(entries);
}

#[test]
fn an_added_column_takes_its_default_or_asks_for_one() {
    let created = migrate(
        TODOS,
        r#"{ "todos": { "columns": { "title": "string", "done": "boolean", "rank": { "type": "int", "default": 3 }, "owner": "uuid" } } }"#,
    );
    let text = migration(&created);
    assert!(
        text.contains(r#""rank": { "add": "int", "default": 3 }"#),
        "{text}"
    );
    assert!(
        text.contains(r#""add": "uuid","#)
            && text.contains(r#""default": "TODO: added required column `owner` needs the value existing rows get; replace this with it.""#),
        "{text}"
    );
    assert_eq!(
        created.todos,
        vec!["migrations[0].migrate.todos.owner.default".to_string()]
    );
}

#[test]
fn a_dropped_column_keeps_a_backwards_default_or_asks_for_one() {
    let created = migrate(
        r#"{ "todos": { "columns": { "title": "string", "done": "boolean", "note": "string?", "rank": "int" } } }"#,
        TODOS,
    );
    let text = migration(&created);
    assert!(
        text.contains(r#""note": { "drop": "string", "backwardsDefault": null }"#),
        "{text}"
    );
    assert!(
        text.contains(r#""rank": {"#) && text.contains("removed required column `rank`"),
        "{text}"
    );
    assert_eq!(
        created.todos,
        vec!["migrations[0].migrate.todos.rank.backwardsDefault".to_string()]
    );
}

#[test]
fn an_enum_gaining_variants_at_the_end_needs_no_step() {
    let created = migrate(
        r#"{ "todos": { "columns": { "status": { "enum": ["open", "done"] } } } }"#,
        r#"{ "todos": { "columns": { "status": { "enum": ["open", "done", "archived"] } } } }"#,
    );
    assert!(created.todos.is_empty(), "{:?}", created.todos);
    assert!(
        migration(&created).contains(r#""$comment": "`status` gains enum variant \"archived\": existing rows are unchanged, so it needs no step.""#),
        "{}",
        migration(&created)
    );
    // Reordering or dropping variants is refused, saying what works.
    let error = refused(
        r#"{ "todos": { "columns": { "status": { "enum": ["open", "done"] } } } }"#,
        r#"{ "todos": { "columns": { "status": { "enum": ["done", "open"] } } } }"#,
    );
    assert!(
        error.contains(
            "Changing the order of the enum variants of column `todos.status` is not supported"
        ),
        "{error}"
    );
    let error = refused(
        r#"{ "todos": { "columns": { "status": { "enum": ["open", "done"] } } } }"#,
        r#"{ "todos": { "columns": { "status": { "enum": ["open"] } } } }"#,
    );
    assert!(error.contains("it no longer declares \"done\""), "{error}");
}

#[test]
fn type_and_optionality_changes_are_not_supported_yet() {
    let error = refused(
        TODOS,
        r#"{ "todos": { "columns": { "title": "string?", "done": "boolean" } } }"#,
    );
    assert!(
        error.contains(
            "Changing column `todos.title` from required to optional is not supported yet"
        ),
        "{error}"
    );
    let error = refused(
        TODOS,
        r#"{ "todos": { "columns": { "title": "int", "done": "boolean" } } }"#,
    );
    assert!(
        error.contains("Changing the type of column `todos.title` is not supported yet"),
        "{error}"
    );
    let bindings = generate(Lang::TypeScript, file(TODOS).as_bytes()).unwrap();
    let error = create(
        Lang::TypeScript,
        file(r#"{ "todos": { "columns": { "title": "int", "done": "boolean" } } }"#).as_bytes(),
        Some(&bindings),
        "x",
    )
    .unwrap_err();
    assert!(error.is_unsupported());
}

#[test]
fn tables_are_created_dropped_and_renamed() {
    let created = migrate(
        TODOS,
        r#"{ "todos": { "columns": { "title": "string", "done": "boolean" } }, "tags": { "columns": { "label": "string" } } }"#,
    );
    assert!(
        migration(&created).contains(r#""createTables": ["tags"]"#),
        "{}",
        migration(&created)
    );
    assert!(created.todos.is_empty());
    App::load(created.app.as_bytes()).unwrap();

    let created = migrate(
        r#"{ "todos": { "columns": { "title": "string", "done": "boolean" } }, "tags": { "columns": { "label": "string" } } }"#,
        TODOS,
    );
    assert!(
        migration(&created).contains(r#""dropTables": ["tags"]"#),
        "{}",
        migration(&created)
    );
    App::load(created.app.as_bytes()).unwrap();

    let created = migrate(
        TODOS,
        r#"{ "tasks": { "columns": { "title": "string", "done": "boolean" } } }"#,
    );
    assert!(
        migration(&created).contains(r#""renameTables": { "tasks": "todos" }"#),
        "{}",
        migration(&created)
    );
    App::load(created.app.as_bytes()).unwrap();
}

#[test]
fn a_change_that_keeps_every_row_needs_an_empty_step() {
    for (after, what) in [
        (
            r#"{ "todos": { "columns": { "title": "string", "done": "boolean" }, "history": "latest" } }"#,
            "history retention",
        ),
        // A composite index is part of the runtime version, so it keeps its
        // empty step; single-column indexes need no migration at all (#5494,
        // see `an_index_only_edit_needs_no_migration`).
        (
            r#"{ "todos": { "columns": { "title": "string", "done": "boolean" }, "indexes": [["done", "title"]] } }"#,
            "indexes",
        ),
        (
            r#"{ "todos": { "columns": { "title": { "type": "string", "merge": "text" }, "done": "boolean" } } }"#,
            "merge strategies",
        ),
        (
            r#"{ "todos": { "columns": { "done": "boolean", "title": "string" } } }"#,
            "the column order",
        ),
    ] {
        let created = migrate(TODOS, after);
        assert!(created.todos.is_empty(), "{what}: {:?}", created.todos);
        let text = migration(&created);
        assert!(
            text.contains(&format!(
                "\"todos\": {{ \"$comment\": \"Rows pass unchanged: only {what} changed.\" }}"
            )),
            "{text}"
        );
        App::load(created.app.as_bytes()).unwrap_or_else(|e| panic!("{what}: {e}"));
    }
}

#[test]
fn bindings_that_arent_the_generators_are_refused() {
    let error = create(
        Lang::TypeScript,
        file(TODOS).as_bytes(),
        Some("export const app = {};"),
        "x",
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("not a module jazz-tools generated"),
        "{error}"
    );
    let bindings = generate(Lang::TypeScript, file(TODOS).as_bytes()).unwrap();
    let edited = bindings.replace("\"current\": \"", "\"current\": \"00");
    let error = create(Lang::TypeScript, file(TODOS).as_bytes(), Some(&edited), "x").unwrap_err();
    assert!(error.to_string().contains("schema.current"), "{error}");
}

#[test]
fn the_written_file_stays_canonical_and_keeps_the_rest() {
    let before = r#"{ "todos": { "columns": { "title": "string", "done": "boolean" }, "$comment": "kept" } }"#;
    let after = r#"{ "todos": { "columns": { "title": "string", "done": "boolean", "n": "int?" }, "$comment": "kept" } }"#;
    let created = migrate(before, after);
    assert!(created.app.contains("\"$comment\": \"kept\""));
    assert_eq!(
        created.app,
        json::print(&json::parse(created.app.as_bytes()).unwrap())
    );
    assert!(artifact::todos(&json::parse(created.app.as_bytes()).unwrap()).is_empty());
}

#[test]
fn a_renamed_table_may_reorder_its_columns() {
    let created = migrate(
        TODOS,
        r#"{ "tasks": { "columns": { "done": "boolean", "title": "string" } } }"#,
    );
    assert!(
        migration(&created).contains(r#""renameTables": { "tasks": "todos" }"#),
        "{}",
        migration(&created)
    );
}

#[test]
fn bindings_refuse_a_version_no_migration_leads_from() {
    let stranded = r#"{ "format": 1,
        "tables": { "todos": { "columns": { "title": "string", "note": "string?" } } },
        "history": { "v1": { "tables": { "todos": { "columns": { "title": "string" } } } } } }"#;
    let error = generate(Lang::TypeScript, stranded.as_bytes())
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("history.v1: no migration leads from it"),
        "{error}"
    );
    assert!(jazz_codegen::compile_app(stranded.as_bytes()).is_err());
    // `migrations create` still runs on such a file: it's what repairs it.
    let bindings = generate(
        Lang::TypeScript,
        file(r#"{ "todos": { "columns": { "title": "string" } } }"#).as_bytes(),
    )
    .unwrap();
    // The bindings' version is the stranded one, so nothing new is written,
    // but it runs.
    let created = create(
        Lang::TypeScript,
        stranded.as_bytes(),
        Some(&bindings),
        "note",
    )
    .unwrap();
    assert!(!created.changed);
}

#[test]
fn moving_a_reference_to_another_table_is_not_supported_yet() {
    let before = r#"{ "a": { "columns": { "x": "string" } }, "b": { "columns": { "x": "string" } }, "notes": { "columns": { "owner": { "type": "uuid", "references": "a" } } } }"#;
    let after = r#"{ "a": { "columns": { "x": "string" } }, "b": { "columns": { "x": "string" } }, "notes": { "columns": { "owner": { "type": "uuid", "references": "b" } } } }"#;
    let error = refused(before, after);
    assert!(
        error.contains(
            "Moving the reference of column `notes.owner` from a to b is not supported yet"
        ),
        "{error}"
    );
}

#[test]
fn a_reference_follows_a_renamed_table() {
    let created = migrate(
        r#"{ "users": { "columns": { "name": "string" } }, "todos": { "columns": { "owner": { "type": "uuid", "references": "users" } } } }"#,
        r#"{ "people": { "columns": { "name": "string" } }, "todos": { "columns": { "owner": { "type": "uuid", "references": "people" } } } }"#,
    );
    let text = migration(&created);
    assert!(
        text.contains(r#""renameTables": { "people": "users" }"#),
        "{text}"
    );
    assert!(created.todos.is_empty(), "{:?}", created.todos);
    App::load(created.app.as_bytes()).unwrap_or_else(|e| panic!("{e}"));
}

/// The decisions every language renders its stub from, as the JSON the TS
/// CLI reads (jazz-napi `migrationDiff`): a table renamed, one created,
/// one dropped; in a kept table, a column that may be renamed, one added
/// without the value existing rows get, one dropped with its default, and
/// an enum gaining a variant.
#[test]
fn the_diff_steps_are_what_every_stub_is_rendered_from() {
    let schema = |tables: &str| {
        App::load(file(tables).as_bytes())
            .unwrap()
            .compiled
            .current_schema()
            .clone()
    };
    let before = schema(
        r#"{ "todos": { "columns": { "title": "string", "note": "string?", "status": { "enum": ["open"] } } },
             "projects": { "columns": { "name": "string" } },
             "legacy": { "columns": { "n": "int" } } }"#,
    );
    let after = schema(
        r#"{ "todos": { "columns": { "name": "string", "rank": "int", "status": { "enum": ["open", "done"] } } },
             "lists": { "columns": { "name": "string" } },
             "tags": { "columns": { "label": "string" } } }"#,
    );
    let steps = jazz_codegen::migrations::diff_steps(&before, &after).unwrap();
    assert_eq!(
        serde_json::to_value(&steps).unwrap(),
        serde_json::json!({
            "createTables": ["tags"],
            "dropTables": ["legacy"],
            "renameTables": [{ "from": "projects", "to": "lists" }],
            "migrate": [{
                "table": "todos",
                "steps": [
                    { "step": "maybeRenamed", "column": "name", "candidates": ["title"] },
                    { "step": "add", "column": "rank", "needsDefault": true },
                    { "step": "drop", "column": "note", "needsBackwardsDefault": false },
                    { "step": "maybeRenamedAway", "column": "title" },
                ],
                "addedVariants": [{ "column": "status", "variants": ["done"] }],
                "unchangedRows": [],
            }],
        })
    );
}

/// A row's branch columns decide which branch it belongs to, and a
/// deployment refuses any migration that changes them: removing a column
/// from `branchBy` or changing a branch column's default is refused here,
/// before a stub no deploy accepts is written. Adding `branchBy` is not.
#[test]
fn branch_columns_are_kept_as_they_are() {
    let notes = |branch_by: bool, default: &str| {
        format!(
            r#"{{ "notes": {{ "columns": {{ "title": "string", "branch": {{ "type": "string", "default": "{default}" }} }}{} }} }}"#,
            if branch_by {
                r#", "branchBy": ["branch"]"#
            } else {
                ""
            }
        )
    };
    let removed = refused(&notes(true, "main"), &notes(false, "main"));
    assert!(
        removed.contains("Removing `branch` from the branchBy of table `notes` is not supported"),
        "{removed}"
    );
    let default = refused(&notes(true, "main"), &notes(true, "draft"));
    assert!(
        default.contains("Changing the default of branch column `notes.branch` is not supported"),
        "{default}"
    );
    // Gaining branchBy is an empty migration, which deploys.
    let added = migrate(&notes(false, "main"), &notes(true, "main"));
    assert!(added.changed && added.todos.is_empty());
    App::load(added.app.as_bytes()).unwrap();
    // A default outside branchBy may change.
    let created = migrate(
        &notes(true, "main"),
        &notes(true, "main").replace(
            r#""title": "string""#,
            r#""title": { "type": "string", "default": "x" }"#,
        ),
    );
    App::load(created.app.as_bytes()).unwrap();
}

/// `text` with its current `tables` replaced by `tables`, history and
/// migrations kept.
fn with_tables(text: &str, tables: &str) -> String {
    let mut root = json::parse(text.as_bytes()).unwrap();
    let json::Json::Object(top) = &mut root else {
        panic!("not an object")
    };
    let at = top.iter().position(|(k, _)| k == "tables").unwrap();
    top[at].1 = json::parse(tables.as_bytes()).unwrap();
    json::print(&root)
}

/// #6296: a migration written without `to` targets the current `tables`;
/// once those are edited for a third version, it must keep targeting the
/// version it was written for, which `migrations create` moves into
/// `history` and names in the migration's `to`.
#[test]
fn a_third_version_keeps_the_earlier_migrations_target() {
    const V1: &str = r#"{ "boards": { "columns": { "name": "string" } } }"#;
    const V2: &str = r#"{ "boards": { "columns": { "name": "string", "priority": "int?" } } }"#;
    const V3: &str =
        r#"{ "boards": { "columns": { "name": "string", "priority": "int?", "score": "int?" } } }"#;
    let second = migrate(V1, V2);
    assert!(second.changed && second.todos.is_empty(), "{second:?}");
    let v2_bindings = generate(Lang::TypeScript, second.app.as_bytes()).unwrap();
    let edited = with_tables(&second.app, V3);
    let third = create(
        Lang::TypeScript,
        edited.as_bytes(),
        Some(&v2_bindings),
        "score",
    )
    .unwrap();
    assert!(third.changed && third.todos.is_empty(), "{third:?}");
    let app = App::load(third.app.as_bytes()).unwrap();
    assert_eq!(app.compiled.schemas.len(), 3);
    let root = json::parse(third.app.as_bytes()).unwrap();
    let history: Vec<String> = root
        .get("history")
        .unwrap()
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, _)| k.clone())
        .collect();
    assert_eq!(history.len(), 2);
    let Some(json::Json::Array(items)) = root.get("migrations") else {
        panic!("no migrations")
    };
    // v1 -> v2, now explicit; v2 -> current, implicit as before.
    assert_eq!(
        items[0].get("from").unwrap().as_str(),
        Some(history[0].as_str())
    );
    assert_eq!(
        items[0].get("to").unwrap().as_str(),
        Some(history[1].as_str())
    );
    assert_eq!(
        items[1].get("from").unwrap().as_str(),
        Some(history[1].as_str())
    );
    assert!(items[1].get("to").is_none());
    // Running it again changes nothing.
    let again = create(
        Lang::TypeScript,
        third.app.as_bytes(),
        Some(&v2_bindings),
        "score",
    )
    .unwrap();
    assert!(!again.changed);
}

/// Running `migrations create` again before resolving the stub's TODOs
/// (the bindings still name the old version) says so, rather than that
/// `history` already has that version.
#[test]
fn running_again_over_an_unresolved_stub_names_its_todos() {
    let stub = migrate(
        TODOS,
        r#"{ "todos": { "columns": { "title": "string", "done": "boolean", "rank": "int" } } }"#,
    );
    assert!(!stub.todos.is_empty(), "{stub:?}");
    let old_bindings = generate(Lang::TypeScript, file(TODOS).as_bytes()).unwrap();
    let error = create(
        Lang::TypeScript,
        stub.app.as_bytes(),
        Some(&old_bindings),
        "change",
    )
    .expect_err("a TODO is left")
    .to_string();
    assert!(error.contains("TODO"), "{error}");
    assert!(!error.contains("already has a version"), "{error}");
}

/// Alice supplies a default in her migration, but its enum downgrade is
/// invalid. Creating a migration reports the load error and leaves her files alone.
#[test]
fn an_invalid_authored_migration_keeps_the_app_file_and_bindings() {
    let shipped = serde_json::json!({
        "format": 1,
        "tables": {
            "notes": {
                "columns": { "status": "bytes" },
                "encryption": {
                    "space": "id", "scope": "notes",
                    "columns": {
                        "status": { "type": { "enum": ["a", "b"] }, "sealedType": { "enum": ["a"] } }
                    }
                }
            }
        }
    });
    let lang = Lang::Rust;
    let bindings = generate(lang, shipped.to_string().as_bytes()).unwrap();
    let mut edited = shipped.clone();
    edited["history"]["shipped"]["tables"] = shipped["tables"].clone();
    edited["tables"]["notes"]["encryption"]["columns"]["status"]["type"]["enum"] =
        serde_json::json!(["a"]);
    edited["tables"]["notes"]["columns"]["rank"] = serde_json::json!("int");
    edited["migrations"] = serde_json::json!([{
        "from": "shipped", "migrate": { "notes": { "rank": { "add": "int", "default": 0 } } }
    }]);
    let edited = edited.to_string();
    let load_error = App::load(edited.as_bytes()).unwrap_err().to_string();
    assert!(
        load_error.contains("notes.status: only appending enum variants"),
        "{load_error}"
    );

    let dir = std::env::temp_dir().join(format!("jazz-invalid-migration-{}", std::process::id()));
    std::fs::create_dir(&dir).unwrap();
    let app_path = dir.join(artifact::APP_FILE);
    let bindings_path = dir.join(lang.file());
    std::fs::create_dir_all(bindings_path.parent().unwrap()).unwrap();
    std::fs::write(&app_path, &edited).unwrap();
    std::fs::write(&bindings_path, &bindings).unwrap();
    let result = jazz_codegen::project::migrations_create(&dir, &[lang.into()], "retry");
    let app_after = std::fs::read_to_string(app_path).unwrap();
    let bindings_after = std::fs::read_to_string(bindings_path).unwrap();
    std::fs::remove_dir_all(dir).unwrap();

    assert_eq!(
        result.expect_err("the authored migration is invalid"),
        load_error
    );
    assert_eq!(app_after, edited);
    assert_eq!(bindings_after, bindings);
}

/// Alice tries to replace a required encrypted string with a required
/// encrypted boolean. Neither a rename nor an add and drop works.
#[test]
fn an_impossible_encrypted_rename_leaves_the_project_unchanged() {
    let shipped = file(
        r#"{ "messages": {
            "columns": { "text": "bytes" },
            "encryption": { "space": "id", "scope": "messages", "columns": { "text": "string" } }
        } }"#,
    );
    let edited = shipped
        .replace(r#""text""#, r#""subject""#)
        .replace(r#""string""#, r#""boolean""#);
    let lang = Lang::Rust;
    let bindings = generate(lang, shipped.as_bytes()).unwrap();
    let dir = std::env::temp_dir().join(format!("jazz-impossible-rename-{}", std::process::id()));
    let migrations_dir = dir.join("migrations");
    std::fs::create_dir_all(&migrations_dir).unwrap();
    let files = || {
        std::fs::read_dir(&migrations_dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>()
    };
    let before = files();
    let app_path = dir.join(artifact::APP_FILE);
    let bindings_path = dir.join(lang.file());
    std::fs::create_dir_all(bindings_path.parent().unwrap()).unwrap();
    std::fs::write(&app_path, &edited).unwrap();
    std::fs::write(&bindings_path, &bindings).unwrap();

    let result = jazz_codegen::project::migrations_create(&dir, &[lang.into()], "replace-text");
    let after = files();
    let app_after = std::fs::read_to_string(app_path).unwrap();
    let bindings_after = std::fs::read_to_string(bindings_path).unwrap();
    std::fs::remove_dir_all(dir).unwrap();

    let error = result.expect_err("neither interpretation is valid");
    assert!(
        error.contains("messages.subject: the migration adds a required encrypted column"),
        "{error}"
    );
    assert_eq!(after, before);
    assert_eq!(app_after, edited);
    assert_eq!(bindings_after, bindings);
}

/// Alice replaces one required encrypted string with two. Creating the
/// migration refuses because the two targets cannot share a rename source.
#[test]
fn encrypted_renames_cannot_share_a_source() {
    let shipped = file(
        r#"{ "messages": {
            "columns": { "text": "bytes" },
            "encryption": { "space": "id", "scope": "messages", "columns": { "text": "string" } }
        } }"#,
    );
    let edited = file(
        r#"{ "messages": {
            "columns": { "body": "bytes", "subject": "bytes" },
            "encryption": {
                "space": "id", "scope": "messages", "columns": { "body": "string", "subject": "string" }
            }
        } }"#,
    );
    let lang = Lang::Rust;
    let bindings = generate(lang, shipped.as_bytes()).unwrap();
    let dir =
        std::env::temp_dir().join(format!("jazz-shared-rename-source-{}", std::process::id()));
    let app_path = dir.join(artifact::APP_FILE);
    let bindings_path = dir.join(lang.file());
    std::fs::create_dir_all(bindings_path.parent().unwrap()).unwrap();
    std::fs::write(&app_path, &edited).unwrap();
    std::fs::write(&bindings_path, &bindings).unwrap();

    let result = jazz_codegen::project::migrations_create(&dir, &[lang.into()], "replace-text");
    let app_after = std::fs::read_to_string(app_path).unwrap();
    let bindings_after = std::fs::read_to_string(bindings_path).unwrap();
    let migration_written = dir.join("migrations").exists();
    std::fs::remove_dir_all(dir).unwrap();

    let error = result.expect_err("one source cannot supply both required columns");
    assert!(
        error.contains("the migration adds a required encrypted column"),
        "{error}"
    );
    assert!(!migration_written);
    assert_eq!(app_after, edited);
    assert_eq!(bindings_after, bindings);
}
