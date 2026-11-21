//! How an app's relations resolve, for every client.
//!
//! This is a lower-level test on purpose: `fixtures/relations_parity.json`
//! is the shared contract between jazz's relation resolution (which the
//! loader and the FFI use) and TS's `analyzeRelations`, which differed
//! (#5927 R14: the FFI dropped bad reverse relations and ignored array and
//! nullable relations). This test pins the Rust side;
//! `packages/jazz-tools/src/codegen/relations-parity.test.ts` runs the same
//! fixture through TS.

use jazz::tools::deployment::artifact::load_app;
use jazz::tools::deployment::artifact::relations::RelationKind;
use serde_json::{Value, json};

#[derive(serde::Deserialize)]
struct Case {
    description: String,
    tables: Value,
    #[serde(default)]
    relations: Option<Value>,
    #[serde(default)]
    refused: bool,
}

/// Each fixture app's relations resolve as the fixture says, or the app is
/// refused where it says so.
#[test]
fn relations_resolve_as_the_parity_fixture_says() {
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("fixtures/relations_parity.json")).unwrap();
    assert!(!cases.is_empty());
    for case in cases {
        let file = json!({ "format": 1, "tables": case.tables }).to_string();
        let loaded = load_app(file.as_bytes());
        if case.refused {
            assert!(loaded.is_err(), "{}: loaded", case.description);
            continue;
        }
        let app = loaded.unwrap_or_else(|error| panic!("{}: {error}", case.description));
        let resolved = app
            .relations()
            .iter()
            .map(|(table, relations)| {
                let relations = relations
                    .iter()
                    .map(|r| {
                        json!({
                            "name": r.name,
                            "type": if r.kind == RelationKind::Forward { "forward" } else { "reverse" },
                            "fromTable": r.from_table,
                            "toTable": r.to_table,
                            "fromColumn": r.from_column,
                            "toColumn": r.to_column,
                            "isArray": r.is_array,
                            "nullable": r.nullable,
                        })
                    })
                    .collect::<Vec<_>>();
                (table.clone(), Value::Array(relations))
            })
            .collect::<serde_json::Map<_, _>>();
        // Tables without relations may be left out of the resolution.
        let mut expected = case.relations.expect("relations or refused");
        expected
            .as_object_mut()
            .unwrap()
            .retain(|_, relations| relations.as_array().is_some_and(|r| !r.is_empty()));
        let resolved = Value::Object(resolved)
            .as_object()
            .unwrap()
            .clone()
            .into_iter()
            .filter(|(_, relations)| relations.as_array().is_some_and(|r| !r.is_empty()))
            .collect::<serde_json::Map<_, _>>();
        assert_eq!(Value::Object(resolved), expected, "{}", case.description);
    }
}
