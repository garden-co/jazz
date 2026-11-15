//! jazz-ffi v2: upsert and restore.

mod common;

use common::*;
use jazz_ffi::JazzHandle;
use serde_json::json;

/// alice upserts a show under an id she chose (an insert), upserts it again
/// with a new venue (a whole row, as for an insert), deletes it and restores it as it was, then with new
/// content (a whole row); inside a transaction too.
#[test]
fn upsert_writes_by_id_and_restore_brings_rows_back() {
    let jazz = JazzHandle::open(client_config()).unwrap();
    let id = "01a1e200-0000-7000-8000-000000000001".to_owned();
    let row = parse(
        &jazz
            .upsert("shows".into(), id.clone(), show(nil_uuid()))
            .unwrap(),
    );
    assert_eq!(row["id"], id);
    assert_eq!(row["venue"], "Main Hall");
    let row = parse(
        &jazz
            .upsert(
                "shows".into(),
                id.clone(),
                json!({ "venue": "Annex" }).to_string(),
            )
            .unwrap(),
    );
    assert_eq!(row["venue"], "Annex");
    assert_eq!(row["name"], "Spring Gala");

    jazz.delete("shows".into(), id.clone()).unwrap();
    assert_eq!(
        jazz.one(json!({ "table": "shows" }).to_string()).unwrap(),
        None
    );
    let row = parse(&jazz.restore("shows".into(), id.clone(), None).unwrap());
    assert_eq!(row["venue"], "Annex");
    jazz.delete("shows".into(), id.clone()).unwrap();
    let tx = jazz.clone().begin().unwrap();
    let row = parse(
        &tx.restore(
            "shows".into(),
            id.clone(),
            Some({
                let mut row = parse(&show(nil_uuid()));
                row["name"] = json!("Autumn Gala");
                row.to_string()
            }),
        )
        .unwrap(),
    );
    assert_eq!(row["name"], "Autumn Gala");
    tx.commit(None).unwrap();
    assert_eq!(
        parse(
            &jazz
                .one(json!({ "table": "shows" }).to_string())
                .unwrap()
                .unwrap()
        )["name"],
        "Autumn Gala"
    );
}
