//! A never-written cell of a required encrypted column that declares no
//! default (`E2EE_PROTOCOL.md` §4.1): there is no value for it to read as, so
//! its row is withheld and the cell is reported `empty-cell`, while the other
//! rows are returned.

mod common;

use common::*;
use jazz::db::block_on;
use jazz::e2ee::UnreadableCode;
use jazz::query::Query;
use jazz::records::Value;
use jazz_e2ee_client::Report;

/// Contract: a row whose required encrypted column (no declared default)
/// holds the empty value is not returned and is reported `empty-cell` for
/// that column; a row written through the runtime beside it is returned.
///
/// ```text
/// mallory ──body = empty bytes──► server ──► alice: row withheld, empty-cell
/// alice ──body = "kept"──► server ──► alice: row returned
/// ```
#[test]
fn an_empty_cell_of_a_required_column_without_a_default_withholds_its_row() {
    let (server, client, db, _link, space) = client_in_space();

    let mut stored_empty = note(space, "ignored");
    for column in ["body", "note", "attachment", "rank"] {
        stored_empty.insert(column.to_owned(), Value::Bytes(Vec::new()));
    }
    let withheld = block_on(server.insert("notes", stored_empty, Default::default()))
        .unwrap()
        .row_uuid();
    let kept = block_on(db.insert("notes", note(space, "kept"), Default::default()))
        .unwrap()
        .row_uuid();
    let everything = Query::from("notes");
    let _live = block_on(db.subscribe(&db.prepare_query(&everything).unwrap(), Default::default()))
        .unwrap();
    pump(&server, &db);
    client.take_reports();

    let rows = all(&db, everything).unwrap();
    assert_eq!(
        rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        [kept]
    );
    assert_eq!(rows[0].cell("body"), Some(text("kept")));
    let reports: Vec<_> = client
        .take_reports()
        .into_iter()
        .filter_map(|report| match report {
            Report::Unreadable(unreadable) => Some(unreadable),
            _ => None,
        })
        .collect();
    assert!(
        reports.iter().any(|report| report.id == Some(withheld)
            && report.column.as_deref() == Some("body")
            && report.code == UnreadableCode::EmptyCell),
        "{reports:?}"
    );
}
