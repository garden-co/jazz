//! Authentication-mode policies derive from the admitted author, never claims.
use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, WriteIdentity, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    CmpOp, ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use std::collections::BTreeMap;

/// Alice's local-first identity, Bob's external identity and Mallory's anonymous
/// identity each see only their mode's rows. Forged top-level and provider claims
/// cannot change either the read gate or the insert check. Anonymous sessions
/// cannot write, even when their table policy would otherwise allow it.
#[test]
fn authentication_mode_controls_reads_and_writes_without_trusting_claims() {
    let modes = [
        (
            "local-first",
            AuthorSubject::reserved(AuthorSubject::LOCAL_FIRST_ISSUER, "alice").unwrap(),
        ),
        (
            "external",
            AuthorSubject::authenticated("https://issuer.example", "bob").unwrap(),
        ),
        (
            "anonymous",
            AuthorSubject::reserved(AuthorSubject::ANONYMOUS_ISSUER, "mallory").unwrap(),
        ),
    ];
    let mut builder = SchemaBuilder::new();
    for (mode, _) in &modes {
        builder = builder.table(
            TableSchemaBuilder::new(mode)
                .column("mode", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::SessionCmp {
                            path: vec!["authMode".into()],
                            op: CmpOp::Eq,
                            value: jazz::tools::Value::Text((*mode).into()),
                        })
                        .with_insert(PolicyExpr::eq_session("mode", vec!["authMode".into()])),
                ),
        );
    }
    let db = block_on(Db::open(DbConfig::new(
        JazzSchema::new(&builder.build()).unwrap(),
        DbIdentity {
            node: NodeUuid::from_bytes([90; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    for (mode, _) in &modes {
        block_on(db.insert(
            mode,
            BTreeMap::from([("mode".into(), Value::String((*mode).into()))]),
            Default::default(),
        ))
        .unwrap();
    }
    for (mode, author) in modes {
        for forged in ["local-first", "external", "anonymous"] {
            db.set_identity_claims(
                author,
                BTreeMap::from([
                    ("authMode".into(), Value::String(forged.into())),
                    (
                        jazz::query::provider_claim_key("authMode"),
                        Value::String(forged.into()),
                    ),
                ]),
            );
            for table in ["local-first", "external", "anonymous"] {
                let query = db.prepare_query(&Query::from(table)).unwrap();
                let rows =
                    block_on(db.all_for_identity(&query, ReadOpts::default(), author)).unwrap();
                assert_eq!(
                    !rows.is_empty(),
                    table == mode,
                    "{mode} reading {table} with forged {forged}"
                );
                let write = block_on(db.insert(
                    table,
                    BTreeMap::from([("mode".into(), Value::String(table.into()))]),
                    InsertOptions {
                        identity: WriteIdentity::Session(author),
                        ..Default::default()
                    },
                ));
                assert_eq!(
                    write.is_ok(),
                    mode != "anonymous" && table == mode,
                    "{mode} writing {table} with forged {forged}"
                );
                if mode == "anonymous" {
                    let error = write.unwrap_err();
                    assert_eq!(error.code, jazz::db::ErrorCode::WriteRejected);
                    assert_eq!(error.reason, Some(jazz::db::Reason::AuthorizationDenied));
                }
            }
        }
    }
}
