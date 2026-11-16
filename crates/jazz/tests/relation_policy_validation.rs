//! Relation policies (`PolicyExpr::ExistsRel`) a deployment would refuse are
//! refused by `jazz-tools validate` and the dev server too, not first by a
//! deploy: all three run the schema compile and `check_deployable_policies`
//! (#5931), and nothing else decides.

use std::collections::BTreeMap;

use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{
    PolicyExpr, RelationCmpOp, RelationColumnRef, RelationExpr, RelationPredicate,
    RelationUnionArm, RelationValueRef, Schema as PublicSchema, TablePolicies,
};
use jazz::tools::public_schema_convert::check_deployable_policies;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

/// `docs`, with `read` as its select policy; `shares` point at a doc.
fn schema(read: PolicyExpr) -> PublicSchema {
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("docs")
                .column("title", ColumnType::Text)
                .policies(TablePolicies::new().with_select(read)),
        )
        .table(
            TableSchemaBuilder::new("shares")
                .column("user_id", ColumnType::Uuid)
                .column("role", ColumnType::Text)
                .fk_column("doc", "docs")
                .policies(TablePolicies::new().with_select(PolicyExpr::True)),
        )
        .build()
}

/// What `jazz-tools validate` runs (`validate_deployable_schema`): compile,
/// then the deployability checks.
fn validate(schema: &PublicSchema) -> Result<(), String> {
    JazzSchema::new(schema).map_err(|error| error.to_string())?;
    check_deployable_policies(schema).map_err(|error| error.to_string())
}

/// `shares where shares.<column> = <right>`.
fn shares_where(column: &str, right: RelationValueRef) -> RelationExpr {
    RelationExpr::Filter {
        input: Box::new(RelationExpr::TableScan {
            table: "shares".into(),
            alias: None,
        }),
        predicate: RelationPredicate::Cmp {
            left: RelationColumnRef {
                scope: None,
                column: column.to_owned(),
            },
            op: RelationCmpOp::Eq,
            right,
        },
    }
}

/// `shares where shares.user_id = session.user`.
fn my_shares() -> RelationExpr {
    shares_where(
        "user_id",
        RelationValueRef::SessionRef(vec!["user".to_owned()]),
    )
}

fn union(labels: &[&str]) -> RelationExpr {
    RelationExpr::Union {
        inputs: labels
            .iter()
            .map(|label| RelationUnionArm {
                label: (*label).to_owned(),
                input: my_shares(),
            })
            .collect(),
    }
}

/// alice writes `exists (from shares where … limit 1)` and the like in her
/// permissions; `validate` tells her, before a deploy would.
///
/// ```text
/// alice ──permissions──► validate ──✗ "relation policies … not supported"
/// ```
#[test]
fn relation_policies_a_deploy_refuses_fail_validation() {
    let cases: [(&str, RelationExpr, &str); 6] = [
        (
            "limit",
            RelationExpr::Limit {
                input: Box::new(my_shares()),
                limit: 1,
            },
            "Distinct, OrderBy, Offset or Limit",
        ),
        (
            "query parameter",
            shares_where("role", RelationValueRef::Param("role".to_owned())),
            "query parameters",
        ),
        ("duplicate union labels", union(&["a", "a"]), "union labels"),
        ("empty union label", union(&[""]), "union labels"),
        ("NUL in a union label", union(&["a\0b"]), "union labels"),
        (
            "4097-byte union label",
            union(&[&"x".repeat(4097)]),
            "union labels",
        ),
    ];
    let mut slipped = BTreeMap::new();
    for (name, rel, expected) in cases {
        match validate(&schema(PolicyExpr::ExistsRel { rel })) {
            Err(error) if error.contains(expected) => {}
            other => {
                slipped.insert(name, format!("{other:?}"));
            }
        }
    }
    assert!(
        slipped.is_empty(),
        "validate let these through: {slipped:#?}"
    );
}

/// A relation policy with a session reference, a typed literal or a plain
/// one (`role = 'editor'`, as the policy compiler writes a hop condition)
/// still validates: the compile reads a plain literal with the column's type.
#[test]
fn supported_relation_policies_still_validate() {
    validate(&schema(PolicyExpr::ExistsRel { rel: my_shares() })).unwrap();
    for literal in [
        serde_json::json!({"type": "Text", "value": "editor"}),
        serde_json::json!("editor"),
    ] {
        validate(&schema(PolicyExpr::ExistsRel {
            rel: shares_where("role", RelationValueRef::Literal(literal)),
        }))
        .unwrap();
    }
}
