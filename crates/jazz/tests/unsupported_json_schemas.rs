//! A JSON column's declared schema may only use JSON Schema features every
//! Jazz runtime evaluates. The browser validates JSON columns with a JS
//! validator that cannot evaluate `$dynamicRef`, the internationalized
//! formats, draft 6/7 `contentMediaType` and a few regex constructs, so a
//! deployment refuses a schema using one, naming the feature, the column and
//! where in the schema it is. A store whose
//! schema already uses one still opens: the refusal never runs on opening.

use crate::common;

use jazz::binding_host::{decode_schema, validate_deployable_schema};
use jazz::tools::deployment::{DeploymentCatalogue, DeploymentRequest, prepare_deployment};
use jazz::tools::public_schema::{Schema as PublicSchema, SchemaHash};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use serde_json::{Value, json};

use common::allow_all_policies;

/// `notes` with a JSON column `body` declaring `schema`.
fn notes(schema: Value) -> PublicSchema {
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("notes")
                .column(
                    "body",
                    ColumnType::Json {
                        schema: Some(schema),
                    },
                )
                .policies(allow_all_policies()),
        )
        .build()
}

/// A linked list whose `next` recurses through `$dynamicRef`.
fn linked_list() -> Value {
    json!({
        "$dynamicAnchor": "node",
        "type": "object",
        "properties": { "next": { "$dynamicRef": "#node" } }
    })
}

/// What a deployment of `schema` says, if it refuses it.
fn deploy(schema: &PublicSchema) -> Result<(), String> {
    let hash = SchemaHash::compute(schema);
    prepare_deployment(
        &DeploymentCatalogue::default(),
        DeploymentRequest {
            target_schema_hash: hash,
            schemas: vec![(hash, schema.clone())],
            migrations: Vec::new(),
            permissions: schema
                .iter()
                .map(|(name, table)| (*name, table.policies.clone()))
                .collect(),
            allow_dropping_history: false,
        },
    )
    .map(|_| ())
    .map_err(|error| error.to_string())
}

/// What the `validateSchema` binding says about `schema`, if it refuses it.
fn validate(schema: &PublicSchema) -> Result<(), String> {
    validate_deployable_schema(&serde_json::to_vec(schema).unwrap())
        .map(|_| ())
        .map_err(|error| format!("{error:?}"))
}

/// ```
/// developer ──deploy / validate──► $dynamicRef in notes.body ──✗── "not supported … yet"
/// ```
#[test]
fn a_schema_the_browser_cannot_evaluate_is_refused_on_deploy_and_validate() {
    let schema = notes(linked_list());
    for error in [deploy(&schema).unwrap_err(), validate(&schema).unwrap_err()] {
        assert!(
            error.contains(
                "unsupported JSON schema for column `body`: the `$dynamicAnchor` keyword is \
                 not supported in Jazz JSON Schemas yet"
            ),
            "{error}"
        );
        assert!(
            error.contains("$.notes.body"),
            "it names the column: {error}"
        );
    }

    let nested = notes(json!({
        "type": "object",
        "properties": {
            "tags": { "type": "array", "items": { "type": "string", "pattern": "^a*+$" } }
        }
    }));
    let error = deploy(&nested).unwrap_err();
    assert!(
        error.contains(
            "the regular expression \"^a*+$\" is not supported in Jazz JSON Schemas yet: \
             possessive quantifiers (at /properties/tags/items)"
        ),
        "it names the construct and where: {error}"
    );
}

#[test]
fn schemas_every_runtime_evaluates_still_deploy() {
    for schema in [
        // Recursion through `$ref` instead of `$dynamicRef`.
        json!({
            "$defs": { "node": {
                "type": "object",
                "properties": { "next": { "$ref": "#/$defs/node" } }
            } },
            "$ref": "#/$defs/node"
        }),
        // 2020-12 only annotates formats and media types.
        json!({ "format": "idn-hostname", "contentMediaType": "application/json" }),
        // `(?i)` at the very start of a pattern.
        json!({ "type": "string", "pattern": "(?i)^[a-z]+$" }),
    ] {
        let schema = notes(schema);
        deploy(&schema).unwrap();
        validate(&schema).unwrap();
    }
}

#[test]
fn a_store_whose_schema_uses_one_still_opens() {
    // Opening decodes the schema without the deployment checks.
    decode_schema(&serde_json::to_vec(&notes(linked_list())).unwrap()).unwrap();
}
