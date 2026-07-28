//! Shared public-API schema and fixture helpers for multi-tenant SaaS benchmarks.
//!
//! This module deliberately contains no benchmark runner. It keeps the simple
//! team-membership policy and the cumulative real-world policy on the same table
//! shape so callers can attribute the additional authorization cost.

#![allow(dead_code)]

use std::collections::BTreeMap;

use jazz::db::RowCells;
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::ids::{AuthorId, RowUuid};
use jazz::query::{PolicyBranch, Query, claim, col, eq, in_list, lit};
use jazz::schema::{JazzSchema, Policy, TableSchema};

pub const ORGANIZATIONS: &str = "organizations";
pub const TEAMS: &str = "teams";
pub const TEAM_MEMBERSHIPS: &str = "team_memberships";
pub const ORGANIZATION_MEMBERSHIPS: &str = "organization_memberships";
pub const DOCUMENTS: &str = "documents";
pub const DOCUMENT_ACL: &str = "document_acl";

const ORGANIZATION_ROW_TAG: u8 = 0x61;
const TEAM_ROW_TAG: u8 = 0x62;
const TEAM_MEMBERSHIP_ROW_TAG: u8 = 0x63;
const ORGANIZATION_MEMBERSHIP_ROW_TAG: u8 = 0x64;
const DOCUMENT_ROW_TAG: u8 = 0x65;
const DOCUMENT_ACL_ROW_TAG: u8 = 0x66;
const USER_TAG: u8 = 0x67;

/// Team-membership-only document read policy.
///
/// A reader must have an active membership for the document's team with a
/// read-capable role. The same branch is the base of
/// [`real_world_document_policy`].
pub fn membership_baseline_document_policy() -> Query {
    Query::from(DOCUMENTS).join_via_column(
        TEAM_MEMBERSHIPS,
        "team",
        "team",
        [
            eq(col("user"), claim("sub")),
            eq(col("active"), lit(true)),
            in_list(col("role"), [lit("viewer"), lit("editor"), lit("admin")]),
        ],
    )
}

/// Cumulative document read policy for common multi-tenant SaaS access paths.
///
/// Access is granted by any one of:
///
/// - active team membership with a read-capable role;
/// - active organization owner/admin membership with all-team access;
/// - an active direct document ACL;
/// - public and published document visibility;
/// - the trusted built-in `isAdmin` session claim.
///
/// The organization id is intentionally denormalized onto `documents`. This
/// keeps the primary benchmark on maintained join shapes that are covered by
/// the current public API, without a source-lookup hop through `teams`.
pub fn real_world_document_policy() -> Query {
    document_policy_with_branches(5)
}

/// Build the cumulative policy through the requested access branch.
///
/// `1` is team membership only; `2` adds organization admin; `3` direct ACL;
/// `4` public/published documents; and `5` the trusted admin claim.
pub fn document_policy_with_branches(branches: usize) -> Query {
    assert!((1..=5).contains(&branches), "policy branches must be 1..=5");
    let mut policy = membership_baseline_document_policy();
    if branches >= 2 {
        policy = policy.policy_branch(policy_alternative(Query::from(DOCUMENTS).join_via_column(
            ORGANIZATION_MEMBERSHIPS,
            "organization",
            "organization",
            [
                eq(col("user"), claim("sub")),
                eq(col("active"), lit(true)),
                eq(col("all_teams"), lit(true)),
                in_list(col("role"), [lit("owner"), lit("admin")]),
            ],
        )));
    }
    if branches >= 3 {
        policy = policy.policy_branch(policy_alternative(Query::from(DOCUMENTS).join_via(
            DOCUMENT_ACL,
            "document",
            [
                eq(col("user"), claim("sub")),
                eq(col("active"), lit(true)),
                in_list(col("permission"), [lit("view"), lit("edit")]),
            ],
        )));
    }
    if branches >= 4 {
        policy = policy.policy_branch(policy_alternative(
            Query::from(DOCUMENTS)
                .filter(eq(col("visibility"), lit("public")))
                .filter(eq(col("published"), lit(true))),
        ));
    }
    if branches >= 5 {
        policy = policy.policy_branch(policy_alternative(
            Query::from(DOCUMENTS).filter(eq(claim("isAdmin"), lit(true))),
        ));
    }
    policy
}

/// Schema using only the active team-membership document policy.
pub fn membership_baseline_schema() -> JazzSchema {
    schema_with_document_policy(membership_baseline_document_policy())
}

/// Schema using the cumulative real-world document policy.
pub fn real_world_permission_schema() -> JazzSchema {
    permission_schema_with_branches(5)
}

/// Schema for a cumulative real-world policy tier.
pub fn permission_schema_with_branches(branches: usize) -> JazzSchema {
    schema_with_document_policy(document_policy_with_branches(branches))
}

fn schema_with_document_policy(document_policy: Query) -> JazzSchema {
    JazzSchema::new([
        TableSchema::new(
            ORGANIZATIONS,
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("suspended", ColumnType::Bool),
            ],
        )
        .with_indexed_column("suspended")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            TEAMS,
            [
                ColumnSchema::new("organization", ColumnType::Uuid),
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("archived", ColumnType::Bool),
            ],
        )
        .with_reference("organization", ORGANIZATIONS)
        .with_indexed_columns(["organization", "archived"])
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            TEAM_MEMBERSHIPS,
            [
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("user", ColumnType::Uuid),
                ColumnSchema::new("role", ColumnType::String),
                ColumnSchema::new("active", ColumnType::Bool),
            ],
        )
        .with_reference("team", TEAMS)
        .with_indexed_columns(["team", "user", "role", "active"])
        .with_read_policy(Policy::owner_only(TEAM_MEMBERSHIPS, "user"))
        .with_write_policy(Policy::public()),
        TableSchema::new(
            ORGANIZATION_MEMBERSHIPS,
            [
                ColumnSchema::new("organization", ColumnType::Uuid),
                ColumnSchema::new("user", ColumnType::Uuid),
                ColumnSchema::new("role", ColumnType::String),
                ColumnSchema::new("active", ColumnType::Bool),
                ColumnSchema::new("all_teams", ColumnType::Bool),
            ],
        )
        .with_reference("organization", ORGANIZATIONS)
        .with_indexed_columns(["organization", "user", "role", "active", "all_teams"])
        .with_read_policy(Policy::owner_only(ORGANIZATION_MEMBERSHIPS, "user"))
        .with_write_policy(Policy::public()),
        TableSchema::new(
            DOCUMENTS,
            [
                ColumnSchema::new("organization", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("owner", ColumnType::Uuid),
                ColumnSchema::new("updated_at", ColumnType::U64),
                ColumnSchema::new("status", ColumnType::String),
                ColumnSchema::new("archived", ColumnType::Bool),
                ColumnSchema::new("visibility", ColumnType::String),
                ColumnSchema::new("published", ColumnType::Bool),
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("body", ColumnType::String),
            ],
        )
        .with_reference("organization", ORGANIZATIONS)
        .with_reference("team", TEAMS)
        .with_indexed_columns([
            "organization",
            "team",
            "owner",
            "updated_at",
            "status",
            "archived",
            "visibility",
            "published",
        ])
        .with_read_policy(Policy::shape(document_policy))
        .with_write_policy(Policy::public()),
        TableSchema::new(
            DOCUMENT_ACL,
            [
                ColumnSchema::new("document", ColumnType::Uuid),
                ColumnSchema::new("user", ColumnType::Uuid),
                ColumnSchema::new("permission", ColumnType::String),
                ColumnSchema::new("active", ColumnType::Bool),
            ],
        )
        .with_reference("document", DOCUMENTS)
        .with_indexed_columns(["document", "user", "permission", "active"])
        .with_read_policy(Policy::owner_only(DOCUMENT_ACL, "user"))
        .with_write_policy(Policy::public()),
    ])
}

fn policy_alternative(query: Query) -> PolicyBranch {
    PolicyBranch::single_alternative_from_query(query)
}

/// Deterministic, anonymized organization row id.
pub fn organization_row(index: u64) -> RowUuid {
    tagged_row_uuid(ORGANIZATION_ROW_TAG, index)
}

/// Deterministic, anonymized team row id.
pub fn team_row(index: u64) -> RowUuid {
    tagged_row_uuid(TEAM_ROW_TAG, index)
}

/// Deterministic, anonymized team-membership row id.
pub fn team_membership_row(index: u64) -> RowUuid {
    tagged_row_uuid(TEAM_MEMBERSHIP_ROW_TAG, index)
}

/// Deterministic, anonymized organization-membership row id.
pub fn organization_membership_row(index: u64) -> RowUuid {
    tagged_row_uuid(ORGANIZATION_MEMBERSHIP_ROW_TAG, index)
}

/// Deterministic, anonymized document row id.
pub fn document_row(index: u64) -> RowUuid {
    tagged_row_uuid(DOCUMENT_ROW_TAG, index)
}

/// Deterministic, anonymized direct-ACL row id.
pub fn document_acl_row(index: u64) -> RowUuid {
    tagged_row_uuid(DOCUMENT_ACL_ROW_TAG, index)
}

/// Deterministic, anonymized user identity.
pub fn user_identity(index: u64) -> AuthorId {
    AuthorId::from_bytes(tagged_bytes(USER_TAG, index))
}

pub fn organization_cells(index: u64, suspended: bool) -> RowCells {
    BTreeMap::from([
        (
            "name".to_owned(),
            Value::String(format!("Organization {index}")),
        ),
        ("suspended".to_owned(), Value::Bool(suspended)),
    ])
}

pub fn team_cells(index: u64, organization: RowUuid, archived: bool) -> RowCells {
    BTreeMap::from([
        ("organization".to_owned(), Value::Uuid(organization.0)),
        ("name".to_owned(), Value::String(format!("Team {index}"))),
        ("archived".to_owned(), Value::Bool(archived)),
    ])
}

pub fn team_membership_cells(team: RowUuid, user: AuthorId, role: &str, active: bool) -> RowCells {
    BTreeMap::from([
        ("team".to_owned(), Value::Uuid(team.0)),
        ("user".to_owned(), Value::Uuid(user.0)),
        ("role".to_owned(), Value::String(role.to_owned())),
        ("active".to_owned(), Value::Bool(active)),
    ])
}

pub fn organization_membership_cells(
    organization: RowUuid,
    user: AuthorId,
    role: &str,
    active: bool,
    all_teams: bool,
) -> RowCells {
    BTreeMap::from([
        ("organization".to_owned(), Value::Uuid(organization.0)),
        ("user".to_owned(), Value::Uuid(user.0)),
        ("role".to_owned(), Value::String(role.to_owned())),
        ("active".to_owned(), Value::Bool(active)),
        ("all_teams".to_owned(), Value::Bool(all_teams)),
    ])
}

/// Deterministic document cells with varied list filters and public visibility.
pub fn document_cells(
    index: u64,
    organization: RowUuid,
    team: RowUuid,
    owner: AuthorId,
    updated_at: u64,
) -> RowCells {
    let status = match index % 5 {
        0 => "draft",
        1 => "closed",
        _ => "active",
    };
    let archived = index.is_multiple_of(20);
    // Keep about 1% public without overlapping the every-20th archived rows.
    let public = index % 100 == 7;

    BTreeMap::from([
        ("organization".to_owned(), Value::Uuid(organization.0)),
        ("team".to_owned(), Value::Uuid(team.0)),
        ("owner".to_owned(), Value::Uuid(owner.0)),
        ("updated_at".to_owned(), Value::U64(updated_at)),
        ("status".to_owned(), Value::String(status.to_owned())),
        ("archived".to_owned(), Value::Bool(archived)),
        (
            "visibility".to_owned(),
            Value::String(if public { "public" } else { "private" }.to_owned()),
        ),
        ("published".to_owned(), Value::Bool(public && !archived)),
        (
            "title".to_owned(),
            Value::String(format!("Document {index}")),
        ),
        (
            "body".to_owned(),
            Value::String(format!("Anonymized benchmark document {index}")),
        ),
    ])
}

pub fn document_acl_cells(
    document: RowUuid,
    user: AuthorId,
    permission: &str,
    active: bool,
) -> RowCells {
    BTreeMap::from([
        ("document".to_owned(), Value::Uuid(document.0)),
        ("user".to_owned(), Value::Uuid(user.0)),
        (
            "permission".to_owned(),
            Value::String(permission.to_owned()),
        ),
        ("active".to_owned(), Value::Bool(active)),
    ])
}

fn tagged_row_uuid(tag: u8, index: u64) -> RowUuid {
    RowUuid::from_bytes(tagged_bytes(tag, index))
}

fn tagged_bytes(tag: u8, index: u64) -> [u8; 16] {
    let mut bytes = [tag; 16];
    bytes[8..].copy_from_slice(&index.to_be_bytes());
    bytes
}
