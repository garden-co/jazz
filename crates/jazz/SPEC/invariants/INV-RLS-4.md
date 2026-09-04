# INV-RLS-4

- Status: now
- Coverage: ✓

## Invariant

A table policy MUST validate as a query shape rooted at the table that carries the policy.

## Enforced by (tests)

`jazz::schema::tests::read_policy_must_name_attached_table`; `jazz::schema::tests::write_policy_must_name_attached_table`; `jazz::schema::tests::read_policy_validates_against_complete_schema`

## Implementation

jazz/src/schema.rs::JazzSchema::new
