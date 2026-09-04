# INV-DATA-12

- Status: now
- Coverage: ✓

## Invariant

A table read or write policy, when present, MUST name the table it is attached to and MUST validate against the complete `JazzSchema`.

## Enforced by (tests)

`jazz::schema::tests::read_policy_must_name_attached_table`; `jazz::schema::tests::write_policy_must_name_attached_table`; `jazz::schema::tests::read_policy_validates_against_complete_schema`

## Implementation

`schema.rs::JazzSchema::validated`
