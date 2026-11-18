# Equality tokens for JSON and array columns: encodings and compatibility

Status: draft for review by the jazz-core owners. The two encodings below let
an encrypted JSON column, and an encrypted array column whose element type has
an equality encoding, carry equality tokens
(`crates/jazz/SPEC/E2EE_EQUALITY_FORMAT.md`, "Value encoding").

## 1. What is stored

| Encoding                 | Where it lives                                         | Change               |
| ------------------------ | ------------------------------------------------------ | -------------------- |
| Equality value `V`       | Hashed into an equality token, stored in `__e2ee_eq_*` | Two additive types   |
| Equality token (`JE2E`)  | The stored token column of an equality-indexed column  | None for old columns |
| Declared column / values | `jazz-e2ee` (`EqualityColumn`, `EqualityValue`)        | Additive API         |

## 2. The encodings, named and versioned

- `json.v1`: `V` is `01` followed by the exact UTF-8 bytes of the stored JSON
  text (null is `00`). No canonicalisation: Jazz compares a JSON column by its
  stored text.
- `array.v1`: `V` is `01`, the element count (u32, big-endian), then for each
  element its own `V` behind its length (u32, big-endian). Elements are in
  order; a null element is `00`. Nested arrays apply the rule recursively.

Neither is a new mechanism: the token mechanism stays `jazz.sodium.equality`
version 1 and the context policy `jazz.e2ee.equality.v1`. The column's type
bytes `T` (part of the token context) already differ between a JSON, an array
and any other column, so a token of one type never matches another's.

## 3. Byte-level fixtures

`equality_values_of_json_and_arrays` (`crates/jazz-e2ee/tests/vectors.rs`)
pins: `{"a":1}` as `017b2261223a317d`; `{"a": 1}` as `017b2261223a20317d`;
surrounding whitespace and key order kept; `["a","b"]` as
`0100000002000000020161000000020162`; the empty array, an array with a null,
integers, the length framing (`["a","bc"]` against `["ab","c"]`) and a nested
array; and the refusals of a value of another shape. `equality_values` still
pins every earlier encoding unchanged.

## 4. Compatibility

- **Existing tokens.** Unchanged: no earlier type's encoding, the mechanism,
  the context policy or the envelope changes. A stored token written before
  this change is read as before.
- **New columns only.** JSON and array columns could not carry equality
  before (a declaration was refused), so no stored token of these types exists
  from before.
- **Older readers.** A deploy declaring equality on such a column is refused
  by a server that predates this change (its check refuses the type), so a
  token of these encodings cannot reach one.
- **Changing an encoding later** needs a new name (`json.v2`, `array.v2`) and
  an explicit compatibility decision: tokens are one-way, so existing ones
  cannot be recomputed.
- **Semantics.** A token match only selects candidates, which the client
  decrypts and compares with Jazz's own rule (exact JSON text, arrays element
  by element), so a token can never accept a row the plain rule rejects.
