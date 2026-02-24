# Real (Float) Column Type

## Overview

The TypeScript DSL supports `col.float()` and emits `REAL` in SQL, but the Rust SQL parser rejects it with `UnsupportedType`. The WASM bridge also lacks a Real variant, so the codegen path currently maps REAL to Integer as a lossy workaround.

This spec adds end-to-end support for floating-point columns.

## Design decisions

### One float type, always f64

SQL distinguishes REAL (32-bit) from DOUBLE PRECISION (64-bit), but JavaScript `Number` is f64. Using f32 internally would silently lose precision on JS round-trips. Store and operate on f64 everywhere.

### Accept multiple SQL keywords

The parser should accept `REAL`, `FLOAT`, and `DOUBLE` as aliases, all mapping to the same `ColumnType::Real`. Emit `REAL` when serialising back to SQL.

### No separate DECIMAL/NUMERIC type

Those imply exact fixed-point arithmetic, which is a different feature. Out of scope.

### Negative zero: faithful storage with IEEE 754 query semantics

IEEE 754 defines `-0.0 == 0.0` (they compare equal). However, the two values have different bit patterns (`0x8000000000000000` vs `0x0000000000000000`), and the sign carries real-world meaning: a small negative value that underflows to `-0.0` _was_ negative. As a database, our primary concern is persisting what the user gives us faithfully.

Our index key encoding maps f64 bit patterns to bytes that sort lexicographically, which means `-0.0` and `0.0` naturally become distinct, adjacent index entries, with `-0.0` sorting immediately below `0.0`. This is correct for storage identity but deviates from IEEE 754 equality.

We considered three options:

1. **Normalise `-0.0` to `0.0` on insert.** IEEE 754 compliant for equality. Simple. But intentionally discards information the user gave us. A small negative value that underflowed is genuinely below zero, and we'd be erasing that provenance.

2. **Store distinct bits, accept the deviation.** Faithful storage, simple implementation. But violates IEEE 754 equality, which could surprise users: `WHERE price = 0.0` would not match a stored `-0.0`.

3. **Store distinct bits, match both on query.** Faithful storage and IEEE 754 compliant equality semantics. More complex, but gives us options in future (e.g. exposing raw bit patterns if a use case arises, while keeping standard behaviour as the default).

We chose option 3. The complexity is contained to two index methods:

**Exact lookup** (`index_lookup`): when the value is `Real(0.0)` or `Real(-0.0)`, query both encoded keys and merge the results.

**Range queries** (`index_range`): when a bound involves a Real zero, adjust the bound to account for both representations. Specifically:

- `>= 0.0` should include `-0.0` (they're equal per IEEE 754), but our encoding would miss it since `-0.0` sorts below. Fix: expand lower bound to `Included(-0.0)`.
- `< 0.0` should exclude `-0.0` (not strictly less than an equal value), but our encoding would include it. Fix: tighten upper bound to `Excluded(-0.0)`.
- `<= 0.0` already works: `-0.0` sorts below `0.0`, so it falls within the range naturally.
- `> 0.0` already works: `-0.0` sorts below `0.0`, so it's excluded from "strictly greater" naturally.

Implement this as a small helper (`fn ieee754_adjusted_bounds(...)` or similar) rather than spreading the logic across call sites.

**Binary row encoding** is unaffected: it stores raw f64 bits faithfully, no special-casing.

## Architecture / Components

### 1. Rust core types

Files:

- `crates/jazz-tools/src/query_manager/types/schema.rs`
- `crates/jazz-tools/src/query_manager/types/value.rs`

Changes:

- Add `ColumnType::Real` with doc comment `/// 8-byte IEEE 754 double-precision float (f64).`
- Add `fixed_size` arm returning `Some(8)`.
- Add `Value::Real(f64)` variant.
- Add `Value::Real(_) => Some(ColumnType::Real)` in `column_type()`.

Note: `ColumnType` derives `Eq`. `f64` is not `Eq`. `Value` currently derives `PartialEq` and `Eq` via the blanket derive. Options:

- Implement `PartialEq`/`Eq` manually on `Value`, using `f64::to_bits()` for the Real arm (bitwise equality, NaN == NaN). This is the right semantics for storage identity comparison.
- Or remove `Eq` from `Value` and fix any call sites. Likely more disruptive.

Recommend the manual impl approach.

### 2. Rust binary encoding

File: `crates/jazz-tools/src/query_manager/encoding.rs`

Changes:

- Encode: `Value::Real(f) => buf.extend_from_slice(&f.to_le_bytes())` (8 bytes, little-endian, matching BigInt).
- Decode: `ColumnType::Real => f64::from_le_bytes(bytes[..8].try_into()...)`.
- Type validation: `ColumnType::Real` accepts `Value::Real(_)`.

### 3. Rust index key encoding

File: `crates/jazz-tools/src/storage/mod.rs` (`encode_value`)

Add a `Value::Real(f)` arm with a new tag byte (e.g. `0x09`, after Row's `0x08`) and order-preserving encoding:

```rust
Value::Real(f) => {
    let mut bytes = vec![0x09];
    let bits = f.to_bits();
    // Flip for lexicographic ordering: if sign bit set, flip all bits;
    // otherwise flip only the sign bit.
    let ordered = if bits & (1u64 << 63) != 0 {
        !bits
    } else {
        bits ^ (1u64 << 63)
    };
    bytes.extend_from_slice(&ordered.to_be_bytes());
    bytes
}
```

This gives correct ordering: negative floats sort below positive, with magnitude ordering correct in both directions. NaN sorts to one end (acceptable for index purposes).

### 4. Rust index query methods (negative zero handling)

Files:

- `crates/jazz-tools/src/storage/mod.rs` (MemoryStorage)
- `crates/jazz-tools/src/storage/opfs_btree.rs` (OpfsBTreeStorage)
- `crates/jazz-tools/src/storage/surrealkv.rs` (SurrealKvStorage)
- Or, if index operations are shared via `storage_core`, the adjustment may live there.

Add a helper for IEEE 754 negative zero adjustment:

```rust
/// Adjusts index bounds for IEEE 754 negative zero semantics.
///
/// -0.0 and 0.0 have distinct bit patterns (and thus distinct index keys)
/// but are equal per IEEE 754. This helper expands/tightens bounds so that
/// range queries treat both zeros as equal while the storage layer preserves
/// the original bit patterns.
fn ieee754_adjusted_bounds(
    start: Bound<&Value>,
    end: Bound<&Value>,
) -> (Bound<Value>, Bound<Value>) { ... }
```

For `index_lookup`: when the value is `Real(0.0)` or `Real(-0.0)`, query both keys and merge results.

### 5. Rust SQL parser and emitter

File: `crates/jazz-tools/src/schema_manager/sql.rs`

Changes:

- `parse_column_type`: add `"REAL" | "FLOAT" | "DOUBLE" => ColumnType::Real` to the match.
- `column_type_to_sql`: add `ColumnType::Real => "REAL".to_string()`.

### 6. Rust schema manager encoding

Files:

- `crates/jazz-tools/src/schema_manager/encoding.rs`
- `crates/jazz-tools/src/schema_manager/diff.rs`
- `crates/jazz-tools/src/schema_manager/auto_lens.rs`

Changes:

- Add catalogue encoding tag for Real (follows the pattern of existing scalar types).
- Diff and auto-lens should treat Real as a simple scalar, same as Integer/BigInt.

### 7. WASM/NAPI bridge

Files:

- `crates/jazz-wasm/src/types.rs`
- `crates/jazz-napi/src/lib.rs`

Changes:

- Add `WasmColumnType::Real` variant.
- Add `WasmValue::Real(f64)` variant.
- Add `From` impl arms in both directions (trivial).
- tsify auto-generates the TypeScript discriminated union member `{ type: "Real" }` and `{ type: "Real", value: number }`.

### 8. TypeScript codegen and runtime

Files:

- `packages/jazz-tools/src/codegen/schema-reader.ts`
- `packages/jazz-tools/src/runtime/value-converter.ts`

Changes:

- `schema-reader.ts`: change `REAL: { type: "Integer" }` to `REAL: { type: "Real" }`.
- `value-converter.ts`: add `case "Real": return { type: "Real", value: Number(value) }`.

### 9. TypeScript DSL

No changes needed. `col.float()` already emits `"REAL"` and the TS type mapping already maps REAL to `number`.

## Testing strategy

Tests live inline (`#[cfg(test)] mod tests`) in the files being changed, following existing convention.

### Rust

- `schema_manager/sql.rs`: parse/emit round-trip for `REAL`, `FLOAT`, `DOUBLE` keywords.
- `query_manager/encoding.rs`: encode/decode round-trip for Real values including negative, zero, subnormal, infinity, NaN.
- `storage/mod.rs`:
  - `encode_value` ordering: negative < zero < positive for Real values; Real values sort after Row in cross-type ordering.
  - Negative zero index lookup: store as `-0.0`, lookup with `0.0`, expect match (and vice versa).
  - Negative zero range queries: verify `>= 0.0` includes `-0.0`; verify `< 0.0` excludes `-0.0`.

### TypeScript

- `sql-gen.test.ts`: already passes (no changes to SQL generation).
- `codegen/codegen.test.ts`: update the "converts REAL to Integer" test to expect `{ type: "Real" }` instead.
- `runtime/value-converter.test.ts`: add Real conversion case.

### Integration

The existing TS DSL to native build pipeline path should exercise the full round-trip once the Rust parser accepts REAL.

## Known limitations

### Negative zero may be lost crossing the WASM boundary (bidirectional)

Negative zero can be silently converted to positive zero when crossing the WASM boundary in either direction: JS to Rust (inserts/updates) and Rust to JS (query results).

This is caused by `serde_wasm_bindgen`'s `deserialize_any` implementation. When serde needs to buffer the `content` field of an adjacently tagged enum (`#[serde(tag = "type", content = "value")]`), it calls `deserialize_any` on the value. That implementation checks `Number.isSafeInteger()` to decide whether to store the value as an integer or a float. JavaScript's `Number.isSafeInteger(-0)` returns `true` (because `-0 === 0` in JS), so `-0.0` gets buffered as `Content::I64(0)`, which replays as `f64(0.0)`. The negative sign is lost.

This means:

- **JS to Rust (writes):** if a user inserts `-0.0` from JavaScript and buffering occurs, Rust receives `0.0` and persists it as such. The storage layer faithfully stores whatever it receives, but the value may already be wrong before it reaches storage.
- **Rust to JS (reads):** if `-0.0` is stored correctly in Rust, it may appear as `0.0` when returned to JavaScript.

The buffering only occurs when the `value` field precedes the `type` field in the JS object. When `type` comes first, serde reads the tag first and calls `deserialize_f64` directly, which preserves `-0.0`. Our `value-converter.ts` constructs objects with `type` first (`{ type: "Real", value: Number(value) }`), and Rust's serde serialisation follows struct field declaration order (also `type` first). So the common path through our own code should preserve `-0.0`, but this depends on JS object field ordering, which is not something we can strictly enforce.

This is not easily fixable on our side; the behaviour is deep inside serde's Content buffering machinery and `serde_wasm_bindgen`'s `deserialize_any` implementation. However, because our storage layer, index encoding, and query methods all handle negative zero correctly, our implementation will begin faithfully persisting and returning `-0.0` end to end as soon as the upstream serialisation issue is resolved. No changes to our code will be needed.

## Other edge cases

- **NaN and Infinity**: the binary encoding and index layer handle these correctly (NaN sorts to one end, infinities to their respective ends). However, non-finite values are rejected as column defaults because `format!("{f:?}")` produces `inf`/`NaN` which are not valid SQL or JavaScript literals. The SQL tokeniser naturally rejects these (they are identifiers, not numbers), and the `value_to_sql`/`value_to_ts_literal` functions assert finiteness as a safety net.

- **Scientific notation**: SQL literals like `1e10` or `2.5e-3` are out of scope. The tokeniser only consumes digits and `.`, so scientific notation is not recognised. This could be added later if needed.
