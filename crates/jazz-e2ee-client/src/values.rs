//! Plaintext of encrypted columns (`E2EE_PROTOCOL.md` §4.1; crypto format,
//! cell record): a cell's plaintext is Jazz's native row encoding of the
//! single value, its context binds the column's sealed type as canonical
//! JSON, and an equality index hashes the value's logical form.
//!
//! - The native encoding is the record encoding of a one-field record of
//!   the column's logical type (nullable when the column is optional), so
//!   null is sealed like any other value and the server cannot tell it
//!   apart.
//! - `cellType` is `{"column_type": <type>, "nullable": <bool>}`, keys
//!   sorted by UTF-16 code unit at every level and no whitespace, `<type>`
//!   being the column type as app files serialise it (`{"type":"Text"}`).
//! - A streamed value's plaintext is its raw bytes: only non-indexed
//!   `bytes` columns stream.

use jazz::e2ee::PlainType;
use jazz::records::{RecordDescriptor, Value, ValueType};
use jazz::tools::deployment::artifact::encryption::{cell_type_integer, encrypted_value_type};
use jazz::tools::{ColumnType, Value as DeclaredValue};
use jazz_e2ee::protocol::json::{push_string, utf16_cmp};
use jazz_e2ee::records::EqualityValue;
use jazz_e2ee::records::{EqualityColumn, EqualityColumnType};

use crate::error::ClientError;

/// How one encrypted column's values are encoded, typed and tagged.
#[derive(Debug, Clone)]
pub(crate) struct ColumnCodec {
    /// The type a reader receives the value as: optional columns nullable.
    pub(crate) value_type: ValueType,
    pub(crate) nullable: bool,
    descriptor: RecordDescriptor,
    /// The canonical JSON of the sealed type.
    pub(crate) cell_type: Vec<u8>,
    pub(crate) equality: Option<EqualityColumn>,
    pub(crate) is_bytes: bool,
    column_type: ColumnType,
    /// What a cell that was never written reads as (protocol §4.1): the
    /// declared default; else null for an optional column; else nothing
    /// (a required column without a default).
    pub(crate) empty: Option<Value>,
}

fn unsupported(column: &str, what: &str) -> ClientError {
    ClientError::Unsupported(format!(
        "encrypted column `{column}`: {what} is not supported yet"
    ))
}

fn equality_type(column_type: &ColumnType) -> EqualityColumnType {
    match column_type {
        ColumnType::Integer => EqualityColumnType::Integer,
        ColumnType::BigInt => EqualityColumnType::BigInt,
        ColumnType::Boolean => EqualityColumnType::Boolean,
        ColumnType::Text => EqualityColumnType::Text,
        ColumnType::Enum { .. } => EqualityColumnType::Enum,
        ColumnType::Json { .. } => EqualityColumnType::Json,
        ColumnType::Timestamp => EqualityColumnType::Timestamp,
        ColumnType::Double => EqualityColumnType::Double,
        ColumnType::Uuid => EqualityColumnType::Uuid,
        ColumnType::Bytea => EqualityColumnType::Bytes,
        ColumnType::TransactionId => EqualityColumnType::TransactionId,
        ColumnType::Array { .. } => EqualityColumnType::Array,
        _ => EqualityColumnType::Row,
    }
}

/// A JSON schema value: objects with keys sorted by UTF-16 code unit,
/// integers in decimal; a number with a fraction or exponent is refused.
fn push_schema(out: &mut String, value: &serde_json::Value) -> Result<(), ()> {
    match value {
        serde_json::Value::Null => out.push_str("null"),
        serde_json::Value::Bool(value) => out.push_str(if *value { "true" } else { "false" }),
        serde_json::Value::Number(number) => {
            out.push_str(&cell_type_integer(number).ok_or(())?.to_string())
        }
        serde_json::Value::String(value) => push_string(out, value),
        serde_json::Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                push_schema(out, item)?;
            }
            out.push(']');
        }
        serde_json::Value::Object(map) => {
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_by(|a, b| utf16_cmp(a.0, b.0));
            out.push('{');
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                push_string(out, key);
                out.push(':');
                push_schema(out, value)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

/// A column type in the cell type encoding: `{"type":<name>}`, with
/// `variants` for an enum, `schema` for a JSON column that has one and
/// `element` for an array, keys in UTF-16 code unit order.
fn push_column_type(
    out: &mut String,
    column: &str,
    column_type: &ColumnType,
) -> Result<(), ClientError> {
    let simple = |name: &str| format!(r#"{{"type":"{name}"}}"#);
    match column_type {
        ColumnType::Integer => out.push_str(&simple("Integer")),
        ColumnType::BigInt => out.push_str(&simple("BigInt")),
        ColumnType::Boolean => out.push_str(&simple("Boolean")),
        ColumnType::Text => out.push_str(&simple("Text")),
        ColumnType::Timestamp => out.push_str(&simple("Timestamp")),
        ColumnType::Double => out.push_str(&simple("Double")),
        ColumnType::Uuid => out.push_str(&simple("Uuid")),
        ColumnType::Bytea => out.push_str(&simple("Bytea")),
        ColumnType::Enum { variants } => {
            out.push_str(r#"{"type":"Enum","variants":["#);
            for (index, variant) in variants.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                push_string(out, variant);
            }
            out.push_str("]}");
        }
        ColumnType::Json { schema } => {
            out.push('{');
            if let Some(schema) = schema {
                out.push_str(r#""schema":"#);
                push_schema(out, schema)
                    .map_err(|()| unsupported(column, "a JSON schema with a non-integer number"))?;
                out.push(',');
            }
            out.push_str(r#""type":"Json"}"#);
        }
        ColumnType::Array { element } => {
            out.push_str(r#"{"element":"#);
            push_column_type(out, column, element)?;
            out.push_str(r#","type":"Array"}"#);
        }
        _ => return Err(unsupported(column, "this column type")),
    }
    Ok(())
}

/// The `cellType` of a sealed type, in the `jazz-e2ee-cell-type-v1`
/// encoding: `{"column_type":<column type>,"nullable":<bool>}`.
pub(crate) fn cell_type(column: &str, plain: &PlainType) -> Result<Vec<u8>, ClientError> {
    let mut out = String::from(r#"{"column_type":"#);
    push_column_type(&mut out, column, &plain.column_type)?;
    out.push_str(if plain.nullable {
        r#","nullable":true}"#
    } else {
        r#","nullable":false}"#
    });
    Ok(out.into_bytes())
}

impl ColumnCodec {
    /// The codec of encrypted column `column`, declared `plain` and sealed
    /// as `sealed`, with an equality index when `indexed`.
    pub(crate) fn new(
        column: &str,
        plain: &PlainType,
        sealed: &PlainType,
        indexed: bool,
    ) -> Result<Self, ClientError> {
        let mut value_type = encrypted_value_type(&plain.column_type).map_err(|reason| {
            ClientError::Unsupported(format!("encrypted column `{column}`: {reason}"))
        })?;
        if plain.nullable {
            value_type = value_type.nullable();
        }
        let equality = if indexed {
            Some(equality_column(&plain.column_type).map_err(|error| unsupported(column, &error))?)
        } else {
            None
        };
        let mut codec = Self {
            descriptor: RecordDescriptor::new([("value", value_type.clone())]),
            value_type,
            nullable: plain.nullable,
            cell_type: cell_type(column, sealed)?,
            equality,
            is_bytes: plain.column_type == ColumnType::Bytea,
            column_type: plain.column_type.clone(),
            empty: None,
        };
        let default = match &plain.default {
            Some(default) => Some(
                record_value(default)
                    .and_then(|value| codec.typed(&value))
                    .ok_or_else(|| {
                        ClientError::Settings(format!(
                            "encrypted column `{column}`: its default is not a value of its type"
                        ))
                    })?,
            ),
            None => None,
        };
        codec.empty = match &default {
            Some(default) => Some(default.clone()),
            None if plain.nullable => Some(Value::Nullable(None)),
            // A required column without a default has no value a never
            // written cell could read as: deploy refuses to add one to rows
            // already stored (protocol §4.1).
            None => None,
        };
        Ok(codec)
    }

    /// `value` as the column's type: an optional column takes a bare value
    /// or null.
    fn typed(&self, value: &Value) -> Option<Value> {
        let value = match (self.nullable, value) {
            (true, Value::Nullable(_)) | (false, _) => value.clone(),
            (true, value) => Value::Nullable(Some(Box::new(value.clone()))),
        };
        value.check_type(&self.value_type).is_ok().then_some(value)
    }

    /// Whether `value` is what a never-written cell of this column reads
    /// as: rows stored before a migration added the column hold no
    /// equality token for it, so an index cannot find them by it.
    pub(crate) fn is_empty_value(&self, value: &Value) -> bool {
        self.typed(value)
            .is_some_and(|value| self.empty.as_ref() == Some(&value))
    }

    /// The native encoding of a plaintext `value`, or `None` when it is not
    /// a value of the column's type.
    pub(crate) fn encode(&self, value: &Value) -> Option<Vec<u8>> {
        self.descriptor.create(&[self.typed(value)?]).ok()
    }

    /// The value a native encoding holds, or `None` when the bytes are not
    /// one of the column's type.
    pub(crate) fn decode(&self, plaintext: &[u8]) -> Option<Value> {
        let value = self.descriptor.get_idx(plaintext, 0).ok()?;
        // Only the exact encoding of that value is accepted. The record
        // decoder already refuses every other form found, so this guards
        // against a later, more lenient decoder.
        (self.descriptor.create(std::slice::from_ref(&value)).ok()? == plaintext).then_some(value)
    }

    /// The equality-index form of a value of the column's type.
    pub(crate) fn equality_value(&self, value: &Value) -> Option<EqualityValue> {
        equality_value(&self.column_type, self.typed(value)?)
    }
}

/// The equality-index form of `value`, a value of `column_type` (null
/// elements of an array included): JSON by its exact stored text, an array
/// element by element as the element's type.
fn equality_value(column_type: &ColumnType, value: Value) -> Option<EqualityValue> {
    let inner = match value {
        Value::Nullable(None) => return Some(EqualityValue::Null),
        Value::Nullable(Some(inner)) => *inner,
        value => value,
    };
    Some(match (column_type, inner) {
        (ColumnType::Array { element }, Value::Array(items)) => EqualityValue::Array(
            items
                .into_iter()
                .map(|item| equality_value(element, item))
                .collect::<Option<_>>()?,
        ),
        (ColumnType::Json { .. }, Value::String(text)) => EqualityValue::Json(text),
        (ColumnType::Enum { .. }, Value::String(variant)) => EqualityValue::Enum(variant),
        (_, Value::Bool(value)) => EqualityValue::Bool(value),
        (_, Value::I32(value)) => EqualityValue::Int(value.into()),
        (_, Value::I64(value)) => EqualityValue::Int(value),
        (_, Value::U64(value)) => EqualityValue::Timestamp(value),
        (_, Value::F64(value)) => EqualityValue::Float(value),
        (_, Value::String(value)) => EqualityValue::Text(value),
        (_, Value::Bytes(value)) => EqualityValue::Bytes(value),
        (_, Value::Uuid(value)) => EqualityValue::Uuid(*value.as_bytes()),
        _ => return None,
    })
}

/// The equality index of a column of `column_type`: an array's elements
/// are indexed as their own type.
fn equality_column(column_type: &ColumnType) -> Result<EqualityColumn, String> {
    match column_type {
        ColumnType::Array { element } => {
            EqualityColumn::array_of(equality_column(element)?).map_err(|error| error.to_string())
        }
        column_type => {
            EqualityColumn::declare(equality_type(column_type)).map_err(|error| error.to_string())
        }
    }
}

/// A declared default as the database carries values.
fn record_value(value: &DeclaredValue) -> Option<Value> {
    Some(match value {
        DeclaredValue::Integer(value) => Value::I32(*value),
        DeclaredValue::BigInt(value) => Value::I64(*value),
        DeclaredValue::Double(value) => Value::F64(*value),
        DeclaredValue::Boolean(value) => Value::Bool(*value),
        DeclaredValue::Text(value) => Value::String(value.clone()),
        DeclaredValue::Timestamp(value) => Value::U64(*value),
        DeclaredValue::Uuid(value) => Value::Uuid(*value.uuid()),
        DeclaredValue::Bytea(value) => Value::Bytes(value.clone()),
        DeclaredValue::Array(items) => {
            Value::Array(items.iter().map(record_value).collect::<Option<_>>()?)
        }
        DeclaredValue::Null => Value::Nullable(None),
        _ => return None,
    })
}

/// The encodings here are durable: every stored cell's plaintext and
/// context depend on them, and nothing outside the crate can observe them
/// except by decrypting, so they are pinned at this level.
#[cfg(test)]
mod tests {
    use super::*;

    fn codec(column_type: ColumnType, nullable: bool, indexed: bool) -> ColumnCodec {
        let plain = PlainType {
            column_type,
            nullable,
            default: None,
        };
        ColumnCodec::new("c", &plain, &plain, indexed).unwrap()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn every_cell_type_has_its_pinned_encoding() {
        let encoded = |column_type: ColumnType, nullable: bool| {
            String::from_utf8(
                cell_type(
                    "c",
                    &PlainType {
                        column_type,
                        nullable,
                        default: None,
                    },
                )
                .unwrap(),
            )
            .unwrap()
        };
        for (column_type, name) in [
            (ColumnType::Integer, "Integer"),
            (ColumnType::BigInt, "BigInt"),
            (ColumnType::Boolean, "Boolean"),
            (ColumnType::Text, "Text"),
            (ColumnType::Timestamp, "Timestamp"),
            (ColumnType::Double, "Double"),
            (ColumnType::Uuid, "Uuid"),
            (ColumnType::Bytea, "Bytea"),
        ] {
            assert_eq!(
                encoded(column_type, true),
                format!(r#"{{"column_type":{{"type":"{name}"}},"nullable":true}}"#)
            );
        }
        assert_eq!(
            encoded(
                ColumnType::Enum {
                    variants: vec!["open".into(), "done".into()]
                },
                true
            ),
            r#"{"column_type":{"type":"Enum","variants":["open","done"]},"nullable":true}"#
        );
        assert_eq!(
            encoded(ColumnType::Json { schema: None }, false),
            r#"{"column_type":{"type":"Json"},"nullable":false}"#
        );
        assert_eq!(
            encoded(
                ColumnType::Json {
                    schema: Some(
                        serde_json::json!({ "type": "object", "maxProperties": 3, "title": "a\"b\n" })
                    )
                },
                false
            ),
            r#"{"column_type":{"schema":{"maxProperties":3,"title":"a\"b\n","type":"object"},"type":"Json"},"nullable":false}"#
        );
        assert_eq!(
            encoded(
                ColumnType::Array {
                    element: Box::new(ColumnType::Enum {
                        variants: vec!["x".into()]
                    })
                },
                false
            ),
            r#"{"column_type":{"element":{"type":"Enum","variants":["x"]},"type":"Array"},"nullable":false}"#
        );
        let fraction = PlainType {
            column_type: ColumnType::Json {
                schema: Some(serde_json::json!({ "minimum": 0.5 })),
            },
            nullable: false,
            default: None,
        };
        assert!(cell_type("c", &fraction).is_err());
    }

    // These tests guard stored encrypted cells across schema changes. The native
    // bytes are pinned for each type, and any type change accepted at deploy
    // must let the next client read what the previous client wrote.
    fn native_examples() -> Vec<(ColumnType, Value, &'static str, &'static str)> {
        // Required value bytes and the bytes of an optional null, respectively.
        vec![
            (ColumnType::Integer, Value::I32(7), "07000000", "0000000000"),
            (
                ColumnType::BigInt,
                Value::I64(-2),
                "feffffffffffffff",
                "000000000000000000",
            ),
            (ColumnType::Boolean, Value::Bool(true), "01", "0000"),
            (ColumnType::Text, Value::String("hi".into()), "006869", "00"),
            (
                ColumnType::Timestamp,
                Value::U64(258),
                "0201000000000000",
                "000000000000000000",
            ),
            (
                ColumnType::Double,
                Value::F64(1.5),
                "000000000000f83f",
                "000000000000000000",
            ),
            (
                ColumnType::Uuid,
                Value::Uuid(uuid::Uuid::from_u128(0x00112233445566778899aabbccddeeff)),
                "00112233445566778899aabbccddeeff",
                "0000000000000000000000000000000000",
            ),
            (
                ColumnType::Bytea,
                Value::Bytes(vec![0, 255]),
                "0000ff",
                "00",
            ),
            (enum_type(&["a"]), Value::String("a".into()), "0061", "00"),
            (
                ColumnType::Json { schema: None },
                Value::String("{}".into()),
                "007b7d",
                "00",
            ),
            (
                ColumnType::Array {
                    element: Box::new(ColumnType::Integer),
                },
                Value::Array(vec![Value::I32(1), Value::I32(-2)]),
                "01000000feffffff",
                "00",
            ),
            (
                ColumnType::Array {
                    element: Box::new(enum_type(&["a", "b"])),
                },
                Value::Array(vec![Value::String("b".into()), Value::String("a".into())]),
                "020000000a00000000620061",
                "00",
            ),
        ]
    }

    fn enum_type(names: &[&str]) -> ColumnType {
        ColumnType::Enum {
            variants: names.iter().map(|name| (*name).to_owned()).collect(),
        }
    }

    #[test]
    fn native_encodings_are_pinned_and_strict() {
        for (ty, value, required, null) in native_examples() {
            for nullable in [false, true] {
                let codec = codec(ty.clone(), nullable, false);
                let bytes = codec.encode(&value).unwrap();
                let (expected, decoded) = if nullable {
                    let none = Value::Nullable(None);
                    let encoded = codec.encode(&none).unwrap();
                    assert_eq!(hex(&encoded), null, "{ty:?} null");
                    assert_eq!(codec.decode(&encoded), Some(none));
                    (
                        format!("01{required}"),
                        Value::Nullable(Some(Box::new(value.clone()))),
                    )
                } else {
                    (required.to_owned(), value.clone())
                };
                assert_eq!(hex(&bytes), expected, "{ty:?}, nullable={nullable}");
                assert_eq!(codec.decode(&bytes), Some(decoded));
            }
        }
        assert!(
            codec(ColumnType::Text, false, true)
                .encode(&Value::I32(1))
                .is_none()
        );
        let rank = codec(ColumnType::Integer, true, false);
        let mut trailing = rank.encode(&Value::I32(7)).unwrap();
        trailing.push(0);
        assert_eq!(rank.decode(&trailing), None);
    }

    /// Run the deploy's transition check with the same physical bytes column
    /// in both versions and the column's original type pinned in both.
    fn transition(
        original: &PlainType,
        before: &PlainType,
        after: &PlainType,
    ) -> Result<(), String> {
        use jazz::tools::deployment::artifact::encryption::{Entries, check_transition};
        use jazz::tools::deployment::published::PublishMigrationRequest;
        use jazz::tools::{SchemaBuilder, TableSchemaBuilder};

        let entries = |plain: &PlainType| -> Entries {
            serde_json::from_value(serde_json::json!({
                "notes": { "scope": "notes", "space": "id", "columns": {
                    "value": { "type": plain, "sealedType": original }
                } }
            }))
            .unwrap()
        };
        let schema = SchemaBuilder::new()
            .table(TableSchemaBuilder::new("notes").column("value", ColumnType::Bytea))
            .build();
        check_transition(
            (&schema, Some(&entries(before))),
            (&schema, Some(&entries(after))),
            &PublishMigrationRequest {
                from_hash: String::new(),
                to_hash: String::new(),
                forward: Vec::new(),
            },
        )
    }

    #[test]
    fn every_accepted_transition_reads_the_previous_value() {
        let mut examples = native_examples();
        examples.push((
            enum_type(&["a", "b"]),
            Value::String("b".into()),
            "0062",
            "00",
        ));
        for (old, value, _, _) in &examples {
            for nullable in [false, true] {
                let before = PlainType::new(old.clone(), nullable);
                let writer = ColumnCodec::new("value", &before, &before, false).unwrap();
                let mut values = vec![value.clone()];
                if nullable {
                    values.push(Value::Nullable(None));
                }
                for (new, _, _, _) in &examples {
                    for next_nullable in [false, true] {
                        let after = PlainType::new(new.clone(), next_nullable);
                        let accepted = transition(&before, &before, &after);
                        if before == after {
                            assert!(
                                accepted.is_ok(),
                                "identical declaration {before:?}: {accepted:?}"
                            );
                        }
                        if accepted.is_err() {
                            continue;
                        }
                        let reader = ColumnCodec::new("value", &after, &before, false).unwrap();
                        assert_eq!(writer.cell_type, reader.cell_type);
                        for value in &values {
                            let bytes = writer.encode(value).unwrap();
                            let expected = writer.decode(&bytes).unwrap();
                            assert_eq!(
                                reader.decode(&bytes),
                                Some(expected),
                                "{before:?} -> {after:?}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn encrypted_enums_keep_every_previously_declared_variant() {
        for nullable in [false, true] {
            let first = PlainType::new(enum_type(&["a"]), nullable);
            let second = PlainType::new(enum_type(&["a", "b"]), nullable);
            let third = PlainType::new(enum_type(&["a", "b", "c"]), nullable);
            transition(&first, &first, &second).unwrap();
            transition(&first, &second, &third).unwrap();
            let error = transition(&first, &second, &first).unwrap_err();
            assert!(error.contains("appending enum variants"), "{error}");

            let latest = ColumnCodec::new("value", &third, &first, false).unwrap();
            for (declared, name) in [(&first, "a"), (&second, "b"), (&third, "c")] {
                let writer = ColumnCodec::new("value", declared, &first, false).unwrap();
                let name = Value::String(name.into());
                let bytes = writer.encode(&name).unwrap();
                assert_eq!(latest.decode(&bytes), writer.typed(&name));
            }
        }
    }

    #[test]
    fn an_empty_cell_reads_the_default_null_or_nothing() {
        let with = |column_type: ColumnType, nullable: bool, default: Option<DeclaredValue>| {
            let plain = PlainType {
                column_type,
                nullable,
                default,
            };
            ColumnCodec::new("c", &plain, &plain, false)
        };
        let declared = with(ColumnType::Integer, false, Some(DeclaredValue::Integer(7)));
        assert_eq!(declared.unwrap().empty, Some(Value::I32(7)));
        let optional = with(ColumnType::Integer, true, Some(DeclaredValue::Integer(7)));
        assert_eq!(
            optional.unwrap().empty,
            Some(Value::Nullable(Some(Box::new(Value::I32(7)))))
        );
        assert_eq!(
            with(ColumnType::Text, true, None).unwrap().empty,
            Some(Value::Nullable(None))
        );
        assert_eq!(with(ColumnType::Text, false, None).unwrap().empty, None);
        assert!(matches!(
            with(
                ColumnType::Integer,
                false,
                Some(DeclaredValue::Text("x".into()))
            ),
            Err(ClientError::Settings(_))
        ));
    }

    #[test]
    fn equality_values_follow_the_column_type() {
        let status = codec(
            ColumnType::Enum {
                variants: vec!["open".into()],
            },
            true,
            true,
        );
        assert_eq!(
            status.equality_value(&Value::String("open".into())),
            Some(EqualityValue::Enum("open".into()))
        );
        assert_eq!(
            status.equality_value(&Value::Nullable(None)),
            Some(EqualityValue::Null)
        );
        let json = codec(ColumnType::Json { schema: None }, false, true);
        assert_eq!(
            json.equality_value(&Value::String(r#"{"b":1,"a":2}"#.into())),
            Some(EqualityValue::Json(r#"{"b":1,"a":2}"#.into()))
        );
        let tags = codec(
            ColumnType::Array {
                element: Box::new(ColumnType::Text),
            },
            false,
            true,
        );
        assert_eq!(
            tags.equality_value(&Value::Array(vec![
                Value::String("x".into()),
                Value::String("y".into())
            ])),
            Some(EqualityValue::Array(vec![
                EqualityValue::Text("x".into()),
                EqualityValue::Text("y".into())
            ]))
        );
        let rows = PlainType {
            column_type: ColumnType::Array {
                element: Box::new(ColumnType::Array {
                    element: Box::new(ColumnType::Integer),
                }),
            },
            nullable: false,
            default: None,
        };
        assert!(ColumnCodec::new("c", &rows, &rows, true).is_ok());
    }
}
