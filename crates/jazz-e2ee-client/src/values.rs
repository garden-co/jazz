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
use jazz::tools::{ColumnType, Value as DeclaredValue};
use jazz_e2ee::protocol::content::EqualityValueBuf;
use jazz_e2ee::protocol::json::{push_string, utf16_cmp};
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
    /// The declared default, as the column's type: what an insert that
    /// leaves the column out writes.
    pub(crate) default: Option<Value>,
}

fn unsupported(column: &str, what: &str) -> ClientError {
    ClientError::Unsupported(format!(
        "encrypted column `{column}`: {what} is not supported yet"
    ))
}

/// The logical type of a column type an encrypted column may have.
fn logical(column: &str, column_type: &ColumnType) -> Result<ValueType, ClientError> {
    Ok(match column_type {
        ColumnType::Integer => ValueType::I32,
        ColumnType::BigInt => ValueType::I64,
        ColumnType::Boolean => ValueType::Bool,
        ColumnType::Text | ColumnType::Enum { .. } | ColumnType::Json { .. } => ValueType::String,
        ColumnType::Timestamp => ValueType::U64,
        ColumnType::Double => ValueType::F64,
        ColumnType::Uuid => ValueType::Uuid,
        ColumnType::Bytea => ValueType::Bytes,
        ColumnType::Array { element } => logical(column, element)?.array_of(),
        _ => return Err(unsupported(column, "this column type")),
    })
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

/// The name of the cell type encoding: the `T` every cell record of
/// version 1 binds (crypto format, "Cell type, version 1").
pub const CELL_TYPE_ENCODING: &str = "jazz-e2ee-cell-type-v1";

/// A JSON schema value: objects with keys sorted by UTF-16 code unit,
/// integers in decimal; a number with a fraction or exponent is refused.
fn push_schema(out: &mut String, value: &serde_json::Value) -> Result<(), ()> {
    match value {
        serde_json::Value::Null => out.push_str("null"),
        serde_json::Value::Bool(value) => out.push_str(if *value { "true" } else { "false" }),
        serde_json::Value::Number(number) => match (number.as_i64(), number.as_u64()) {
            (Some(value), _) => out.push_str(&value.to_string()),
            (None, Some(value)) => out.push_str(&value.to_string()),
            _ => return Err(()),
        },
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
        let mut value_type = logical(column, &plain.column_type)?;
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
            default: None,
        };
        codec.default = match &plain.default {
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
        codec.empty = match &codec.default {
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
    pub(crate) fn equality_value(&self, value: &Value) -> Option<EqualityValueBuf> {
        equality_buf(&self.column_type, self.typed(value)?)
    }
}

/// The equality-index form of `value`, a value of `column_type` (null
/// elements of an array included): JSON by its exact stored text, an array
/// element by element as the element's type.
fn equality_buf(column_type: &ColumnType, value: Value) -> Option<EqualityValueBuf> {
    let inner = match value {
        Value::Nullable(None) => return Some(EqualityValueBuf::Null),
        Value::Nullable(Some(inner)) => *inner,
        value => value,
    };
    Some(match (column_type, inner) {
        (ColumnType::Array { element }, Value::Array(items)) => EqualityValueBuf::Array(
            items
                .into_iter()
                .map(|item| equality_buf(element, item))
                .collect::<Option<_>>()?,
        ),
        (ColumnType::Json { .. }, Value::String(text)) => EqualityValueBuf::Json(text),
        (ColumnType::Enum { .. }, Value::String(variant)) => EqualityValueBuf::Enum(variant),
        (_, Value::Bool(value)) => EqualityValueBuf::Bool(value),
        (_, Value::I32(value)) => EqualityValueBuf::Int(value.into()),
        (_, Value::I64(value)) => EqualityValueBuf::Int(value),
        (_, Value::U64(value)) => EqualityValueBuf::Timestamp(value),
        (_, Value::F64(value)) => EqualityValueBuf::Float(value),
        (_, Value::String(value)) => EqualityValueBuf::Text(value),
        (_, Value::Bytes(value)) => EqualityValueBuf::Bytes(value),
        (_, Value::Uuid(value)) => EqualityValueBuf::Uuid(*value.as_bytes()),
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
    fn cell_types_are_pinned() {
        let text = codec(ColumnType::Text, false, true);
        assert_eq!(
            String::from_utf8(text.cell_type).unwrap(),
            r#"{"column_type":{"type":"Text"},"nullable":false}"#
        );
        let status = codec(
            ColumnType::Enum {
                variants: vec!["open".into(), "done".into()],
            },
            true,
            true,
        );
        assert_eq!(
            String::from_utf8(status.cell_type).unwrap(),
            r#"{"column_type":{"type":"Enum","variants":["open","done"]},"nullable":true}"#
        );
    }

    #[test]
    fn every_cell_type_has_its_pinned_encoding() {
        assert_eq!(CELL_TYPE_ENCODING, "jazz-e2ee-cell-type-v1");
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

    #[test]
    fn native_encodings_are_pinned_and_strict() {
        let text = codec(ColumnType::Text, false, true);
        let encoded = text.encode(&Value::String("hi".into())).unwrap();
        assert_eq!(hex(&encoded), "006869");
        assert_eq!(text.decode(&encoded), Some(Value::String("hi".into())));
        assert_eq!(text.encode(&Value::I32(1)), None);

        let rank = codec(ColumnType::Integer, true, false);
        let some = rank.encode(&Value::I32(7)).unwrap();
        let none = rank.encode(&Value::Nullable(None)).unwrap();
        assert_eq!(hex(&some), "0107000000");
        assert_eq!(hex(&none), "0000000000");
        assert_eq!(
            rank.decode(&some),
            Some(Value::Nullable(Some(Box::new(Value::I32(7)))))
        );
        assert_eq!(rank.decode(&none), Some(Value::Nullable(None)));
        let mut trailing = some.clone();
        trailing.push(0);
        assert_eq!(rank.decode(&trailing), None);
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
            Some(EqualityValueBuf::Enum("open".into()))
        );
        assert_eq!(
            status.equality_value(&Value::Nullable(None)),
            Some(EqualityValueBuf::Null)
        );
        let json = codec(ColumnType::Json { schema: None }, false, true);
        assert_eq!(
            json.equality_value(&Value::String(r#"{"b":1,"a":2}"#.into())),
            Some(EqualityValueBuf::Json(r#"{"b":1,"a":2}"#.into()))
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
            Some(EqualityValueBuf::Array(vec![
                EqualityValueBuf::Text("x".into()),
                EqualityValueBuf::Text("y".into())
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
