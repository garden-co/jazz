//! Behavior guards for compact record encoding and typed wrapper access.
//!
//! These tests own descriptor layout, scalar/nullable/tuple/array encoding,
//! projection, patching, seeded round-trip oracles, and generated record
//! wrappers. Database and IVM behavior is covered from [`crate::db::tests`];
//! this module stays focused on bytes and descriptor semantics.

use super::*;

fn descriptor(value_types: impl IntoIterator<Item = ValueType>) -> RecordDescriptor {
    RecordDescriptor::new(
        value_types
            .into_iter()
            .enumerate()
            .map(|(idx, value_type)| (format!("f{idx}"), value_type)),
    )
}

crate::define_record! {
    struct TestStaticRow {
        0 => id: u64,
        1 => name: String,
        2 => active: bool,
    }
}

crate::define_record! {
    struct TestTailRow {
        0 => row_id: Vec<u8>,
        .. user_cells,
    }
}

crate::define_record! {
    struct TestUuidRow {
        0 => id: uuid::Uuid,
        1 => maybe_owner: Option<uuid::Uuid>,
    }
}

crate::define_record! {
    struct TestTupleRow {
        0 => id: (uuid::Uuid, u64),
        1 => maybe_id: Option<(uuid::Uuid, u64)>,
    }
}

#[test]
fn descriptor_fields_remain_in_declaration_order() {
    let schema = RecordDescriptor::new([
        ("name", ValueType::String),
        ("age", ValueType::U8),
        ("payload", ValueType::Bytes),
        ("active", ValueType::Bool),
    ]);

    let names = schema
        .fields()
        .iter()
        .map(|field| field.name.as_deref().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(names, ["name", "age", "payload", "active"]);
}

#[test]
fn record_newtype_static_wrapper_round_trips_in_logical_order() {
    let descriptor = RecordDescriptor::new([
        ("id", ValueType::U64),
        ("name", ValueType::String),
        ("active", ValueType::Bool),
    ]);
    TestStaticRow::assert_layout(&descriptor);

    let row = TestStaticRow::encode(&descriptor, 7, "Monk".to_owned(), true).unwrap();

    assert_eq!(row.id().unwrap(), 7);
    assert_eq!(row.name().unwrap(), "Monk");
    assert!(row.active().unwrap());
    assert_eq!(row.record().to_values().unwrap()[0], Value::U64(7));
}

#[test]
fn record_newtype_tail_wrapper_uses_logical_tail_despite_physical_reordering() {
    let descriptor = RecordDescriptor::new([
        ("row_id", ValueType::Bytes),
        ("user_count", ValueType::Nullable(Box::new(ValueType::U64))),
        (
            "user_title",
            ValueType::Nullable(Box::new(ValueType::String)),
        ),
    ]);
    TestTailRow::assert_layout(&descriptor);

    let row = TestTailRow::encode(
        &descriptor,
        vec![1, 2, 3],
        &[
            Some(Value::U64(42)),
            Some(Value::String("logical tail".to_owned())),
        ],
    )
    .unwrap();

    assert_eq!(row.row_id().unwrap(), vec![1, 2, 3]);
    assert_eq!(row.cell(0).unwrap(), Some(Value::U64(42)));
    assert_eq!(
        row.cell(1).unwrap(),
        Some(Value::String("logical tail".to_owned()))
    );
    assert_eq!(
        row.cells().collect::<Result<Vec<_>, _>>().unwrap(),
        vec![
            Some(Value::U64(42)),
            Some(Value::String("logical tail".to_owned()))
        ]
    );
}

#[test]
#[should_panic(expected = "record field index drifted")]
fn record_newtype_layout_assertion_catches_name_drift() {
    let descriptor = RecordDescriptor::new([
        ("name", ValueType::U64),
        ("id", ValueType::String),
        ("active", ValueType::Bool),
    ]);

    TestStaticRow::assert_layout(&descriptor);
}

#[test]
fn uuid_fields_round_trip_order_and_nullable() {
    let low = uuid::Uuid::from_bytes([0; 16]);
    let high = uuid::Uuid::from_bytes([0xff; 16]);
    let descriptor = RecordDescriptor::new([
        ("id", ValueType::Uuid),
        (
            "maybe_owner",
            ValueType::Nullable(Box::new(ValueType::Uuid)),
        ),
    ]);
    TestUuidRow::assert_layout(&descriptor);

    let row = TestUuidRow::encode(&descriptor, high, Some(low)).unwrap();

    assert_eq!(row.id().unwrap(), high);
    assert_eq!(row.maybe_owner().unwrap(), Some(low));
    assert_eq!(row.record().to_values().unwrap()[0], Value::Uuid(high));
    assert!(low.as_bytes() < high.as_bytes());
}

#[test]
fn tuple_fields_round_trip_order_nullable_and_layout() {
    let low = uuid::Uuid::from_bytes([0; 16]);
    let high = uuid::Uuid::from_bytes([0xff; 16]);
    let tuple_type = ValueType::Tuple(vec![ValueType::Uuid, ValueType::U64]);
    let descriptor = RecordDescriptor::new([
        ("id", tuple_type.clone()),
        ("maybe_id", ValueType::Nullable(Box::new(tuple_type))),
    ]);
    TestTupleRow::assert_layout(&descriptor);

    let row = TestTupleRow::encode(&descriptor, (high, 9), Some((low, 7))).unwrap();

    assert_eq!(row.id().unwrap(), (high, 9));
    assert_eq!(row.maybe_id().unwrap(), Some((low, 7)));
    assert_eq!(
        row.record().to_values().unwrap()[0],
        Value::Tuple(vec![Value::Uuid(high), Value::U64(9)])
    );
}

#[test]
fn creates_and_reads_mixed_records() {
    let schema = RecordDescriptor::new([
        ("name", ValueType::String),
        ("age", ValueType::U8),
        ("payload", ValueType::Bytes),
        ("active", ValueType::Bool),
    ]);
    let record = schema
        .create(&[
            Value::String("Blue Note".to_owned()),
            Value::U8(42),
            Value::Bytes(vec![1, 2, 3]),
            Value::Bool(true),
        ])
        .unwrap();

    assert_eq!(schema.get(&record, "age").unwrap(), Value::U8(42));
    assert_eq!(schema.get(&record, "active").unwrap(), Value::Bool(true));
    assert_eq!(
        schema.get(&record, "name").unwrap(),
        Value::String("Blue Note".to_owned())
    );
    assert_eq!(schema.get_idx(&record, 3).unwrap(), Value::Bool(true));
}

#[test]
fn pure_fixed_schema_bytes_are_unchanged() {
    let schema = RecordDescriptor::new([
        ("a", ValueType::U8),
        ("b", ValueType::Bool),
        ("c", ValueType::U64),
    ]);
    let record = schema
        .create(&[Value::U8(3), Value::Bool(true), Value::U64(0x0102)])
        .unwrap();

    let mut expected = vec![3, 1];
    expected.extend(0x0102_u64.to_le_bytes());
    assert_eq!(record, expected);
    assert_eq!(schema.get_idx(&record, 0).unwrap(), Value::U8(3));
    assert_eq!(schema.get_idx(&record, 1).unwrap(), Value::Bool(true));
    assert_eq!(schema.get_idx(&record, 2).unwrap(), Value::U64(0x0102));
}

#[test]
fn patch_field_uses_logical_index_for_physically_relocated_field() {
    let schema = RecordDescriptor::new([
        ("title", ValueType::String),
        ("count", ValueType::U64),
        ("blob", ValueType::Bytes),
    ]);
    let record = schema
        .create(&[
            Value::String("before".to_owned()),
            Value::U64(10),
            Value::Bytes(vec![1, 2, 3]),
        ])
        .unwrap();

    let patched = schema
        .patch_field(&record, 1, &Value::U64(99))
        .expect("patch logical fixed field");

    assert_eq!(
        schema.get_idx(&patched, 0).unwrap(),
        Value::String("before".to_owned())
    );
    assert_eq!(schema.get_idx(&patched, 1).unwrap(), Value::U64(99));
    assert_eq!(
        schema.get_idx(&patched, 2).unwrap(),
        Value::Bytes(vec![1, 2, 3])
    );
}

#[test]
fn logical_order_reads_match_full_decode_for_interleaved_seeded_schemas() {
    let status = EnumSchema::new("status", ["new", "seen", "done"]).unwrap();
    let schemas = [
        RecordDescriptor::new([
            ("text", ValueType::String),
            ("id", ValueType::U64),
            ("maybe", ValueType::Nullable(Box::new(ValueType::String))),
            ("status", ValueType::Enum(status.clone())),
            ("bytes", ValueType::Bytes),
        ]),
        RecordDescriptor::new([
            ("blob", ValueType::Bytes),
            ("flag", ValueType::Bool),
            ("maybe_seq", ValueType::Nullable(Box::new(ValueType::U64))),
            ("tail", ValueType::String),
        ]),
    ];

    let mut rng = 0x1234_abcd_9876_5555_u64;
    for (schema_idx, schema) in schemas.iter().enumerate() {
        for step in 0..128 {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let values = if schema_idx == 0 {
                vec![
                    Value::String(format!("t-{step}-{rng:x}")),
                    Value::U64(rng),
                    if rng.is_multiple_of(2) {
                        Value::Nullable(None)
                    } else {
                        Value::Nullable(Some(Box::new(Value::String(format!("m-{rng:x}")))))
                    },
                    Value::Enum((rng % 3) as u8),
                    Value::Bytes(rng.to_be_bytes()[..(step % 8)].to_vec()),
                ]
            } else {
                vec![
                    Value::Bytes(rng.to_le_bytes()[..(step % 8)].to_vec()),
                    Value::Bool(rng & 1 == 1),
                    if rng.is_multiple_of(3) {
                        Value::Nullable(None)
                    } else {
                        Value::Nullable(Some(Box::new(Value::U64(rng.rotate_left(9)))))
                    },
                    Value::String(format!("tail-{rng:x}")),
                ]
            };
            let record = schema.create(&values).unwrap();
            assert_eq!(schema.bind(&record).to_values().unwrap(), values);
            for (idx, expected) in values.iter().enumerate() {
                assert_eq!(schema.get_idx(&record, idx).unwrap(), expected.clone());
            }
        }
    }
}

#[test]
fn encoded_record_accessors_read_only_requested_fields() {
    let schema = RecordDescriptor::new([
        ("id", ValueType::U64),
        ("flag", ValueType::Bool),
        ("small", ValueType::U8),
        ("count", ValueType::U32),
        ("maybe_seq", ValueType::Nullable(Box::new(ValueType::U64))),
        ("empty", ValueType::String),
        ("name", ValueType::String),
        ("blob", ValueType::Bytes),
        (
            "maybe_name",
            ValueType::Nullable(Box::new(ValueType::String)),
        ),
        (
            "missing_name",
            ValueType::Nullable(Box::new(ValueType::String)),
        ),
    ]);
    let values = vec![
        Value::U64(42),
        Value::Bool(true),
        Value::U8(7),
        Value::U32(9),
        Value::Nullable(Some(Box::new(Value::U64(11)))),
        Value::String(String::new()),
        Value::String("Monk".to_owned()),
        Value::Bytes(vec![1, 2, 3]),
        Value::Nullable(Some(Box::new(Value::String("Trane".to_owned())))),
        Value::Nullable(None),
    ];
    let record = schema.create(&values).unwrap();
    let encoded = schema.bind(&record);

    // Invariant: typed accessors compute the requested field span and decode
    // only that span; they do not materialize a Vec<Value> or allocate strings.
    assert_eq!(
        encoded.get_u64(schema.field_index("id").unwrap()).unwrap(),
        42
    );
    assert!(
        encoded
            .get_bool(schema.field_index("flag").unwrap())
            .unwrap()
    );
    assert_eq!(
        encoded
            .get_u8(schema.field_index("small").unwrap())
            .unwrap(),
        7
    );
    assert_eq!(
        encoded
            .get_u32(schema.field_index("count").unwrap())
            .unwrap(),
        9
    );
    assert_eq!(
        encoded
            .get_nullable_u64(schema.field_index("maybe_seq").unwrap())
            .unwrap(),
        Some(11)
    );
    assert_eq!(
        encoded
            .get_str(schema.field_index("empty").unwrap())
            .unwrap(),
        ""
    );
    assert_eq!(
        encoded
            .get_str(schema.field_index("name").unwrap())
            .unwrap(),
        "Monk"
    );
    assert_eq!(
        encoded
            .get_bytes(schema.field_index("blob").unwrap())
            .unwrap(),
        &[1, 2, 3]
    );
    assert_eq!(
        encoded
            .get_nullable_string(schema.field_index("maybe_name").unwrap())
            .unwrap(),
        Some("Trane")
    );
    assert_eq!(
        encoded
            .get_nullable_string(schema.field_index("missing_name").unwrap())
            .unwrap(),
        None
    );
}

#[test]
fn enum_values_decode_as_discriminants_and_store_discriminants() {
    let status = EnumSchema::new("status", ["draft", "ready", "done"]).unwrap();
    let schema = RecordDescriptor::new([
        ("id", ValueType::U64),
        ("status", ValueType::Enum(status.clone())),
        (
            "maybe_status",
            ValueType::Nullable(Box::new(ValueType::Enum(status))),
        ),
    ]);
    let record = schema
        .create(&[
            Value::U64(7),
            Value::String("ready".to_owned()),
            Value::Nullable(Some(Box::new(Value::String("done".to_owned())))),
        ])
        .unwrap();
    let encoded = schema.bind(&record);

    assert_eq!(schema.get(&record, "status").unwrap(), Value::Enum(1));
    assert_eq!(
        schema.get(&record, "maybe_status").unwrap(),
        Value::Nullable(Some(Box::new(Value::Enum(2))))
    );
    assert_eq!(
        encoded
            .get_enum(schema.field_index("status").unwrap())
            .unwrap(),
        1
    );
    assert_eq!(
        encoded
            .get_enum_name(schema.field_index("status").unwrap())
            .unwrap(),
        "ready"
    );
    assert_eq!(
        encoded
            .get_nullable_enum(schema.field_index("maybe_status").unwrap())
            .unwrap(),
        Some(2)
    );
}

#[test]
fn enum_nullable_layout_stays_fixed_width_and_patchable() {
    let status = EnumSchema::new("status", ["draft", "ready", "done"]).unwrap();
    let schema = RecordDescriptor::new([
        ("id", ValueType::U64),
        (
            "maybe_status",
            ValueType::Nullable(Box::new(ValueType::Enum(status))),
        ),
        ("body", ValueType::String),
    ]);
    let record = schema
        .create(&[
            Value::U64(7),
            Value::Nullable(None),
            Value::String("payload".to_owned()),
        ])
        .unwrap();
    let maybe_idx = schema.field_index("maybe_status").unwrap();
    let patched = schema
        .patch_field(
            &record,
            maybe_idx,
            &Value::Nullable(Some(Box::new(Value::String("done".to_owned())))),
        )
        .unwrap();

    assert_eq!(patched.len(), record.len());
    assert_eq!(
        schema.bind(&patched).get_nullable_enum(maybe_idx).unwrap(),
        Some(2)
    );
    assert_eq!(
        schema.get(&patched, "body").unwrap(),
        Value::String("payload".to_owned())
    );
}

#[test]
fn encoded_record_accessors_match_full_decode_under_seeded_rows() {
    let schema = RecordDescriptor::new([
        ("id", ValueType::U64),
        ("kind", ValueType::U8),
        ("active", ValueType::Bool),
        ("bytes", ValueType::Bytes),
        ("text", ValueType::String),
        (
            "maybe_text",
            ValueType::Nullable(Box::new(ValueType::String)),
        ),
        ("maybe_seq", ValueType::Nullable(Box::new(ValueType::U64))),
    ]);
    let id_idx = schema.field_index("id").unwrap();
    let kind_idx = schema.field_index("kind").unwrap();
    let active_idx = schema.field_index("active").unwrap();
    let bytes_idx = schema.field_index("bytes").unwrap();
    let text_idx = schema.field_index("text").unwrap();
    let maybe_text_idx = schema.field_index("maybe_text").unwrap();
    let maybe_seq_idx = schema.field_index("maybe_seq").unwrap();
    let mut rng = 0x5eed_cafe_u64;
    for _ in 0..256 {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let id = rng;
        let kind = (rng >> 8) as u8;
        let active = rng & 1 == 1;
        let bytes = rng.to_le_bytes()[..((rng as usize) % 8)].to_vec();
        let text = if rng.is_multiple_of(5) {
            String::new()
        } else {
            format!("v-{rng:x}")
        };
        let maybe_text = if rng.is_multiple_of(3) {
            Value::Nullable(None)
        } else {
            Value::Nullable(Some(Box::new(Value::String(text.clone()))))
        };
        let maybe_seq = if rng.is_multiple_of(4) {
            Value::Nullable(None)
        } else {
            Value::Nullable(Some(Box::new(Value::U64(rng.rotate_left(7)))))
        };
        let values = vec![
            Value::U64(id),
            Value::U8(kind),
            Value::Bool(active),
            Value::Bytes(bytes.clone()),
            Value::String(text.clone()),
            maybe_text.clone(),
            maybe_seq.clone(),
        ];
        let record = schema.create(&values).unwrap();
        let encoded = schema.bind(&record);
        assert_eq!(schema.get_idx(&record, id_idx).unwrap(), Value::U64(id));
        assert_eq!(encoded.get_u64(id_idx).unwrap(), id);
        assert_eq!(schema.get_idx(&record, kind_idx).unwrap(), Value::U8(kind));
        assert_eq!(encoded.get_u8(kind_idx).unwrap(), kind);
        assert_eq!(
            schema.get_idx(&record, active_idx).unwrap(),
            Value::Bool(active)
        );
        assert_eq!(encoded.get_bool(active_idx).unwrap(), active);
        assert_eq!(
            schema.get_idx(&record, bytes_idx).unwrap(),
            Value::Bytes(bytes.clone())
        );
        assert_eq!(encoded.get_bytes(bytes_idx).unwrap(), bytes.as_slice());
        assert_eq!(
            schema.get_idx(&record, text_idx).unwrap(),
            Value::String(text.clone())
        );
        assert_eq!(encoded.get_str(text_idx).unwrap(), text.as_str());
        let expected_nullable_text = match &maybe_text {
            Value::Nullable(Some(value)) => match value.as_ref() {
                Value::String(value) => Some(value.as_str()),
                _ => unreachable!(),
            },
            Value::Nullable(None) => None,
            _ => unreachable!(),
        };
        assert_eq!(schema.get_idx(&record, maybe_text_idx).unwrap(), maybe_text);
        assert_eq!(
            encoded.get_nullable_string(maybe_text_idx).unwrap(),
            expected_nullable_text
        );
        let expected_nullable_seq = match &maybe_seq {
            Value::Nullable(Some(value)) => match value.as_ref() {
                Value::U64(value) => Some(*value),
                _ => unreachable!(),
            },
            Value::Nullable(None) => None,
            _ => unreachable!(),
        };
        assert_eq!(schema.get_idx(&record, maybe_seq_idx).unwrap(), maybe_seq);
        assert_eq!(
            encoded.get_nullable_u64(maybe_seq_idx).unwrap(),
            expected_nullable_seq
        );
    }
}

#[test]
fn encodes_all_scalar_value_types_little_endian() {
    let descriptor = descriptor([
        ValueType::U8,
        ValueType::U16,
        ValueType::U32,
        ValueType::U64,
        ValueType::F64,
        ValueType::Bool,
    ]);

    let record = descriptor
        .create(&[
            Value::U8(0x12),
            Value::U16(0x3456),
            Value::U32(0x789a_bcde),
            Value::U64(0x0123_4567_89ab_cdef),
            Value::F64(1.5),
            Value::Bool(true),
        ])
        .unwrap();

    let mut expected = Vec::new();
    expected.push(0x12);
    expected.extend(0x3456_u16.to_le_bytes());
    expected.extend(0x789a_bcde_u32.to_le_bytes());
    expected.extend(0x0123_4567_89ab_cdef_u64.to_le_bytes());
    expected.extend(1.5_f64.to_le_bytes());
    expected.push(1);

    assert_eq!(record, expected);
    assert_eq!(descriptor.get_idx(&record, 4).unwrap(), Value::F64(1.5));
}

#[test]
fn f64_accessors_reject_nan_and_record_field_round_trips() {
    let descriptor = RecordDescriptor::new([
        ("id", ValueType::U64),
        ("score", ValueType::F64),
        ("maybe_score", ValueType::Nullable(Box::new(ValueType::F64))),
    ]);
    let record = descriptor
        .create(&[
            Value::U64(7),
            1.25_f64.to_value(),
            Some(-2.5_f64).to_value(),
        ])
        .unwrap();
    let borrowed = descriptor.bind(&record);
    assert_eq!(borrowed.get_u64(0).unwrap(), 7);
    assert_eq!(borrowed.get_f64(1).unwrap(), 1.25);
    assert_eq!(borrowed.get_nullable_f64(2).unwrap(), Some(-2.5));
    assert_eq!(f64::read(&borrowed, 1).unwrap(), 1.25);
    assert_eq!(Option::<f64>::read(&borrowed, 2).unwrap(), Some(-2.5));
    assert_eq!(
        descriptor.create(&[Value::U64(1), Value::F64(f64::NAN), Value::Nullable(None)]),
        Err(Error::InvalidF64NaN)
    );
    assert_eq!(
        descriptor.create(&[
            Value::U64(1),
            Value::F64(0.0),
            Value::Nullable(Some(Box::new(Value::F64(f64::NAN)))),
        ]),
        Err(Error::InvalidF64NaN)
    );
}

#[test]
fn encodes_nullable_fixed_size_values_with_flag_and_reserved_width() {
    let descriptor = descriptor([
        ValueType::Nullable(Box::new(ValueType::U16)),
        ValueType::Nullable(Box::new(ValueType::Bool)),
    ]);
    let record = descriptor
        .create(&[
            Value::Nullable(Some(Box::new(Value::U16(0x1234)))),
            Value::Nullable(None),
        ])
        .unwrap();

    assert_eq!(record, [1, 0x34, 0x12, 0, 0]);
    assert_eq!(
        descriptor.get_idx(&record, 0).unwrap(),
        Value::Nullable(Some(Box::new(Value::U16(0x1234))))
    );
    assert_eq!(
        descriptor.get_idx(&record, 1).unwrap(),
        Value::Nullable(None)
    );
}

#[test]
fn encodes_nullable_variable_size_null_as_only_flag_byte() {
    let descriptor = descriptor([
        ValueType::Nullable(Box::new(ValueType::String)),
        ValueType::Nullable(Box::new(ValueType::Bytes)),
    ]);
    let record = descriptor
        .create(&[
            Value::Nullable(Some(Box::new(Value::String("yes".to_owned())))),
            Value::Nullable(None),
        ])
        .unwrap();

    let mut expected = Vec::new();
    expected.extend(8_u32.to_le_bytes());
    expected.extend([1]);
    expected.extend(b"yes");
    expected.extend([0]);
    assert_eq!(record, expected);
    assert_eq!(
        descriptor.get_idx(&record, 0).unwrap(),
        Value::Nullable(Some(Box::new(Value::String("yes".to_owned()))))
    );
    assert_eq!(
        descriptor.get_idx(&record, 1).unwrap(),
        Value::Nullable(None)
    );
}

#[test]
fn encodes_arrays_of_nullable_fixed_size_values() {
    let descriptor = descriptor([ValueType::Array(Box::new(ValueType::Nullable(Box::new(
        ValueType::U8,
    ))))]);
    let record = descriptor
        .create(&[Value::Array(vec![
            Value::Nullable(Some(Box::new(Value::U8(7)))),
            Value::Nullable(None),
        ])])
        .unwrap();

    assert_eq!(record, [1, 7, 0, 0]);
    assert_eq!(
        descriptor.get_idx(&record, 0).unwrap(),
        Value::Array(vec![
            Value::Nullable(Some(Box::new(Value::U8(7)))),
            Value::Nullable(None)
        ])
    );
}

#[test]
fn tuple_encoding_is_concatenated_fixed_member_encoding() {
    let uuid = uuid::Uuid::from_bytes([
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
        0x1f,
    ]);
    let descriptor = descriptor([ValueType::Tuple(vec![ValueType::Uuid, ValueType::U64])]);
    let record = descriptor
        .create(&[Value::Tuple(vec![
            Value::Uuid(uuid),
            Value::U64(0x0102_0304_0506_0708),
        ])])
        .unwrap();

    let mut expected = Vec::new();
    expected.extend_from_slice(uuid.as_bytes());
    expected.extend_from_slice(&0x0102_0304_0506_0708_u64.to_be_bytes());
    assert_eq!(record, expected);
    assert_eq!(
        descriptor.get_idx(&record, 0).unwrap(),
        Value::Tuple(vec![Value::Uuid(uuid), Value::U64(0x0102_0304_0506_0708)])
    );
}

#[test]
fn tuple_integer_members_are_big_endian_even_inside_little_endian_records() {
    let descriptor = descriptor([ValueType::U64, ValueType::Tuple(vec![ValueType::U64])]);
    let record = descriptor
        .create(&[
            Value::U64(0x0102_0304_0506_0708),
            Value::Tuple(vec![Value::U64(0x0102_0304_0506_0708)]),
        ])
        .unwrap();

    assert_eq!(
        record,
        [
            0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, // record scalar
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // tuple member
        ]
    );
}

#[test]
fn nullable_tuple_round_trips() {
    let uuid = uuid::Uuid::from_bytes([0x22; 16]);
    let tuple_type = ValueType::Tuple(vec![ValueType::Uuid, ValueType::U64]);
    let descriptor = descriptor([ValueType::Nullable(Box::new(tuple_type))]);
    let record = descriptor
        .create(&[Value::Nullable(Some(Box::new(Value::Tuple(vec![
            Value::Uuid(uuid),
            Value::U64(4),
        ]))))])
        .unwrap();

    assert_eq!(
        descriptor.get_idx(&record, 0).unwrap(),
        Value::Nullable(Some(Box::new(Value::Tuple(vec![
            Value::Uuid(uuid),
            Value::U64(4)
        ]))))
    );
}

#[test]
fn fixed_tuple_arrays_support_indexed_element_reads() {
    let first = uuid::Uuid::from_bytes([0x01; 16]);
    let second = uuid::Uuid::from_bytes([0x02; 16]);
    let tuple_type = ValueType::Tuple(vec![ValueType::Uuid, ValueType::U64]);
    let descriptor = descriptor([ValueType::Array(Box::new(tuple_type))]);
    let record = descriptor
        .create(&[Value::Array(vec![
            Value::Tuple(vec![Value::Uuid(first), Value::U64(10)]),
            Value::Tuple(vec![Value::Uuid(second), Value::U64(20)]),
        ])])
        .unwrap();
    let borrowed = descriptor.bind(&record);

    assert_eq!(
        borrowed.get_array_element(0, 1).unwrap(),
        Value::Tuple(vec![Value::Uuid(second), Value::U64(20)])
    );
    assert_eq!(
        borrowed.get_array_element(0, 2).unwrap_err(),
        Error::FieldIndexOutOfBounds { index: 2, len: 2 }
    );
}

#[test]
#[should_panic(expected = "tuple members must be fixed-width")]
fn descriptor_rejects_variable_width_tuple_members() {
    let _ = descriptor([ValueType::Tuple(vec![ValueType::Uuid, ValueType::String])]);
}

#[test]
fn tuple_round_trip_matches_seeded_oracle() {
    let descriptor = descriptor([
        ValueType::Tuple(vec![ValueType::Uuid, ValueType::U64]),
        ValueType::Nullable(Box::new(ValueType::Tuple(vec![
            ValueType::Uuid,
            ValueType::U64,
        ]))),
        ValueType::Array(Box::new(ValueType::Tuple(vec![
            ValueType::Uuid,
            ValueType::U64,
        ]))),
    ]);
    let mut seed = 0x7b1e_5eed_u64;
    for _ in 0..128 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let uuid_a = uuid::Uuid::from_bytes(seed.to_be_bytes().repeat(2).try_into().unwrap());
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let uuid_b = uuid::Uuid::from_bytes(seed.to_be_bytes().repeat(2).try_into().unwrap());
        let nullable = if seed & 1 == 0 {
            Value::Nullable(None)
        } else {
            Value::Nullable(Some(Box::new(Value::Tuple(vec![
                Value::Uuid(uuid_b),
                Value::U64(seed),
            ]))))
        };
        let values = vec![
            Value::Tuple(vec![Value::Uuid(uuid_a), Value::U64(seed.rotate_left(7))]),
            nullable,
            Value::Array(vec![
                Value::Tuple(vec![Value::Uuid(uuid_a), Value::U64(1)]),
                Value::Tuple(vec![Value::Uuid(uuid_b), Value::U64(2)]),
            ]),
        ];
        let record = descriptor.create(&values).unwrap();
        assert_eq!(descriptor.bind(&record).to_values().unwrap(), values);
    }
}

#[test]
fn exhaustive_value_type_matrix_round_trips_through_codec_projection_and_postcard() {
    let status = EnumSchema::new("status", ["draft", "ready", "done"]).unwrap();
    let id = uuid::Uuid::from_bytes([0x31; 16]);
    let nested_id = uuid::Uuid::from_bytes([0x42; 16]);
    let descriptor = RecordDescriptor::new([
        ("u8_min", ValueType::U8),
        ("u16_max", ValueType::U16),
        ("u32_max", ValueType::U32),
        ("u64_zero", ValueType::U64),
        ("u64_max", ValueType::U64),
        ("f64", ValueType::F64),
        ("bool", ValueType::Bool),
        ("string", ValueType::String),
        ("bytes", ValueType::Bytes),
        ("uuid", ValueType::Uuid),
        ("enum", ValueType::Enum(status.clone())),
        (
            "nullable_none",
            ValueType::Nullable(Box::new(ValueType::String)),
        ),
        (
            "nullable_some_tuple",
            ValueType::Nullable(Box::new(ValueType::Tuple(vec![
                ValueType::Uuid,
                ValueType::U64,
            ]))),
        ),
        (
            "array_nested",
            ValueType::Array(Box::new(ValueType::Array(Box::new(ValueType::U16)))),
        ),
        (
            "array_tuple",
            ValueType::Array(Box::new(ValueType::Tuple(vec![
                ValueType::Uuid,
                ValueType::U64,
            ]))),
        ),
        (
            "tuple",
            ValueType::Tuple(vec![ValueType::Uuid, ValueType::U64, ValueType::Bool]),
        ),
    ]);
    let values = vec![
        Value::U8(u8::MIN),
        Value::U16(u16::MAX),
        Value::U32(u32::MAX),
        Value::U64(0),
        Value::U64(u64::MAX),
        Value::F64(-42.25),
        Value::Bool(true),
        Value::String("all value types".to_owned()),
        Value::Bytes(vec![0, 1, 2, 3, 254, 255]),
        Value::Uuid(id),
        Value::Enum(2),
        Value::Nullable(None),
        Value::Nullable(Some(Box::new(Value::Tuple(vec![
            Value::Uuid(nested_id),
            Value::U64(u64::MAX - 1),
        ])))),
        Value::Array(vec![
            Value::Array(vec![Value::U16(1), Value::U16(2)]),
            Value::Array(vec![]),
            Value::Array(vec![Value::U16(u16::MAX)]),
        ]),
        Value::Array(vec![
            Value::Tuple(vec![Value::Uuid(id), Value::U64(1)]),
            Value::Tuple(vec![Value::Uuid(nested_id), Value::U64(u64::MAX)]),
        ]),
        Value::Tuple(vec![Value::Uuid(id), Value::U64(9), Value::Bool(false)]),
    ];

    let raw = descriptor.create(&values).unwrap();
    assert_eq!(descriptor.bind(&raw).to_values().unwrap(), values);

    let encoded_descriptor = postcard::to_allocvec(&descriptor).unwrap();
    let decoded_descriptor: RecordDescriptor = postcard::from_bytes(&encoded_descriptor).unwrap();
    assert_eq!(decoded_descriptor.fields(), descriptor.fields());
    assert_eq!(decoded_descriptor.bind(&raw).to_values().unwrap(), values);

    let owned = OwnedRecord::new(raw.clone(), descriptor);
    let encoded_record = postcard::to_allocvec(&owned).unwrap();
    let decoded_record: OwnedRecord = postcard::from_bytes(&encoded_record).unwrap();
    assert_eq!(decoded_record.raw(), raw.as_slice());
    assert_eq!(decoded_record.to_values().unwrap(), values);

    let (projected_descriptor, projected_raw) = RecordDescriptor::project(
        &[*decoded_record.descriptor()],
        &[decoded_record.raw()],
        &[(0, 10), (0, 14), (0, 4), (0, 12)],
    )
    .unwrap();
    assert_eq!(
        projected_descriptor
            .bind(&projected_raw)
            .to_values()
            .unwrap(),
        vec![
            Value::Enum(2),
            Value::Array(vec![
                Value::Tuple(vec![Value::Uuid(id), Value::U64(1)]),
                Value::Tuple(vec![Value::Uuid(nested_id), Value::U64(u64::MAX)]),
            ]),
            Value::U64(u64::MAX),
            Value::Nullable(Some(Box::new(Value::Tuple(vec![
                Value::Uuid(nested_id),
                Value::U64(u64::MAX - 1),
            ])))),
        ]
    );
}

#[test]
fn encodes_record_offsets_relative_to_record_start() {
    let descriptor = descriptor([ValueType::U8, ValueType::String, ValueType::Bytes]);
    let record = descriptor
        .create(&[
            Value::U8(9),
            Value::String("abc".to_owned()),
            Value::Bytes(vec![4, 5]),
        ])
        .unwrap();

    let mut expected = vec![9];
    expected.extend(8_u32.to_le_bytes());
    expected.extend(b"abc");
    expected.extend([4, 5]);

    assert_eq!(record, expected);
}

#[test]
fn encodes_fixed_size_arrays_without_count() {
    let descriptor = descriptor([ValueType::Array(Box::new(ValueType::U16))]);
    let record = descriptor
        .create(&[Value::Array(vec![
            Value::U16(10),
            Value::U16(20),
            Value::U16(30),
        ])])
        .unwrap();

    assert_eq!(
        descriptor.get_idx(&record, 0).unwrap(),
        Value::Array(vec![Value::U16(10), Value::U16(20), Value::U16(30)])
    );
}

#[test]
fn encodes_empty_fixed_size_arrays_as_empty_payloads() {
    let descriptor = descriptor([ValueType::Array(Box::new(ValueType::U32))]);
    let record = descriptor.create(&[Value::Array(Vec::new())]).unwrap();

    assert!(record.is_empty());
    assert_eq!(
        descriptor.get_idx(&record, 0).unwrap(),
        Value::Array(Vec::new())
    );
}

#[test]
fn encodes_variable_size_arrays_with_offsets() {
    let descriptor = descriptor([ValueType::Array(Box::new(ValueType::String))]);
    let record = descriptor
        .create(&[Value::Array(vec![
            Value::String("a".to_owned()),
            Value::String("bop".to_owned()),
            Value::String("c".to_owned()),
        ])])
        .unwrap();

    assert_eq!(
        descriptor.get_idx(&record, 0).unwrap(),
        Value::Array(vec![
            Value::String("a".to_owned()),
            Value::String("bop".to_owned()),
            Value::String("c".to_owned())
        ])
    );
}

#[test]
fn encodes_variable_array_offsets_relative_to_array_start() {
    let descriptor = descriptor([ValueType::Array(Box::new(ValueType::String))]);
    let record = descriptor
        .create(&[Value::Array(vec![
            Value::String("hi".to_owned()),
            Value::String("j".to_owned()),
        ])])
        .unwrap();

    let mut expected = Vec::new();
    expected.extend(2_u32.to_le_bytes());
    expected.extend(10_u32.to_le_bytes());
    expected.extend(b"hi");
    expected.extend(b"j");

    assert_eq!(record, expected);
}

#[test]
fn encodes_empty_variable_size_arrays_with_zero_count() {
    let descriptor = descriptor([ValueType::Array(Box::new(ValueType::String))]);
    let record = descriptor.create(&[Value::Array(Vec::new())]).unwrap();

    assert_eq!(record, 0_u32.to_le_bytes());
    assert_eq!(
        descriptor.get_idx(&record, 0).unwrap(),
        Value::Array(Vec::new())
    );
}

#[test]
fn encodes_nested_variable_arrays() {
    let descriptor = descriptor([ValueType::Array(Box::new(ValueType::Array(Box::new(
        ValueType::String,
    ))))]);
    let value = Value::Array(vec![
        Value::Array(vec![
            Value::String("a".to_owned()),
            Value::String("bb".to_owned()),
        ]),
        Value::Array(vec![Value::String("ccc".to_owned())]),
    ]);
    let record = descriptor.create(std::slice::from_ref(&value)).unwrap();

    assert_eq!(descriptor.get_idx(&record, 0).unwrap(), value);
}

#[test]
fn projects_fields_from_source_records() {
    let left = RecordDescriptor::new([("id", ValueType::U32), ("name", ValueType::String)]);
    let right = RecordDescriptor::new([("enabled", ValueType::Bool), ("blob", ValueType::Bytes)]);
    let left_record = left
        .create(&[Value::U32(7), Value::String("Kind of Blue".to_owned())])
        .unwrap();
    let right_record = right
        .create(&[Value::Bool(false), Value::Bytes(vec![9, 8])])
        .unwrap();

    let (projected_descriptor, projected_record) = RecordDescriptor::project(
        &[left, right],
        &[left_record.as_ref(), right_record.as_ref()],
        &[(1, 0), (0, 1)],
    )
    .unwrap();

    assert_eq!(
        projected_descriptor
            .fields()
            .iter()
            .map(|field| field.name.as_deref().unwrap())
            .collect::<Vec<_>>(),
        ["enabled", "name"]
    );
    assert_eq!(
        projected_descriptor.get_idx(&projected_record, 0).unwrap(),
        Value::Bool(false)
    );
    assert_eq!(
        projected_descriptor.get_idx(&projected_record, 1).unwrap(),
        Value::String("Kind of Blue".to_owned())
    );
}

#[test]
fn project_preserves_logical_mapping_order() {
    let source = RecordDescriptor::new([("name", ValueType::String), ("id", ValueType::U32)]);
    let source_record = source
        .create(&[Value::String("Monk".to_owned()), Value::U32(5)])
        .unwrap();

    let (descriptor, record) =
        RecordDescriptor::project(&[source], &[source_record.as_ref()], &[(0, 1), (0, 0)]).unwrap();

    assert_eq!(
        descriptor
            .fields()
            .iter()
            .map(|field| field.name.as_deref().unwrap())
            .collect::<Vec<_>>(),
        ["id", "name"]
    );
    assert_eq!(descriptor.get_idx(&record, 0).unwrap(), Value::U32(5));
    assert_eq!(
        descriptor.get_idx(&record, 1).unwrap(),
        Value::String("Monk".to_owned())
    );
}

#[test]
fn record_projector_copies_encoded_spans_equivalent_to_decode_reencode() {
    let status = EnumSchema::new("status", ["draft", "ready", "done"]).unwrap();
    let source = RecordDescriptor::new([
        ("payload", ValueType::Bytes),
        ("id", ValueType::U64),
        (
            "maybe_title",
            ValueType::Nullable(Box::new(ValueType::String)),
        ),
        ("status", ValueType::Enum(status.clone())),
        ("title", ValueType::String),
        (
            "maybe_status",
            ValueType::Nullable(Box::new(ValueType::Enum(status.clone()))),
        ),
        ("flag", ValueType::Bool),
    ]);
    let target = RecordDescriptor::new([
        ("title", ValueType::String),
        ("status", ValueType::Enum(status.clone())),
        ("id", ValueType::U64),
        ("payload", ValueType::Bytes),
        (
            "maybe_status",
            ValueType::Nullable(Box::new(ValueType::Enum(status))),
        ),
        (
            "maybe_title",
            ValueType::Nullable(Box::new(ValueType::String)),
        ),
    ]);
    let mapping = [
        (
            source.field_index("title").unwrap(),
            target.field_index("title").unwrap(),
        ),
        (
            source.field_index("status").unwrap(),
            target.field_index("status").unwrap(),
        ),
        (
            source.field_index("id").unwrap(),
            target.field_index("id").unwrap(),
        ),
        (
            source.field_index("payload").unwrap(),
            target.field_index("payload").unwrap(),
        ),
        (
            source.field_index("maybe_status").unwrap(),
            target.field_index("maybe_status").unwrap(),
        ),
        (
            source.field_index("maybe_title").unwrap(),
            target.field_index("maybe_title").unwrap(),
        ),
    ];
    let projector = RecordProjector::new(source, target, mapping).unwrap();

    let mut rng = 0x90ab_cdef_1234_5678_u64;
    for idx in 0..256 {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let title = if idx % 17 == 0 {
            String::new()
        } else {
            format!("title-{rng:x}")
        };
        let maybe_title = if rng.is_multiple_of(3) {
            Value::Nullable(None)
        } else {
            Value::Nullable(Some(Box::new(Value::String(title.clone()))))
        };
        let maybe_status = if rng.is_multiple_of(4) {
            Value::Nullable(None)
        } else {
            Value::Nullable(Some(Box::new(Value::Enum((rng % 3) as u8))))
        };
        let source_values = vec![
            Value::Bytes(rng.to_be_bytes()[..(idx % 8)].to_vec()),
            Value::U64(rng),
            maybe_title.clone(),
            Value::Enum(((rng >> 8) % 3) as u8),
            Value::String(title),
            maybe_status.clone(),
            Value::Bool(rng & 1 == 1),
        ];
        let source_raw = source.create(&source_values).unwrap();
        let projected = projector.project(source.bind(&source_raw)).unwrap();

        let target_values = (0..target.fields().len())
            .map(|target_idx| {
                let source_idx = mapping
                    .iter()
                    .find_map(|(source_idx, mapped_target)| {
                        (*mapped_target == target_idx).then_some(*source_idx)
                    })
                    .unwrap();
                source.get_idx(&source_raw, source_idx)
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let expected = target.create(&target_values).unwrap();

        assert_eq!(projected.raw(), expected.as_slice());
        for field_idx in 0..target.fields().len() {
            assert_eq!(
                projected.get_idx(field_idx).unwrap(),
                target.get_idx(&expected, field_idx).unwrap()
            );
        }
    }
}

#[test]
fn record_projector_rejects_incomplete_duplicate_and_type_mismatched_mappings() {
    let source = RecordDescriptor::new([("id", ValueType::U64), ("name", ValueType::String)]);
    let target = RecordDescriptor::new([("id", ValueType::U64), ("name", ValueType::String)]);

    assert!(matches!(
        RecordProjector::new(source, target, [(0, 0)]),
        Err(Error::ProjectMissingTarget { target_idx: 1 })
    ));
    assert!(matches!(
        RecordProjector::new(source, target, [(0, 0), (0, 0), (1, 1)]),
        Err(Error::ProjectDuplicateTarget { target_idx: 0 })
    ));
    assert!(matches!(
        RecordProjector::new(source, target, [(1, 0), (0, 1)]),
        Err(Error::ProjectTypeMismatch { .. })
    ));
}

#[test]
fn patch_field_overwrites_fixed_width_values_without_shifting_layout() {
    let schema = RecordDescriptor::new([
        ("id", ValueType::U64),
        ("until", ValueType::U64),
        (
            "maybe_global",
            ValueType::Nullable(Box::new(ValueType::U64)),
        ),
        ("title", ValueType::String),
    ]);
    let record = schema
        .create(&[
            Value::U64(1),
            Value::U64(u64::MAX),
            Value::Nullable(None),
            Value::String("before".to_owned()),
        ])
        .unwrap();

    let until_idx = schema.field_index("until").unwrap();
    let patched = schema
        .patch_field(&record, until_idx, &Value::U64(7))
        .unwrap();
    assert_eq!(patched.len(), record.len());
    assert_eq!(schema.get_idx(&patched, until_idx).unwrap(), Value::U64(7));
    assert_eq!(
        schema.get(&patched, "title").unwrap(),
        Value::String("before".to_owned())
    );

    let maybe_idx = schema.field_index("maybe_global").unwrap();
    let patched = schema
        .patch_field(
            &patched,
            maybe_idx,
            &Value::Nullable(Some(Box::new(Value::U64(9)))),
        )
        .unwrap();
    assert_eq!(patched.len(), record.len());
    assert_eq!(
        schema.get_idx(&patched, maybe_idx).unwrap(),
        Value::Nullable(Some(Box::new(Value::U64(9))))
    );
    let patched = schema
        .patch_field(&patched, maybe_idx, &Value::Nullable(None))
        .unwrap();
    assert_eq!(patched.len(), record.len());
    assert_eq!(
        schema.get_idx(&patched, maybe_idx).unwrap(),
        Value::Nullable(None)
    );
}

#[test]
fn patch_field_rebuilds_when_variable_width_layout_changes() {
    let schema = RecordDescriptor::new([
        ("id", ValueType::U64),
        ("title", ValueType::String),
        ("body", ValueType::String),
    ]);
    let record = schema
        .create(&[
            Value::U64(1),
            Value::String("a".to_owned()),
            Value::String("body".to_owned()),
        ])
        .unwrap();
    let patched = schema
        .patch_field(
            &record,
            schema.field_index("title").unwrap(),
            &Value::String("longer title".to_owned()),
        )
        .unwrap();
    assert_eq!(
        schema.get(&patched, "title").unwrap(),
        Value::String("longer title".to_owned())
    );
    assert_eq!(
        schema.get(&patched, "body").unwrap(),
        Value::String("body".to_owned())
    );
}

#[test]
fn empty_descriptor_creates_empty_record() {
    let descriptor = descriptor([]);
    let record = descriptor.create(&[]).unwrap();

    assert!(record.is_empty());
    assert_eq!(
        descriptor.get_idx(&record, 0).unwrap_err(),
        Error::FieldIndexOutOfBounds { index: 0, len: 0 }
    );
}

#[test]
fn lookup_reports_unknown_field_name() {
    let schema = RecordDescriptor::new([("id", ValueType::U8)]);
    let record = schema.create(&[Value::U8(1)]).unwrap();

    assert_eq!(
        schema.get(&record, "missing").unwrap_err(),
        Error::FieldNotFound("missing".to_owned())
    );
}

#[test]
fn create_rejects_wrong_value_count() {
    let descriptor = descriptor([ValueType::U8, ValueType::U16]);

    assert_eq!(
        descriptor.create(&[Value::U8(1)]).unwrap_err(),
        Error::ArityMismatch {
            expected: 2,
            actual: 1
        }
    );
}

#[test]
fn create_rejects_wrong_scalar_type() {
    let descriptor = descriptor([ValueType::U16]);

    assert_eq!(
        descriptor.create(&[Value::U8(1)]).unwrap_err(),
        Error::TypeMismatch {
            expected: ValueType::U16
        }
    );
}

#[test]
fn create_rejects_wrong_array_element_type() {
    let descriptor = descriptor([ValueType::Array(Box::new(ValueType::U8))]);

    assert_eq!(
        descriptor
            .create(&[Value::Array(vec![Value::U16(1)])])
            .unwrap_err(),
        Error::TypeMismatch {
            expected: ValueType::U8
        }
    );
}

#[test]
fn lookup_rejects_truncated_fixed_record() {
    let descriptor = descriptor([ValueType::U32]);

    assert_eq!(
        descriptor.get_idx(&[1, 2, 3], 0).unwrap_err(),
        Error::UnexpectedEof
    );
}

#[test]
fn lookup_rejects_trailing_bytes_in_fixed_only_record() {
    let descriptor = descriptor([ValueType::U8]);

    assert_eq!(
        descriptor.get_idx(&[1, 2], 0).unwrap_err(),
        Error::InvalidOffset
    );
}

#[test]
fn lookup_rejects_invalid_boolean_byte() {
    let descriptor = descriptor([ValueType::Bool]);

    assert_eq!(
        descriptor.get_idx(&[2], 0).unwrap_err(),
        Error::InvalidBool(2)
    );
}

#[test]
fn lookup_rejects_invalid_nullable_flag() {
    let descriptor = descriptor([ValueType::Nullable(Box::new(ValueType::U8))]);

    assert_eq!(
        descriptor.get_idx(&[2, 0], 0).unwrap_err(),
        Error::InvalidNullFlag(2)
    );
}

#[test]
fn lookup_rejects_null_variable_nullable_with_payload() {
    let descriptor = descriptor([ValueType::Nullable(Box::new(ValueType::String))]);

    assert_eq!(
        descriptor.get_idx(&[0, b'x'], 0).unwrap_err(),
        Error::InvalidOffset
    );
}

#[test]
fn lookup_rejects_invalid_utf8_string() {
    let descriptor = descriptor([ValueType::String]);

    assert_eq!(
        descriptor.get_idx(&[0xff], 0).unwrap_err(),
        Error::InvalidUtf8
    );
}

#[test]
fn lookup_rejects_offset_before_variable_payload_start() {
    let descriptor = descriptor([ValueType::U8, ValueType::String, ValueType::Bytes]);
    let mut record = vec![1];
    record.extend(4_u32.to_le_bytes());
    record.extend(b"ab");

    assert_eq!(
        descriptor.get_idx(&record, 1).unwrap_err(),
        Error::InvalidOffset
    );
}

#[test]
fn lookup_rejects_offset_past_record_end() {
    let descriptor = descriptor([ValueType::String, ValueType::Bytes]);
    let mut record = Vec::new();
    record.extend(99_u32.to_le_bytes());
    record.extend(b"ab");

    assert_eq!(
        descriptor.get_idx(&record, 0).unwrap_err(),
        Error::InvalidOffset
    );
}

#[test]
fn lookup_rejects_truncated_offset_table() {
    let descriptor = descriptor([ValueType::String, ValueType::Bytes]);

    assert_eq!(
        descriptor.get_idx(&[1, 2, 3], 0).unwrap_err(),
        Error::UnexpectedEof
    );
}

#[test]
fn lookup_rejects_fixed_array_with_partial_element() {
    let descriptor = descriptor([ValueType::Array(Box::new(ValueType::U16))]);

    assert_eq!(
        descriptor.get_idx(&[1, 2, 3], 0).unwrap_err(),
        Error::InvalidOffset
    );
}

#[test]
fn lookup_rejects_empty_variable_array_missing_count() {
    let descriptor = descriptor([ValueType::Array(Box::new(ValueType::String))]);

    assert_eq!(
        descriptor.get_idx(&[], 0).unwrap_err(),
        Error::UnexpectedEof
    );
}

#[test]
fn lookup_rejects_zero_count_variable_array_with_payload() {
    let descriptor = descriptor([ValueType::Array(Box::new(ValueType::String))]);
    let mut record = Vec::new();
    record.extend(0_u32.to_le_bytes());
    record.extend(b"extra");

    assert_eq!(
        descriptor.get_idx(&record, 0).unwrap_err(),
        Error::InvalidOffset
    );
}

#[test]
fn lookup_rejects_variable_array_offset_before_payload_start() {
    let descriptor = descriptor([ValueType::Array(Box::new(ValueType::String))]);
    let mut record = Vec::new();
    record.extend(2_u32.to_le_bytes());
    record.extend(6_u32.to_le_bytes());
    record.extend(b"ab");

    assert_eq!(
        descriptor.get_idx(&record, 0).unwrap_err(),
        Error::InvalidOffset
    );
}

#[test]
fn project_rejects_source_record_count_mismatch() {
    let descriptor = descriptor([ValueType::U8]);

    assert_eq!(
        RecordDescriptor::project(&[descriptor], &[], &[]).unwrap_err(),
        Error::ArityMismatch {
            expected: 1,
            actual: 0
        }
    );
}

#[test]
fn project_rejects_descriptor_index_out_of_bounds() {
    assert_eq!(
        RecordDescriptor::project(&[], &[], &[(0, 0)]).unwrap_err(),
        Error::FieldIndexOutOfBounds { index: 0, len: 0 }
    );
}

#[test]
fn project_rejects_field_index_out_of_bounds() {
    let descriptor = descriptor([ValueType::U8]);
    let record = descriptor.create(&[Value::U8(1)]).unwrap();

    assert_eq!(
        RecordDescriptor::project(&[descriptor], &[record.as_ref()], &[(0, 1)]).unwrap_err(),
        Error::FieldIndexOutOfBounds { index: 1, len: 1 }
    );
}

#[test]
fn descriptor_round_trips_through_postcard_as_schema_fields() {
    let descriptor = RecordDescriptor::new([
        ("id", ValueType::Uuid),
        ("name", ValueType::String),
        ("flags", ValueType::Array(Box::new(ValueType::Bool))),
        (
            "rating",
            ValueType::Nullable(Box::new(ValueType::Tuple(vec![
                ValueType::U8,
                ValueType::U16,
            ]))),
        ),
    ]);

    let encoded = postcard::to_allocvec(&descriptor).unwrap();
    let decoded: RecordDescriptor = postcard::from_bytes(&encoded).unwrap();

    assert_eq!(decoded.fields(), descriptor.fields());
}

#[test]
fn owned_record_round_trips_through_postcard_as_descriptor_and_raw_bytes() {
    let descriptor = RecordDescriptor::new([
        ("id", ValueType::U32),
        ("name", ValueType::String),
        ("payload", ValueType::Bytes),
    ]);
    let raw = descriptor
        .create(&[
            Value::U32(42),
            Value::String("blue note".to_owned()),
            Value::Bytes(vec![1, 3, 5, 8]),
        ])
        .unwrap();
    let record = OwnedRecord::new(raw.clone(), descriptor);

    let encoded = postcard::to_allocvec(&record).unwrap();
    let decoded: OwnedRecord = postcard::from_bytes(&encoded).unwrap();

    assert_eq!(decoded.raw(), raw.as_slice());
    assert_eq!(decoded.descriptor().fields(), record.descriptor().fields());
    assert_eq!(decoded.get("id").unwrap(), Value::U32(42));
    assert_eq!(
        decoded.get("name").unwrap(),
        Value::String("blue note".to_owned())
    );
    assert_eq!(
        decoded.get("payload").unwrap(),
        Value::Bytes(vec![1, 3, 5, 8])
    );
}
