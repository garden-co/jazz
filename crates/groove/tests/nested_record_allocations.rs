//! Allocation and canonical-admission regressions for Groove records.
//!
//! These tests install a counting global allocator: allocation counts are not
//! observable through Groove's public results, so a behavioural test cannot pin
//! them. The counter records allocation requests (not bytes), so the bounds do
//! not depend on which allocator backs `System`. Inline-scalar borrowing is
//! covered separately in `inline_scalar_allocations.rs`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use groove::large_values::{
    LargeValueKind, StoredScalar, decode_stored_scalar, encode_stored_scalar, inline_scalar_bytes,
};
use groove::records::{
    EnumCase, EnumSchema, EnumValue, OwnedRecord, RecordDescriptor, Value, ValueType,
};

struct CountingAllocator;

thread_local! {
    static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|count| count.set(count.get().map(|value| value + 1)));
        // SAFETY: forward the caller's layout unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: all allocations use System; preserve its pointer and layout.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[test]
fn borrowed_inline_scalars_preserve_owned_decoder_admission() {
    for (kind, bytes) in [
        (LargeValueKind::Bytes, vec![0, 1, 255]),
        (LargeValueKind::String, "hello 🦀".as_bytes().to_vec()),
        (LargeValueKind::Json, br#"{"a":1}"#.to_vec()),
    ] {
        let encoded = encode_stored_scalar(kind, &StoredScalar::Primitive(bytes)).unwrap();
        let check = |raw: &[u8]| {
            let owned = decode_stored_scalar(kind, raw);
            let borrowed = inline_scalar_bytes(kind, raw);
            match owned {
                Ok(StoredScalar::Primitive(bytes)) => assert_eq!(borrowed.unwrap(), bytes),
                _ => assert!(borrowed.is_err(), "accepted invalid scalar: {raw:?}"),
            }
        };
        check(&encoded);
        for length in 0..encoded.len() {
            check(&encoded[..length]);
        }
        for index in 0..encoded.len() {
            for byte in 0..=255 {
                let mut changed = encoded.clone();
                changed[index] = byte;
                check(&changed);
            }
        }
    }
}

fn allocations_for_nested_record(depth: usize) -> usize {
    let mut descriptor = RecordDescriptor::new([("value", ValueType::U64)]);
    let mut bytes = descriptor.create(&[Value::U64(7)]).unwrap();
    for _ in 0..depth {
        let parent = RecordDescriptor::new([("child", ValueType::Record(Box::new(descriptor)))]);
        bytes = parent
            .create(&[Value::Record(OwnedRecord::new(bytes, descriptor))])
            .unwrap();
        descriptor = parent;
    }
    let parent = RecordDescriptor::new([("child", ValueType::Record(Box::new(descriptor)))]);
    let value = Value::Record(OwnedRecord::new(bytes, descriptor));
    ALLOCATIONS.with(|count| count.set(Some(0)));
    let result = parent.create(&[value]);
    let count = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    assert!(result.is_ok());
    count
}

#[test]
fn nesting_does_not_multiply_record_construction_allocations() {
    let shallow = allocations_for_nested_record(1);
    let deep = allocations_for_nested_record(4);
    // Building an owned record always allocates its buffer; a zero here would
    // make the ratio bound vacuous rather than prove anything.
    assert!(
        shallow > 0,
        "record construction should allocate at least once"
    );
    assert!(
        deep <= shallow * 4,
        "four levels should cost at most four times one level: shallow={shallow}, deep={deep}"
    );
}

#[test]
fn embedding_records_and_enums_preserves_canonical_admission() {
    let child = RecordDescriptor::new([
        ("enabled", ValueType::Bool),
        ("optional", ValueType::Nullable(Box::new(ValueType::U8))),
        ("number", ValueType::F64),
        ("text", ValueType::String),
        ("bytes", ValueType::Bytes),
        ("items", ValueType::Array(Box::new(ValueType::U16))),
    ]);
    let valid = child
        .create(&[
            Value::Bool(true),
            Value::Nullable(None),
            Value::F64(-0.0),
            Value::String("example".into()),
            Value::Bytes(vec![0, 1, 255]),
            Value::Array(vec![Value::U16(7), Value::U16(300)]),
        ])
        .unwrap();
    let record_parent = RecordDescriptor::new([("child", ValueType::Record(Box::new(child)))]);
    let schema = EnumSchema::new("event", [EnumCase::new("value", child)]).unwrap();
    let enum_parent = RecordDescriptor::new([("child", ValueType::Enum(Box::new(schema)))]);
    let check = |raw: &[u8]| {
        // This is the previous embedding admission rule, using public APIs.
        let expected = child
            .bind(raw)
            .to_values()
            .and_then(|values| child.create(&values))
            .is_ok_and(|encoded| encoded == raw);
        let record = record_parent.create(&[Value::Record(OwnedRecord::new(raw.to_vec(), child))]);
        let event = enum_parent.create(&[Value::Enum(EnumValue::new(
            0,
            OwnedRecord::new(raw.to_vec(), child),
        ))]);
        assert_eq!(record.is_ok(), expected, "record admission for {raw:?}");
        assert_eq!(event.is_ok(), expected, "enum admission for {raw:?}");
    };
    check(&valid);
    for length in 0..valid.len() {
        check(&valid[..length]);
    }
    let mut extended = valid.clone();
    extended.push(0);
    check(&extended);
    for index in 0..valid.len() {
        for byte in 0..=255 {
            let mut corrupt = valid.clone();
            corrupt[index] = byte;
            check(&corrupt);
        }
    }
}
