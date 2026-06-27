//! Typed record-wrapper macros and field conversion glue.
//!
//! This module owns [`RecordField`] and the exported helper macros that define
//! small typed wrappers over encoded records. The wrappers provide named field
//! access and layout assertions without changing the underlying
//! [`RecordDescriptor`] format. Actual binary layout, descriptor spans, and
//! value encoding live in [`super`]; callers use these macros to keep storage
//! rows typed at module boundaries.

use super::{BorrowedRecord, Error, RecordDescriptor, Value, ValueType, decode_value};

pub use paste;

/// Broad record column kind used by generated wrapper layout assertions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldKind {
    U64,
    U32,
    F64,
    U8,
    Bool,
    Bytes,
    Uuid,
    String,
    Enum,
    Tuple,
    Array,
    Nullable,
}

impl FieldKind {
    fn matches(self, value_type: &ValueType) -> bool {
        matches!(
            (self, value_type),
            (Self::U64, ValueType::U64)
                | (Self::U32, ValueType::U32)
                | (Self::F64, ValueType::F64)
                | (Self::U8, ValueType::U8)
                | (Self::Bool, ValueType::Bool)
                | (Self::Bytes, ValueType::Bytes)
                | (Self::Uuid, ValueType::Uuid)
                | (Self::String, ValueType::String)
                | (Self::Enum, ValueType::Enum(_))
                | (Self::Tuple, ValueType::Tuple(_))
                | (Self::Array, ValueType::Array(_))
                | (Self::Nullable, ValueType::Nullable(_))
        )
    }
}

/// Typed field conversion for record newtype wrappers.
pub trait RecordField: Sized {
    fn read(record: &BorrowedRecord<'_>, idx: usize) -> Result<Self, Error>;

    fn to_value(&self) -> Value;

    const COLUMN_KIND: FieldKind;

    #[doc(hidden)]
    fn read_raw(bytes: &[u8], value_type: &ValueType) -> Result<Self, Error> {
        let _ = bytes;
        Err(Error::TypeMismatch {
            expected: value_type.clone(),
        })
    }

    #[doc(hidden)]
    fn read_tuple_raw(bytes: &[u8], value_type: &ValueType) -> Result<Self, Error> {
        Self::read_raw(bytes, value_type)
    }
}

impl RecordField for u64 {
    fn read(record: &BorrowedRecord<'_>, idx: usize) -> Result<Self, Error> {
        record.get_u64(idx)
    }

    fn to_value(&self) -> Value {
        Value::U64(*self)
    }

    const COLUMN_KIND: FieldKind = FieldKind::U64;

    fn read_raw(bytes: &[u8], value_type: &ValueType) -> Result<Self, Error> {
        if value_type != &ValueType::U64 {
            return Err(Error::TypeMismatch {
                expected: ValueType::U64,
            });
        }
        read_exact_array::<8>(bytes).map(u64::from_le_bytes)
    }

    fn read_tuple_raw(bytes: &[u8], value_type: &ValueType) -> Result<Self, Error> {
        if value_type != &ValueType::U64 {
            return Err(Error::TypeMismatch {
                expected: ValueType::U64,
            });
        }
        read_exact_array::<8>(bytes).map(u64::from_be_bytes)
    }
}

impl RecordField for u32 {
    fn read(record: &BorrowedRecord<'_>, idx: usize) -> Result<Self, Error> {
        record.get_u32(idx)
    }

    fn to_value(&self) -> Value {
        Value::U32(*self)
    }

    const COLUMN_KIND: FieldKind = FieldKind::U32;

    fn read_raw(bytes: &[u8], value_type: &ValueType) -> Result<Self, Error> {
        if value_type != &ValueType::U32 {
            return Err(Error::TypeMismatch {
                expected: ValueType::U32,
            });
        }
        read_exact_array::<4>(bytes).map(u32::from_le_bytes)
    }

    fn read_tuple_raw(bytes: &[u8], value_type: &ValueType) -> Result<Self, Error> {
        if value_type != &ValueType::U32 {
            return Err(Error::TypeMismatch {
                expected: ValueType::U32,
            });
        }
        read_exact_array::<4>(bytes).map(u32::from_be_bytes)
    }
}

impl RecordField for f64 {
    fn read(record: &BorrowedRecord<'_>, idx: usize) -> Result<Self, Error> {
        record.get_f64(idx)
    }

    fn to_value(&self) -> Value {
        Value::F64(*self)
    }

    const COLUMN_KIND: FieldKind = FieldKind::F64;

    fn read_raw(bytes: &[u8], value_type: &ValueType) -> Result<Self, Error> {
        if value_type != &ValueType::F64 {
            return Err(Error::TypeMismatch {
                expected: ValueType::F64,
            });
        }
        let value = read_exact_array::<8>(bytes).map(f64::from_le_bytes)?;
        if value.is_nan() {
            return Err(Error::InvalidF64NaN);
        }
        Ok(value)
    }

    fn read_tuple_raw(_bytes: &[u8], value_type: &ValueType) -> Result<Self, Error> {
        Err(Error::InvalidTupleMember {
            member_type: value_type.clone(),
        })
    }
}

impl RecordField for u8 {
    fn read(record: &BorrowedRecord<'_>, idx: usize) -> Result<Self, Error> {
        record.get_u8(idx)
    }

    fn to_value(&self) -> Value {
        Value::U8(*self)
    }

    const COLUMN_KIND: FieldKind = FieldKind::U8;

    fn read_raw(bytes: &[u8], value_type: &ValueType) -> Result<Self, Error> {
        if value_type != &ValueType::U8 {
            return Err(Error::TypeMismatch {
                expected: ValueType::U8,
            });
        }
        read_exact_array::<1>(bytes).map(|bytes| bytes[0])
    }
}

impl RecordField for bool {
    fn read(record: &BorrowedRecord<'_>, idx: usize) -> Result<Self, Error> {
        record.get_bool(idx)
    }

    fn to_value(&self) -> Value {
        Value::Bool(*self)
    }

    const COLUMN_KIND: FieldKind = FieldKind::Bool;

    fn read_raw(bytes: &[u8], value_type: &ValueType) -> Result<Self, Error> {
        if value_type != &ValueType::Bool {
            return Err(Error::TypeMismatch {
                expected: ValueType::Bool,
            });
        }
        match read_exact_array::<1>(bytes)?[0] {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(Error::InvalidBool(value)),
        }
    }
}

impl RecordField for Vec<u8> {
    fn read(record: &BorrowedRecord<'_>, idx: usize) -> Result<Self, Error> {
        Ok(record.get_bytes(idx)?.to_vec())
    }

    fn to_value(&self) -> Value {
        Value::Bytes(self.clone())
    }

    const COLUMN_KIND: FieldKind = FieldKind::Bytes;

    fn read_raw(bytes: &[u8], value_type: &ValueType) -> Result<Self, Error> {
        if value_type != &ValueType::Bytes {
            return Err(Error::TypeMismatch {
                expected: ValueType::Bytes,
            });
        }
        Ok(bytes.to_vec())
    }

    fn read_tuple_raw(_bytes: &[u8], value_type: &ValueType) -> Result<Self, Error> {
        Err(Error::InvalidTupleMember {
            member_type: value_type.clone(),
        })
    }
}

impl RecordField for uuid::Uuid {
    fn read(record: &BorrowedRecord<'_>, idx: usize) -> Result<Self, Error> {
        record.get_uuid(idx)
    }

    fn to_value(&self) -> Value {
        Value::Uuid(*self)
    }

    const COLUMN_KIND: FieldKind = FieldKind::Uuid;

    fn read_raw(bytes: &[u8], value_type: &ValueType) -> Result<Self, Error> {
        if value_type != &ValueType::Uuid {
            return Err(Error::TypeMismatch {
                expected: ValueType::Uuid,
            });
        }
        read_exact_array::<16>(bytes).map(uuid::Uuid::from_bytes)
    }
}

impl RecordField for String {
    fn read(record: &BorrowedRecord<'_>, idx: usize) -> Result<Self, Error> {
        Ok(record.get_str(idx)?.to_owned())
    }

    fn to_value(&self) -> Value {
        Value::String(self.clone())
    }

    const COLUMN_KIND: FieldKind = FieldKind::String;

    fn read_raw(bytes: &[u8], value_type: &ValueType) -> Result<Self, Error> {
        if value_type != &ValueType::String {
            return Err(Error::TypeMismatch {
                expected: ValueType::String,
            });
        }
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| Error::InvalidUtf8)
    }

    fn read_tuple_raw(_bytes: &[u8], value_type: &ValueType) -> Result<Self, Error> {
        Err(Error::InvalidTupleMember {
            member_type: value_type.clone(),
        })
    }
}

impl RecordField for Value {
    fn read(record: &BorrowedRecord<'_>, idx: usize) -> Result<Self, Error> {
        let field = record.field(idx)?;
        let bytes = record.field_bytes_unchecked(idx)?;
        decode_value(bytes, &field.value_type)
    }

    fn to_value(&self) -> Value {
        self.clone()
    }

    const COLUMN_KIND: FieldKind = FieldKind::Nullable;

    fn read_raw(bytes: &[u8], value_type: &ValueType) -> Result<Self, Error> {
        decode_value(bytes, value_type)
    }

    fn read_tuple_raw(bytes: &[u8], value_type: &ValueType) -> Result<Self, Error> {
        decode_value(bytes, value_type)
    }
}

impl<T: RecordField> RecordField for Option<T> {
    fn read(record: &BorrowedRecord<'_>, idx: usize) -> Result<Self, Error> {
        let field = record.field(idx)?;
        let ValueType::Nullable(inner) = &field.value_type else {
            return Err(Error::TypeMismatch {
                expected: ValueType::Nullable(Box::new(ValueType::Bytes)),
            });
        };
        let bytes = record.field_bytes_unchecked(idx)?;
        read_nullable_raw(bytes, inner, T::read_raw)
    }

    fn to_value(&self) -> Value {
        Value::Nullable(self.as_ref().map(|value| Box::new(value.to_value())))
    }

    const COLUMN_KIND: FieldKind = FieldKind::Nullable;

    fn read_raw(bytes: &[u8], value_type: &ValueType) -> Result<Self, Error> {
        let ValueType::Nullable(inner) = value_type else {
            return Err(Error::TypeMismatch {
                expected: ValueType::Nullable(Box::new(ValueType::Bytes)),
            });
        };
        read_nullable_raw(bytes, inner, T::read_raw)
    }

    fn read_tuple_raw(bytes: &[u8], value_type: &ValueType) -> Result<Self, Error> {
        let ValueType::Nullable(inner) = value_type else {
            return Err(Error::TypeMismatch {
                expected: ValueType::Nullable(Box::new(ValueType::Bytes)),
            });
        };
        read_nullable_raw(bytes, inner, T::read_tuple_raw)
    }
}

impl<A: RecordField, B: RecordField> RecordField for (A, B) {
    fn read(record: &BorrowedRecord<'_>, idx: usize) -> Result<Self, Error> {
        let field = record.field(idx)?;
        let bytes = record.field_bytes_unchecked(idx)?;
        Self::read_raw(bytes, &field.value_type)
    }

    fn to_value(&self) -> Value {
        Value::Tuple(vec![self.0.to_value(), self.1.to_value()])
    }

    const COLUMN_KIND: FieldKind = FieldKind::Tuple;

    fn read_raw(bytes: &[u8], value_type: &ValueType) -> Result<Self, Error> {
        let ValueType::Tuple(members) = value_type else {
            return Err(Error::TypeMismatch {
                expected: ValueType::Tuple(vec![ValueType::Bytes, ValueType::Bytes]),
            });
        };
        if members.len() != 2 {
            return Err(Error::ArityMismatch {
                expected: 2,
                actual: members.len(),
            });
        }
        let left_width = members[0]
            .fixed_size()
            .ok_or_else(|| Error::InvalidTupleMember {
                member_type: members[0].clone(),
            })?;
        let right_width = members[1]
            .fixed_size()
            .ok_or_else(|| Error::InvalidTupleMember {
                member_type: members[1].clone(),
            })?;
        let expected_width = left_width
            .checked_add(right_width)
            .ok_or(Error::LengthOverflow)?;
        if bytes.len() != expected_width {
            return Err(Error::UnexpectedEof);
        }
        let (left, right) = bytes.split_at(left_width);
        Ok((
            A::read_tuple_raw(left, &members[0])?,
            B::read_tuple_raw(right, &members[1])?,
        ))
    }

    fn read_tuple_raw(bytes: &[u8], value_type: &ValueType) -> Result<Self, Error> {
        Self::read_raw(bytes, value_type)
    }
}

fn read_nullable_raw<T>(
    bytes: &[u8],
    inner: &ValueType,
    read_present: impl FnOnce(&[u8], &ValueType) -> Result<T, Error>,
) -> Result<Option<T>, Error> {
    let (&flag, payload) = bytes.split_first().ok_or(Error::UnexpectedEof)?;
    match flag {
        0 => {
            if inner.fixed_size().is_some() {
                if payload.iter().any(|byte| *byte != 0) {
                    return Err(Error::InvalidOffset);
                }
            } else if !payload.is_empty() {
                return Err(Error::InvalidOffset);
            }
            Ok(None)
        }
        1 => read_present(payload, inner).map(Some),
        value => Err(Error::InvalidNullFlag(value)),
    }
}

fn read_exact_array<const N: usize>(bytes: &[u8]) -> Result<[u8; N], Error> {
    if bytes.len() != N {
        return Err(Error::UnexpectedEof);
    }
    bytes.try_into().map_err(|_| Error::UnexpectedEof)
}

#[doc(hidden)]
pub fn assert_record_field_layout(
    descriptor: &RecordDescriptor,
    idx: usize,
    name: &str,
    kind: FieldKind,
) {
    let field = descriptor
        .fields()
        .get(idx)
        .unwrap_or_else(|| panic!("record field index {idx} missing for {name}"));
    debug_assert_eq!(
        field.name.as_deref(),
        Some(name),
        "record field index drifted for {name}"
    );
    debug_assert!(
        kind.matches(&field.value_type),
        "record field {name} has kind {:?}, expected {:?}",
        field.value_type,
        kind
    );
}

#[macro_export]
macro_rules! impl_record_field_u64 {
    ($ty:ty) => {
        impl $crate::records::RecordField for $ty {
            fn read(
                record: &$crate::records::BorrowedRecord<'_>,
                idx: usize,
            ) -> Result<Self, $crate::records::Error> {
                record.get_u64(idx).map(Self)
            }

            fn to_value(&self) -> $crate::records::Value {
                $crate::records::Value::U64(self.0)
            }

            const COLUMN_KIND: $crate::records::FieldKind = $crate::records::FieldKind::U64;

            fn read_raw(
                bytes: &[u8],
                value_type: &$crate::records::ValueType,
            ) -> Result<Self, $crate::records::Error> {
                <u64 as $crate::records::RecordField>::read_raw(bytes, value_type).map(Self)
            }

            fn read_tuple_raw(
                bytes: &[u8],
                value_type: &$crate::records::ValueType,
            ) -> Result<Self, $crate::records::Error> {
                <u64 as $crate::records::RecordField>::read_tuple_raw(bytes, value_type).map(Self)
            }
        }
    };
}

#[macro_export]
macro_rules! impl_record_field_bytes16 {
    ($ty:ty) => {
        impl $crate::records::RecordField for $ty {
            fn read(
                record: &$crate::records::BorrowedRecord<'_>,
                idx: usize,
            ) -> Result<Self, $crate::records::Error> {
                Ok(Self(
                    record
                        .get_bytes(idx)?
                        .try_into()
                        .map_err(|_| $crate::records::Error::InvalidOffset)?,
                ))
            }

            fn to_value(&self) -> $crate::records::Value {
                $crate::records::Value::Bytes(self.0.to_vec())
            }

            const COLUMN_KIND: $crate::records::FieldKind = $crate::records::FieldKind::Bytes;

            fn read_raw(
                bytes: &[u8],
                value_type: &$crate::records::ValueType,
            ) -> Result<Self, $crate::records::Error> {
                <Vec<u8> as $crate::records::RecordField>::read_raw(bytes, value_type).and_then(
                    |bytes| {
                        Ok(Self(
                            bytes
                                .try_into()
                                .map_err(|_| $crate::records::Error::InvalidOffset)?,
                        ))
                    },
                )
            }
        }
    };
}

#[macro_export]
macro_rules! impl_record_field_uuid {
    ($ty:ty) => {
        impl $crate::records::RecordField for $ty {
            fn read(
                record: &$crate::records::BorrowedRecord<'_>,
                idx: usize,
            ) -> Result<Self, $crate::records::Error> {
                record.get_uuid(idx).map(Self)
            }

            fn to_value(&self) -> $crate::records::Value {
                $crate::records::Value::Uuid(self.0)
            }

            const COLUMN_KIND: $crate::records::FieldKind = $crate::records::FieldKind::Uuid;

            fn read_raw(
                bytes: &[u8],
                value_type: &$crate::records::ValueType,
            ) -> Result<Self, $crate::records::Error> {
                <uuid::Uuid as $crate::records::RecordField>::read_raw(bytes, value_type).map(Self)
            }

            fn read_tuple_raw(
                bytes: &[u8],
                value_type: &$crate::records::ValueType,
            ) -> Result<Self, $crate::records::Error> {
                <uuid::Uuid as $crate::records::RecordField>::read_tuple_raw(bytes, value_type)
                    .map(Self)
            }
        }
    };
}

#[macro_export]
macro_rules! impl_record_field_enum {
    ($ty:ty { $($variant:path = $disc:expr),+ $(,)? }) => {
        impl $ty {
            #[doc(hidden)]
            pub fn from_discriminant(discriminant: u8) -> Result<Self, $crate::records::Error> {
                match discriminant {
                    $($disc => Ok($variant),)+
                    value => Err($crate::records::Error::InvalidEnumDiscriminant {
                        enum_name: stringify!($ty).to_owned(),
                        discriminant: value,
                    }),
                }
            }

            #[doc(hidden)]
            pub fn discriminant(self) -> u8 {
                match self {
                    $($variant => $disc,)+
                }
            }
        }

        impl $crate::records::RecordField for $ty {
            fn read(
                record: &$crate::records::BorrowedRecord<'_>,
                idx: usize,
            ) -> Result<Self, $crate::records::Error> {
                Self::from_discriminant(record.get_enum(idx)?)
            }

            fn to_value(&self) -> $crate::records::Value {
                $crate::records::Value::Enum((*self).discriminant())
            }

            const COLUMN_KIND: $crate::records::FieldKind = $crate::records::FieldKind::Enum;

            fn read_raw(
                bytes: &[u8],
                value_type: &$crate::records::ValueType,
            ) -> Result<Self, $crate::records::Error> {
                match value_type {
                    $crate::records::ValueType::Enum(schema) => {
                        let discriminant = <u8 as $crate::records::RecordField>::read_raw(
                            bytes,
                            &$crate::records::ValueType::U8,
                        )?;
                        schema.variant(discriminant)?;
                        Self::from_discriminant(discriminant)
                    }
                    _ => Err($crate::records::Error::TypeMismatch {
                        expected: $crate::records::ValueType::U8,
                    }),
                }
            }

            fn read_tuple_raw(
                bytes: &[u8],
                value_type: &$crate::records::ValueType,
            ) -> Result<Self, $crate::records::Error> {
                Self::read_raw(bytes, value_type)
            }
        }
    };
}

#[macro_export]
macro_rules! define_record {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $($idx:literal => $field:ident: $ty:ty,)*
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, Eq)]
        $vis struct $name {
            record: $crate::records::OwnedRecord,
        }

        #[allow(dead_code)]
        impl $name {
            $crate::records::macros::paste::paste! {
                $(pub const [<FIELD_ $field:upper _IDX>]: usize = $idx;)*
            }

            pub fn new(record: $crate::records::OwnedRecord) -> Self {
                Self { record }
            }

            #[allow(dead_code)]
            pub fn record(&self) -> &$crate::records::OwnedRecord {
                &self.record
            }

            $(
                pub fn $field(&self) -> Result<$ty, $crate::records::Error> {
                    $crate::records::macros::paste::paste! {
                        <$ty as $crate::records::RecordField>::read(&self.record.borrowed(), Self::[<FIELD_ $field:upper _IDX>])
                    }
                }
            )*

            #[allow(clippy::too_many_arguments)]
            pub fn encode(
                descriptor: &$crate::records::RecordDescriptor,
                $($field: $ty,)*
            ) -> Result<Self, $crate::records::Error> {
                let values = vec![$(<$ty as $crate::records::RecordField>::to_value(&$field),)*];
                Ok(Self::new($crate::records::OwnedRecord::new(
                    descriptor.create(&values)?,
                    descriptor.clone(),
                )))
            }

            pub fn assert_layout(descriptor: &$crate::records::RecordDescriptor) {
                $(
                    $crate::records::macros::paste::paste! {
                        $crate::records::assert_record_field_layout(
                            descriptor,
                            Self::[<FIELD_ $field:upper _IDX>],
                            stringify!($field),
                            <$ty as $crate::records::RecordField>::COLUMN_KIND,
                        );
                    }
                )*
                debug_assert_eq!(descriptor.fields().len(), 0 $(+ { let _ = stringify!($field); 1 })*);
            }
        }
    };
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $($idx:literal => $field:ident: $ty:ty,)*
            .. $tail:ident,
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, Eq)]
        $vis struct $name {
            record: $crate::records::OwnedRecord,
        }

        #[allow(dead_code)]
        impl $name {
            $crate::records::macros::paste::paste! {
                $(pub const [<FIELD_ $field:upper _IDX>]: usize = $idx;)*
            }
            pub const USER_BASE: usize = 0 $(+ { let _ = stringify!($field); 1 })*;
            pub const USER_CELLS: usize = Self::USER_BASE;

            pub fn new(record: $crate::records::OwnedRecord) -> Self {
                Self { record }
            }

            #[allow(dead_code)]
            pub fn record(&self) -> &$crate::records::OwnedRecord {
                &self.record
            }

            $(
                pub fn $field(&self) -> Result<$ty, $crate::records::Error> {
                    $crate::records::macros::paste::paste! {
                        <$ty as $crate::records::RecordField>::read(&self.record.borrowed(), Self::[<FIELD_ $field:upper _IDX>])
                    }
                }
            )*

            pub fn cell(&self, i: usize) -> Result<Option<$crate::records::Value>, $crate::records::Error> {
                <Option<$crate::records::Value> as $crate::records::RecordField>::read(
                    &self.record.borrowed(),
                    Self::USER_BASE + i,
                )
            }

            pub fn cells(&self) -> impl Iterator<Item = Result<Option<$crate::records::Value>, $crate::records::Error>> + '_ {
                (0..self.record.descriptor().fields().len().saturating_sub(Self::USER_BASE))
                    .map(|idx| self.cell(idx))
            }

            #[allow(clippy::too_many_arguments)]
            pub fn encode(
                descriptor: &$crate::records::RecordDescriptor,
                $($field: $ty,)*
                $tail: &[Option<$crate::records::Value>],
            ) -> Result<Self, $crate::records::Error> {
                let mut values = vec![$(<$ty as $crate::records::RecordField>::to_value(&$field),)*];
                values.extend($tail.iter().map(<Option<$crate::records::Value> as $crate::records::RecordField>::to_value));
                Ok(Self::new($crate::records::OwnedRecord::new(
                    descriptor.create(&values)?,
                    descriptor.clone(),
                )))
            }

            pub fn assert_layout(descriptor: &$crate::records::RecordDescriptor) {
                $(
                    $crate::records::macros::paste::paste! {
                        $crate::records::assert_record_field_layout(
                            descriptor,
                            Self::[<FIELD_ $field:upper _IDX>],
                            stringify!($field),
                            <$ty as $crate::records::RecordField>::COLUMN_KIND,
                        );
                    }
                )*
                debug_assert!(descriptor.fields().len() >= Self::USER_BASE);
            }
        }
    };
}
