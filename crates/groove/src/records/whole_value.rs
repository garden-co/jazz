//! Borrowed traversal for complete-value SQL keys and exact binding identity.
//! This is an evaluation encoding, never a record, storage or wire codec.

use super::{Error, Value, ValueType, values};

pub(crate) fn supports_whole_value(value_type: &ValueType) -> bool {
    fn tuple_member(value_type: &ValueType) -> bool {
        match value_type {
            ValueType::U8
            | ValueType::U16
            | ValueType::U32
            | ValueType::U64
            | ValueType::I32
            | ValueType::I64
            | ValueType::Bool
            | ValueType::Uuid
            | ValueType::EnumTag(_) => true,
            ValueType::Tuple(members) => members.iter().all(tuple_member),
            _ => false,
        }
    }
    fn plain(value_type: &ValueType) -> bool {
        match value_type {
            ValueType::U8
            | ValueType::U16
            | ValueType::U32
            | ValueType::U64
            | ValueType::I32
            | ValueType::I64
            | ValueType::F64
            | ValueType::Bool
            | ValueType::String
            | ValueType::Bytes
            | ValueType::Uuid
            | ValueType::EnumTag(_) => true,
            ValueType::Array(inner) => inner.fixed_size() != Some(0) && plain(inner),
            ValueType::Tuple(members) => members.iter().all(tuple_member),
            ValueType::Nullable(_)
            | ValueType::Record(_)
            | ValueType::Enum(_)
            | ValueType::Internal(_) => false,
        }
    }
    match value_type {
        ValueType::Nullable(inner) => plain(inner),
        value_type => plain(value_type),
    }
}

#[derive(Clone, Copy)]
pub(crate) struct EncodedValue<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) value_type: &'a ValueType,
    tuple_member: bool,
}

impl<'a> EncodedValue<'a> {
    pub(crate) fn new(bytes: &'a [u8], value_type: &'a ValueType) -> Self {
        Self {
            bytes,
            value_type,
            tuple_member: false,
        }
    }

    pub(crate) fn nullable(self) -> Result<Option<Self>, Error> {
        let ValueType::Nullable(inner) = self.value_type else {
            return Err(Error::TypeMismatch {
                expected: self.value_type.clone(),
            });
        };
        let (&flag, bytes) = self.bytes.split_first().ok_or(Error::UnexpectedEof)?;
        match flag {
            0 => Ok(None),
            1 => Ok(Some(Self {
                bytes,
                value_type: inner,
                tuple_member: self.tuple_member,
            })),
            flag => Err(Error::InvalidBool(flag)),
        }
    }

    pub(crate) fn children(self) -> Result<EncodedChildren<'a>, Error> {
        match self.value_type {
            ValueType::Array(inner) => {
                if let Some(size) = inner.fixed_size() {
                    if size == 0 || !self.bytes.len().is_multiple_of(size) {
                        return Err(Error::InvalidOffset);
                    }
                    Ok(EncodedChildren {
                        parent: self,
                        count: self.bytes.len() / size,
                        index: 0,
                        start: 0,
                        fixed: Some(size),
                    })
                } else {
                    let count = read_offset(self.bytes, 0)?;
                    let start = 4usize
                        .checked_add(
                            count
                                .saturating_sub(1)
                                .checked_mul(4)
                                .ok_or(Error::InvalidOffset)?,
                        )
                        .ok_or(Error::InvalidOffset)?;
                    if start > self.bytes.len() {
                        return Err(Error::UnexpectedEof);
                    }
                    if count == 0 && self.bytes.len() != 4 {
                        return Err(Error::InvalidOffset);
                    }
                    Ok(EncodedChildren {
                        parent: self,
                        count,
                        index: 0,
                        start,
                        fixed: None,
                    })
                }
            }
            ValueType::Tuple(members) => Ok(EncodedChildren {
                parent: self,
                count: members.len(),
                index: 0,
                start: 0,
                fixed: None,
            }),
            _ => Err(Error::TypeMismatch {
                expected: self.value_type.clone(),
            }),
        }
    }

    pub(crate) fn scalar_value(self) -> Result<Value, Error> {
        if self.tuple_member {
            values::decode_tuple_member(self.bytes, self.value_type)
        } else {
            values::decode_value(self.bytes, self.value_type)
        }
    }

    pub(crate) fn enum_ordinal(self) -> Result<u8, Error> {
        if self.bytes.len() != 1 {
            return Err(Error::UnexpectedEof);
        }
        Ok(self.bytes[0])
    }

    pub(crate) fn scalar_bytes(self) -> Result<&'a [u8], Error> {
        match self.value_type {
            ValueType::String | ValueType::Bytes => {
                crate::large_values::trusted_primitive_scalar_bytes(self.bytes).map_err(Into::into)
            }
            _ => Ok(self.bytes),
        }
    }

    pub(crate) fn write_key(self, key: &mut smallvec::SmallVec<[u8; 64]>) -> Result<(), Error> {
        match self.value_type {
            ValueType::Array(_) | ValueType::Tuple(_) => {
                key.push(if matches!(self.value_type, ValueType::Array(_)) {
                    1
                } else {
                    2
                });
                let children = self.children()?;
                key.extend_from_slice(&(children.len() as u64).to_le_bytes());
                for child in children {
                    child?.write_key(key)?;
                }
            }
            ValueType::Nullable(_) => {
                key.push(3);
                match self.nullable()? {
                    None => key.push(0),
                    Some(child) => {
                        key.push(1);
                        child.write_key(key)?;
                    }
                }
            }
            ValueType::F64 => {
                key.push(4);
                let Value::F64(value) = self.scalar_value()? else {
                    unreachable!()
                };
                key.extend_from_slice(
                    &if value == 0.0 { 0u64 } else { value.to_bits() }.to_le_bytes(),
                );
            }
            _ => {
                key.push(0);
                let bytes = self.scalar_bytes()?;
                key.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
                key.extend_from_slice(bytes);
            }
        }
        Ok(())
    }
}

pub(crate) struct EncodedChildren<'a> {
    parent: EncodedValue<'a>,
    count: usize,
    index: usize,
    start: usize,
    fixed: Option<usize>,
}

impl EncodedChildren<'_> {
    pub(crate) fn len(&self) -> usize {
        self.count - self.index
    }
}

impl<'a> Iterator for EncodedChildren<'a> {
    type Item = Result<EncodedValue<'a>, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.count {
            return None;
        }
        let result = (|| {
            let (value_type, end, tuple_member) = match self.parent.value_type {
                ValueType::Array(inner) => {
                    let end = if let Some(size) = self.fixed {
                        self.start.checked_add(size).ok_or(Error::InvalidOffset)?
                    } else if self.index + 1 == self.count {
                        self.parent.bytes.len()
                    } else {
                        read_offset(self.parent.bytes, 4 + self.index * 4)?
                    };
                    (inner.as_ref(), end, false)
                }
                ValueType::Tuple(members) => {
                    let member = &members[self.index];
                    let size = member.fixed_size().ok_or(Error::InvalidOffset)?;
                    (
                        member,
                        self.start.checked_add(size).ok_or(Error::InvalidOffset)?,
                        true,
                    )
                }
                _ => unreachable!(),
            };
            if end < self.start
                || end > self.parent.bytes.len()
                || (self.index + 1 == self.count && end != self.parent.bytes.len())
            {
                return Err(Error::InvalidOffset);
            }
            let child = EncodedValue {
                bytes: &self.parent.bytes[self.start..end],
                value_type,
                tuple_member,
            };
            self.start = end;
            Ok(child)
        })();
        self.index += 1;
        Some(result)
    }
}

fn read_offset(bytes: &[u8], offset: usize) -> Result<usize, Error> {
    let end = offset.checked_add(4).ok_or(Error::InvalidOffset)?;
    let bytes: [u8; 4] = bytes
        .get(offset..end)
        .ok_or(Error::UnexpectedEof)?
        .try_into()
        .map_err(|_| Error::UnexpectedEof)?;
    usize::try_from(u32::from_le_bytes(bytes)).map_err(|_| Error::InvalidOffset)
}
