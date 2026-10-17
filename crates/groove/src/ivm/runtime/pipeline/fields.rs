//! Process-local field routes. Total projections change this mapping, not row
//! bytes. Fallible semantic expressions remain materialization boundaries.

use super::*;
use crate::ivm::runtime::key_encoding::{
    FieldLiteralOrdering, PredicateRecord, record_field_literal_ordering,
};

#[derive(Clone, Debug)]
enum Origin {
    Source(Vec<(RecordDescriptor, usize)>),
    Constant(Arc<[u8]>),
}

#[derive(Clone, Debug)]
struct Field {
    origin: Origin,
    present_wrappers: usize,
}

impl Field {
    fn bytes<'a>(&'a self, raw: &'a [u8]) -> Result<&'a [u8], IvmRuntimeError> {
        match &self.origin {
            Origin::Constant(bytes) => Ok(bytes),
            Origin::Source(path) => {
                let mut bytes = raw;
                for (descriptor, index) in path {
                    bytes = &bytes[descriptor.field_span(bytes, *index)?];
                }
                Ok(bytes)
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct FieldRoutes {
    source: RecordDescriptor,
    descriptor: RecordDescriptor,
    fields: Arc<[Field]>,
    pub(super) reuses_input: bool,
}

impl FieldRoutes {
    pub(super) fn identity(descriptor: RecordDescriptor) -> Self {
        Self {
            source: descriptor,
            descriptor,
            fields: (0..descriptor.fields().len())
                .map(|index| Field {
                    origin: Origin::Source(vec![(descriptor, index)]),
                    present_wrappers: 0,
                })
                .collect(),
            reuses_input: true,
        }
    }

    pub(super) fn compose(
        &self,
        descriptor: RecordDescriptor,
        projection: &PreparedProjection,
    ) -> Option<Self> {
        let fields = projection
            .fields
            .iter()
            .map(|field| {
                Some(match field {
                    RawProjectionField::Copy { source_idx } => {
                        self.fields.get(*source_idx)?.clone()
                    }
                    RawProjectionField::WrapNullable { source_idx } => {
                        let mut field = self.fields.get(*source_idx)?.clone();
                        field.present_wrappers = field.present_wrappers.checked_add(1)?;
                        field
                    }
                    RawProjectionField::Nested { path } => {
                        let (first, rest) = path.split_first()?;
                        let mut field = self.fields.get(first.1)?.clone();
                        // A nested record cannot be read through a nullable tag.
                        if field.present_wrappers != 0 {
                            return None;
                        }
                        match &mut field.origin {
                            Origin::Source(source) => source.extend_from_slice(rest),
                            Origin::Constant(bytes) => {
                                let mut value: &[u8] = bytes;
                                for (descriptor, index) in rest {
                                    value = &value[descriptor.field_span(value, *index).ok()?];
                                }
                                *bytes = Arc::from(value);
                            }
                        }
                        field
                    }
                    RawProjectionField::Encoded { bytes } => Field {
                        origin: Origin::Constant(Arc::from(bytes.as_slice())),
                        present_wrappers: 0,
                    },
                    // Never elide an expression which can fail or omit a row,
                    // even if no downstream output refers to that expression.
                    RawProjectionField::Error(_) | RawProjectionField::Evaluate => return None,
                })
            })
            .collect::<Option<Arc<[_]>>>()?;
        let copies = fields
            .iter()
            .map(|field| {
                let Origin::Source(path) = &field.origin else {
                    return None;
                };
                if field.present_wrappers != 0 || path.len() != 1 {
                    return None;
                }
                Some(RawProjectionField::Copy {
                    source_idx: path[0].1,
                })
            })
            .collect::<Option<Vec<_>>>();
        let reuses_input = copies.is_some_and(|fields| {
            PreparedProjection::new(self.source, descriptor, fields).reuses_input
        });
        Some(Self {
            source: self.source,
            descriptor,
            fields,
            reuses_input,
        })
    }

    pub(super) fn append(
        &self,
        raw: &[u8],
        output: &mut BytesMut,
    ) -> Result<std::ops::Range<usize>, IvmRuntimeError> {
        self.descriptor
            .write_projected_fields_into(output, |index, output| {
                let field = &self.fields[index];
                output.resize(output.len() + field.present_wrappers, 1);
                output.extend_from_slice(field.bytes(raw)?);
                Ok(())
            })
    }

    pub(super) fn record<'a>(&'a self, raw: &'a [u8]) -> RoutedRecord<'a> {
        RoutedRecord { routes: self, raw }
    }
}

#[derive(Clone, Copy)]
pub(super) struct RoutedRecord<'a> {
    routes: &'a FieldRoutes,
    raw: &'a [u8],
}

impl PredicateRecord for RoutedRecord<'_> {
    fn value(self, name: &str) -> Result<Value, IvmRuntimeError> {
        let index = resolve_field_name(&self.routes.descriptor, name)
            .ok_or_else(|| records::Error::FieldNotFound(name.to_owned()))?;
        let field = &self.routes.fields[index];
        let mut ty = &self.routes.descriptor.fields()[index].value_type;
        for _ in 0..field.present_wrappers {
            let ValueType::Nullable(inner) = ty else {
                unreachable!("validated field route")
            };
            ty = inner;
        }
        let mut value = records::decode_single_field_value(field.bytes(self.raw)?, ty)?;
        for _ in 0..field.present_wrappers {
            value = Value::Nullable(Some(Box::new(value)));
        }
        Ok(value)
    }

    fn literal_ordering(
        self,
        name: &str,
        value: &LiteralValue,
    ) -> Result<FieldLiteralOrdering, IvmRuntimeError> {
        let index = resolve_field_name(&self.routes.descriptor, name)
            .ok_or_else(|| IvmRuntimeError::GraphFieldNotFound(name.to_owned()))?;
        let field = &self.routes.fields[index];
        if field.present_wrappers == 0
            && let Origin::Source(path) = &field.origin
            && let Some(((descriptor, index), prefix)) = path.split_last()
        {
            let mut raw = self.raw;
            for (descriptor, index) in prefix {
                raw = &raw[descriptor.field_span(raw, *index)?];
            }
            return record_field_literal_ordering(
                BorrowedRecord::new(raw, descriptor),
                *index,
                value,
            );
        }
        Ok(FieldLiteralOrdering::Unsupported)
    }
}
