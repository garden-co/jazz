//! Process-local field routes. Total projections change this mapping, not row
//! bytes. Fallible semantic expressions remain materialization boundaries.

use super::*;
use crate::ivm::ValueDictionary;
use crate::ivm::runtime::key_encoding::{
    FieldLiteralOrdering, PredicateRecord, record_field_literal_ordering,
};
use crate::ivm::runtime::record_projection::expand_dictionary_code;

#[derive(Clone, Debug)]
enum Origin {
    Source(Vec<(RecordDescriptor, usize)>),
    Constant(Arc<[u8]>),
    /// A `U32`/`U64` code at `code` expanded through `dictionary`. Canonical
    /// entries are copied; anything else takes the semantic expansion, so the
    /// bytes and errors match an unfused `ProjectExpr::Dictionary`.
    Dictionary {
        code: Vec<(RecordDescriptor, usize)>,
        code_type: ValueType,
        dictionary: ValueDictionary,
    },
}

#[derive(Clone, Debug)]
struct Field {
    origin: Origin,
    present_wrappers: usize,
}

fn source_bytes<'a>(
    path: &[(RecordDescriptor, usize)],
    raw: &'a [u8],
) -> Result<&'a [u8], IvmRuntimeError> {
    let mut bytes = raw;
    for (descriptor, index) in path {
        bytes = &bytes[descriptor.field_span(bytes, *index)?];
    }
    Ok(bytes)
}

impl Field {
    /// Run `f` over this field's encoded bytes in the routed record `raw`.
    fn with_bytes<R>(&self, raw: &[u8], f: impl FnOnce(&[u8]) -> R) -> Result<R, IvmRuntimeError> {
        match &self.origin {
            Origin::Constant(bytes) => Ok(f(bytes)),
            Origin::Source(path) => Ok(f(source_bytes(path, raw)?)),
            Origin::Dictionary {
                code,
                code_type,
                dictionary,
            } => {
                let code = source_bytes(code, raw)?;
                let index = match (code_type, code.len()) {
                    (ValueType::U32, 4) => code
                        .try_into()
                        .ok()
                        .map(|code| u64::from(u32::from_le_bytes(code))),
                    (ValueType::U64, 8) => code.try_into().ok().map(u64::from_le_bytes),
                    _ => None,
                };
                let mut f = Some(f);
                if let Some(result) = index.and_then(|index| {
                    dictionary.with_canonical_encoding(index, |bytes| {
                        (f.take().expect("called once"))(bytes)
                    })
                }) {
                    return Ok(result);
                }
                let code = records::decode_single_field_value(code, code_type)?;
                let value = expand_dictionary_code(code, dictionary)?;
                let encoded = records::encode_single_field_value(&value, dictionary.value_type())?;
                Ok((f.take().expect("called once"))(&encoded))
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct FieldRoutes {
    source: RecordDescriptor,
    descriptor: RecordDescriptor,
    fields: Arc<[Field]>,
    /// Fallible fields introduced by the latest composed projection, in the
    /// order that projection would have evaluated them. They are resolved at
    /// the projection's own stage so a later filter or projection that drops
    /// them can never elide their errors.
    checks: Arc<[usize]>,
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
            checks: Arc::from([]),
            reuses_input: true,
        }
    }

    /// Resolve the fallible fields introduced by the latest composition.
    pub(super) fn check(&self, raw: &[u8]) -> Result<(), IvmRuntimeError> {
        for index in self.checks.iter() {
            self.fields[*index].with_bytes(raw, |_| ())?;
        }
        Ok(())
    }

    pub(super) fn has_checks(&self) -> bool {
        !self.checks.is_empty()
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
                            // Reading into an expanded value stays unfused.
                            Origin::Dictionary { .. } => return None,
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
                    RawProjectionField::Dictionary {
                        source_idx,
                        code_width: _,
                        dictionary,
                    } => {
                        let code = self.fields.get(*source_idx)?;
                        let Origin::Source(path) = &code.origin else {
                            return None;
                        };
                        if code.present_wrappers != 0 {
                            return None;
                        }
                        let (descriptor, index) = path.last()?;
                        Field {
                            origin: Origin::Dictionary {
                                code: path.clone(),
                                code_type: descriptor.fields().get(*index)?.value_type.clone(),
                                dictionary: dictionary.clone(),
                            },
                            present_wrappers: 0,
                        }
                    }
                    // Never elide an expression which can fail or omit a row,
                    // even if no downstream output refers to that expression.
                    RawProjectionField::Error(_) | RawProjectionField::Evaluate => return None,
                })
            })
            .collect::<Option<Arc<[_]>>>()?;
        // Dictionary expansion can fail (an absent code): resolve it at this
        // projection's stage, in this projection's evaluation order.
        let checks = descriptor
            .projected_field_order()
            .into_iter()
            .filter(|index| {
                matches!(
                    projection.fields.get(*index),
                    Some(RawProjectionField::Dictionary { .. })
                )
            })
            .collect::<Arc<[_]>>();
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
            checks,
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
                field.with_bytes(raw, |bytes| output.extend_from_slice(bytes))
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
        let mut value = field.with_bytes(self.raw, |bytes| {
            records::decode_single_field_value(bytes, ty)
        })??;
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
