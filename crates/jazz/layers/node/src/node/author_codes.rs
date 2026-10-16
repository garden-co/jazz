//! Row and transaction authors as the node stores them: codes of the
//! `authors` dictionary. Inside the node an author column holds its code and
//! queries, policies and IVM work on codes; a code becomes an
//! [`AuthorSubject`] only where node logic needs the identity, and a row
//! leaves the node with its codes resolved ([`AuthorCodes::resolve_record`]).

use groove::dictionary::Dictionaries;
use groove::records::{RecordDescriptor, Value};

use super::Error;
use crate::ids::{AuthorSubject, RowAuthor};
use crate::schema::AUTHORS_DICTIONARY;

/// The code of a row author in the node's `authors` dictionary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct AuthorCode(pub(crate) u32);

groove::impl_record_field_dictionary_code!(AuthorCode);

/// The node's `authors` dictionary: interns authors on write and resolves
/// codes where an identity or a value is needed.
#[derive(Clone)]
pub struct AuthorCodes(Dictionaries);

impl AuthorCodes {
    pub(crate) fn new(dictionaries: Dictionaries) -> Self {
        Self(dictionaries)
    }

    /// An `authors` dictionary of no node, for codec tests.
    #[cfg(test)]
    pub(crate) fn detached() -> Self {
        Self(Dictionaries::detached([
            &groove::schema::DictionarySchema::new(AUTHORS_DICTIONARY, RowAuthor::value_type()),
        ]))
    }

    /// The code of a durable author, interning it when new. Only subjects
    /// that have crossed the local write boundary have a durable form.
    pub(crate) fn code(&self, subject: AuthorSubject) -> Result<AuthorCode, Error> {
        let author =
            RowAuthor::from_persisted_subject(subject).map_err(|_| Error::UnadmittedWriteAuthor)?;
        self.code_of(author)
    }

    pub(crate) fn code_of(&self, author: RowAuthor) -> Result<AuthorCode, Error> {
        self.0
            .intern(AUTHORS_DICTIONARY, author.encoded_record().raw())
            .map(AuthorCode)
            .map_err(|error| groove::db::Error::from(error).into())
    }

    /// The code of a durable author as a column value.
    pub(crate) fn value(&self, subject: AuthorSubject) -> Result<Value, Error> {
        Ok(Value::U32(self.code(subject)?.0))
    }

    /// The durable author a code stands for.
    pub(crate) fn row_author(&self, code: AuthorCode) -> Result<RowAuthor, Error> {
        let bytes = self
            .0
            .resolve(AUTHORS_DICTIONARY, code.0)
            .map_err(groove::db::Error::from)?;
        let descriptor = match RowAuthor::value_type() {
            groove::records::ValueType::Record(descriptor) => *descriptor,
            _ => unreachable!("row authors are records"),
        };
        RowAuthor::from_record(descriptor.bind(&bytes))
            .map_err(|_| Error::Record(groove::records::Error::NonCanonicalRecord))
    }

    /// The subject a code stands for.
    pub(crate) fn subject(&self, code: AuthorCode) -> Result<AuthorSubject, Error> {
        Ok(self.row_author(code)?.as_author_subject())
    }

    /// `record` with every author code replaced by the author it stands
    /// for, for a row leaving the node.
    pub(crate) fn resolve_record(
        &self,
        descriptor: RecordDescriptor,
        record: &[u8],
    ) -> Result<(Vec<u8>, RecordDescriptor), Error> {
        self.0
            .resolve_record(descriptor, record)
            .map_err(|error| groove::db::Error::from(error).into())
    }

    /// [`Self::resolve_record`] of an owned record.
    pub(crate) fn resolve_owned(
        &self,
        record: &groove::records::OwnedRecord,
    ) -> Result<groove::records::OwnedRecord, Error> {
        let (raw, descriptor) = self.resolve_record(*record.descriptor(), record.raw())?;
        Ok(groove::records::OwnedRecord::new(raw, descriptor))
    }

    /// `value`, of `value_type`, with any author code replaced by the author
    /// it stands for, together with the logical type of the result.
    pub(crate) fn resolve_value(
        &self,
        value: Value,
        value_type: &groove::records::ValueType,
    ) -> Result<(Value, groove::records::ValueType), Error> {
        if !value_type.contains_interned() {
            return Ok((value, value_type.clone()));
        }
        let descriptor = RecordDescriptor::new([("value", value_type.clone())]);
        let raw = descriptor.create(&[value])?;
        let (raw, logical) = self.resolve_record(descriptor, &raw)?;
        Ok((logical.bind(&raw).get_idx(0)?, value_type.logical()))
    }

    /// The code `subject` already has, without interning it: a subject
    /// without a code authored nothing stored here.
    pub(crate) fn existing_code(&self, subject: AuthorSubject) -> Option<AuthorCode> {
        let author = RowAuthor::from_persisted_subject(subject).ok()?;
        self.0
            .code(AUTHORS_DICTIONARY, author.encoded_record().raw())
            .map(AuthorCode)
    }
}

/// Refuse a descriptor that still carries author codes where a row leaves
/// the node: codes are node-local, so egress must resolve the row first.
pub(crate) fn refuse_codes(descriptor: &RecordDescriptor) -> Result<(), Error> {
    for field in descriptor.fields() {
        if field.value_type.contains_interned() {
            let dictionary = match &field.value_type {
                groove::records::ValueType::Nullable(inner) => inner.as_interned(),
                other => other.as_interned(),
            }
            .map_or_else(
                || AUTHORS_DICTIONARY.to_owned(),
                |(name, _)| name.to_owned(),
            );
            return Err(groove::records::Error::InternedTypeNotEncodable { dictionary }.into());
        }
    }
    Ok(())
}
