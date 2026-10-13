//! Node-local author codes. A transaction record names its author by a
//! 4-byte code in the `jazz_authors` Groove dictionary instead of the full
//! author record, which is the largest field of the record and repeats across
//! an author's transactions. The wire keeps full authors; codes never leave
//! the node.

use super::*;

/// An author's code in `jazz_authors`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct AuthorCode(pub(super) u32);

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// The code of `author`, staging its `jazz_authors` entry into `batch`
    /// when it is new, so the entry is durable with the record that names it.
    pub(super) async fn stage_author_code(
        &mut self,
        batch: &mut DatabaseBatch,
        author: AuthorSubject,
    ) -> Result<AuthorCode, Error> {
        let author =
            RowAuthor::from_persisted_subject(author).map_err(|_| Error::UnadmittedWriteAuthor)?;
        let code = self
            .authors
            .stage(&self.database, batch, author.encoded_record().raw())
            .await?;
        Ok(AuthorCode(code))
    }

    /// The code of `author`, if any stored transaction names it.
    pub(super) async fn author_code(
        &mut self,
        author: AuthorSubject,
    ) -> Result<Option<AuthorCode>, Error> {
        let Ok(author) = RowAuthor::from_persisted_subject(author) else {
            return Ok(None);
        };
        Ok(self
            .authors
            .code(&self.database, author.encoded_record().raw())
            .await?
            .map(AuthorCode))
    }

    /// The author that `code` stands for.
    pub(super) async fn row_author_for_code(
        &mut self,
        code: AuthorCode,
    ) -> Result<RowAuthor, Error> {
        let bytes = self.authors.value(&self.database, code.0).await?;
        let record = OwnedRecord::new(bytes, row_author_descriptor());
        RowAuthor::from_record(record.borrowed())
            .map_err(|_| Error::InvalidStoredValue("jazz_authors entry is not a canonical author"))
    }
}

fn row_author_descriptor() -> records::RecordDescriptor {
    static DESCRIPTOR: std::sync::LazyLock<records::RecordDescriptor> =
        std::sync::LazyLock::new(|| {
            let records::ValueType::Record(descriptor) = RowAuthor::value_type() else {
                unreachable!("a row author is a record")
            };
            *descriptor
        });
    DESCRIPTOR.clone()
}
