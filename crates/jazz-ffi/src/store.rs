//! The typed UniFFI callback for a device's private store.

use std::{future::ready, rc::Rc, sync::Arc};

use futures_util::future::LocalBoxFuture;
use jazz_app_support::{EncryptionConfig, PrivateStore as NativeStore, StoreError, Stored};
use uniffi::{Lift, Lower, TypeId};
use zeroize::Zeroizing;

use crate::UniFfiTag;

// SAFETY: host bindings see the same UniFFI byte sequence as Vec<u8>.
// Serialize directly from the guarded copy so it is wiped as soon as UniFFI
// lowers it. The serialized buffer belongs to UniFFI and we cannot wipe it.
unsafe impl uniffi::FfiConverter<UniFfiTag> for Zeroizing<Vec<u8>> {
    uniffi::ffi_converter_rust_buffer_lift_and_lower!(UniFfiTag);

    const TYPE_ID_META: uniffi::MetadataBuffer = <Vec<u8> as TypeId<UniFfiTag>>::TYPE_ID_META;

    fn write(bytes: Self, buf: &mut Vec<u8>) {
        <i32 as Lower<UniFfiTag>>::write(i32::try_from(bytes.len()).unwrap(), buf);
        buf.extend_from_slice(&bytes);
    }

    fn try_read(buf: &mut &[u8]) -> uniffi::Result<Self> {
        <Vec<u8> as Lift<UniFfiTag>>::try_read(buf).map(Zeroizing::new)
    }
}

uniffi::derive_ffi_traits!(local Zeroizing<Vec<u8>>);

/// A device's private store. Callbacks run synchronously on the client's
/// thread. The app provides storage and platform protection for its keys.
#[uniffi::export(with_foreign)]
pub trait PrivateStore: Send + Sync {
    fn open_scope(&self, scope: String) -> Result<u64, PrivateStoreError>;
    fn load(&self, scope: String, doc: String)
    -> Result<Option<StoredDocument>, PrivateStoreError>;
    fn save(
        &self,
        scope: String,
        incarnation: u64,
        doc: String,
        bytes: Zeroizing<Vec<u8>>,
        expected: Option<u64>,
    ) -> Result<u64, PrivateStoreError>;
    fn list(&self, scope: String) -> Result<Vec<String>, PrivateStoreError>;
    fn delete(&self, scope: String) -> Result<(), PrivateStoreError>;
    fn protect(&self, secret: Vec<u8>) -> Result<Vec<u8>, PrivateStoreError>;
    fn unprotect(&self, blob: Vec<u8>) -> Result<Vec<u8>, PrivateStoreError>;
}

#[derive(uniffi::Record)]
pub struct StoredDocument {
    pub bytes: Vec<u8>,
    pub revision: u64,
}

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum PrivateStoreError {
    #[error("the private store changed since it was read")]
    Conflict,
    #[error("the private store was wiped since it was opened")]
    Wiped,
    #[error("{reason}")]
    Failed { reason: String },
}

impl From<uniffi::UnexpectedUniFFICallbackError> for PrivateStoreError {
    fn from(error: uniffi::UnexpectedUniFFICallbackError) -> Self {
        Self::Failed {
            reason: error.reason,
        }
    }
}

impl From<PrivateStoreError> for StoreError {
    fn from(error: PrivateStoreError) -> Self {
        match error {
            PrivateStoreError::Conflict => Self::Conflict,
            PrivateStoreError::Wiped => Self::Wiped,
            PrivateStoreError::Failed { reason } => Self::Failed(reason),
        }
    }
}

pub(crate) fn config(store: Arc<dyn PrivateStore>) -> EncryptionConfig {
    EncryptionConfig::store(move || Rc::new(CallbackStore(store.clone())))
}

struct CallbackStore(Arc<dyn PrivateStore>);

type Answer<'a, T> = LocalBoxFuture<'a, Result<T, StoreError>>;

impl NativeStore for CallbackStore {
    fn open_scope<'a>(&'a self, scope: &'a str) -> Answer<'a, u64> {
        Box::pin(ready(
            self.0.open_scope(scope.to_owned()).map_err(Into::into),
        ))
    }

    fn load<'a>(&'a self, scope: &'a str, doc: &'a str) -> Answer<'a, Option<Stored>> {
        let result = self.0.load(scope.to_owned(), doc.to_owned());
        Box::pin(ready(
            result
                .map(|document| {
                    document.map(|StoredDocument { bytes, revision }| Stored { bytes, revision })
                })
                .map_err(Into::into),
        ))
    }

    fn save<'a>(
        &'a self,
        scope: &'a str,
        incarnation: u64,
        doc: &'a str,
        bytes: &'a [u8],
        expected: Option<u64>,
    ) -> Answer<'a, u64> {
        let result = self.0.save(
            scope.to_owned(),
            incarnation,
            doc.to_owned(),
            Zeroizing::new(bytes.to_vec()),
            expected,
        );
        Box::pin(ready(result.map_err(Into::into)))
    }

    fn list<'a>(&'a self, scope: &'a str) -> Answer<'a, Vec<String>> {
        Box::pin(ready(self.0.list(scope.to_owned()).map_err(Into::into)))
    }

    fn delete<'a>(&'a self, scope: &'a str) -> Answer<'a, ()> {
        Box::pin(ready(self.0.delete(scope.to_owned()).map_err(Into::into)))
    }

    fn protect<'a>(&'a self, secret: &'a [u8]) -> Answer<'a, Vec<u8>> {
        // UniFFI owns this copy once lowered. Hosts wipe the secret they receive.
        Box::pin(ready(self.0.protect(secret.to_vec()).map_err(Into::into)))
    }

    fn unprotect<'a>(&'a self, blob: &'a [u8]) -> Answer<'a, Zeroizing<Vec<u8>>> {
        Box::pin(ready(
            self.0
                .unprotect(blob.to_vec())
                .map(Zeroizing::new)
                .map_err(Into::into),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Alice's host reads the guarded save argument as its usual UniFFI byte
    /// sequence. Exercise lowering directly because a Rust callback skips it.
    #[test]
    fn guarded_document_matches_the_host_byte_sequence() {
        let bytes = Zeroizing::new(vec![0, 128, 255]);
        let buffer = <Zeroizing<Vec<u8>> as Lower<UniFfiTag>>::lower(bytes);
        assert_eq!(buffer.destroy_into_vec(), [0, 0, 0, 3, 0, 128, 255]);
    }
}
