//! Host-admitted storage-owner scope binding a durable client relay.
//! Re-exported from `crate::db`.

use crate::ids::AuthorSubject;

/// The non-secret, canonical storage ownership scope admitted by a relay host.
///
/// This is deliberately opaque to Jazz: the browser/native host derives it from
/// its authenticated app/environment/account scope and binds the durable relay
/// to that exact value before application code can use the database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientRelayScope {
    storage_owner: String,
    admitted_session: Option<AuthorSubject>,
}

impl ClientRelayScope {
    pub(crate) fn durable_digest(&self) -> [u8; 32] {
        let mut material = Vec::new();
        material.extend_from_slice(&(self.storage_owner.len() as u64).to_be_bytes());
        material.extend_from_slice(self.storage_owner.as_bytes());
        match self.admitted_session {
            Some(subject) => {
                material.push(1);
                material.extend_from_slice(&(subject.canonical().len() as u64).to_be_bytes());
                material.extend_from_slice(subject.canonical().as_bytes());
            }
            None => material.push(0),
        }
        blake3::derive_key("jazz.scope-relay-repair-ledger.v1", &material)
    }

    pub(crate) fn durable_components(&self) -> (&str, Option<String>) {
        (
            &self.storage_owner,
            self.admitted_session
                .map(|subject| subject.canonical().to_owned()),
        )
    }
    /// Construct a scope from a host-admitted canonical owner encoding.
    ///
    /// # Safety
    /// The caller must have authenticated and durably bound this exact value to
    /// the storage root. This is a host capability, never an application API.
    #[doc(hidden)]
    pub unsafe fn from_admitted_storage_owner(
        storage_owner: String,
        admitted_session: AuthorSubject,
    ) -> Self {
        Self {
            storage_owner,
            admitted_session: Some(admitted_session),
        }
    }

    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        self.storage_owner == other.storage_owner && self.admitted_session == other.admitted_session
    }

    pub(crate) fn admits_session(&self, session: AuthorSubject) -> bool {
        self.admitted_session
            .is_none_or(|admitted| admitted == session)
    }

    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn test_unbound_storage_owner(owner: String) -> Self {
        Self {
            storage_owner: owner,
            admitted_session: None,
        }
    }
}
