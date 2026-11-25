//! Private storage for one device's encryption state (`E2EE_PROTOCOL.md`
//! §6.1; core hooks note H9): documents per scope `Dₐ(account)`, saved by
//! compare and swap, and the platform's protection of secrets at rest.
//!
//! The runtime stores, per scope, one device document (`device`), one
//! held-key document per space root (`held-<root ID>`) and one floor record
//! per account it verified (`floor-<account ID>`). A store only keeps
//! bytes: it never parses them. Every saved document has a revision, and a
//! save names the revision it replaces, so two writers of one store (two
//! tabs, two runtimes) never silently lose each other's keys: the later one
//! gets [`StoreError::Conflict`], reads again and merges.
//!
//! A scope has an incarnation, a random number fixed when the scope is
//! created ([`PrivateStore::open_scope`]) and gone when it is deleted. A
//! save names the incarnation its writer opened, and a save into a deleted
//! or recreated scope is [`StoreError::Wiped`]: a writer that opened the
//! scope before a wipe never recreates it, and a revision it read before
//! the wipe never matches a document written after. A store removes the
//! incarnation before the documents when it deletes a scope, and treats
//! documents left without one as wiped ([`PrivateStore::delete`]).
//!
//! [`PrivateStore::protect`] and [`PrivateStore::unprotect`] are the
//! platform's at-rest protection of the device's private keys and approval
//! nonce, which are never stored in the clear: a non-extractable WebCrypto
//! key in browsers, the Keychain on Apple platforms, the Android Keystore,
//! or what the application supplies elsewhere: Jazz ships no default store,
//! the application brings its own.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use futures::future::LocalBoxFuture;
use jazz_e2ee::Zeroizing;

/// Why a store operation failed. A store failure fails the operation
/// loudly; it is never reported as a missing key.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    /// The document's revision is not the one the save names: another
    /// writer saved it (or created or deleted it) since it was read.
    #[error("the private store changed since it was read")]
    Conflict,
    /// The scope was deleted, and perhaps created again, since the writer
    /// opened it: nothing is written.
    #[error("the private store was wiped since it was opened")]
    Wiped,
    /// The store, or the platform's protection, failed.
    #[error("the private store failed: {0}")]
    Failed(String),
}

/// A stored document and its revision.
#[derive(Clone, PartialEq, Eq)]
pub struct Stored {
    pub bytes: Vec<u8>,
    pub revision: u64,
}

/// The names, bytes and revisions of the documents in one scope.
pub type ScopeDocuments = Vec<(String, Stored)>;

impl std::fmt::Debug for Stored {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stored")
            .field("bytes", &self.bytes.len())
            .field("revision", &self.revision)
            .finish()
    }
}

/// The host's private storage. Futures are polled on the database's
/// thread, so they need not be `Send`.
pub trait PrivateStore {
    /// The document `doc` of `scope`, if stored.
    fn load<'a>(
        &'a self,
        scope: &'a str,
        doc: &'a str,
    ) -> LocalBoxFuture<'a, Result<Option<Stored>, StoreError>>;

    /// An optional read of an entire scope. Read the incarnation and the
    /// documents atomically, returning [`StoreError::Wiped`] if the scope is
    /// absent or has another incarnation.
    fn load_all<'a>(
        &'a self,
        _scope: &'a str,
        _incarnation: u64,
    ) -> Option<LocalBoxFuture<'a, Result<ScopeDocuments, StoreError>>> {
        None
    }

    /// The incarnation of `scope`, creating the scope with a new random
    /// incarnation if it does not exist.
    fn open_scope<'a>(&'a self, scope: &'a str) -> LocalBoxFuture<'a, Result<u64, StoreError>>;

    /// Replaces document `doc` of `scope` with `bytes` if the scope is still
    /// of `incarnation` and the document's revision is still `expected`
    /// (`None`: the document must not exist), atomically, and returns the
    /// new revision. Otherwise [`StoreError::Wiped`] or
    /// [`StoreError::Conflict`], and nothing is written; a save never
    /// creates a scope.
    fn save<'a>(
        &'a self,
        scope: &'a str,
        incarnation: u64,
        doc: &'a str,
        bytes: &'a [u8],
        expected: Option<u64>,
    ) -> LocalBoxFuture<'a, Result<u64, StoreError>>;

    /// The names of the documents of `scope`.
    fn list<'a>(&'a self, scope: &'a str) -> LocalBoxFuture<'a, Result<Vec<String>, StoreError>>;

    /// Removes every document of `scope`. Removing a scope that has none
    /// succeeds. The order is part of the contract: remove the scope's
    /// incarnation first, then its documents, so a delete that stops half
    /// way leaves documents without an incarnation. A store that finds a
    /// scope in that state treats it as wiped: [`PrivateStore::open_scope`]
    /// removes what is left and makes a new scope with a new incarnation,
    /// and nothing left over is ever read or adopted again (a wiped
    /// device's keys must not come back).
    fn delete<'a>(&'a self, scope: &'a str) -> LocalBoxFuture<'a, Result<(), StoreError>>;

    /// Protects a secret at rest.
    fn protect<'a>(&'a self, secret: &'a [u8]) -> LocalBoxFuture<'a, Result<Vec<u8>, StoreError>>;

    /// Recovers a secret [`PrivateStore::protect`] protected.
    fn unprotect<'a>(
        &'a self,
        blob: &'a [u8],
    ) -> LocalBoxFuture<'a, Result<Zeroizing<Vec<u8>>, StoreError>>;
}

/// Whether `doc` is a document name every store accepts: 1 to 100
/// lowercase ASCII letters, digits and hyphens.
pub fn is_document_name(doc: &str) -> bool {
    (1..=100).contains(&doc.len())
        && doc
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

/// A new scope's incarnation: random and never zero.
fn new_incarnation() -> u64 {
    let (high, _) = uuid::Uuid::new_v4().as_u64_pair();
    high.max(1)
}

fn check_name(doc: &str) -> Result<(), StoreError> {
    if is_document_name(doc) {
        Ok(())
    } else {
        Err(StoreError::Failed(format!("invalid document name `{doc}`")))
    }
}

/// A store in memory, for tests and sessions that keep nothing. Clones
/// share the same documents, so a runtime opened again on a clone sees
/// what the first one saved. Nothing reaches disk, so
/// [`PrivateStore::protect`] keeps secrets as they are.
#[derive(Clone, Default)]
pub struct MemoryStore {
    scopes: Arc<Mutex<BTreeMap<String, MemoryScope>>>,
}

#[derive(Default)]
struct MemoryScope {
    incarnation: u64,
    docs: BTreeMap<String, Stored>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces a stored document's bytes as they are, keeping its
    /// revision: what a damaged or tampered store looks like.
    pub fn overwrite(&self, scope: &str, doc: &str, bytes: Vec<u8>) {
        let mut scopes = self.scopes.lock().unwrap();
        let docs = &mut scopes
            .entry(scope.to_owned())
            .or_insert_with(|| MemoryScope {
                incarnation: new_incarnation(),
                docs: BTreeMap::new(),
            })
            .docs;
        let revision = docs.get(doc).map_or(1, |stored| stored.revision);
        docs.insert(doc.to_owned(), Stored { bytes, revision });
    }

    /// The scopes that hold at least one document.
    pub fn scopes(&self) -> Vec<String> {
        self.scopes
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, scope)| !scope.docs.is_empty())
            .map(|(scope, _)| scope.clone())
            .collect()
    }
}

impl PrivateStore for MemoryStore {
    fn load<'a>(
        &'a self,
        scope: &'a str,
        doc: &'a str,
    ) -> LocalBoxFuture<'a, Result<Option<Stored>, StoreError>> {
        Box::pin(async move {
            Ok(self
                .scopes
                .lock()
                .unwrap()
                .get(scope)
                .and_then(|scope| scope.docs.get(doc))
                .cloned())
        })
    }

    fn open_scope<'a>(&'a self, scope: &'a str) -> LocalBoxFuture<'a, Result<u64, StoreError>> {
        Box::pin(async move {
            Ok(self
                .scopes
                .lock()
                .unwrap()
                .entry(scope.to_owned())
                .or_insert_with(|| MemoryScope {
                    incarnation: new_incarnation(),
                    docs: BTreeMap::new(),
                })
                .incarnation)
        })
    }

    fn save<'a>(
        &'a self,
        scope: &'a str,
        incarnation: u64,
        doc: &'a str,
        bytes: &'a [u8],
        expected: Option<u64>,
    ) -> LocalBoxFuture<'a, Result<u64, StoreError>> {
        Box::pin(async move {
            check_name(doc)?;
            let mut scopes = self.scopes.lock().unwrap();
            let docs = match scopes.get_mut(scope) {
                Some(scope) if scope.incarnation == incarnation => &mut scope.docs,
                _ => return Err(StoreError::Wiped),
            };
            let current = docs.get(doc).map(|stored| stored.revision);
            if current != expected {
                return Err(StoreError::Conflict);
            }
            let revision = current.map_or(1, |revision| revision + 1);
            docs.insert(
                doc.to_owned(),
                Stored {
                    bytes: bytes.to_vec(),
                    revision,
                },
            );
            Ok(revision)
        })
    }

    fn list<'a>(&'a self, scope: &'a str) -> LocalBoxFuture<'a, Result<Vec<String>, StoreError>> {
        Box::pin(async move {
            Ok(self
                .scopes
                .lock()
                .unwrap()
                .get(scope)
                .map(|scope| scope.docs.keys().cloned().collect())
                .unwrap_or_default())
        })
    }

    fn delete<'a>(&'a self, scope: &'a str) -> LocalBoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.scopes.lock().unwrap().remove(scope);
            Ok(())
        })
    }

    fn protect<'a>(&'a self, secret: &'a [u8]) -> LocalBoxFuture<'a, Result<Vec<u8>, StoreError>> {
        Box::pin(async move { Ok(secret.to_vec()) })
    }

    fn unprotect<'a>(
        &'a self,
        blob: &'a [u8],
    ) -> LocalBoxFuture<'a, Result<Zeroizing<Vec<u8>>, StoreError>> {
        Box::pin(async move { Ok(Zeroizing::new(blob.to_vec())) })
    }
}
