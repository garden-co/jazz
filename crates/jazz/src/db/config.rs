//! Database construction identity, storage, and row-id configuration.

use super::*;

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

/// Identity attached to locally-authored writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct DbIdentity {
    /// Node identity.
    pub node: NodeUuid,
    /// Application author identity.
    pub author: AuthorSubject,
}

/// Configuration for [`Db::open`].
pub struct DbConfig<S> {
    /// Runtime schema.
    pub schema: JazzSchema,
    /// Storage implementation.
    pub storage: S,
    /// Local identity.
    pub identity: DbIdentity,
    /// Row id source used by [`Db::insert`].
    ///
    /// `None` selects the production source.
    pub id_source: Option<Box<dyn RowIdSource>>,
}

impl<S> DbConfig<S> {
    /// Build a config using the production row id source.
    pub fn new(schema: JazzSchema, storage: S, identity: DbIdentity) -> Self {
        Self {
            schema,
            storage,
            identity,
            id_source: None,
        }
    }

    /// Override the row id source, typically with [`SeededRowIdSource`] in tests.
    pub fn with_id_source(mut self, id_source: impl RowIdSource + 'static) -> Self {
        self.id_source = Some(Box::new(id_source));
        self
    }
}

/// Source of uuidv7-shaped row ids for [`Db::insert`].
pub trait RowIdSource {
    /// Return the next row id.
    fn next_row_id(&mut self) -> RowUuid;
}

/// Production row id source using the system clock and OS randomness.
///
/// Tests and simulations should use [`SeededRowIdSource`] instead.
#[derive(Clone, Debug, Default)]
pub struct ProductionRowIdSource;

impl RowIdSource for ProductionRowIdSource {
    fn next_row_id(&mut self) -> RowUuid {
        RowUuid(uuid::Uuid::now_v7())
    }
}

/// Deterministic uuidv7-shaped row id source for tests and simulations.
#[derive(Clone, Debug)]
pub struct SeededRowIdSource {
    millis: u64,
    state: u64,
}

impl SeededRowIdSource {
    /// Create a deterministic source from a caller-provided seed.
    pub fn new(seed: u64) -> Self {
        Self {
            millis: seed & ((1_u64 << 48) - 1),
            state: seed ^ 0x9e37_79b9_7f4a_7c15,
        }
    }
}

impl RowIdSource for SeededRowIdSource {
    fn next_row_id(&mut self) -> RowUuid {
        let millis = self.millis & ((1_u64 << 48) - 1);
        self.millis = self.millis.wrapping_add(1);

        let rand_a = (splitmix64(&mut self.state) & 0x0fff) as u16;
        let rand_b = splitmix64(&mut self.state) & ((1_u64 << 62) - 1);

        let mut bytes = [0_u8; 16];
        bytes[..6].copy_from_slice(&millis.to_be_bytes()[2..]);
        let version_and_rand_a = 0x7000_u16 | rand_a;
        bytes[6..8].copy_from_slice(&version_and_rand_a.to_be_bytes());
        let variant_and_rand_b = 0x8000_0000_0000_0000_u64 | rand_b;
        bytes[8..16].copy_from_slice(&variant_and_rand_b.to_be_bytes());
        RowUuid::from_bytes(bytes)
    }
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}
