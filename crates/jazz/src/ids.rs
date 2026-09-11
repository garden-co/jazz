//! Wire-stable identifier newtypes for nodes, rows, schemas, branches, lenses,
//! and compact storage aliases. This module owns identity vocabulary and UUID
//! byte ordering only; allocation, alias persistence, and recovery live in
//! [`crate::node::codec`] and [`crate::node::recovery`]. These ids are shared
//! across every layer from `Db` facade calls through protocol messages to groove
//! storage keys.

/// Globally stable node identity used on the wire.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct NodeUuid(pub uuid::Uuid);

impl NodeUuid {
    /// Construct from UUID bytes in wire order.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }

    /// Return the UUID bytes in wire order.
    pub fn to_bytes(self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    /// Borrow the UUID bytes in wire order.
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

/// Node-local integer alias for compact storage.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct NodeAlias(pub u64);

/// Content-addressed schema version identity.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct SchemaVersionId(pub uuid::Uuid);

impl SchemaVersionId {
    /// Construct from UUID bytes in wire order.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }

    /// Return the UUID bytes in wire order.
    pub fn to_bytes(self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    /// Borrow the UUID bytes in wire order.
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

/// Node-local integer alias for compact schema-version storage.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct SchemaVersionAlias(pub u64);

/// Node-local identity for one shared physical table lineage.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct PhysicalTableId(pub u64);

/// Node-local identity for one physical column epoch.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct PhysicalColumnId(pub u64);

/// Authority-allocated permanent identity for a physical table lineage.
/// Unlike `PhysicalTableId`, this value crosses catalogue replicas and never
/// serves as a compact local storage alias.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct GlobalPhysicalTableId(pub uuid::Uuid);

/// Authority-allocated permanent identity for one physical column epoch.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct GlobalPhysicalColumnId(pub uuid::Uuid);

/// Authority-allocated permanent identity for one enum variant occurrence.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct GlobalPhysicalEnumVariantId(pub uuid::Uuid);

/// Content-addressed migration-lens identity.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct MigrationLensId(pub uuid::Uuid);

impl MigrationLensId {
    /// Construct from UUID bytes in wire order.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }

    /// Return the UUID bytes in wire order.
    pub fn to_bytes(self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    /// Borrow the UUID bytes in wire order.
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

/// Content-addressed atomic schema-lineage publication identity.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct SchemaLineagePublicationId(pub uuid::Uuid);

impl SchemaLineagePublicationId {
    /// Borrow the UUID bytes in wire order.
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

/// Stable identity for a schema family across catalogue projections.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct SchemaFamilyId(pub uuid::Uuid);

impl SchemaFamilyId {
    /// Construct from UUID bytes in wire order.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }

    /// Return the UUID bytes in wire order.
    pub fn to_bytes(self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    /// Borrow the UUID bytes in wire order.
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

/// Stable row identity shared by every historical version of a row.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct RowUuid(pub uuid::Uuid);

impl RowUuid {
    /// Construct from UUID bytes in wire order.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }

    /// Return the UUID bytes in wire order.
    pub fn to_bytes(self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    /// Borrow the UUID bytes in wire order.
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

/// Whole-structure interned author identity. The cached portable spelling is
/// derived once; equality and field access never repeatedly parse JSON.
pub struct InternedAuthor {
    account: Option<crate::account_registry::AccountId>,
    issuer: String,
    subject: String,
    canonical: String,
    row_record: std::sync::OnceLock<crate::groove::records::OwnedRecord>,
    session_record: std::sync::OnceLock<crate::groove::records::OwnedRecord>,
}

// Derived caches must not change diagnostic identity as they initialize.
impl std::fmt::Debug for InternedAuthor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InternedAuthor")
            .field("account", &self.account)
            .field("issuer", &self.issuer)
            .field("subject", &self.subject)
            .field("canonical", &self.canonical)
            .finish()
    }
}

// Borrowed composite lookup uses the existing intern pool. Derived encodings
// do not participate in identity; construct them only after a lookup misses.
trait AuthorIdentity {
    fn identity(&self) -> (Option<crate::account_registry::AccountId>, &str, &str);
}
impl AuthorIdentity for InternedAuthor {
    fn identity(&self) -> (Option<crate::account_registry::AccountId>, &str, &str) {
        (self.account, &self.issuer, &self.subject)
    }
}
impl PartialEq for InternedAuthor {
    fn eq(&self, other: &Self) -> bool {
        self.identity() == other.identity()
    }
}
impl Eq for InternedAuthor {}
impl std::hash::Hash for InternedAuthor {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.identity(), state);
    }
}
impl PartialEq for dyn AuthorIdentity + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.identity() == other.identity()
    }
}
impl Eq for dyn AuthorIdentity + '_ {}
impl std::hash::Hash for dyn AuthorIdentity + '_ {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.identity(), state);
    }
}
impl<'a> std::borrow::Borrow<dyn AuthorIdentity + 'a> for InternedAuthor {
    fn borrow(&self) -> &(dyn AuthorIdentity + 'a) {
        self
    }
}
impl<'a> From<&(dyn AuthorIdentity + 'a)> for InternedAuthor {
    fn from(value: &(dyn AuthorIdentity + 'a)) -> Self {
        let (account, issuer, subject) = value.identity();
        let canonical = match account {
            Some(account) => serde_json::to_string(&(account.0.to_string(), issuer, subject)),
            None => serde_json::to_string(&(issuer, subject)),
        }
        .expect("author components have a canonical JSON spelling");
        Self {
            account,
            issuer: issuer.to_owned(),
            subject: subject.to_owned(),
            canonical,
            row_record: std::sync::OnceLock::new(),
            session_record: std::sync::OnceLock::new(),
        }
    }
}
struct BorrowedAuthor<'a>(Option<crate::account_registry::AccountId>, &'a str, &'a str);
impl AuthorIdentity for BorrowedAuthor<'_> {
    fn identity(&self) -> (Option<crate::account_registry::AccountId>, &str, &str) {
        (self.0, self.1, self.2)
    }
}
fn intern_author(
    account: Option<crate::account_registry::AccountId>,
    issuer: &str,
    subject: &str,
) -> internment::Intern<InternedAuthor> {
    internment::Intern::from_ref(&BorrowedAuthor(account, issuer, subject) as &dyn AuthorIdentity)
}

/// Authenticated subject recorded on transactions and row provenance.
///
/// Account and exact principal are interned together. Row metadata uses the
/// structured native record returned by `to_value`; the intern handle is never persisted, sent,
/// exposed to queries, or used as public ordering.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AuthorSubject {
    /// Internal authority capability; never admitted from JWT claims.
    System,
    /// Persisted system attribution. This is data, never an authority capability.
    SystemAt(internment::Intern<InternedAuthor>),
    /// One interned account and exact principal structure.
    Authenticated(internment::Intern<InternedAuthor>),
}

/// Durable row and transaction attribution.
///
/// Unlike [`AuthorSubject`], this type always carries an account.  A system
/// write uses the reserved nil account and retains the UUID of the node that
/// authored it.  Decoding this type never produces the in-process
/// [`AuthorSubject::System`] authority capability.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RowAuthor(RowAuthorKind);

/// Private durable-attribution variants. Construction stays behind
/// [`RowAuthor`] so an accountless [`AuthorSubject`] cannot be rewrapped as a
/// non-null row author.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum RowAuthorKind {
    /// A durable system write, attributed to its originating node.
    SystemAt(internment::Intern<InternedAuthor>),
    /// A registry-admitted account and exact authenticating principal.
    Account(internment::Intern<InternedAuthor>),
}

/// Rejection returned when constructing or decoding an author subject.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AuthorSubjectError {
    /// External authentication did not provide an issuer.
    #[error("author issuer must be non-empty")]
    MissingIssuer,
    /// Authentication did not provide a subject.
    #[error("author subject must be non-empty")]
    MissingSubject,
    /// An external credential attempted to claim an internal Jazz issuer.
    #[error("author issuer is reserved: {0}")]
    ReservedIssuer(String),
    /// The portable value was not a two-string JSON array.
    #[error("invalid canonical author subject: {0}")]
    InvalidCanonical(String),
    /// The portable value used a non-canonical JSON spelling.
    #[error("author subject is not canonically JSON encoded")]
    NonCanonical,
    /// Durable row metadata cannot be written for an unadmitted principal.
    #[error("persisted row author requires an admitted account")]
    MissingAccount,
    /// The reserved system account may not be used by an ordinary principal.
    #[error("author account is reserved")]
    ReservedAccount,
    /// A system row author did not retain a valid node UUID subject.
    #[error("system row author must retain a node UUID subject")]
    InvalidSystemOrigin,
}

fn author_identity_descriptor() -> crate::groove::records::RecordDescriptor {
    use crate::groove::records::{RecordDescriptor, ValueType};
    static DESCRIPTOR: std::sync::OnceLock<RecordDescriptor> = std::sync::OnceLock::new();
    *DESCRIPTOR.get_or_init(|| {
        RecordDescriptor::new([
            ("issuer", ValueType::String),
            ("subject", ValueType::String),
        ])
    })
}

impl RowAuthor {
    /// Create durable system attribution for work originating at `node`.
    pub fn system_at(node: NodeUuid) -> Self {
        let AuthorSubject::SystemAt(author) = AuthorSubject::system_at(node) else {
            unreachable!("system attribution has a dedicated author variant")
        };
        Self(RowAuthorKind::SystemAt(author))
    }

    /// Convert a local session/capability into durable provenance.
    ///
    /// This is the only local-authoring bridge. A bare authenticated principal
    /// must first be admitted to an account; it is never upgraded to system.
    pub fn from_session(
        subject: AuthorSubject,
        node: NodeUuid,
    ) -> Result<Self, AuthorSubjectError> {
        match subject {
            AuthorSubject::System => Ok(Self::system_at(node)),
            AuthorSubject::SystemAt(_) => Err(AuthorSubjectError::InvalidSystemOrigin),
            AuthorSubject::Authenticated(author) => Self::from_authenticated(author),
        }
    }

    /// Reify a subject that has already crossed the local write boundary.
    ///
    /// `System` deliberately has no representation here: only
    /// `AuthorSubject::system_at` may become durable system metadata.
    pub fn from_persisted_subject(subject: AuthorSubject) -> Result<Self, AuthorSubjectError> {
        match subject {
            AuthorSubject::System => Err(AuthorSubjectError::InvalidSystemOrigin),
            AuthorSubject::SystemAt(author) => Self::from_system_at(author),
            AuthorSubject::Authenticated(author) => Self::from_authenticated(author),
        }
    }

    fn from_authenticated(
        author: internment::Intern<InternedAuthor>,
    ) -> Result<Self, AuthorSubjectError> {
        // `AuthorSubject::Authenticated` is public for compatibility, so a
        // caller can rewrap a `SystemAt` intern. The system issuer is an
        // authority namespace and cannot become an account principal.
        if author.issuer == AuthorSubject::SYSTEM_ISSUER {
            return Err(AuthorSubjectError::ReservedIssuer(author.issuer.clone()));
        }
        match author.account {
            Some(account) if !account.is_system() => Ok(Self(RowAuthorKind::Account(author))),
            Some(_) => Err(AuthorSubjectError::ReservedAccount),
            None => Err(AuthorSubjectError::MissingAccount),
        }
    }

    fn from_system_at(
        author: internment::Intern<InternedAuthor>,
    ) -> Result<Self, AuthorSubjectError> {
        if author.account != Some(crate::account_registry::SYSTEM_ACCOUNT_ID) {
            return Err(AuthorSubjectError::ReservedAccount);
        }
        if author.issuer != AuthorSubject::SYSTEM_ISSUER {
            return Err(AuthorSubjectError::InvalidSystemOrigin);
        }
        Ok(Self(RowAuthorKind::SystemAt(author)))
    }

    /// Stable non-null account owning this durable attribution.
    pub fn account_id(&self) -> crate::account_registry::AccountId {
        match self.0 {
            RowAuthorKind::SystemAt(author) | RowAuthorKind::Account(author) => author
                .account
                .expect("row author construction requires an account"),
        }
    }

    /// Exact principal retained with the durable account attribution.
    pub fn principal_parts(&self) -> (String, String) {
        match self.0 {
            RowAuthorKind::SystemAt(author) | RowAuthorKind::Account(author) => {
                (author.issuer.clone(), author.subject.clone())
            }
        }
    }

    /// Origin node for a system attribution.
    pub fn system_origin(&self) -> Option<NodeUuid> {
        let RowAuthorKind::SystemAt(author) = self.0 else {
            return None;
        };
        uuid::Uuid::parse_str(&author.subject).ok().map(NodeUuid)
    }

    /// Recover the non-authoritative subject representation used by existing
    /// transaction and row APIs. `SystemAt` remains distinct from `System`.
    pub fn as_author_subject(self) -> AuthorSubject {
        match self.0 {
            RowAuthorKind::SystemAt(author) => AuthorSubject::SystemAt(author),
            RowAuthorKind::Account(author) => AuthorSubject::Authenticated(author),
        }
    }

    /// Native record type exposed for persisted row and transaction metadata.
    pub fn value_type() -> crate::groove::records::ValueType {
        crate::groove::records::ValueType::Record(Box::new(Self::record_descriptor()))
    }

    fn record_descriptor() -> crate::groove::records::RecordDescriptor {
        use crate::groove::records::{RecordDescriptor, ValueType};
        static DESCRIPTOR: std::sync::OnceLock<RecordDescriptor> = std::sync::OnceLock::new();
        *DESCRIPTOR.get_or_init(|| {
            RecordDescriptor::new([
                ("account", ValueType::Uuid),
                (
                    "identity",
                    ValueType::Record(Box::new(author_identity_descriptor())),
                ),
            ])
        })
    }

    /// Structured durable attribution value.
    pub fn to_value(self) -> crate::groove::records::Value {
        crate::groove::records::Value::Record(self.encoded_record().clone())
    }

    pub(crate) fn encoded_record(self) -> &'static crate::groove::records::OwnedRecord {
        let author = match self.0 {
            RowAuthorKind::SystemAt(author) | RowAuthorKind::Account(author) => author.as_ref(),
        };
        author.row_record.get_or_init(|| self.build_record())
    }

    fn build_record(self) -> crate::groove::records::OwnedRecord {
        use crate::groove::records::{OwnedRecord, Value};
        let (issuer, subject) = self.principal_parts();
        let identity = author_identity_descriptor();
        let raw = identity
            .create(&[Value::String(issuer), Value::String(subject)])
            .expect("row author identity record");
        let identity = Value::Record(OwnedRecord::new(raw, identity));
        let descriptor = Self::record_descriptor();
        let raw = descriptor
            .create(&[Value::Uuid(self.account_id().0), identity])
            .expect("row author record");
        OwnedRecord::new(raw, descriptor)
    }

    /// Decode durable metadata without granting the system capability.
    pub fn from_value(value: crate::groove::records::Value) -> Result<Self, AuthorSubjectError> {
        use crate::groove::records::Value;
        let bad = || AuthorSubjectError::InvalidCanonical("invalid row author record".into());
        let Value::Record(record) = value else {
            return Err(bad());
        };
        Self::from_record(record.borrowed())
    }

    pub(crate) fn from_record(
        record: crate::groove::records::BorrowedRecord<'_>,
    ) -> Result<Self, AuthorSubjectError> {
        let bad = || AuthorSubjectError::InvalidCanonical("invalid row author record".into());
        let expected = Self::record_descriptor();
        if record.descriptor() != expected {
            return Err(bad());
        }
        let borrowed = record;
        let account = crate::account_registry::AccountId(borrowed.get_uuid(0).map_err(|_| bad())?);
        let span = record
            .descriptor()
            .field_span(record.raw(), 1)
            .map_err(|_| bad())?;
        let identity_descriptor = author_identity_descriptor();
        let identity = identity_descriptor.bind(&record.raw()[span]);
        let issuer = identity.get_str(0).map_err(|_| bad())?;
        let subject = identity.get_str(1).map_err(|_| bad())?;
        if !principal_is_nonempty(issuer) {
            return Err(AuthorSubjectError::MissingIssuer);
        }
        if !principal_is_nonempty(subject) {
            return Err(AuthorSubjectError::MissingSubject);
        }
        if issuer == AuthorSubject::SYSTEM_ISSUER {
            if account != crate::account_registry::SYSTEM_ACCOUNT_ID {
                return Err(AuthorSubjectError::ReservedAccount);
            }
            let node = uuid::Uuid::parse_str(subject)
                .map(NodeUuid)
                .map_err(|_| AuthorSubjectError::InvalidSystemOrigin)?;
            if &*node
                .0
                .hyphenated()
                .encode_lower(&mut uuid::Uuid::encode_buffer())
                != subject
            {
                return Err(AuthorSubjectError::InvalidSystemOrigin);
            }
            return Ok(Self(RowAuthorKind::SystemAt(intern_author(
                Some(account),
                issuer,
                subject,
            ))));
        }
        if account.is_system() {
            return Err(AuthorSubjectError::ReservedAccount);
        }
        Ok(Self(RowAuthorKind::Account(intern_author(
            Some(account),
            issuer,
            subject,
        ))))
    }

    /// Portable canonical spelling for durable-attribution fixtures and wire tests.
    pub fn canonical(&self) -> &str {
        match &self.0 {
            RowAuthorKind::SystemAt(author) | RowAuthorKind::Account(author) => &author.canonical,
        }
    }
}

impl AuthorSubject {
    /// Internal authority subject that bypasses policy checks.
    pub const SYSTEM: Self = Self::System;
    /// Reserved issuer namespace for internal authority work.
    pub const SYSTEM_ISSUER: &'static str = "urn:jazz:system";
    /// Subject component of the internal authority identity.
    pub const SYSTEM_SUBJECT: &'static str = "system";
    /// Portable canonical representation of the internal authority identity.
    pub const SYSTEM_CANONICAL: &'static str = r#"["urn:jazz:system","system"]"#;
    /// Reserved issuer for self-signed local-first JWTs.
    pub const LOCAL_FIRST_ISSUER: &'static str = "urn:jazz:local-first";
    /// Reserved issuer for process-local static bearer sessions.
    pub const STATIC_BEARER_ISSUER: &'static str = "urn:jazz:static-bearer";
    /// Reserved issuer for sessions without an external credential.
    pub const ANONYMOUS_ISSUER: &'static str = "urn:jazz:anonymous";

    /// Construct a subject from externally authenticated JWT components.
    pub fn authenticated(issuer: &str, subject: &str) -> Result<Self, AuthorSubjectError> {
        if !principal_is_nonempty(issuer) {
            return Err(AuthorSubjectError::MissingIssuer);
        }
        if !principal_is_nonempty(subject) {
            return Err(AuthorSubjectError::MissingSubject);
        }
        if Self::is_reserved_issuer(issuer) {
            return Err(AuthorSubjectError::ReservedIssuer(issuer.to_owned()));
        }
        Ok(Self::intern(issuer, subject))
    }

    /// Construct an identity in a Jazz-owned issuer namespace.
    pub(crate) fn reserved(issuer: &str, subject: &str) -> Result<Self, AuthorSubjectError> {
        if !principal_is_nonempty(subject) {
            return Err(AuthorSubjectError::MissingSubject);
        }
        if !matches!(
            issuer,
            Self::LOCAL_FIRST_ISSUER | Self::STATIC_BEARER_ISSUER | Self::ANONYMOUS_ISSUER
        ) {
            return Err(AuthorSubjectError::ReservedIssuer(issuer.to_owned()));
        }
        Ok(Self::intern(issuer, subject))
    }

    fn intern(issuer: &str, subject: &str) -> Self {
        Self::Authenticated(intern_author(None, issuer, subject))
    }

    /// Attribute a durable system write to its originating node.
    pub fn system_at(node: NodeUuid) -> Self {
        Self::SystemAt(intern_author(
            Some(crate::account_registry::SYSTEM_ACCOUNT_ID),
            Self::SYSTEM_ISSUER,
            &node.0.to_string(),
        ))
    }

    fn is_reserved_issuer(issuer: &str) -> bool {
        matches!(
            issuer,
            Self::SYSTEM_ISSUER
                | Self::LOCAL_FIRST_ISSUER
                | Self::STATIC_BEARER_ISSUER
                | Self::ANONYMOUS_ISSUER
        )
    }

    /// Deterministic identity for internal fixtures and simulations.
    pub fn for_test_bytes(bytes: [u8; 16]) -> Self {
        Self::for_test_uuid(uuid::Uuid::from_bytes(bytes))
    }

    /// Deterministic identity for fixtures that already use UUID values.
    pub fn for_test_uuid(value: uuid::Uuid) -> Self {
        Self::authenticated("urn:jazz:test", &value.to_string())
            .expect("the test issuer is external")
            .with_account(crate::account_registry::AccountId(uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_OID,
                value.as_bytes(),
            )))
    }

    /// Recover the UUID subject used by deterministic legacy fixtures.
    ///
    /// This is test support only; production identity semantics use the full
    /// canonical issuer-and-subject string.
    #[doc(hidden)]
    pub fn test_uuid(&self) -> uuid::Uuid {
        let (issuer, subject) = self.principal_parts();
        assert_eq!(issuer, "urn:jazz:test", "not a UUID-backed test subject");
        uuid::Uuid::parse_str(&subject).expect("test subject is a UUID")
    }

    /// Bind an authenticated principal to its registry-admitted account.
    /// This is not authentication: callers must verify assignment first.
    pub fn with_account(self, account: crate::account_registry::AccountId) -> Self {
        assert!(
            matches!(self, Self::Authenticated(_)),
            "system is not an account principal"
        );
        assert!(!account.is_system(), "system account is not user-owned");
        let Self::Authenticated(author) = self else {
            unreachable!()
        };
        Self::Authenticated(intern_author(
            Some(account),
            &author.issuer,
            &author.subject,
        ))
    }

    /// Exact authenticating principal, independent of account ownership.
    pub fn principal_parts(&self) -> (String, String) {
        match self {
            Self::System => (Self::SYSTEM_ISSUER.into(), Self::SYSTEM_SUBJECT.into()),
            Self::SystemAt(author) | Self::Authenticated(author) => {
                (author.issuer.clone(), author.subject.clone())
            }
        }
    }

    /// Stable account ownership, when this is an account-admitted author.
    pub fn account_id(&self) -> Option<crate::account_registry::AccountId> {
        match self {
            Self::System => None,
            Self::SystemAt(author) | Self::Authenticated(author) => author.account,
        }
    }

    /// Native record type exposed for structured author metadata.
    pub fn value_type() -> crate::groove::records::ValueType {
        crate::groove::records::ValueType::Record(Box::new(Self::record_descriptor()))
    }

    fn record_descriptor() -> crate::groove::records::RecordDescriptor {
        use crate::groove::records::{RecordDescriptor, ValueType};
        static DESCRIPTOR: std::sync::OnceLock<RecordDescriptor> = std::sync::OnceLock::new();
        *DESCRIPTOR.get_or_init(|| {
            RecordDescriptor::new([
                ("account", ValueType::Nullable(Box::new(ValueType::Uuid))),
                (
                    "identity",
                    ValueType::Record(Box::new(author_identity_descriptor())),
                ),
            ])
        })
    }

    /// Valid nested author metadata paths. Field names are never interpreted
    /// as provider claims or user-defined columns.
    pub fn metadata_path(column: &str) -> Option<(&str, &'static [&'static str])> {
        let (root, path) = column.split_once('.')?;
        if !matches!(root, "$createdBy" | "$updatedBy") {
            return None;
        }
        let path: &'static [&'static str] = match path {
            "account" => &["account"],
            "identity" => &["identity"],
            "identity.issuer" => &["identity", "issuer"],
            "identity.subject" => &["identity", "subject"],
            _ => return None,
        };
        Some((root, path))
    }

    /// Declared native type for an author root or an explicit nested field.
    pub fn metadata_type(column: &str) -> Option<&'static crate::groove::records::ValueType> {
        use crate::groove::records::ValueType;
        static AUTHOR: std::sync::OnceLock<ValueType> = std::sync::OnceLock::new();
        let mut ty = AUTHOR.get_or_init(RowAuthor::value_type);
        if matches!(column, "$createdBy" | "$updatedBy") {
            return Some(ty);
        }
        let (_, path) = Self::metadata_path(column)?;
        for name in path {
            let ValueType::Record(record) = ty else {
                return None;
            };
            ty = &record.fields().get(record.field_index(name)?)?.value_type;
        }
        Some(ty)
    }

    /// Structured native author value; intern handles are never serialized.
    pub fn to_value(self) -> crate::groove::records::Value {
        crate::groove::records::Value::Record(self.encoded_record().clone())
    }

    pub(crate) fn encoded_record(self) -> &'static crate::groove::records::OwnedRecord {
        match self {
            Self::System => {
                static RECORD: std::sync::OnceLock<crate::groove::records::OwnedRecord> =
                    std::sync::OnceLock::new();
                RECORD.get_or_init(|| self.build_record())
            }
            Self::SystemAt(author) | Self::Authenticated(author) => author
                .as_ref()
                .session_record
                .get_or_init(|| self.build_record()),
        }
    }

    fn build_record(self) -> crate::groove::records::OwnedRecord {
        use crate::groove::records::{OwnedRecord, Value};
        let (issuer, subject) = self.principal_parts();
        let identity = author_identity_descriptor();
        let raw = identity
            .create(&[Value::String(issuer), Value::String(subject)])
            .expect("author identity record");
        let identity = Value::Record(OwnedRecord::new(raw, identity));
        let descriptor = Self::record_descriptor();
        let account = Value::Nullable(
            self.account_id()
                .map(|account| Box::new(Value::Uuid(account.0))),
        );
        let raw = descriptor
            .create(&[account, identity])
            .expect("author record");
        OwnedRecord::new(raw, descriptor)
    }

    /// Decode structured provenance using its exact native descriptor.
    pub fn from_value(value: crate::groove::records::Value) -> Result<Self, AuthorSubjectError> {
        use crate::groove::records::Value;
        let bad = || AuthorSubjectError::InvalidCanonical("invalid author record".into());
        let Value::Record(record) = value else {
            return Err(bad());
        };
        let session_descriptor = Self::record_descriptor();
        let row_descriptor = RowAuthor::record_descriptor();
        let row_author = record.descriptor() == &row_descriptor;
        if !row_author && record.descriptor() != &session_descriptor {
            return Err(bad());
        }
        let borrowed = record.borrowed();
        let account = if row_author {
            Some(borrowed.get_uuid(0).map_err(|_| bad())?)
        } else {
            borrowed.get_nullable_uuid(0).map_err(|_| bad())?
        };
        let span = record
            .descriptor()
            .field_span(record.raw(), 1)
            .map_err(|_| bad())?;
        let identity_descriptor = author_identity_descriptor();
        let identity = identity_descriptor.bind(&record.raw()[span]);
        let issuer = identity.get_str(0).map_err(|_| bad())?;
        let subject = identity.get_str(1).map_err(|_| bad())?;
        if issuer == Self::SYSTEM_ISSUER {
            // The nullable session descriptor is used only for the separately
            // typed transaction permission capability. It may round-trip the
            // in-process SYSTEM marker; row metadata has the non-null
            // RowAuthor descriptor and never takes this branch.
            if !row_author && account.is_none() && subject == Self::SYSTEM_SUBJECT {
                return Ok(Self::SYSTEM);
            }
            let Some(account) = account else {
                return Err(AuthorSubjectError::InvalidSystemOrigin);
            };
            if account != crate::account_registry::SYSTEM_ACCOUNT_ID.0 {
                return Err(AuthorSubjectError::ReservedAccount);
            }
            let node = uuid::Uuid::parse_str(subject)
                .map(NodeUuid)
                .map_err(|_| AuthorSubjectError::InvalidSystemOrigin)?;
            if &*node
                .0
                .hyphenated()
                .encode_lower(&mut uuid::Uuid::encode_buffer())
                != subject
            {
                return Err(AuthorSubjectError::InvalidSystemOrigin);
            }
            return Ok(Self::SystemAt(intern_author(
                Some(crate::account_registry::AccountId(account)),
                issuer,
                subject,
            )));
        }
        if account.is_some_and(|account| account == crate::account_registry::SYSTEM_ACCOUNT_ID.0) {
            return Err(AuthorSubjectError::ReservedAccount);
        }
        if !principal_is_nonempty(issuer) {
            return Err(AuthorSubjectError::MissingIssuer);
        }
        if !principal_is_nonempty(subject) {
            return Err(AuthorSubjectError::MissingSubject);
        }
        Ok(Self::Authenticated(intern_author(
            account.map(crate::account_registry::AccountId),
            issuer,
            subject,
        )))
    }

    /// Ownership value exposed by account policies and row owner metadata.
    /// Bare principals remain available to internal/test contexts.
    pub fn ownership_id(&self) -> String {
        self.account_id()
            .map(|account| account.0.to_string())
            .unwrap_or_else(|| self.canonical().to_owned())
    }

    /// Parse a portable canonical subject, rejecting alternate JSON spellings.
    pub fn from_canonical(canonical: &str) -> Result<Self, AuthorSubjectError> {
        if canonical == Self::SYSTEM_CANONICAL {
            return Ok(Self::SYSTEM);
        }
        let parts: Vec<String> = serde_json::from_str(canonical)
            .map_err(|error| AuthorSubjectError::InvalidCanonical(error.to_string()))?;
        let (account, issuer, subject) = match parts.as_slice() {
            [issuer, subject] => (None, issuer.clone(), subject.clone()),
            [account, issuer, subject] => {
                let uuid = uuid::Uuid::parse_str(account)
                    .map_err(|error| AuthorSubjectError::InvalidCanonical(error.to_string()))?;
                (
                    Some(crate::account_registry::AccountId(uuid)),
                    issuer.clone(),
                    subject.clone(),
                )
            }
            _ => {
                return Err(AuthorSubjectError::InvalidCanonical(
                    "expected principal or account author".into(),
                ));
            }
        };
        if issuer == Self::SYSTEM_ISSUER {
            let Some(account) = account else {
                return Err(AuthorSubjectError::ReservedIssuer(issuer));
            };
            if account != crate::account_registry::SYSTEM_ACCOUNT_ID {
                return Err(AuthorSubjectError::ReservedAccount);
            }
            let node = uuid::Uuid::parse_str(&subject)
                .map(NodeUuid)
                .map_err(|_| AuthorSubjectError::InvalidSystemOrigin)?;
            if &*node
                .0
                .hyphenated()
                .encode_lower(&mut uuid::Uuid::encode_buffer())
                != subject.as_str()
            {
                return Err(AuthorSubjectError::InvalidSystemOrigin);
            }
            let author = Self::system_at(node);
            if author.canonical() != canonical {
                return Err(AuthorSubjectError::NonCanonical);
            }
            return Ok(author);
        }
        if account.is_some_and(|account| account.is_system()) {
            return Err(AuthorSubjectError::ReservedAccount);
        }
        if !principal_is_nonempty(&issuer) {
            return Err(AuthorSubjectError::MissingIssuer);
        }
        if !principal_is_nonempty(&subject) {
            return Err(AuthorSubjectError::MissingSubject);
        }
        let principal = Self::intern(&issuer, &subject);
        let author = account.map_or(principal, |account| principal.with_account(account));
        if author.canonical() != canonical {
            return Err(AuthorSubjectError::NonCanonical);
        }
        Ok(author)
    }

    /// Parse a canonical subject received through an untrusted public binding.
    ///
    /// Jazz-reserved issuers are capabilities selected only by verified or
    /// in-process authority paths; serialized callers may never select them.
    pub fn from_untrusted_canonical(canonical: &str) -> Result<Self, AuthorSubjectError> {
        let parts: Vec<String> = serde_json::from_str(canonical)
            .map_err(|error| AuthorSubjectError::InvalidCanonical(error.to_string()))?;
        let (account, issuer, subject) = match parts.as_slice() {
            [issuer, subject] => (None, issuer.clone(), subject.clone()),
            [account, issuer, subject] => {
                let uuid = uuid::Uuid::parse_str(account)
                    .map_err(|error| AuthorSubjectError::InvalidCanonical(error.to_string()))?;
                (
                    Some(crate::account_registry::AccountId(uuid)),
                    issuer.clone(),
                    subject.clone(),
                )
            }
            _ => {
                return Err(AuthorSubjectError::InvalidCanonical(
                    "expected principal or account author".into(),
                ));
            }
        };
        let principal = Self::authenticated(&issuer, &subject)?;
        if account.is_some_and(|account| account.is_system()) {
            return Err(AuthorSubjectError::ReservedAccount);
        }
        let author = account.map_or(principal, |account| principal.with_account(account));
        if author.canonical() != canonical {
            return Err(AuthorSubjectError::NonCanonical);
        }
        Ok(author)
    }

    /// Deserialize an author received through an untrusted public binding.
    pub fn deserialize_untrusted<'de, D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let canonical = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::from_untrusted_canonical(&canonical).map_err(serde::de::Error::custom)
    }

    /// Return the portable canonical JSON string.
    pub fn canonical(&self) -> &str {
        match self {
            Self::System => Self::SYSTEM_CANONICAL,
            Self::SystemAt(value) | Self::Authenticated(value) => &value.canonical,
        }
    }

    /// Whether this subject belongs to the read-only anonymous issuer.
    pub(crate) fn is_anonymous(&self) -> bool {
        matches!(
            self,
            Self::Authenticated(value)
                if value.issuer == Self::ANONYMOUS_ISSUER
        )
    }
}

fn principal_is_nonempty(value: &str) -> bool {
    value
        .as_bytes()
        .iter()
        .any(|byte| !matches!(byte, b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r' | b' '))
}

impl PartialOrd for AuthorSubject {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AuthorSubject {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.canonical().cmp(other.canonical())
    }
}

impl serde::Serialize for AuthorSubject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.canonical())
    }
}

impl<'de> serde::Deserialize<'de> for AuthorSubject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let canonical = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::from_canonical(&canonical).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthorSubject, AuthorSubjectError, NodeUuid, RowAuthor, RowUuid, SchemaVersionId};

    #[test]
    fn uuid_newtypes_preserve_hard_coded_wire_bytes_and_lexicographic_order() {
        let low = [
            0x00, 0xff, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x01,
        ];
        let high = [
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
        ];
        assert!(low.as_slice() < high.as_slice());
        assert_eq!(NodeUuid::from_bytes(low).to_bytes(), low);
        assert_eq!(RowUuid::from_bytes(low).to_bytes(), low);
        assert_eq!(SchemaVersionId::from_bytes(low).to_bytes(), low);
        assert!(NodeUuid::from_bytes(low) < NodeUuid::from_bytes(high));
        assert!(RowUuid::from_bytes(low) < RowUuid::from_bytes(high));
        assert!(SchemaVersionId::from_bytes(low) < SchemaVersionId::from_bytes(high));
    }

    #[test]
    fn author_subject_is_canonical_json_and_interned() {
        let first =
            AuthorSubject::authenticated("https://issuer.example", "opaque:subject").unwrap();
        let second =
            AuthorSubject::authenticated("https://issuer.example", "opaque:subject").unwrap();
        assert_eq!(first, second);
        assert_eq!(
            first.canonical(),
            r#"["https://issuer.example","opaque:subject"]"#
        );
        assert_eq!(
            AuthorSubject::SYSTEM.canonical(),
            AuthorSubject::SYSTEM_CANONICAL
        );
    }

    // This internal test checks process-local interning, which cannot be
    // observed through a client query, alongside the portable record boundary.
    #[test]
    fn account_authors_intern_the_whole_structure_and_round_trip_native_records() {
        use crate::account_registry::AccountId;
        let account = AccountId(uuid::Uuid::from_bytes([1; 16]));
        let other_account = AccountId(uuid::Uuid::from_bytes([2; 16]));
        let principal = AuthorSubject::authenticated("https://issuer.example", "alice").unwrap();
        let author = principal.with_account(account);
        let same = AuthorSubject::authenticated("https://issuer.example", "alice")
            .unwrap()
            .with_account(account);
        let linked = AuthorSubject::authenticated("https://issuer.example", "bob")
            .unwrap()
            .with_account(account);
        let other_issuer = AuthorSubject::authenticated("https://other.example", "alice")
            .unwrap()
            .with_account(account);
        assert_eq!(author, same);
        let (AuthorSubject::Authenticated(first), AuthorSubject::Authenticated(second)) =
            (author, same)
        else {
            panic!("account authors are authenticated");
        };
        assert!(std::ptr::eq(&*first, &*second));
        assert_eq!(author.account_id(), linked.account_id());
        assert_ne!(author, linked);
        assert_ne!(author, other_issuer);
        assert_ne!(author, principal.with_account(other_account));
        assert_ne!(author, principal);
        for value in [
            author,
            linked,
            other_issuer,
            principal,
            AuthorSubject::SYSTEM,
        ] {
            assert_eq!(AuthorSubject::from_value(value.to_value()).unwrap(), value);
        }
    }

    // Internal: allocation reuse is a process-local representation property.
    #[test]
    fn author_encoded_records_are_reused_without_changing_identity_or_bytes() {
        let principal = AuthorSubject::authenticated("https://cache.example", "subject").unwrap();
        let account = crate::account_registry::AccountId(uuid::Uuid::from_bytes([47; 16]));
        let subject = principal.with_account(account);
        let row = RowAuthor::from_persisted_subject(subject).unwrap();
        let session_record = subject.encoded_record();
        let row_record = row.encoded_record();
        assert_eq!(session_record, &subject.build_record());
        assert_eq!(row_record, &row.build_record());
        for _ in 0..10 {
            let same = AuthorSubject::authenticated("https://cache.example", "subject")
                .unwrap()
                .with_account(account);
            assert_eq!(same, subject);
            assert!(std::ptr::eq(same.encoded_record(), session_record));
            let decoded = RowAuthor::from_record(row_record.borrowed()).unwrap();
            assert_eq!(decoded, row);
            assert!(std::ptr::eq(decoded.encoded_record(), row_record));
        }
    }

    #[test]
    fn row_author_system_origin_is_nonnull_and_never_rehydrates_authority() {
        let node = NodeUuid(uuid::uuid!("11111111-2222-4333-8444-555555555555"));
        let author = RowAuthor::from_session(AuthorSubject::SYSTEM, node).unwrap();
        assert_eq!(
            author.account_id(),
            crate::account_registry::SYSTEM_ACCOUNT_ID
        );
        assert_eq!(author.system_origin(), Some(node));
        assert_eq!(RowAuthor::from_value(author.to_value()).unwrap(), author);
        assert_eq!(
            RowAuthor::from_persisted_subject(AuthorSubject::system_at(node)).unwrap(),
            author
        );
        assert_eq!(author.as_author_subject(), AuthorSubject::system_at(node));
        assert_ne!(author.as_author_subject(), AuthorSubject::SYSTEM);
    }

    #[test]
    fn row_author_rejects_rewrapped_non_system_subject() {
        let AuthorSubject::Authenticated(author) =
            AuthorSubject::authenticated("https://issuer.example", "alice").unwrap()
        else {
            unreachable!("external subject is authenticated")
        };

        assert_eq!(
            RowAuthor::from_persisted_subject(AuthorSubject::SystemAt(author)),
            Err(AuthorSubjectError::ReservedAccount),
            "a public AuthorSubject variant cannot be rewrapped into system provenance"
        );
    }

    #[test]
    fn row_author_rejects_system_attribution_rewrapped_as_account_principal() {
        let node = NodeUuid(uuid::uuid!("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"));
        let AuthorSubject::SystemAt(system_at) = AuthorSubject::system_at(node) else {
            unreachable!("system attribution has a dedicated subject variant")
        };
        let forged = AuthorSubject::Authenticated(system_at).with_account(
            crate::account_registry::AccountId(uuid::uuid!("22222222-3333-4444-8555-666666666666")),
        );
        let expected: std::result::Result<RowAuthor, AuthorSubjectError> = Err(
            AuthorSubjectError::ReservedIssuer(AuthorSubject::SYSTEM_ISSUER.to_owned()),
        );

        assert_eq!(RowAuthor::from_session(forged, node), expected);
        assert_eq!(RowAuthor::from_persisted_subject(forged), expected);
    }

    #[test]
    fn row_author_rejects_account_principal_rewrapped_as_system_attribution() {
        let account = crate::account_registry::AccountId(uuid::Uuid::from_u128(1));
        let AuthorSubject::Authenticated(author) =
            AuthorSubject::authenticated("https://issuer.example", "alice")
                .unwrap()
                .with_account(account)
        else {
            unreachable!("admitted external subject is authenticated")
        };

        assert_eq!(
            RowAuthor::from_persisted_subject(AuthorSubject::SystemAt(author)),
            Err(AuthorSubjectError::ReservedAccount),
            "a public account principal cannot be relabeled as system provenance"
        );
    }

    #[test]
    fn system_capability_decode_is_limited_to_the_nullable_permission_descriptor() {
        use crate::groove::records::{OwnedRecord, Value, ValueType};

        assert_eq!(
            AuthorSubject::from_value(AuthorSubject::SYSTEM.to_value()).unwrap(),
            AuthorSubject::SYSTEM
        );
        assert!(RowAuthor::from_value(AuthorSubject::SYSTEM.to_value()).is_err());

        let ValueType::Record(descriptor) = RowAuthor::value_type() else {
            unreachable!()
        };
        let ValueType::Record(identity_descriptor) = &descriptor.fields()[1].value_type else {
            unreachable!()
        };
        let identity = Value::Record(OwnedRecord::new(
            identity_descriptor
                .create(&[
                    Value::String(AuthorSubject::SYSTEM_ISSUER.into()),
                    Value::String(AuthorSubject::SYSTEM_SUBJECT.into()),
                ])
                .unwrap(),
            identity_descriptor.as_ref().clone(),
        ));
        let raw = descriptor
            .create(&[
                Value::Uuid(crate::account_registry::SYSTEM_ACCOUNT_ID.0),
                identity,
            ])
            .unwrap();
        assert_eq!(
            AuthorSubject::from_value(Value::Record(OwnedRecord::new(raw, *descriptor))),
            Err(AuthorSubjectError::InvalidSystemOrigin),
            "a row descriptor must carry a canonical node UUID, never SYSTEM"
        );
    }

    #[test]
    fn row_author_rejects_noncanonical_system_origin() {
        use crate::groove::records::{OwnedRecord, Value, ValueType};
        let ValueType::Record(descriptor) = RowAuthor::value_type() else {
            unreachable!()
        };
        let ValueType::Record(identity_descriptor) = &descriptor.fields()[1].value_type else {
            unreachable!()
        };
        let identity = Value::Record(OwnedRecord::new(
            identity_descriptor
                .create(&[
                    Value::String(AuthorSubject::SYSTEM_ISSUER.into()),
                    Value::String("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee".to_uppercase()),
                ])
                .unwrap(),
            identity_descriptor.as_ref().clone(),
        ));
        let raw = descriptor
            .create(&[
                Value::Uuid(crate::account_registry::SYSTEM_ACCOUNT_ID.0),
                identity,
            ])
            .unwrap();
        assert_eq!(
            RowAuthor::from_value(Value::Record(OwnedRecord::new(raw, *descriptor))),
            Err(AuthorSubjectError::InvalidSystemOrigin)
        );
    }

    #[test]
    fn row_author_round_trips_admitted_local_first_identity() {
        let account =
            crate::account_registry::AccountId(uuid::uuid!("22222222-3333-4444-8555-666666666666"));
        let session = AuthorSubject::reserved(AuthorSubject::LOCAL_FIRST_ISSUER, "key-subject")
            .unwrap()
            .with_account(account);
        let author = RowAuthor::from_session(
            session,
            NodeUuid(uuid::uuid!("77777777-8888-4999-8aaa-bbbbbbbbbbbb")),
        )
        .unwrap();
        assert_eq!(RowAuthor::from_persisted_subject(session).unwrap(), author);
        assert_eq!(RowAuthor::from_value(author.to_value()).unwrap(), author);
    }

    #[test]
    fn fixture_uuid_survives_account_assignment() {
        let subject = uuid::uuid!("00000000-0000-0000-0000-0000000000a1");
        let account =
            crate::account_registry::AccountId(uuid::uuid!("00000000-0000-0000-0000-0000000000b2"));
        let principal = AuthorSubject::for_test_uuid(subject);
        assert_eq!(principal.test_uuid(), subject);
        assert_eq!(principal.with_account(account).test_uuid(), subject);
    }

    #[test]
    fn author_subject_canonical_json_escapes_components_and_scopes_subject_by_issuer() {
        let escaped =
            AuthorSubject::authenticated("https://issuer.example/a\"b", "line\nfeed").unwrap();
        assert_eq!(
            escaped.canonical(),
            r#"["https://issuer.example/a\"b","line\nfeed"]"#
        );
        assert_eq!(
            AuthorSubject::from_canonical(escaped.canonical()),
            Ok(escaped)
        );

        let left = AuthorSubject::authenticated("https://left.example", "same").unwrap();
        let right = AuthorSubject::authenticated("https://right.example", "same").unwrap();
        assert_ne!(left, right);
    }

    #[test]
    fn author_subject_preserves_exact_ascii_space_and_unicode_whitespace_components() {
        let spaced = AuthorSubject::authenticated(" https://issuer.example ", " alice ").unwrap();
        assert_eq!(
            spaced.canonical(),
            r#"[" https://issuer.example "," alice "]"#
        );

        for value in ["\u{85}", "\u{feff}", "\u{85}provider", "provider\u{feff}"] {
            let author = AuthorSubject::authenticated(value, value).unwrap();
            assert_eq!(
                AuthorSubject::from_canonical(author.canonical()),
                Ok(author)
            );
        }
    }

    #[test]
    fn external_author_subject_rejects_missing_and_reserved_components() {
        assert_eq!(
            AuthorSubject::authenticated("", "user"),
            Err(AuthorSubjectError::MissingIssuer)
        );
        assert_eq!(
            AuthorSubject::authenticated(" \t\n", "user"),
            Err(AuthorSubjectError::MissingIssuer)
        );
        assert_eq!(
            AuthorSubject::authenticated("https://issuer.example", ""),
            Err(AuthorSubjectError::MissingSubject)
        );
        assert_eq!(
            AuthorSubject::authenticated("https://issuer.example", "\t \n\r"),
            Err(AuthorSubjectError::MissingSubject)
        );
        for issuer in [
            AuthorSubject::SYSTEM_ISSUER,
            AuthorSubject::LOCAL_FIRST_ISSUER,
            AuthorSubject::STATIC_BEARER_ISSUER,
            AuthorSubject::ANONYMOUS_ISSUER,
        ] {
            assert_eq!(
                AuthorSubject::authenticated(issuer, "user"),
                Err(AuthorSubjectError::ReservedIssuer(issuer.to_owned()))
            );
        }
    }

    #[test]
    fn untrusted_canonical_author_rejects_every_reserved_issuer() {
        let external = r#"["https://issuer.example","alice"]"#;
        assert_eq!(
            AuthorSubject::from_untrusted_canonical(external)
                .unwrap()
                .canonical(),
            external
        );

        for issuer in [
            AuthorSubject::SYSTEM_ISSUER,
            AuthorSubject::LOCAL_FIRST_ISSUER,
            AuthorSubject::STATIC_BEARER_ISSUER,
            AuthorSubject::ANONYMOUS_ISSUER,
        ] {
            let canonical = serde_json::to_string(&(issuer, "caller")).unwrap();
            assert_eq!(
                AuthorSubject::from_untrusted_canonical(&canonical),
                Err(AuthorSubjectError::ReservedIssuer(issuer.to_owned()))
            );
        }
    }

    #[test]
    fn canonical_author_subject_has_no_legacy_uuid_or_noncanonical_decoder() {
        assert!(AuthorSubject::from_canonical("00000000-0000-4000-8000-000000000001").is_err());
        assert!(AuthorSubject::from_canonical(r#"[ "issuer", "subject" ]"#).is_err());
        assert!(AuthorSubject::from_canonical(r#"["urn:jazz:system","user"]"#).is_err());
    }

    #[test]
    fn canonical_author_subject_rejects_lone_surrogates_but_accepts_valid_code_points() {
        assert!(AuthorSubject::from_canonical(r#"["issuer","\ud800"]"#).is_err());
        assert!(AuthorSubject::from_canonical(r#"["issuer","\udc00"]"#).is_err());

        let emoji = AuthorSubject::authenticated("issuer🚀", "subject🚀").unwrap();
        assert_eq!(emoji.canonical(), r#"["issuer🚀","subject🚀"]"#);
        assert_eq!(AuthorSubject::from_canonical(emoji.canonical()), Ok(emoji));

        assert!(AuthorSubject::from_canonical(r#"["issuer","\ud83d\ude80"]"#).is_err());
    }
}
