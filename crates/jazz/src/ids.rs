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
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct InternedAuthor {
    account: Option<crate::account_registry::AccountId>,
    issuer: String,
    subject: String,
    canonical: String,
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
    /// One interned account and exact principal structure.
    Authenticated(internment::Intern<InternedAuthor>),
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
        let canonical = serde_json::to_string(&(issuer, subject))
            .expect("two strings always have a canonical JSON encoding");
        Self::Authenticated(internment::Intern::new(InternedAuthor {
            account: None,
            issuer: issuer.to_owned(),
            subject: subject.to_owned(),
            canonical,
        }))
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
        Self::authenticated("urn:jazz:test", &uuid::Uuid::from_bytes(bytes).to_string())
            .expect("the test issuer is external")
    }

    /// Deterministic identity for fixtures that already use UUID values.
    pub fn for_test_uuid(value: uuid::Uuid) -> Self {
        Self::authenticated("urn:jazz:test", &value.to_string())
            .expect("the test issuer is external")
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
            !matches!(self, Self::System),
            "system is not an account principal"
        );
        let (issuer, subject) = self.principal_parts();
        let canonical = serde_json::to_string(&(account.0.to_string(), &issuer, &subject))
            .expect("account author is serializable");
        Self::Authenticated(internment::Intern::new(InternedAuthor {
            account: Some(account),
            issuer,
            subject,
            canonical,
        }))
    }

    /// Exact authenticating principal, independent of account ownership.
    pub fn principal_parts(&self) -> (String, String) {
        match self {
            Self::System => (Self::SYSTEM_ISSUER.into(), Self::SYSTEM_SUBJECT.into()),
            Self::Authenticated(author) => (author.issuer.clone(), author.subject.clone()),
        }
    }

    /// Stable account ownership, when this is an account-admitted author.
    pub fn account_id(&self) -> Option<crate::account_registry::AccountId> {
        match self {
            Self::System => None,
            Self::Authenticated(author) => author.account,
        }
    }

    /// Native record type exposed for structured author metadata.
    pub fn value_type() -> crate::groove::records::ValueType {
        use crate::groove::records::{RecordDescriptor, ValueType};
        ValueType::Record(Box::new(RecordDescriptor::new([
            ("account", ValueType::Nullable(Box::new(ValueType::Uuid))),
            (
                "identity",
                ValueType::Record(Box::new(RecordDescriptor::new([
                    ("issuer", ValueType::String),
                    ("subject", ValueType::String),
                ]))),
            ),
        ])))
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
        let mut ty = AUTHOR.get_or_init(Self::value_type);
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
        use crate::groove::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
        let (issuer, subject) = self.principal_parts();
        let identity = RecordDescriptor::new([
            ("issuer", ValueType::String),
            ("subject", ValueType::String),
        ]);
        let raw = identity
            .create(&[Value::String(issuer), Value::String(subject)])
            .expect("author identity record");
        let identity = Value::Record(OwnedRecord::new(raw, identity));
        let ValueType::Record(descriptor) = Self::value_type() else {
            unreachable!()
        };
        let account = Value::Nullable(
            self.account_id()
                .map(|account| Box::new(Value::Uuid(account.0))),
        );
        let raw = descriptor
            .create(&[account, identity])
            .expect("author record");
        Value::Record(OwnedRecord::new(raw, *descriptor))
    }

    /// Decode structured provenance using its exact native descriptor.
    pub fn from_value(value: crate::groove::records::Value) -> Result<Self, AuthorSubjectError> {
        use crate::groove::records::{Value, ValueType};
        let bad = || AuthorSubjectError::InvalidCanonical("invalid author record".into());
        let Value::Record(record) = value else {
            return Err(bad());
        };
        let ValueType::Record(expected) = Self::value_type() else {
            unreachable!()
        };
        if record.descriptor() != expected.as_ref() {
            return Err(bad());
        }
        let borrowed = record.borrowed();
        let account = borrowed.get_nullable_uuid(0).map_err(|_| bad())?;
        let Value::Record(identity) = borrowed.get_idx(1).map_err(|_| bad())? else {
            return Err(bad());
        };
        let identity = identity.borrowed();
        let issuer = identity.get_str(0).map_err(|_| bad())?;
        let subject = identity.get_str(1).map_err(|_| bad())?;
        let canonical = if let Some(account) = account {
            serde_json::to_string(&(account.to_string(), issuer, subject))
        } else {
            serde_json::to_string(&(issuer, subject))
        }
        .map_err(|_| bad())?;
        Self::from_canonical(&canonical)
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
            return Err(AuthorSubjectError::ReservedIssuer(issuer));
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
            Self::Authenticated(value) => &value.canonical,
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
    use super::{AuthorSubject, AuthorSubjectError, NodeUuid, RowUuid, SchemaVersionId};

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
