//! Durable Object socket attachment v1. The version and session kind precede
//! u16-length-prefixed sections for admission, schema, and live reads.
use jazz::catalogue::BundleId;
use jazz::db::SyncMessage;
use jazz_server::portable::admission::{
    CommitUnitTrust, SessionRevocation, WebSocketAdmission, WebSocketCredential,
};
use serde::{Deserialize, Deserializer};
use std::collections::BTreeMap;

const VERSION: u8 = 1;
const LIMIT: usize = 16 * 1024;

#[derive(serde::Deserialize)]
pub(super) struct Admission {
    pub client_features: u64,
    pub identity: jazz::ids::AuthorSubject,
    pub claims: BTreeMap<String, jazz::records::Value>,
    #[serde(default, deserialize_with = "decode_bundle")]
    pub bundle: Option<BundleId>,
    #[serde(deserialize_with = "decode_trust")]
    pub trust: CommitUnitTrust,
    pub guest: bool,
    #[serde(default, deserialize_with = "decode_revocation")]
    pub revocation: Option<SessionRevocation>,
}

fn decode_bundle<'de, D: Deserializer<'de>>(input: D) -> Result<Option<BundleId>, D::Error> {
    Option::<String>::deserialize(input)?
        .map(|hex| {
            BundleId::from_hex(&hex).ok_or_else(|| serde::de::Error::custom("invalid bundle"))
        })
        .transpose()
}

fn decode_trust<'de, D: Deserializer<'de>>(input: D) -> Result<CommitUnitTrust, D::Error> {
    match String::deserialize(input)?.as_str() {
        "session" => Ok(CommitUnitTrust::Session),
        "backend" => Ok(CommitUnitTrust::TrustedBackend),
        "authority" => Ok(CommitUnitTrust::TrustedAuthority),
        _ => Err(serde::de::Error::custom("unknown socket trust")),
    }
}

fn decode_revocation<'de, D: Deserializer<'de>>(
    input: D,
) -> Result<Option<SessionRevocation>, D::Error> {
    Ok(
        Option::<(_, u64)>::deserialize(input)?.map(|(principal, authenticated_at_ms)| {
            SessionRevocation {
                principal,
                authenticated_at_ms,
            }
        }),
    )
}

pub(super) enum SavedReads {
    Sync(Vec<Vec<u8>>),
    Thin {
        version: Option<jazz::ids::SchemaVersionId>,
        requests: Vec<jazz::thin::ThinRequest>,
    },
}

pub(super) struct SocketAttachment {
    thin: bool,
    admission: Vec<u8>,
    join: Vec<u8>,
    reads: BTreeMap<u64, Vec<u8>>,
    len: usize,
    dirty: bool,
    partial: bool,
}

impl SocketAttachment {
    pub fn new(
        admission: &WebSocketAdmission,
        client_features: u64,
        thin: bool,
    ) -> Result<Self, String> {
        let trust = match admission.trust {
            CommitUnitTrust::Session => "session",
            CommitUnitTrust::TrustedBackend => "backend",
            CommitUnitTrust::TrustedAuthority => "authority",
        };
        let admission = serde_json::to_vec(&serde_json::json!({
            "client_features": client_features,
            "identity": admission.identity.canonical(),
            "claims": admission.claims,
            "bundle": admission.bundle.map(|bundle| bundle.to_hex()),
            "trust": trust,
            "guest": admission.credential == WebSocketCredential::Session,
            "revocation": admission.revocation.as_ref().map(|r| (&r.principal, r.authenticated_at_ms)),
        })).map_err(|error| error.to_string())?;
        let len = 2 + 4 + admission.len();
        Ok(Self {
            thin,
            admission,
            join: Vec::new(),
            reads: BTreeMap::new(),
            len,
            dirty: true,
            partial: false,
        })
    }

    /// Decode only registrations written by this host. Never replay a write
    /// or an incomplete opening, even if a deployment left incompatible bytes.
    pub fn restore(bytes: &[u8]) -> Result<(Self, Admission, SavedReads), &'static str> {
        fn section<'a>(input: &mut &'a [u8]) -> Option<&'a [u8]> {
            let n = usize::from(u16::from_le_bytes(input.split_off(..2)?.try_into().ok()?));
            input.split_off(..n)
        }
        if bytes.len() > LIMIT {
            return Err("oversized socket attachment");
        }
        let (thin, mut input) = match bytes {
            [VERSION, 0, rest @ ..] => (false, rest),
            [VERSION, 1, rest @ ..] => (true, rest),
            _ => return Err("unknown socket attachment"),
        };
        let header = section(&mut input).ok_or("missing admission")?;
        let admission = serde_json::from_slice(header).map_err(|_| "invalid admission")?;
        let join = section(&mut input).ok_or("missing Join")?;
        let mut saved = if thin {
            let version = if join.is_empty() {
                None
            } else {
                Some(jazz::ids::SchemaVersionId::from_bytes(
                    join.try_into().map_err(|_| "invalid thin schema version")?,
                ))
            };
            SavedReads::Thin {
                version,
                requests: Vec::new(),
            }
        } else {
            if !matches!(jazz::db::decode_message(join), Ok(SyncMessage::Join { .. })) {
                return Err("incomplete Join");
            }
            SavedReads::Sync(vec![join.to_vec()])
        };
        let mut state = Self {
            thin,
            admission: header.to_vec(),
            join: join.to_vec(),
            reads: BTreeMap::new(),
            len: bytes.len(),
            dirty: false,
            partial: false,
        };
        while !input.is_empty() {
            let frame = section(&mut input).ok_or("truncated live read")?;
            let id = match &mut saved {
                SavedReads::Thin {
                    version: Some(_),
                    requests,
                } => {
                    let (request, []) = postcard::take_from_bytes::<jazz::thin::ThinRequest>(frame)
                        .map_err(|_| "invalid thin read")?
                    else {
                        return Err("invalid thin read");
                    };
                    if !matches!(request.op, jazz::thin::ThinOp::Subscribe { .. }) {
                        return Err("invalid thin read");
                    }
                    let id = request.id;
                    requests.push(request);
                    id
                }
                SavedReads::Thin { .. } => return Err("missing thin schema"),
                SavedReads::Sync(frames) => {
                    let Ok(SyncMessage::Read {
                        id,
                        span: jazz::db::ReadSpan::Live,
                        since: None,
                        ..
                    }) = jazz::db::decode_message(frame)
                    else {
                        return Err("invalid live read");
                    };
                    frames.push(frame.to_vec());
                    id
                }
            };
            if state.reads.insert(id, frame.to_vec()).is_some() {
                return Err("duplicate live read");
            }
        }
        Ok((state, admission, saved))
    }

    fn invalidate(&mut self) {
        self.len = LIMIT + 1;
        self.dirty = true;
    }

    fn set_schema(&mut self, bytes: &[u8]) {
        self.len = self.len - self.join.len() + bytes.len();
        self.join = bytes.to_vec();
        self.dirty = true;
    }

    fn remove_read(&mut self, id: u64) {
        if let Some(old) = self.reads.remove(&id) {
            self.len -= 2 + old.len();
            self.dirty = true;
        }
    }

    fn save_encoded(&mut self, read: Option<u64>, value: &impl serde::Serialize) {
        let mut buf = [0; LIMIT];
        let Ok(bytes) = postcard::to_slice(value, &mut buf) else {
            self.invalidate();
            return;
        };
        if let Some(id) = read {
            self.remove_read(id);
            self.len += 2 + bytes.len();
            self.reads.insert(id, bytes.to_vec());
            self.dirty = true;
        } else {
            self.set_schema(bytes);
        }
    }

    pub fn record(&mut self, message: &SyncMessage) {
        if self.len > LIMIT {
            return;
        }
        match message {
            SyncMessage::Join { .. } => self.save_encoded(None, message),
            SyncMessage::Read {
                id, since: None, ..
            } => self.save_encoded(Some(*id), message),
            SyncMessage::Read { id, .. } => {
                let mut fresh = message.clone();
                if let SyncMessage::Read { since, .. } = &mut fresh {
                    *since = None;
                }
                self.save_encoded(Some(*id), &fresh);
            }
            SyncMessage::Unsubscribe { id } => self.remove_read(*id),
            _ => {}
        }
    }

    pub fn record_thin(&mut self, event: jazz_server::runtime::ThinRegistration) {
        use jazz_server::runtime::ThinRegistration::*;
        if self.len > LIMIT {
            return;
        }
        match event {
            Schema(version) => {
                if !self.reads.is_empty() && self.join.as_slice() != version.as_bytes() {
                    // Existing reads can still be bound to the previous schema.
                    self.invalidate();
                } else {
                    self.set_schema(version.as_bytes());
                }
            }
            // (id, op) has ThinRequest's pinned field order.
            Subscribe { id, op } => self.save_encoded(Some(id), &(id, op)),
            Remove(id) => self.remove_read(id),
        }
    }

    pub fn set_partial(&mut self, partial: bool) {
        self.dirty |= self.partial != partial;
        self.partial = partial;
    }

    pub fn take_changed(&mut self) -> Option<Vec<u8>> {
        if !std::mem::take(&mut self.dirty) {
            return None;
        }
        if self.partial || self.len > LIMIT {
            return Some(Vec::new());
        }
        let mut out = Vec::with_capacity(self.len);
        out.extend([VERSION, u8::from(self.thin)]);
        for bytes in [&self.admission, &self.join]
            .into_iter()
            .chain(self.reads.values())
        {
            out.extend((bytes.len() as u16).to_le_bytes());
            out.extend(bytes);
        }
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jazz_server::portable::admission::{admit_by_core_secret, UpgradeCredential};

    fn backend_attachment(thin: bool) -> SocketAttachment {
        let prelude = serde_json::from_value(serde_json::json!({
            "peer_identity": jazz::ids::AuthorSubject::SYSTEM_CANONICAL, "auth": {}
        }))
        .unwrap();
        let upgrade = UpgradeCredential {
            credential: "test-secret",
            access: Some("backend"),
        };
        let admission = admit_by_core_secret(
            &prelude,
            Some(upgrade),
            "test-secret",
            0,
            &mut |_, _, _| unreachable!(),
        )
        .unwrap();
        SocketAttachment::new(&admission, 0, thin).unwrap()
    }

    /// Pin the bytes a Worker leaves on a trusted backend's idle socket across a deployment.
    /// This format-level check complements the real guest subscription test in workerd.
    #[test]
    fn backend_attachment_v1_bytes() {
        let mut attachment = backend_attachment(false);
        attachment.record(&SyncMessage::Join {
            version: [0; 16],
            known: Vec::new(),
        });
        let bytes = attachment.take_changed().unwrap();
        assert_eq!(&bytes[..], ATTACHMENT);
        let (mut saved, _, SavedReads::Sync(frames)) = SocketAttachment::restore(&bytes).unwrap()
        else {
            panic!("expected sync reads")
        };
        assert_eq!(frames, vec![attachment.join.clone()]);
        assert!(saved.take_changed().is_none());
        assert!(SocketAttachment::restore(&bytes[..bytes.len() - 1]).is_err());
        saved.set_partial(true);
        assert_eq!(saved.take_changed(), Some(Vec::new()));
        saved.set_partial(false);
        assert_eq!(saved.take_changed(), Some(bytes));
        assert!(attachment.take_changed().is_none());
    }
    /// Pin the thin format independently of the encoder. A restored read keeps Alice's
    /// original request id, query and options; neither its result nor an executable write is saved.
    #[test]
    fn thin_attachment_v1_bytes() {
        use jazz::thin::ThinOp;
        use jazz_server::runtime::ThinRegistration;
        let mut attachment = backend_attachment(true);
        attachment.record_thin(ThinRegistration::Schema(
            jazz::ids::SchemaVersionId::from_bytes([9; 16]),
        ));
        let op = ThinOp::Subscribe {
            query: vec![7],
            options: b"{}".to_vec(),
        };
        attachment.record_thin(ThinRegistration::Subscribe { id: 42, op: &op });
        // Replacing the same read must not consume more attachment space.
        attachment.record_thin(ThinRegistration::Subscribe { id: 42, op: &op });
        // Both kinds use the same admission section.
        let mut expected = vec![1, 1];
        expected.extend_from_slice(&ATTACHMENT[2..147]);
        expected.extend_from_slice(&[16, 0]);
        expected.extend_from_slice(&[9; 16]);
        expected.extend_from_slice(&[7, 0, 42, 1, 1, 7, 2, b'{', b'}']);
        assert_eq!(attachment.len, expected.len());
        assert_eq!(attachment.take_changed(), Some(expected.clone()));
        let (mut restored, _, _) = SocketAttachment::restore(&expected).unwrap();
        restored.record_thin(ThinRegistration::Remove(42));
        assert_eq!(restored.take_changed().unwrap(), expected[..165]);
        expected[168] = 2; // Unsubscribe must never be replayed from the attachment.
        assert!(SocketAttachment::restore(&expected).is_err());
    }

    const ATTACHMENT: &[u8] = b"\x01\x00\x8f\x00{\"bundle\":null,\"claims\":{},\"client_features\":0,\"guest\":false,\"identity\":\"[\\\"urn:jazz:system\\\",\\\"system\\\"]\",\"revocation\":null,\"trust\":\"backend\"}\x12\x00\x10\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";
}
