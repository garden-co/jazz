//! Privileged local-host companion channel. These bytes are NOT peer metadata.
//! The embedding owns both realms and keeps this channel outside network frame
//! dispatch. A port attests only original local submission, never authority.
use super::*;
use jazz_types::ids::NodeUuid;
use std::cell::Cell;

const MAGIC: &[u8; 8] = b"JZLCAP01";
const DESCRIPTOR: usize = 88; // magic, namespace, node, author digest, incarnation
const MAX_COMPANION: usize = 4 * 1024 * 1024;
const MAX_QUEUED: usize = 64;

/// Producer capability held privately by the trusted local embedding. Its
/// descriptor is meaningful only over that embedding's private local channel.
pub struct LocalSubmissionSource {
    owner: Weak<Store>,
    epoch: u64,
    descriptor: [u8; DESCRIPTOR],
    next: Cell<u64>,
}

/// Receiver capability bound to one actual owner and admitted slot/generation.
/// This has no Deserialize implementation. The host must retire it before
/// replacing either realm or its private channel.
#[derive(Clone)]
pub struct LocalSubmissionPort(Rc<Port>);

struct Port {
    owner: Weak<Store>,
    epoch: u64,
    subscriber: SubscriberId,
    descriptor: [u8; DESCRIPTOR],
    active: Cell<bool>,
    last: Cell<u64>,
}

#[derive(Clone)]
pub(crate) struct HostSubmission {
    port: LocalSubmissionPort,
    id: TxId,
    fingerprint: [u8; 32],
    operations: Vec<Option<SubmissionProvenance>>,
}

fn author_digest(author: AuthorSubject) -> [u8; 32] {
    jazz_types::sha256::derive_key(
        "jazz.local-submission-author.v1",
        author.canonical().as_bytes(),
    )
}

impl LocalSubmissionSource {
    pub fn descriptor(&self) -> &[u8] {
        &self.descriptor
    }

    /// Capture from an actual pending Commit, never caller-supplied schema or
    /// claims. The normal wire Commit remains independently admitted/applied.
    pub fn export(&self, receipt: TxReceipt) -> Result<Option<Vec<u8>>, Error> {
        let Some(owner) = self.owner.upgrade() else {
            return Ok(None);
        };
        if owner.1.get() != self.epoch || self.epoch == u64::MAX {
            return Ok(None);
        }
        let inner = owner.borrow();
        if inner.namespace()?.as_ref().map(|v| v.as_slice()) != Some(&self.descriptor[8..24])
            || inner.identity.node.as_bytes() != &self.descriptor[24..40]
            || author_digest(inner.identity.author) != self.descriptor[40..72]
        {
            return Ok(None);
        }
        let header: Option<Vec<u8>> = inner
            .conn
            .cached_row(
                "SELECT header FROM _jazz_pending WHERE tx=?1",
                [receipt.0 as i64],
                |r| r.get(0),
            )
            .optional()
            .map_err(storage)?;
        let Some(header) = header else {
            return Ok(None);
        };
        let message = inner.pending_commit(receipt.0, &header)?;
        let SyncMessage::Commit { id, ref ops, .. } = message else {
            return Ok(None);
        };
        if id.node.as_bytes() != &self.descriptor[24..40] {
            return Ok(None);
        }
        let operations = inner.submission_records(receipt.0, ops.len())?;
        if !operations.iter().any(Option::is_some) {
            return Ok(None);
        }
        let Some(next) = self.next.get().checked_add(1) else {
            return Ok(None);
        };
        if operations.len() > (MAX_COMPANION - DESCRIPTOR - 52) / 49 {
            return Ok(None);
        }
        let mut bytes = self.descriptor.to_vec();
        bytes.extend_from_slice(&next.to_le_bytes());
        bytes.extend_from_slice(&id.time.0.to_le_bytes());
        bytes.extend_from_slice(&commit_fingerprint(&message));
        bytes.extend_from_slice(&(operations.len() as u32).to_le_bytes());
        for operation in operations {
            match operation {
                None => bytes.push(0),
                Some(value) => {
                    bytes.push(1);
                    bytes.extend_from_slice(&value.schema);
                    bytes.extend_from_slice(&value.scope);
                }
            }
        }
        self.next.set(next);
        Ok(Some(bytes))
    }
}

impl LocalSubmissionPort {
    /// Revocation is synchronous; already-persisted accepted evidence remains
    /// subject to its normal observation cut and current-policy validation.
    pub fn retire(&self) {
        self.0.active.set(false);
    }

    fn live(&self, owner: &Rc<Store>) -> Result<bool, Error> {
        let Some(actual) = self.0.owner.upgrade() else {
            return Ok(false);
        };
        if !Rc::ptr_eq(owner, &actual)
            || !self.0.active.get()
            || actual.1.get() != self.0.epoch
            || self.0.epoch == u64::MAX
        {
            return Ok(false);
        }
        let inner = actual.borrow();
        Ok(
            inner.namespace()?.as_ref().map(|v| v.as_slice()) == Some(&self.0.descriptor[8..24])
                && inner
                    .sessions
                    .get(self.0.subscriber.slot)
                    .is_some_and(|session| {
                        session.open
                            && session.generation == self.0.subscriber.generation
                            && author_digest(session.identity) == self.0.descriptor[40..72]
                    }),
        )
    }
}

impl Db {
    /// Called by the privileged local embedding on the actual foreground Db.
    /// Merely possessing a subscriber id or remote frame cannot mint this.
    pub fn local_submission_source(&self) -> Result<LocalSubmissionSource, Error> {
        if self.inner.1.get() == u64::MAX {
            return Err(unobserved("local owner retired"));
        }
        let inner = self.inner.borrow();
        let namespace = inner
            .namespace()?
            .ok_or_else(|| unobserved("no authority namespace"))?;
        let mut descriptor = [0; DESCRIPTOR];
        descriptor[..8].copy_from_slice(MAGIC);
        descriptor[8..24].copy_from_slice(&namespace);
        descriptor[24..40].copy_from_slice(inner.identity.node.as_bytes());
        descriptor[40..72].copy_from_slice(&author_digest(inner.identity.author));
        descriptor[72..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        Ok(LocalSubmissionSource {
            owner: Rc::downgrade(&self.inner),
            epoch: self.inner.1.get(),
            descriptor,
            next: Cell::new(0),
        })
    }

    /// PRIVILEGED LOCAL HOST ENTRYPOINT, not a network decoder. The embedding
    /// must obtain `descriptor` from the actual producer capability over its
    /// private, non-forwardable local bootstrap channel, after normal admission.
    /// No claims, bundle, trust or permission is inferred from these bytes.
    pub fn trusted_local_submission_port(
        &self,
        subscriber: SubscriberId,
        descriptor: &[u8],
    ) -> Result<LocalSubmissionPort, Error> {
        let descriptor: [u8; DESCRIPTOR] = descriptor
            .try_into()
            .map_err(|_| unobserved("invalid local descriptor"))?;
        if &descriptor[..8] != MAGIC {
            return Err(unobserved("unsupported local descriptor"));
        }
        let port = LocalSubmissionPort(Rc::new(Port {
            owner: Rc::downgrade(&self.inner),
            epoch: self.inner.1.get(),
            subscriber,
            descriptor,
            active: Cell::new(true),
            last: Cell::new(0),
        }));
        if !port.live(&self.inner)? {
            return Err(unobserved(
                "local descriptor does not match admitted owner/session",
            ));
        }
        Ok(port)
    }

    /// Optional companion ingress on an already-established private port.
    /// Malformed/stale/missing input never changes ordinary Commit admission.
    /// Storage errors remain storage errors. Bounds prevent a peer-sized cache.
    pub fn attach_host_local_submission(
        &self,
        port: &LocalSubmissionPort,
        bytes: &[u8],
    ) -> Result<bool, Error> {
        if !port.live(&self.inner)?
            || bytes.len() > MAX_COMPANION
            || bytes.len() < DESCRIPTOR + 52
            || bytes[..DESCRIPTOR] != port.0.descriptor
        {
            return Ok(false);
        }
        let mut rest = &bytes[DESCRIPTOR..];
        fn take<const N: usize>(rest: &mut &[u8]) -> Option<[u8; N]> {
            let value = rest.get(..N)?.try_into().ok()?;
            *rest = &rest[N..];
            Some(value)
        }
        let serial = u64::from_le_bytes(take::<8>(&mut rest).unwrap());
        if serial <= port.0.last.get() {
            return Ok(false);
        }
        let time = u64::from_le_bytes(take::<8>(&mut rest).unwrap());
        let fingerprint = take::<32>(&mut rest).unwrap();
        let count = u32::from_le_bytes(take::<4>(&mut rest).unwrap()) as usize;
        if count > (MAX_COMPANION - DESCRIPTOR - 52) / 49 || count > rest.len() {
            return Ok(false);
        }
        let mut operations = Vec::with_capacity(count);
        for _ in 0..count {
            let Some([tag]) = take::<1>(&mut rest) else {
                return Ok(false);
            };
            operations.push(match tag {
                0 => None,
                1 => {
                    let (Some(schema), Some(scope)) =
                        (take::<16>(&mut rest), take::<32>(&mut rest))
                    else {
                        return Ok(false);
                    };
                    Some(SubmissionProvenance { schema, scope })
                }
                _ => return Ok(false),
            });
        }
        if !rest.is_empty() || !operations.iter().any(Option::is_some) {
            return Ok(false);
        }
        let mut inner = self.inner.borrow_mut();
        let queue = &mut inner.sessions[port.0.subscriber.slot].host_submissions;
        if queue.len() >= MAX_QUEUED {
            port.retire();
            queue.clear();
            return Ok(false);
        }
        queue.push_back(HostSubmission {
            port: port.clone(),
            id: TxId::new(
                TxTime(time),
                NodeUuid::from_bytes(port.0.descriptor[24..40].try_into().unwrap()),
            ),
            fingerprint,
            operations,
        });
        port.0.last.set(serial);
        Ok(true)
    }

    /// The same handle, identity and fixed-claim choice used by normal serving.
    /// This is crate-private: a host cannot select a bundle or forge a principal.
    pub(crate) fn admitted_observation_context(
        &self,
        subscriber: SubscriberId,
    ) -> Result<
        (
            Db,
            AuthorSubject,
            BTreeMap<String, Value>,
            Option<AuthorSubject>,
        ),
        Error,
    > {
        self.ensure_storage_owner_active()?;
        let (identity, claims, policy) = {
            let inner = self.inner.borrow();
            let session = inner
                .sessions
                .get(subscriber.slot)
                .filter(|session| session.open && session.generation == subscriber.generation)
                .ok_or_else(|| unobserved("observation session was retired or replaced"))?;
            let fixed = session
                .claims
                .clone()
                .filter(|_| session.identity != AuthorSubject::SYSTEM);
            // Normal serving treats SYSTEM as the store's local read, with
            // the actual owner's operands rather than a SYSTEM registry entry.
            let identity = if session.identity == AuthorSubject::SYSTEM {
                inner.identity.author
            } else {
                session.identity
            };
            (
                identity,
                fixed
                    .or_else(|| inner.claims.get(&identity).cloned())
                    .unwrap_or_default(),
                inner.session_policy(subscriber.slot),
            )
        };
        Ok((self.session_db(subscriber.slot), identity, claims, policy))
    }

    pub(super) fn host_local_submission_for(
        &self,
        index: usize,
        message: &SyncMessage,
    ) -> Result<Option<Vec<Option<SubmissionProvenance>>>, Error> {
        let SyncMessage::Commit { id, ops, .. } = message else {
            return Ok(None);
        };
        let candidate = {
            let mut inner = self.inner.borrow_mut();
            let queue = &mut inner.sessions[index].host_submissions;
            queue
                .iter()
                .position(|value| value.id == *id)
                .and_then(|position| queue.remove(position))
        };
        let Some(value) = candidate else {
            return Ok(None);
        };
        if !value.port.live(&self.inner)?
            || value.operations.len() != ops.len()
            || value.fingerprint != commit_fingerprint(message)
        {
            return Ok(None);
        }
        Ok(Some(value.operations))
    }
}
