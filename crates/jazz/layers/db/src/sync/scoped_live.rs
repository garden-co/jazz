//! Trusted scoped UNION subscriptions reuse the bounded one-shot evidence path.
use super::*;

pub(super) struct Update {
    pub index: usize,
    pub id: u64,
    pub prepared: PreparedQuery,
    pub identity: AuthorSubject,
    pub through: u64,
    pub include_deleted: bool,
    pub roots: Vec<[u8; 16]>,
    pub evidence: ScopedEvidence,
}

impl Db {
    pub(super) fn send_scoped_union_updates(&self, updates: Vec<Update>) -> Result<(), Error> {
        let mut touched = BTreeSet::new();
        for update in updates {
            let Update {
                index,
                id,
                prepared,
                identity,
                through,
                include_deleted,
                roots,
                evidence,
            } = update;
            {
                let inner = self.inner();
                if !inner.sessions[index].open
                    || !inner.sessions[index].scoped_unions.contains_key(&id)
                {
                    continue;
                }
            }
            let db = self.session_db(index);
            match db.serve_fetch(
                index,
                id,
                &prepared.query,
                &prepared,
                Some(identity),
                Some(through),
                include_deleted,
                false,
                Some(&roots),
                Some(evidence),
                &ReadViewSourceSpec::Current,
            ) {
                Ok(()) => {
                    let mut inner = self.inner();
                    inner.sessions[index].outbox.push(SyncMessage::Settled {
                        id,
                        through,
                        window: None,
                        global: true,
                    });
                    if let Some((.., opened)) = inner.sessions[index].subs.get_mut(&id) {
                        *opened = true;
                    }
                }
                Err(error) => self.inner().refuse_subscription(index, id, error)?,
            }
            touched.insert(index);
        }
        let mut inner = self.inner();
        let dropped = inner.fan.dropped.clone();
        for index in touched {
            if !inner.sessions[index].open {
                continue;
            }
            let names = inner
                .session_schema(index)
                .unwrap_or_else(|| inner.owner.clone());
            let out = std::mem::take(&mut inner.sessions[index].outbox);
            for message in out {
                let Some(message) = message_out(&names, message) else {
                    continue;
                };
                if inner.sessions[index].send_output(message).is_err() {
                    let released = inner.sessions[index].close(&dropped);
                    inner.release_session_reads(released);
                    break;
                }
            }
        }
        Ok(())
    }
}
