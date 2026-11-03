//! Accepted evidence is a one-shot read mode. It shares request IDs, answer
//! slots, cancellation, relay lifetimes and deadlines with ordinary reads.
use super::*;

impl Db {
    /// Ask the authority for paired row origins at one admitted schema/cut.
    /// An ordinary authenticated connection determines its own effective
    /// identity and claims; only a trusted SYSTEM connection may delegate.
    /// The returned scope records that evaluation, not permission for a write.
    pub async fn accepted_query(
        &self,
        prepared: &PreparedQuery,
        at: Option<u64>,
        include_deleted: bool,
        scope: Option<AcceptedReadScope>,
    ) -> Result<EncodedAcceptedRead, Error> {
        self.accepted_query_with_timeout(prepared, at, include_deleted, scope, 15_000)
            .await
    }

    /// The same read with a bounded wait, in milliseconds. Cancellation drops
    /// the ordinary pending-fetch guard and unsubscribes any relayed request.
    pub async fn accepted_query_with_timeout(
        &self,
        prepared: &PreparedQuery,
        at: Option<u64>,
        include_deleted: bool,
        scope: Option<AcceptedReadScope>,
        timeout_ms: u64,
    ) -> Result<EncodedAcceptedRead, Error> {
        let (schema, local_scope) = {
            let inner = self.inner();
            let schema = prepared.schema.as_ref().unwrap_or(&inner.owner);
            crate::db::accepted_read::check_accepted_shape(prepared, schema)?;
            (
                *schema.jazz.version_id().as_bytes(),
                AcceptedReadScope {
                    author: inner.identity.author,
                    claims: inner
                        .claims
                        .get(&inner.identity.author)
                        .cloned()
                        .unwrap_or_default(),
                },
            )
        };
        // Freeze any explicitly prepared identity/claims before the first wait.
        let scope = scope.or_else(|| {
            prepared
                .request_claims
                .as_ref()
                .map(|(author, claims)| AcceptedReadScope {
                    author: *author,
                    claims: claims.clone(),
                })
        });
        if !self.is_client() {
            let scope = scope.unwrap_or(local_scope);
            let query = prepared
                .clone()
                .with_identity_claims(scope.author, scope.claims.clone());
            return self
                .accepted_read(
                    &query,
                    at,
                    include_deleted,
                    (scope.author != AuthorSubject::SYSTEM).then_some(scope.author),
                )?
                .encode(schema, scope);
        }
        let deadline = Instant::now()
            .checked_add(std::time::Duration::from_millis(timeout_ms))
            .ok_or_else(|| unsupported("accepted-read timeout outside the clock range"))?;
        let answer = self
            .fetch_once(
                &prepared.query,
                &prepared.binding,
                at.map(Pos::at),
                include_deleted,
                RemoteReach {
                    tier: ReadTier::Remote,
                    wait: Some(timeout_ms),
                },
                Some(deadline),
                None,
                true,
                &ReadViewSourceSpec::Current,
                Some(ReadSpan::AcceptedOnce { at, schema, scope }),
            )
            .await?;
        match answer {
            Some(OnceAnswer::Evidence(read))
                if read.schema == schema && at.is_none_or(|at| at == read.through) =>
            {
                read.validate()?;
                Ok(read)
            }
            None => Err(Error::new(
                ErrorCode::NotObserved,
                "timed out waiting for authority accepted evidence",
            )),
            Some(_) => Err(Error::new(
                ErrorCode::Protocol,
                "authority evidence does not match its requested schema and cut",
            )),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn serve_accepted(
        &self,
        index: usize,
        id: u64,
        query: Query,
        binding: BTreeMap<String, Value>,
        include_deleted: bool,
        at: Option<u64>,
        schema: [u8; 16],
        requested: Option<AcceptedReadScope>,
        connection_claims: Option<BTreeMap<String, Value>>,
        timeout_ms: Option<u64>,
    ) -> Result<(), Error> {
        let scope = {
            let inner = self.inner();
            let session = &inner.sessions[index];
            // The upstream link cannot authenticate this downstream pin. Do
            // not turn a pinned request into an unpinned delegated read.
            if inner.client.is_some() && session.bundle.is_some() {
                return Err(unsupported("accepted reads through a pinned relay session"));
            }
            let authenticated = || AcceptedReadScope {
                author: session.identity,
                claims: connection_claims.unwrap_or_else(|| {
                    inner
                        .claims
                        .get(&session.identity)
                        .cloned()
                        .unwrap_or_default()
                }),
            };
            if session.identity == AuthorSubject::SYSTEM {
                requested.unwrap_or_else(authenticated)
            } else {
                authenticated()
            }
        };
        if self.is_client() {
            return self.relay_scoped_read(
                index,
                SyncMessage::Read {
                    id,
                    query: Box::new(query),
                    binding,
                    include_deleted,
                    tier: ReadTier::Remote,
                    wait: timeout_ms,
                    view: Box::new(ReadViewSourceSpec::Current),
                    since: None,
                    span: ReadSpan::AcceptedOnce {
                        at,
                        schema,
                        scope: Some(scope),
                    },
                },
            );
        }
        let (view, governance) = {
            let inner = self.inner();
            let view = inner
                .governed_by(
                    crate::ids::SchemaVersionId::from_bytes(schema),
                    inner.session_policies(index),
                )
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::Schema,
                        "accepted-read schema is not admitted by the authority",
                    )
                })?;
            (view, inner.session_governance(index))
        };
        let db = Db {
            inner: self.inner.clone(),
            view: Some(view),
            governance,
        };
        let prepared = db
            .prepare_query_bound(&query, binding)?
            .with_identity_claims(scope.author, scope.claims.clone());
        let read = db
            .accepted_read(
                &prepared,
                at,
                include_deleted,
                (scope.author != AuthorSubject::SYSTEM).then_some(scope.author),
            )?
            .encode(schema, scope)?;
        self.inner().sessions[index]
            .outbox
            .push(SyncMessage::Accepted { id, read });
        Ok(())
    }
}
