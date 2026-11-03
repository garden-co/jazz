//! Fresh catalogue admission is a one-shot read mode, independent of row
//! history. The existing catalogue is delivered before its correlated answer.
use super::*;
use std::future::Future;

/// `jazz.sqlite.catalogue-coverage.v1`: metadata provenance for one request.
/// It proves neither row coverage nor permission for a later operation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CatalogueCoverage {
    pub schema: [u8; 16],
    pub namespace: [u8; 16],
    pub names_digest: [u8; 16],
    pub scope: AcceptedReadScope,
}

pub(super) fn carrier() -> Query {
    Query::from("").limit(0)
}

impl Inner {
    fn catalogue_coverage(
        &self,
        schema: [u8; 16],
        scope: AcceptedReadScope,
    ) -> Result<CatalogueCoverage, Error> {
        let version = crate::ids::SchemaVersionId::from_bytes(schema);
        if self.governed(version).is_none() || !self.names_confirmed(&version) {
            return Err(Error::new(
                ErrorCode::Schema,
                "catalogue coverage schema is not admitted",
            ));
        }
        let namespace = self.namespace()?.ok_or_else(|| {
            Error::new(
                ErrorCode::NotObserved,
                "authority catalogue namespace is unavailable",
            )
        })?;
        let names = self.names.get(&version).ok_or_else(|| {
            Error::new(
                ErrorCode::NotObserved,
                "authority catalogue names are unavailable",
            )
        })?;
        Ok(CatalogueCoverage {
            schema,
            namespace,
            names_digest: crate::names::names_digest(names),
            scope,
        })
    }

    pub(super) fn check_catalogue_coverage(
        &self,
        coverage: &CatalogueCoverage,
    ) -> Result<(), Error> {
        let held = self.catalogue_coverage(coverage.schema, coverage.scope.clone())?;
        if held != *coverage {
            return Err(Error::new(
                ErrorCode::Protocol,
                "catalogue coverage does not match admitted metadata",
            ));
        }
        Ok(())
    }
}

impl Db {
    /// Capture this handle's schema, identity, claims and deadline now. Polling
    /// the returned future performs a fresh authority request, even when the
    /// names are already cached. No rows, history or authorization are cached.
    pub fn cover_catalogue_with_timeout(
        &self,
        scope: Option<AcceptedReadScope>,
        timeout_ms: u64,
    ) -> Result<impl Future<Output = Result<(), Error>> + use<>, Error> {
        let deadline = Instant::now()
            .checked_add(std::time::Duration::from_millis(timeout_ms))
            .ok_or_else(|| unsupported("catalogue timeout outside the clock range"))?;
        let (schema, owner, scope, registered) = {
            let inner = self.inner();
            let owner = inner.identity.author;
            let scope = scope.unwrap_or_else(|| AcceptedReadScope {
                author: owner,
                claims: inner.claims.get(&owner).cloned().unwrap_or_default(),
            });
            if owner != AuthorSubject::SYSTEM && scope.author != owner {
                return Err(Error::new(
                    ErrorCode::NotObserved,
                    "catalogue scope does not match the opened peer",
                ));
            }
            let registered = inner.claims.get(&scope.author).cloned();
            (
                *inner.schema.jazz.version_id().as_bytes(),
                owner,
                scope,
                registered,
            )
        };
        let check_scope = move |db: &Db, scope: &AcceptedReadScope| {
            let inner = db.inner();
            if inner.identity.author != owner
                || *inner.schema.jazz.version_id().as_bytes() != schema
                || inner.claims.get(&scope.author).cloned() != registered
            {
                return Err(Error::new(
                    ErrorCode::NotObserved,
                    "catalogue admission scope or schema changed",
                ));
            }
            Ok(())
        };
        let db = Db {
            inner: self.inner.clone(),
            view: self.view.clone(),
            governance: self.governance.clone(),
        };
        Ok(async move {
            check_scope(&db, &scope)?;
            if Instant::now() >= deadline {
                return Err(Error::new(
                    ErrorCode::NotObserved,
                    "catalogue admission deadline expired",
                ));
            }
            let coverage = if db.is_client() {
                let answer = db
                    .fetch_once(
                        &carrier(),
                        &BTreeMap::new(),
                        None,
                        false,
                        RemoteReach {
                            tier: ReadTier::Remote,
                            wait: Some(timeout_ms),
                        },
                        Some(deadline),
                        None,
                        true,
                        &ReadViewSourceSpec::Current,
                        Some(ReadSpan::CatalogueOnce {
                            schema,
                            scope: scope.clone(),
                        }),
                    )
                    .await?;
                match answer {
                    Some(OnceAnswer::Catalogue(coverage)) => coverage,
                    None => {
                        return Err(Error::new(
                            ErrorCode::NotObserved,
                            "timed out waiting for catalogue coverage",
                        ));
                    }
                    Some(_) => {
                        return Err(Error::new(
                            ErrorCode::Protocol,
                            "row coverage cannot replace catalogue admission",
                        ));
                    }
                }
            } else {
                let inner = db.inner();
                if !inner.authority {
                    return Err(Error::new(
                        ErrorCode::NotObserved,
                        "fresh catalogue coverage requires an authority",
                    ));
                }
                inner.catalogue_coverage(schema, scope.clone())?
            };
            check_scope(&db, &scope)?;
            if coverage.schema != schema || coverage.scope != scope {
                return Err(Error::new(
                    ErrorCode::Protocol,
                    "catalogue response scope or schema does not match admission",
                ));
            }
            db.inner().check_catalogue_coverage(&coverage)
        })
    }

    pub(super) fn serve_catalogue_coverage(
        &self,
        index: usize,
        id: u64,
        schema: [u8; 16],
        requested: AcceptedReadScope,
        connection_claims: Option<BTreeMap<String, Value>>,
        timeout_ms: Option<u64>,
    ) -> Result<(), Error> {
        {
            let inner = self.inner();
            let identity = inner.sessions[index].identity;
            if identity != AuthorSubject::SYSTEM {
                let authenticated = AcceptedReadScope {
                    author: identity,
                    claims: connection_claims.unwrap_or_else(|| {
                        inner.claims.get(&identity).cloned().unwrap_or_default()
                    }),
                };
                if requested != authenticated {
                    return Err(Error::new(
                        ErrorCode::NotObserved,
                        "catalogue request scope does not match authenticated session",
                    ));
                }
            }
        }
        if self.is_client() {
            return self.relay_scoped_read(
                index,
                SyncMessage::Read {
                    id,
                    query: Box::new(carrier()),
                    binding: BTreeMap::new(),
                    include_deleted: false,
                    tier: ReadTier::Remote,
                    wait: timeout_ms,
                    view: Box::new(ReadViewSourceSpec::Current),
                    since: None,
                    span: ReadSpan::CatalogueOnce {
                        schema,
                        scope: requested,
                    },
                },
            );
        }
        let mut inner = self.inner();
        if !inner.authority {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "fresh catalogue coverage requires an authority",
            ));
        }
        let coverage = inner.catalogue_coverage(schema, requested)?;
        // Existing catalogue_for publishes changed names before the outbox.
        inner.sessions[index].catalogue_owed = true;
        inner.sessions[index]
            .outbox
            .push(SyncMessage::CatalogueCovered { id, coverage });
        Ok(())
    }

    /// The existing relay lifetime for scoped one-shot modes. It has one
    /// answer slot and uses the same cancellation/deadline registry as rows.
    pub(super) fn relay_scoped_read(
        &self,
        index: usize,
        mut message: SyncMessage,
    ) -> Result<(), Error> {
        let SyncMessage::Read { wait, .. } = &mut message else {
            unreachable!("read carrier")
        };
        let timeout_ms = wait.unwrap_or(15_000).min(15_000);
        *wait = Some(timeout_ms);
        let until = Instant::now() + std::time::Duration::from_millis(timeout_ms);
        let deadline_guard = self.read_deadline(Some(until));
        let mut inner = self.inner();
        let client = inner.client.as_mut().expect("client");
        let upstream = client.next_id;
        client.next_id += 1;
        client.answers.insert(upstream, None);
        let mut ask = message.clone();
        let SyncMessage::Read {
            id,
            query,
            binding,
            include_deleted,
            view,
            ..
        } = &mut ask
        else {
            unreachable!("read carrier")
        };
        *id = upstream;
        inner.register_once_read(upstream, query, binding, *include_deleted, view, false);
        let client = inner.client.as_mut().expect("client");
        client.outbox.push(ask.clone());
        client.relayed_fetches.push(RelayedFetch {
            upstream,
            ask,
            strict: true,
            until: Some(until),
            _deadline_guard: deadline_guard,
            session: index,
            message,
        });
        drop(inner);
        self.schedule_pending_tick();
        Ok(())
    }
}
