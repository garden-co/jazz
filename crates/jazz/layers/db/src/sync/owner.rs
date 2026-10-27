//! A scope-isolated owner's private pending projection. Generic relays keep
//! their journal separate from shared cache rows.
use super::*;

// These additive encodings do not change the existing local-commit-v1 layout.
// jazz.sqlite.owner-position.v1: stable aliases for original transaction IDs.
// jazz.sqlite.owner-scope.v1: the one admitted subject and current schema.
// jazz.sqlite.owner-input-row.v1: exact pending/terminal transaction footprints.
pub(super) const DDL: &str = "
CREATE TABLE IF NOT EXISTS _jazz_owner_positions_v1 (
 seq INTEGER PRIMARY KEY CHECK(seq>=4503599627370496),
 node BLOB NOT NULL CHECK(length(node)=16), tx INTEGER NOT NULL CHECK(tx>0),
 UNIQUE(node,tx));
CREATE TABLE IF NOT EXISTS _jazz_owner_scope_v1 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), subject TEXT NOT NULL,
 schema_id BLOB NOT NULL CHECK(length(schema_id)=16));
CREATE TABLE IF NOT EXISTS _jazz_owner_input_rows_v1 (
 t TEXT NOT NULL, id BLOB NOT NULL CHECK(length(id)=16), k BLOB NOT NULL,
 node BLOB NOT NULL CHECK(length(node)=16), tx INTEGER NOT NULL CHECK(tx>0),
 pending INTEGER NOT NULL CHECK(pending IN(0,1)),
 writes INTEGER NOT NULL CHECK(writes IN(0,1)),
 PRIMARY KEY(t,id,k,node,tx)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS _jazz_owner_input_pending_v1
 ON _jazz_owner_input_rows_v1(t,id,k,node,tx) WHERE pending=1 AND writes=1;
CREATE INDEX IF NOT EXISTS _jazz_owner_input_by_tx_v1
 ON _jazz_owner_input_rows_v1(node,tx);";

// jazz.sqlite.owner-scope.v1: one canonical admitted subject, bound before any
// pending projection is exposed. Reopening under another subject is refused.
pub(crate) fn stored_subject(conn: &Connection) -> Result<Option<AuthorSubject>, Error> {
    let subject: Option<String> = conn
        .query_row(
            "SELECT subject FROM _jazz_owner_scope_v1 WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)?;
    subject
        .map(|subject| {
            AuthorSubject::from_canonical(&subject)
                .map_err(|error| Error::new(ErrorCode::Storage, error.to_string()))
        })
        .transpose()
}

/// jazz.sqlite.private-owner-handoff.v1 changes only the admitted subject in
/// owner-scope.v1. The host holds exclusive ownership and authenticates the
/// stable account scope; this function supplies the durable no-pending fence.
pub(crate) fn handoff_account_owner(
    conn: &Connection,
    previous: AuthorSubject,
    next: AuthorSubject,
) -> Result<(), Error> {
    if !matches!(previous, AuthorSubject::Authenticated(_))
        || !matches!(next, AuthorSubject::Authenticated(_))
        || previous.account_id().is_none()
        || previous.account_id() != next.account_id()
    {
        return Err(Error::new(
            ErrorCode::Storage,
            "private owner handoff requires linked authenticated account identities",
        ));
    }
    let tx = conn.unchecked_transaction().map_err(storage)?;
    // Every probe uses an existing primary key or partial pending index. The
    // check includes owner publications and initialization recovery evidence;
    // it never reattributes, discards or scans settled application history.
    let pending: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM _jazz_pending LIMIT 1) OR \
         EXISTS(SELECT 1 FROM _jazz_relay_upload_v1 WHERE pending=1 LIMIT 1) OR \
         EXISTS(SELECT 1 FROM _jazz_shadow LIMIT 1) OR \
         EXISTS(SELECT 1 FROM _jazz_owner_input_rows_v1 WHERE pending=1 AND writes=1 LIMIT 1) OR \
         EXISTS(SELECT 1 FROM _jazz_local_commits_v1 WHERE initialization=1 AND pending=1 LIMIT 1) OR \
         EXISTS(SELECT 1 FROM _jazz_owner_initialization_v1 WHERE state=1 LIMIT 1)",
        [], |row| row.get(0),
    ).map_err(storage)?;
    if pending {
        return Err(unsupported(
            "switching a private owner's login while original uploads are pending",
        ));
    }
    let changed = tx
        .execute(
            "UPDATE _jazz_owner_scope_v1 SET subject=?1 WHERE singleton=1 AND subject=?2",
            [next.canonical(), previous.canonical()],
        )
        .map_err(storage)?;
    if changed != 1 {
        return Err(Error::new(
            ErrorCode::Storage,
            "private owner changed during handoff",
        ));
    }
    tx.commit().map_err(storage)
}

/// A reopen must refuse an incompatible schema before Names or physical DDL
/// can mutate the file. The scope row exists only after the journals exist.
pub(crate) fn check_open_schema(
    conn: &Connection,
    requested: crate::ids::SchemaVersionId,
) -> Result<(), Error> {
    let recorded: Vec<u8> = conn
        .query_row(
            "SELECT schema_id FROM _jazz_owner_scope_v1 WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .map_err(storage)?;
    if recorded.as_slice() == requested.as_bytes() {
        return Ok(());
    }
    let pending: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM _jazz_pending LIMIT 1) OR EXISTS(SELECT 1 FROM _jazz_relay_upload_v1 WHERE pending=1 LIMIT 1)",
        [], |row| row.get(0),
    ).map_err(storage)?;
    if pending {
        return Err(unsupported(
            "reopening a scope-isolated storage owner under a different schema while writes are pending",
        ));
    }
    Ok(())
}

impl Inner {
    /// Only the admitted owner's identity can recover pending previews before
    /// authority policies arrive. A structural host cache carries no policies.
    /// A foreground must have acknowledged that
    /// same subject and schema with its private owner. Accepted images never
    /// qualify for the compiler's pending-input predicate.
    pub(in crate::db) fn owner_previews(&self, subject: Option<AuthorSubject>) -> bool {
        subject == Some(self.identity.author)
            && (self.scope_isolated_owner
                || (self.non_durable
                    && self.client.as_ref().is_some_and(|client| {
                        client.owner_projection_bound
                            && client.owner_node.is_some()
                            && client.owner_ready == Some(self.owner.jazz.version_id())
                            && client.upstream_schema == client.owner_ready
                    })))
    }

    pub(super) fn owner_pending_preview(
        &self,
        table: &str,
        row: RowUuid,
        key: &[u8],
        subject: Option<AuthorSubject>,
    ) -> Result<bool, Error> {
        if !self.owner_previews(subject)
            || self
                .conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM _jazz_policy_descriptor_v1 WHERE slot=1)",
                    [],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(storage)?
        {
            return Ok(false);
        }
        Ok(self
            .existing(table, row, key)?
            .is_some_and(|(seq, _)| (LOCAL_BASE..FOREGROUND_BASE).contains(&seq))
            && !self
                .inputs_at(&(table.to_owned(), row, key.to_vec()))?
                .is_empty())
    }

    /// Refresh registered views after the metadata transaction commits. A later
    /// invalid message in that transaction must not install an uncommitted grant.
    pub(super) fn refresh_authenticated_views(&mut self) -> Result<(), Error> {
        if !self.view_policies_dirty {
            return Ok(());
        }
        // Private owners also serve retained schemas. Adopt their grants only
        // after the complete metadata batch has committed.
        if self.scope_isolated_owner {
            self.restore_owner_policies()?;
        }
        // Registered authority views can also belong to ordinary foregrounds.
        // Refresh their governor once per metadata change, not once per query.
        if let Some(previous) = &self.view_policy_source {
            let schema = self.policy_descriptor()?.map_or_else(
                || previous.jazz.without_permissions(),
                |descriptor| descriptor.schema.schema,
            );
            let source = self.named_policy_schema(&schema)?;
            if source.jazz != previous.jazz || source.names().tables != previous.names().tables {
                self.view_policy_source = Some(Rc::new(source));
                self.reset_everything();
            }
        }
        self.view_policies_dirty = false;
        Ok(())
    }

    /// A client schema may omit policies. This private serving owner uses only
    /// the active descriptor admitted from its authority,
    /// including after an offline reopen; the caller's schema cannot replace it.
    pub(super) fn restore_owner_policies(&mut self) -> Result<(), Error> {
        self.view_policies_dirty |= self.scope_isolated_owner || self.view_policy_source.is_some();
        // Revocation can happen before commit; a new grant cannot. In
        // particular, owner subscription resets must not reuse a stale view
        // governor while the new head is missing its exact policy descriptor.
        if let Some(previous) = &self.view_policy_source
            && self.policy_descriptor()?.is_none()
        {
            let denied = previous.jazz.without_permissions();
            if denied != previous.jazz {
                self.view_policy_source = Some(Rc::new(self.named_policy_schema(&denied)?));
                self.reset_everything();
            }
        }
        if !self.scope_isolated_owner {
            return Ok(());
        }
        if self.catalogue_head()?.is_none() {
            return Ok(());
        }
        // Keep the owner's physical/opened schema. Resolve current authority
        // policy names through accepted table/column identities, just as an
        // authenticated retained view does. A split head/descriptor revokes
        // the preceding governor before a replacement can grant access.
        let descriptor = self.policy_descriptor()?;
        if descriptor.is_some() && !self.conn.is_autocommit() {
            // A later frame can invalidate this batch. Keep any head-triggered
            // revocation, but publish no grant or subscription reset until
            // refresh_authenticated_views runs after RELEASE sync_tick.
            return Ok(());
        }
        let policies = descriptor.map_or_else(
            || self.owner.jazz.without_permissions(),
            |descriptor| descriptor.schema.schema,
        );
        let governor = self.named_policy_schema(&policies)?;
        if self.owner.policies_from.as_ref().is_some_and(|previous| {
            previous.jazz == governor.jazz && previous.names().tables == governor.names().tables
        }) {
            return Ok(());
        }
        let governor = Rc::new(governor);
        self.view_policy_source = Some(governor.clone());
        self.owner = Rc::new(self.named(&self.owner.jazz).with_policies_of(governor));
        self.schema = self.owner.clone();
        self.reset_everything();
        for session in &mut self.sessions {
            session.revalidate();
        }
        Ok(())
    }

    pub(in crate::db) fn require_foreground_writes(&self, action: &str) -> Result<(), Error> {
        self.require_owner_subject(self.identity.author)?;
        if self.private_owner_required
            && !self.client.as_ref().is_some_and(|client| {
                client.transport.is_some()
                    && client.owner_ready == Some(self.owner.jazz.version_id())
                    && client.upstream_schema == client.owner_ready
            })
        {
            return Err(unsupported(format!(
                "{action} from a non-durable foreground before its current storage owner acknowledges the subject and schema"
            )));
        }
        Ok(())
    }

    pub(super) fn receive_owner_ready(
        &mut self,
        schema: [u8; 16],
        subject: String,
        node: [u8; 16],
    ) -> Result<(), Error> {
        let version = crate::ids::SchemaVersionId::from_bytes(schema);
        if !self.private_owner_required
            || !matches!(self.identity.author, AuthorSubject::Authenticated(_))
            || subject != self.identity.author.canonical()
            || version != self.owner.jazz.version_id()
            || node == *self.identity.node.as_bytes()
        {
            return Err(input_error(
                "private owner acknowledgement has the wrong role, subject, schema or node",
            ));
        }
        let client = self.client.as_mut().expect("foreground");
        if client.upstream_schema != Some(version)
            || client.owner_node.is_some_and(|previous| previous != node)
        {
            return Err(input_error(
                "private owner acknowledgement changed the admitted owner or schema",
            ));
        }
        client.owner_node = Some(node);
        client.owner_ready = Some(version);
        client.owner_projection_bound = true;
        Ok(())
    }

    pub(in crate::db) fn require_owner_schema_change(
        &self,
        version: crate::ids::SchemaVersionId,
    ) -> Result<(), Error> {
        if !self.scope_isolated_owner {
            return Ok(());
        }
        self.require_owner_subject(self.identity.author)?;
        if version != self.owner.jazz.version_id()
            && (self.tx.is_some()
                || !self.batches.is_empty()
                || self.client.as_ref().is_some_and(|client| {
                    !client.pending.is_empty() || !client.relay_uploads.is_empty()
                }))
        {
            return Err(unsupported(
                "changing a scope-isolated storage owner's schema while writes or transactions are pending",
            ));
        }
        Ok(())
    }

    pub(super) fn record_owner_input_rows(
        &self,
        id: UploadId,
        rows: &[RowKey],
        ops: &[StagedOp],
    ) -> Result<(), Error> {
        if !self.scope_isolated_owner && !self.private_owner_required {
            return Ok(());
        }
        let writes = self
            .owner_coordinates(ops, false)?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut statement = self.conn.prepare_cached(
            "INSERT INTO _jazz_owner_input_rows_v1(t,id,k,node,tx,pending,writes) VALUES(?1,?2,?3,?4,?5,1,?6) ON CONFLICT(t,id,k,node,tx) DO NOTHING",
        ).map_err(storage)?;
        for (table, row, key) in rows {
            statement
                .execute(rusqlite::params![
                    table,
                    row.as_bytes().as_slice(),
                    key,
                    id.0.as_slice(),
                    id.1 as i64,
                    writes.contains(&(table.clone(), *row, key.clone()))
                ])
                .map_err(storage)?;
        }
        Ok(())
    }
    pub(super) fn finish_owner_input_rows(&self, id: UploadId) -> Result<(), Error> {
        self.conn
            .execute(
                "UPDATE _jazz_owner_input_rows_v1 SET pending=0 WHERE node=?1 AND tx=?2",
                rusqlite::params![id.0.as_slice(), id.1 as i64],
            )
            .map_err(storage)?;
        Ok(())
    }
    pub(in crate::db) fn require_owner_subject(&self, subject: AuthorSubject) -> Result<(), Error> {
        if let Some(error) = &self.owner_projection_error {
            return Err(error.clone());
        }
        let private = self.scope_isolated_owner
            || self.private_owner_required
            || (self.non_durable
                && self
                    .client
                    .as_ref()
                    .is_some_and(|client| client.owner_projection_bound));
        if private && subject != self.identity.author {
            return Err(Error::new(
                ErrorCode::WriteRejected,
                "scope-isolated storage owner cannot serve another subject",
            ));
        }
        Ok(())
    }

    fn owner_position(&mut self, id: UploadId) -> Result<u64, Error> {
        let origin = TxId::new(TxTime(id.1), NodeUuid::from_bytes(id.0));
        if let Some(position) = self.local_position(origin)? {
            return Ok(position);
        }
        let position = self
            .seq
            .max(LOCAL_BASE)
            .checked_add(1)
            .filter(|n| *n < i64::MAX as u64)
            .ok_or_else(|| Error::new(ErrorCode::Storage, "owner publication range exhausted"))?;
        self.conn
            .execute(
                "INSERT INTO _jazz_owner_positions_v1(seq,node,tx) VALUES(?1,?2,?3)",
                rusqlite::params![position as i64, id.0.as_slice(), id.1 as i64],
            )
            .map_err(storage)?;
        self.record_issued_seq(position)?;
        self.seq = position;
        Ok(position)
    }

    /// Projection coordinates include new rows and branch heads before replay.
    /// A receipt's cached-row lookup alone misses these, leaving them unshadowed
    /// and impossible to remove on rejection without an unrelated eviction.
    pub(super) fn owner_input_coordinates(&self, ops: &[StagedOp]) -> Result<Vec<RowKey>, Error> {
        self.owner_coordinates(ops, true)
    }

    fn owner_coordinates(
        &self,
        ops: &[StagedOp],
        dependencies: bool,
    ) -> Result<Vec<RowKey>, Error> {
        let mut rows: BTreeSet<RowKey> = BTreeSet::new();
        for op in ops {
            let row = op.row().ok_or_else(|| {
                Error::new(
                    ErrorCode::Protocol,
                    "owner input has no original row identity",
                )
            })?;
            let table = self.owner.table(op.table())?;
            let mut inserted = None;
            let target = match op {
                StagedOp::Insert(_, cells, options) => {
                    inserted = Some(cells.clone());
                    Target::exact(&options.target)
                }
                StagedOp::Update(_, _, _, options) => Target::of(&options.target, self)?,
                StagedOp::Upsert(_, _, cells, options) => {
                    inserted = Some(cells.clone());
                    Target::of(&options.target, self)?
                }
                StagedOp::Delete(_, _, options) => Target::of(&options.target, self)?,
                StagedOp::Restore(_, _, _, options) => Target::exact(&options.target),
                StagedOp::Move(..) => return Err(unsupported("owner pending branch moves")),
            };
            let mut keys = BTreeSet::new();
            match target {
                Target::Root if table.branch_by.is_empty() => {
                    keys.insert(Vec::new());
                }
                Target::Root if matches!(op, StagedOp::Insert(..)) => {
                    keys.insert(table.row_branch_key(inserted.as_ref().expect("insert cells")));
                }
                Target::Root => {
                    // Root patches resolve the one branch under this exact id.
                    // Earlier inserts in this same unit have not been replayed.
                    for image in self.images_where(
                        &table.stored,
                        false,
                        "id=?1",
                        &[&row.as_bytes().to_vec()],
                    )? {
                        keys.insert(image.key);
                    }
                    // Earlier qualified units in the same received frame have
                    // coordinates before their rows are replayed into SQLite.
                    if self.non_durable {
                        let mut statement=self.conn.prepare_cached("SELECT DISTINCT k FROM _jazz_owner_input_rows_v1 WHERE t=?1 AND id=?2 AND pending=1 AND writes=1").map_err(storage)?;
                        let preceding = statement
                            .query_map(
                                rusqlite::params![&table.stored, row.as_bytes().as_slice()],
                                |r| r.get::<_, Vec<u8>>(0),
                            )
                            .map_err(storage)?;
                        for key in preceding {
                            keys.insert(key.map_err(storage)?);
                        }
                    }
                    for (stored, previous, key) in &rows {
                        if stored == &table.stored && previous == &row {
                            keys.insert(key.clone());
                        }
                    }
                    if keys.is_empty()
                        && let Some(cells) = &inserted
                    {
                        keys.insert(table.row_branch_key(cells));
                    }
                }
                Target::Exact(selector) => {
                    keys.insert(table.selector_key(&selector)?);
                }
                Target::View { head, base, at } => {
                    keys.insert(table.selector_key(&head)?);
                    if dependencies
                        && at.is_none()
                        && let Some(base) = base
                    {
                        // Inherited data is an input even when the unit writes
                        // only the head. Retain its accepted shadow so a changed
                        // base retriggers replay in the foreground too.
                        keys.insert(table.selector_key(&base)?);
                    }
                }
            }
            for key in keys {
                rows.insert((table.stored.clone(), row, key));
            }
        }
        Ok(rows.into_iter().collect())
    }

    pub(super) fn import_owner_upload(
        &mut self,
        id: UploadId,
        source: AuthorSubject,
        message: &SyncMessage,
    ) -> Result<bool, Error> {
        if !self.scope_isolated_owner {
            return Ok(false);
        }
        self.require_owner_subject(source)?;
        if self
            .client
            .as_ref()
            .expect("owner replica")
            .relay_uploads
            .get(&id)
            .is_some_and(|upload| upload.schema != self.owner.jazz.version_id())
        {
            return Err(unsupported(
                "restoring owner pending writes under a different schema",
            ));
        }
        if self
            .client
            .as_ref()
            .expect("owner replica")
            .pending
            .iter()
            .any(|p| (p.node, p.tx) == id)
        {
            return Ok(false);
        }
        let SyncMessage::Commit {
            ops,
            author,
            session,
            ..
        } = message
        else {
            return Err(Error::new(
                ErrorCode::Storage,
                "owner projection requires a complete upload",
            ));
        };
        if author.as_deref() != Some(source.canonical()) || session.is_none() {
            return Err(Error::new(
                ErrorCode::Storage,
                "owner projection differs from its authenticated subject",
            ));
        }
        let mut ops = ops
            .iter()
            .cloned()
            .map(from_wire)
            .collect::<Result<Vec<_>, _>>()?;
        for op in &mut ops {
            op.attribute(source, true);
        }
        let rows = self.owner_input_coordinates(&ops)?;
        let seq = self.owner_position(id)?;
        self.record_owner_input_rows(id, &rows, &ops)?;
        let mut shadow = Vec::new();
        for coordinate in &rows {
            if !self
                .client
                .as_ref()
                .expect("owner replica")
                .shadow
                .contains_key(coordinate)
            {
                let image = match self.current_image(&coordinate.0, coordinate.1, &coordinate.2)? {
                    Some(image) if image.seq < LOCAL_BASE => Some(image),
                    _ => self.settled_before(&coordinate.0, coordinate.1, &coordinate.2)?,
                };
                shadow.push((coordinate.clone(), image));
            }
        }
        let client = self.client.as_mut().expect("owner replica");
        client
            .shadow_dirty
            .extend(shadow.iter().map(|(key, _)| key.clone()));
        client.shadow.extend(shadow);
        client.pending.push(Pending {
            expected: Vec::new(),
            owner_managed: true,
            locally_durable: false,
            tx: id.1,
            seq,
            node: id.0,
            ops,
            rows,
        });
        client.pending.sort_by_key(|p| p.seq);
        self.persist_shadow()?;
        Ok(true)
    }
}

impl Db {
    pub(super) fn project_owner_upload(&self, id: UploadId) -> Result<(), Error> {
        let result = (|| {
            let changed = {
                let mut inner = self.inner();
                let upload = inner
                    .client
                    .as_ref()
                    .expect("owner replica")
                    .relay_uploads
                    .get(&id)
                    .expect("queued upload");
                let source = upload.source;
                let message = upload.message.clone();
                inner.import_owner_upload(id, source, &message)?
            };
            if changed {
                self.rebase()?;
            }
            Ok::<_, Error>(())
        })();
        if let Err(error) = &result {
            self.inner().owner_projection_error = Some(error.clone());
        }
        result
    }

    /// Whether this runtime was admitted by its host as a private durable owner.
    pub fn is_scope_isolated_storage_owner(&self) -> bool {
        self.inner().scope_isolated_owner
    }

    /// Only a host-admitted private owner may forward claims for its one
    /// immutable subject. This capability is not inferred from wire traffic.
    pub(crate) fn delegated_owner_subject(&self) -> Option<AuthorSubject> {
        let inner = self.inner();
        inner.scope_isolated_owner.then_some(inner.identity.author)
    }

    /// Bind this durable replica's private pending projection to its admitted
    /// subject. A generic multi-reader relay must not enable this capability.
    /// Hosts activate it before handing the runtime to any application or peer.
    pub fn set_scope_isolated_storage_owner(&self) -> Result<(), Error> {
        {
            let inner = self.inner();
            if inner.scope_isolated_owner {
                return inner.require_owner_subject(inner.identity.author);
            }
            if inner.authority
                || inner.non_durable
                || !matches!(inner.identity.author, AuthorSubject::Authenticated(_))
            {
                return Err(unsupported(
                    "scope-isolated storage owner outside an ordinary durable replica",
                ));
            }
            if inner.sessions.iter().any(|s| s.open)
                || !inner.batches.is_empty()
                || inner.tx.is_some()
            {
                return Err(unsupported(
                    "activating a scope-isolated storage owner after peer or transaction admission",
                ));
            }
        }
        self.ensure_client();
        let uploads = {
            let inner = self.inner();
            if inner
                .client
                .as_ref()
                .expect("owner replica")
                .pending
                .iter()
                .flat_map(|p| &p.ops)
                .any(|op| {
                    op.author()
                        .is_some_and(|author| author != inner.identity.author)
                })
            {
                return Err(Error::new(
                    ErrorCode::Storage,
                    "scope-isolated owner has pending writes for another subject",
                ));
            }
            let mut uploads = inner
                .client
                .as_ref()
                .expect("owner replica")
                .relay_uploads
                .iter()
                .map(|(id, upload)| (*id, upload.order, upload.source, upload.message.clone()))
                .collect::<Vec<_>>();
            if uploads
                .iter()
                .any(|(_, _, source, _)| *source != inner.identity.author)
            {
                return Err(Error::new(
                    ErrorCode::Storage,
                    "scope-isolated owner journal contains another subject",
                ));
            }
            uploads.sort_by_key(|(_, order, _, _)| *order);
            uploads
        };
        {
            let inner = self.inner();
            inner.conn.execute(
                "INSERT INTO _jazz_owner_scope_v1(singleton,subject,schema_id) VALUES(1,?1,?2) ON CONFLICT(singleton) DO UPDATE SET schema_id=excluded.schema_id WHERE schema_id<>excluded.schema_id",
                rusqlite::params![inner.identity.author.canonical(), inner.owner.jazz.version_id().as_bytes().as_slice()],
            ).map_err(storage)?;
        }
        self.inner().scope_isolated_owner = true;
        let result = (|| {
            {
                let mut inner = self.inner();
                inner.restore_owner_policies()?;
                let pending = inner
                    .client
                    .as_ref()
                    .expect("owner replica")
                    .pending
                    .iter()
                    .map(|p| ((p.node, p.tx), p.ops.clone()))
                    .collect::<Vec<_>>();
                for (index, (id, ops)) in pending.into_iter().enumerate() {
                    let rows = inner.owner_input_coordinates(&ops)?;
                    inner.record_owner_input_rows(id, &rows, &ops)?;
                    for coordinate in &rows {
                        if inner
                            .client
                            .as_ref()
                            .expect("owner")
                            .shadow
                            .contains_key(coordinate)
                        {
                            continue;
                        }
                        let base = match inner.current_image(
                            &coordinate.0,
                            coordinate.1,
                            &coordinate.2,
                        )? {
                            Some(image) if image.seq < LOCAL_BASE => Some(image),
                            _ => {
                                inner.settled_before(&coordinate.0, coordinate.1, &coordinate.2)?
                            }
                        };
                        inner
                            .client
                            .as_mut()
                            .expect("owner")
                            .shadow
                            .insert(coordinate.clone(), base);
                    }
                    inner.client.as_mut().expect("owner").pending[index].rows = rows;
                }
            }
            for (id, _, source, message) in uploads {
                self.inner().import_owner_upload(id, source, &message)?;
            }
            self.rebase()
        })();
        if let Err(error) = &result {
            self.inner().owner_projection_error = Some(error.clone());
        }
        result
    }
}

const OWNER_INPUT_BUDGET: usize = jazz_protocol::protocol_limits::MAX_WIRE_FRAME_BYTES - 64 * 1024;
const OWNER_INPUT_LIMIT: usize = 4096;

fn input_error(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::Protocol, message)
}

fn decode_input(bytes: &[u8]) -> Result<SyncMessage, Error> {
    let (message, tail): (SyncMessage, _) = postcard::take_from_bytes(bytes)
        .map_err(|error| input_error(format!("invalid owner input: {error}")))?;
    if !tail.is_empty() || relay::upload_id(&message).is_none() {
        return Err(input_error("owner input must contain exactly one Commit"));
    }
    Ok(message)
}

impl Inner {
    fn input_rows(&self, id: UploadId) -> Result<Vec<RowKey>, Error> {
        let mut statement = self
            .conn
            .prepare_cached("SELECT t,id,k FROM _jazz_owner_input_rows_v1 WHERE node=?1 AND tx=?2")
            .map_err(storage)?;
        let rows = statement
            .query_map(rusqlite::params![id.0.as_slice(), id.1 as i64], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                ))
            })
            .map_err(storage)?;
        rows.map(|row| {
            let (table, row, key) = row.map_err(storage)?;
            let row = row
                .try_into()
                .map_err(|_| input_error("invalid owner input row id"))?;
            Ok((table, RowUuid::from_bytes(row), key))
        })
        .collect()
    }

    fn inputs_at(&self, row: &RowKey) -> Result<Vec<UploadId>, Error> {
        let mut statement = self.conn.prepare_cached(
            "SELECT node,tx FROM _jazz_owner_input_rows_v1 WHERE t=?1 AND id=?2 AND k=?3 AND pending=1 AND writes=1",
        ).map_err(storage)?;
        let ids = statement
            .query_map(
                rusqlite::params![&row.0, row.1.as_bytes().as_slice(), &row.2],
                |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, i64>(1)?)),
            )
            .map_err(storage)?;
        ids.map(|id| {
            let (node, tx) = id.map_err(storage)?;
            Ok((
                node.try_into()
                    .map_err(|_| input_error("invalid owner input node"))?,
                tx as u64,
            ))
        })
        .collect()
    }

    fn owner_input(&self, id: UploadId) -> Result<(u64, SyncMessage), Error> {
        let client = self.client.as_ref().expect("owner replica");
        let mut message = match client.relay_uploads.get(&id) {
            Some(upload) => {
                if upload.source != self.identity.author
                    || upload.schema != self.owner.jazz.version_id()
                {
                    return Err(input_error(
                        "owner input differs from the admitted subject or schema",
                    ));
                }
                upload.message.clone()
            }
            None => {
                let mut message = self.own_upload(id)?;
                if let SyncMessage::Commit {
                    author, session, ..
                } = &mut message
                {
                    if author
                        .as_deref()
                        .is_some_and(|author| author != self.identity.author.canonical())
                    {
                        return Err(input_error("owner input belongs to another subject"));
                    }
                    // This describes private replay, not a delegated upstream
                    // upload. The original durable unit remains unchanged.
                    *author = Some(self.identity.author.canonical().to_owned());
                    session.get_or_insert_with(BTreeMap::new);
                }
                message
            }
        };
        if relay::upload_id(&message) != Some(id) {
            return Err(input_error(
                "owner input identity differs from its durable key",
            ));
        }
        if let SyncMessage::Commit {
            author, session, ..
        } = &mut message
            && (author.as_deref() != Some(self.identity.author.canonical()) || session.is_none())
        {
            return Err(input_error("owner input has no authenticated attribution"));
        }
        let position = self
            .local_position(TxId::new(TxTime(id.1), NodeUuid::from_bytes(id.0)))?
            .ok_or_else(|| input_error("owner input has no publication position"))?;
        if !(LOCAL_BASE..FOREGROUND_BASE).contains(&position) {
            return Err(unsupported(
                "owner publications outside the foreground prefix range",
            ));
        }
        Ok((position, message))
    }

    pub(in crate::db) fn frozen_coordinates(
        &self,
        ops: &[StagedOp],
    ) -> Result<BTreeSet<(RowKey, u64)>, Error> {
        let mut out = BTreeSet::new();
        for op in ops {
            let target = match op {
                StagedOp::Update(_, _, _, options) => &options.target,
                StagedOp::Upsert(_, _, _, options) => &options.target,
                StagedOp::Delete(_, _, options) => &options.target,
                _ => continue,
            };
            if let WriteTarget::BranchView {
                base: Some(BranchViewBase::Snapshot { branch, snapshot }),
                ..
            } = target
            {
                if !snapshot.dots.is_empty() {
                    return Err(unsupported(
                        "owner query inputs with pending writes named by a frozen snapshot",
                    ));
                }
                self.resolve_snapshot(snapshot)?;
                let table = self.owner.table(op.table())?;
                let row = op
                    .row()
                    .ok_or_else(|| input_error("frozen owner input has no row identity"))?;
                out.insert((
                    (table.stored.clone(), row, table.selector_key(branch)?),
                    snapshot.global_base.0,
                ));
            }
        }
        Ok(out)
    }

    fn frozen_image(&self, coordinate: &RowKey, at: u64) -> Result<Option<Image>, Error> {
        let Some((seq, _)) = self.existing_at(
            &coordinate.0,
            coordinate.1,
            &coordinate.2,
            Some(&Cut::at(at)),
        )?
        else {
            return Ok(None);
        };
        let history = self
            .existing(&coordinate.0, coordinate.1, &coordinate.2)?
            .is_none_or(|(current, _)| current != seq);
        self.images_where(
            &coordinate.0,
            history,
            "id=?1 AND _b=?2 AND _seq=?3",
            &[
                &coordinate.1.as_bytes().to_vec(),
                &coordinate.2,
                &(seq as i64),
            ],
        )?
        .pop()
        .map(Some)
        .ok_or_else(|| input_error("frozen owner input is missing its selected row image"))
    }

    /// Check a retained accepted image against accepted policy dependencies.
    /// Pending grants cannot make an accepted base readable to a new tab.
    fn owner_base_readable(&self, image: &Image, subject: AuthorSubject) -> Result<bool, Error> {
        if image.seq >= LOCAL_BASE || self.accepted_image_content(image)?.is_none() {
            return Ok(false);
        }
        let revoked: bool = self
            .conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM _jazz_revoked WHERE t=?1 AND id=?2 AND k=?3)",
                rusqlite::params![&image.table, image.id.as_slice(), &image.key],
                |r| r.get(0),
            )
            .map_err(storage)?;
        if revoked {
            return Ok(false);
        }
        let table = owned_table(&self.owner, &image.table)?;
        let binding = BTreeMap::new();
        let session = self.session(subject);
        let cut = self.accepted_cut(self.client.as_ref().expect("owner replica").known)?;
        let mut ctx = Ctx::new(&self.owner, None, &session, &binding)
            .with_catalogue(self)
            .with_cut(Some(cut));
        let condition = ctx.operation_policy(table, Operation::Select, "a", &session)?;
        let preps = std::mem::take(&mut ctx.preps);
        let mut params = ctx.params;
        params.push(Sql::Blob(image.id.to_vec()));
        params.push(Sql::Blob(image.key.clone()));
        params.push(Sql::Integer(image.seq as i64));
        let filter = format!(
            "id=?{} AND _b=?{} AND _seq=?{}",
            params.len() - 2,
            params.len() - 1,
            params.len()
        );
        let source = if self
            .existing(&table.stored, RowUuid::from_bytes(image.id), &image.key)?
            .is_some_and(|(seq, _)| seq == image.seq)
        {
            table.stored.clone()
        } else {
            format!("{}__h", table.stored)
        };
        let sql = format!(
            "SELECT COALESCE({condition},0) FROM {} a WHERE {filter} LIMIT 1",
            quote(&source)
        );
        self.with_preps(&preps, |_| {
            self.conn
                .query_row(&sql, params_from_iter(params.iter()), |r| {
                    r.get::<_, bool>(0)
                })
                .optional()
                .map_err(storage)
        })
        .map(|value| value.unwrap_or(false))
    }

    /// Qualify the normal query's exact row inputs with complete pending units.
    /// Work is bounded by the returned/support coordinates and their unit
    /// closure; unrelated pending writes and terminal history are not scanned.
    pub(super) fn owner_query_inputs(
        &mut self,
        index: usize,
        images: &mut Vec<Image>,
        evictions: &mut Vec<SyncMessage>,
    ) -> Result<(Vec<SyncMessage>, Vec<RowKey>), Error> {
        if !self.scope_isolated_owner || (images.is_empty() && evictions.is_empty()) {
            return Ok((Vec::new(), Vec::new()));
        }
        let subject = self.sessions[index].identity;
        self.require_owner_subject(subject)?;
        let roots = images
            .iter()
            .map(|image| {
                (
                    image.table.clone(),
                    RowUuid::from_bytes(image.id),
                    image.key.clone(),
                )
            })
            .chain(evictions.iter().filter_map(|message| match message {
                SyncMessage::Evict { table, id, key } | SyncMessage::Unlist { table, id, key } => {
                    Some((table.clone(), RowUuid::from_bytes(*id), key.clone()))
                }
                _ => None,
            }))
            .collect::<BTreeSet<_>>();
        let mut queue = VecDeque::from_iter(roots);
        let mut visited = BTreeSet::new();
        let mut selected = BTreeSet::new();
        let mut units = Vec::new();
        let mut pending_rows = BTreeSet::new();
        let mut frozen_rows = BTreeSet::new();
        let mut bytes = 0usize;
        while let Some(coordinate) = queue.pop_front() {
            if !visited.insert(coordinate.clone()) {
                continue;
            }
            if visited.len() > OWNER_INPUT_LIMIT {
                return Err(unsupported(
                    "owner query inputs exceeding the coordinate budget",
                ));
            }
            for id in self.inputs_at(&coordinate)? {
                if !selected.insert(id) {
                    continue;
                }
                if !self.sessions[index].owner_projection {
                    return Err(unsupported(
                        "pending owner query inputs for a peer without owner-projection-v1",
                    ));
                }
                if self.sessions[index].version != Some(self.owner.jazz.version_id()) {
                    return Err(unsupported(
                        "pending owner query inputs across schema versions",
                    ));
                }
                let rows = self.input_rows(id)?;
                queue.extend(rows.iter().cloned());
                pending_rows.extend(rows);
                let (position, message) = self.owner_input(id)?;
                if let SyncMessage::Commit { ops, .. } = &message {
                    let ops = ops
                        .iter()
                        .cloned()
                        .map(from_wire)
                        .collect::<Result<Vec<_>, _>>()?;
                    frozen_rows.extend(self.frozen_coordinates(&ops)?);
                    if frozen_rows.len().saturating_add(visited.len()) > OWNER_INPUT_LIMIT {
                        return Err(unsupported(
                            "owner query inputs exceeding the coordinate budget",
                        ));
                    }
                }
                let encoded = encode(&message);
                bytes = bytes.saturating_add(encoded.len() + 16);
                if bytes > OWNER_INPUT_BUDGET || selected.len() > OWNER_INPUT_LIMIT {
                    return Err(unsupported(
                        "owner query inputs exceeding one complete wire frame",
                    ));
                }
                units.push((position, encoded, message));
            }
        }
        let mut bases = Vec::new();
        let mut manifest = Vec::new();
        for coordinate in &pending_rows {
            let current = self.current_image(&coordinate.0, coordinate.1, &coordinate.2)?;
            if current.is_none()
                || !readable(
                    self,
                    &coordinate.0,
                    coordinate.1,
                    &coordinate.2,
                    Some(subject),
                )?
            {
                return Err(unsupported(
                    "owner input unit containing an unreadable coordinate",
                ));
            }
            let shadow = self
                .client
                .as_ref()
                .expect("owner replica")
                .shadow
                .get(coordinate)
                .ok_or_else(|| input_error("owner input is missing its accepted shadow"))?;
            manifest.push(OwnerInputBaseV1 {
                table: self
                    .owner
                    .by_stored(&coordinate.0)
                    .ok_or_else(|| input_error("owner base has no logical table"))?
                    .name
                    .clone(),
                id: *coordinate.1.as_bytes(),
                key: coordinate.2.clone(),
                seq: shadow.as_ref().map(|image| image.seq),
            });
            if let Some(base) = shadow {
                if !self.owner_base_readable(base, subject)? {
                    return Err(unsupported(
                        "owner input unit without a readable accepted base",
                    ));
                }
                bases.push(base.clone());
            }
        }
        let observed_rows = pending_rows
            .iter()
            .cloned()
            .chain(frozen_rows.iter().map(|(coordinate, _)| coordinate.clone()))
            .collect::<BTreeSet<_>>();
        let mut frozen = Vec::new();
        for (coordinate, at) in frozen_rows {
            let image = self.frozen_image(&coordinate, at)?;
            if let Some(image) = &image {
                if !self.owner_base_readable(image, subject)? {
                    return Err(unsupported(
                        "owner frozen input without a readable accepted base",
                    ));
                }
                bases.push(image.clone());
            }
            frozen.push(OwnerFrozenBaseV1 {
                base: OwnerInputBaseV1 {
                    table: self
                        .owner
                        .by_stored(&coordinate.0)
                        .ok_or_else(|| input_error("frozen owner base has no logical table"))?
                        .name
                        .clone(),
                    id: *coordinate.1.as_bytes(),
                    key: coordinate.2,
                    seq: image.map(|image| image.seq),
                },
                at,
            });
        }
        if bytes
            .saturating_add(encode(&manifest).len())
            .saturating_add(encode(&frozen).len())
            > OWNER_INPUT_BUDGET
        {
            return Err(unsupported(
                "owner query inputs exceeding one complete wire frame",
            ));
        }
        // Missing index entries must fail closed, never produce an apparently
        // complete answer with a provisional row silently omitted.
        if images.iter().any(|image| {
            image.seq >= LOCAL_BASE
                && !pending_rows.contains(&(
                    image.table.clone(),
                    RowUuid::from_bytes(image.id),
                    image.key.clone(),
                ))
        }) {
            return Err(input_error(
                "pending owner image has no complete input unit",
            ));
        }
        // Do not transport private aliases as accepted-looking ordinary images.
        images.retain(|image| image.seq < LOCAL_BASE);
        images.extend(bases);
        for image in images.iter() {
            if !self.owner_base_readable(image, subject)? {
                return Err(unsupported(
                    "owner query image without accepted read evidence",
                ));
            }
        }
        evictions.retain(|message| match message {
            SyncMessage::Evict { table, id, key } | SyncMessage::Unlist { table, id, key } => {
                !pending_rows.contains(&(table.clone(), RowUuid::from_bytes(*id), key.clone()))
            }
            _ => true,
        });
        units.sort_by_key(|(position, _, _)| *position);
        let mut out = Vec::new();
        // An admitted foreground also needs the retained authority boundary
        // when all its inputs are accepted. A cached Settled remains Local;
        // this owner envelope lets subsequent accepted-at-begin observations
        // use the accompanying proofs without claiming fresh remote coverage.
        let admitted = self.sessions[index].owner_projection
            && self.sessions[index].version == Some(self.owner.jazz.version_id());
        if !units.is_empty() || admitted {
            out.push(SyncMessage::OwnerInputsV1 {
                schema: *self.owner.jazz.version_id().as_bytes(),
                through: self.client.as_ref().expect("owner").known,
                bases: manifest,
                frozen,
                units: units
                    .into_iter()
                    .map(|(position, bytes, _)| (position, bytes))
                    .collect(),
            });
        }
        self.sessions[index].owner_observed.extend(selected);
        Ok((out, observed_rows.into_iter().collect()))
    }

    /// Ask through the ordinary authenticated chunk protocol before changing
    /// projection membership. No query answer may overtake these inputs.
    pub(super) fn owner_input_chunks_ready(
        &mut self,
        schema: [u8; 16],
        units: &[(u64, Vec<u8>)],
    ) -> Result<bool, Error> {
        if !self.private_owner_required
            || schema != *self.owner.jazz.version_id().as_bytes()
            || units.len() > OWNER_INPUT_LIMIT
            || units.iter().map(|(_, bytes)| bytes.len()).sum::<usize>() > OWNER_INPUT_BUDGET
        {
            return Err(input_error("invalid owner inputs before chunk hydration"));
        }
        let mut ready = true;
        for (_, bytes) in units {
            let message = decode_input(bytes)?;
            let SyncMessage::Commit {
                author, session, ..
            } = &message
            else {
                unreachable!()
            };
            if author.as_deref() != Some(self.identity.author.canonical()) || session.is_none() {
                return Err(input_error(
                    "owner input chunk request differs from the admitted subject",
                ));
            }
            for value in relay::large_values(&message) {
                match self.large_cell(value) {
                    Ok(_) => {}
                    Err(error) if error.code == ErrorCode::NotObserved => {
                        let demand = self.chunks.take_demand();
                        let client = self.client.as_mut().expect("foreground");
                        if demand
                            .iter()
                            .any(|request| client.unavailable.contains(request))
                        {
                            return Err(input_error(
                                "owner input requires unavailable large-value chunks",
                            ));
                        }
                        client.wanted.extend(demand);
                        ready = false;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(ready)
    }

    pub(super) fn receive_owner_inputs(
        &mut self,
        schema: [u8; 16],
        through: u64,
        bases: Vec<OwnerInputBaseV1>,
        frozen: Vec<OwnerFrozenBaseV1>,
        units: Vec<(u64, Vec<u8>)>,
    ) -> Result<BTreeSet<UploadId>, Error> {
        if through >= LOCAL_BASE {
            return Err(input_error(
                "owner input accepted prefix must be an authority sequence",
            ));
        }
        if !self.private_owner_required || schema != *self.owner.jazz.version_id().as_bytes() {
            return Err(input_error(
                "owner inputs require a foreground with the same admitted schema",
            ));
        }
        let mut observed = BTreeSet::new();
        let mut previous = 0;
        let mut size = encode(&bases).len().saturating_add(encode(&frozen).len());
        if units.len() > OWNER_INPUT_LIMIT
            || bases.len().saturating_add(frozen.len()) > OWNER_INPUT_LIMIT
            || size > OWNER_INPUT_BUDGET
        {
            return Err(input_error("too many owner input units or bases"));
        }
        // A cached Settled does not advance the authority boundary. This
        // admitted owner prefix qualifies retained proofs and frozen inputs;
        // it does not settle a remote query or acknowledge a pending write.
        let client = self.client.as_mut().expect("foreground");
        client.known = client.known.max(through);
        let mut frozen_coordinates = BTreeSet::new();
        for input in frozen {
            let base = input.base;
            let table = self
                .owner
                .stored_table(&base.table)
                .map_err(|_| input_error("frozen owner base names an unknown table"))?
                .to_owned();
            let coordinate = (table, RowUuid::from_bytes(base.id), base.key);
            if input.at > through
                || base.seq.is_some_and(|seq| seq > input.at)
                || !frozen_coordinates.insert((coordinate.clone(), input.at))
            {
                return Err(input_error(
                    "invalid or repeated frozen owner base coordinate",
                ));
            }
            match (base.seq, self.frozen_image(&coordinate, input.at)?) {
                (None, None) => {}
                (Some(seq), Some(image))
                    if image.seq == seq && self.accepted_image_content(&image)?.is_some() => {}
                _ => {
                    return Err(input_error(
                        "owner input is missing its exact frozen base or evidence",
                    ));
                }
            }
        }
        let mut used_frozen_coordinates = BTreeSet::new();
        let mut coordinates = BTreeSet::new();
        for base in bases {
            let table = self
                .owner
                .stored_table(&base.table)
                .map_err(|_| input_error("owner base names an unknown table"))?
                .to_owned();
            let coordinate = (table, RowUuid::from_bytes(base.id), base.key);
            if !coordinates.insert(coordinate.clone()) {
                return Err(input_error(
                    "owner input repeats an accepted base coordinate",
                ));
            }
            let image = match self
                .client
                .as_ref()
                .expect("foreground")
                .shadow
                .get(&coordinate)
            {
                Some(image) => image.clone(),
                None => self.current_image(&coordinate.0, coordinate.1, &coordinate.2)?,
            };
            match (base.seq, image) {
                (None, None) => {}
                (Some(seq), Some(image))
                    if seq < LOCAL_BASE
                        && image.seq == seq
                        && self.accepted_image_content(&image)?.is_some() => {}
                _ => {
                    return Err(input_error(
                        "owner input is missing its exact accepted base or evidence",
                    ));
                }
            }
        }
        let mut used_coordinates = BTreeSet::new();
        for (position, bytes) in units {
            size = size.saturating_add(bytes.len() + 16);
            if size > OWNER_INPUT_BUDGET
                || !(LOCAL_BASE..FOREGROUND_BASE).contains(&position)
                || position <= previous
            {
                return Err(input_error("invalid owner input size or publication order"));
            }
            previous = position;
            let message = decode_input(&bytes)?;
            let id = relay::upload_id(&message).expect("validated Commit");
            if !observed.insert(id) {
                return Err(input_error("owner input repeats an original transaction"));
            }
            if id.1 == 0 || id.1 >= i64::MAX as u64 {
                return Err(input_error("invalid owner input transaction time"));
            }
            let SyncMessage::Commit {
                ops,
                author,
                session,
                ..
            } = &message
            else {
                unreachable!()
            };
            if author.as_deref() != Some(self.identity.author.canonical()) || session.is_none() {
                return Err(input_error(
                    "owner input differs from the foreground's admitted subject",
                ));
            }
            let mut ops = ops
                .iter()
                .cloned()
                .map(from_wire)
                .collect::<Result<Vec<_>, _>>()?;
            used_frozen_coordinates.extend(self.frozen_coordinates(&ops)?);
            if self.settled(id.0, id.1)?.is_some() {
                used_coordinates.extend(self.input_rows(id)?);
                continue;
            }
            let client = self.client.as_ref().expect("foreground");
            if let Some(prior) = client.owner_inputs.get(&id) {
                if prior != &bytes
                    || client
                        .pending
                        .iter()
                        .find(|p| (p.node, p.tx) == id)
                        .is_none_or(|p| p.seq != position)
                {
                    return Err(input_error("owner changed an observed pending input"));
                }
                used_coordinates.extend(
                    client
                        .pending
                        .iter()
                        .find(|p| (p.node, p.tx) == id)
                        .expect("validated pending input")
                        .rows
                        .iter()
                        .cloned(),
                );
                continue;
            }
            if client.pending.iter().any(|p| {
                !p.owner_managed
                    && p.seq < FOREGROUND_BASE
                    && (p.node, p.tx) != id
                    && !client.owner_inputs.contains_key(&(p.node, p.tx))
            }) {
                return Err(unsupported(
                    "owner inputs over legacy foreground publications below the suffix range",
                ));
            }
            let existing = client.pending.iter().position(|p| (p.node, p.tx) == id);
            if existing.is_some() {
                let original: Vec<u8> = self
                    .conn
                    .query_row(
                        "SELECT commit_msg FROM _jazz_pending WHERE tx=?1",
                        [id.1 as i64],
                        |r| r.get(0),
                    )
                    .map_err(storage)?;
                let original = decode_input(&original)?;
                let core = |message: &SyncMessage| match message {
                    SyncMessage::Commit {
                        node,
                        tx,
                        kind,
                        ops,
                        ..
                    } => encode(&(node, tx, kind, ops)),
                    _ => unreachable!(),
                };
                if core(&original) != core(&message) {
                    return Err(input_error(
                        "owner echo differs from the original foreground write",
                    ));
                }
            }
            let alias: Option<(Vec<u8>,i64)> = self.conn.query_row(
                "SELECT node,tx FROM _jazz_owner_positions_v1 WHERE seq=?1 UNION ALL SELECT node,tx FROM _jazz_local_commits_v1 WHERE seq=?1",
                [position as i64],|r|Ok((r.get(0)?,r.get(1)?)),
            ).optional().map_err(storage)?;
            if alias.is_some_and(|(node, tx)| node != id.0 || tx != id.1 as i64) {
                return Err(input_error(
                    "owner position collides with another original transaction",
                ));
            }
            self.conn.execute("INSERT INTO _jazz_owner_positions_v1(seq,node,tx) VALUES(?1,?2,?3) ON CONFLICT(seq) DO NOTHING",rusqlite::params![position as i64,id.0.as_slice(),id.1 as i64]).map_err(storage)?;

            for op in &mut ops {
                op.attribute(self.identity.author, true);
            }
            let rows = self.owner_input_coordinates(&ops)?;
            if rows
                .iter()
                .any(|coordinate| !coordinates.contains(coordinate))
            {
                return Err(input_error("owner input omits an accepted base coordinate"));
            }
            used_coordinates.extend(rows.iter().cloned());
            self.record_owner_input_rows(id, &rows, &ops)?;
            let mut shadows = Vec::new();
            for coordinate in &rows {
                if self
                    .client
                    .as_ref()
                    .expect("foreground")
                    .shadow
                    .contains_key(coordinate)
                {
                    continue;
                }
                let current = self.current_image(&coordinate.0, coordinate.1, &coordinate.2)?;
                if current
                    .as_ref()
                    .is_some_and(|image| image.seq >= LOCAL_BASE)
                {
                    return Err(input_error(
                        "owner input encountered an unqualified optimistic row",
                    ));
                }
                shadows.push((coordinate.clone(), current));
            }
            let client = self.client.as_mut().expect("foreground");
            client
                .shadow_dirty
                .extend(shadows.iter().map(|(key, _)| key.clone()));
            client.shadow.extend(shadows);
            if let Some(index) = existing {
                client.pending[index].seq = position;
                client.pending[index].rows = rows;
            } else {
                client.pending.push(Pending {
                    expected: Vec::new(),
                    owner_managed: true,
                    locally_durable: false,
                    tx: id.1,
                    seq: position,
                    node: id.0,
                    ops,
                    rows,
                });
            }
            client.pending.sort_by_key(|p| p.seq);
            client.owner_inputs.insert(id, bytes);
            client.owner_projection_bound = true;
        }
        if used_coordinates != coordinates || used_frozen_coordinates != frozen_coordinates {
            return Err(input_error(
                "owner input base manifest differs from its complete footprint",
            ));
        }
        Ok(observed)
    }
}

impl Db {
    /// Await the immediate private owner's acknowledgement on this connection.
    /// Authority connectivity and durability receipts are separate conditions.
    pub async fn wait_for_storage_owner(&self) -> Result<(), Error> {
        let connection = {
            let inner = self.inner();
            if !inner.private_owner_required {
                return Err(unsupported(
                    "private-owner admission outside an explicitly private foreground",
                ));
            }
            inner.client.as_ref().expect("foreground").connection
        };
        poll_fn(|_| {
            self.ensure_storage_owner_active()?;
            let inner = self.inner();
            if let Err(error) = inner.require_owner_subject(inner.identity.author) {
                return Poll::Ready(Err(error));
            }
            let client = inner.client.as_ref().expect("foreground");
            if client.connection != connection || client.transport.is_none() {
                return Poll::Ready(Err(Error::new(
                    ErrorCode::NotObserved,
                    "private-owner admission interrupted by disconnect or reconnect",
                )));
            }
            if client.owner_ready == Some(inner.owner.jazz.version_id()) {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        })
        .await
    }

    pub(super) fn admit_owner_observer(
        &self,
        index: usize,
        known: Vec<UploadId>,
    ) -> Result<(), Error> {
        let mut inner = self.inner();
        if known.len() > OWNER_INPUT_LIMIT {
            return Err(input_error("too many known owner input identities"));
        }
        if !inner.scope_isolated_owner {
            return Ok(());
        }
        let version = inner.owner.jazz.version_id();
        if inner.sessions[index].version != Some(version) {
            return Err(unsupported("private owner recovery under another schema"));
        }
        let ready = SyncMessage::OwnerReadyV1 {
            schema: *version.as_bytes(),
            subject: inner.identity.author.canonical().to_owned(),
            node: *inner.identity.node.as_bytes(),
        };
        let mut observed = BTreeSet::new();
        for id in known {
            if !inner.input_rows(id)?.is_empty() {
                observed.insert(id);
            }
        }
        inner.sessions[index].owner_projection = true;
        inner.sessions[index].owner_observed.extend(observed);
        inner.sessions[index].outbox.push(ready);
        Ok(())
    }

    pub(super) fn progress_owner_input_fates(&self) -> Result<(), Error> {
        let mut inner = self.inner();
        if !inner.scope_isolated_owner {
            return Ok(());
        }
        for index in 0..inner.sessions.len() {
            if !inner.sessions[index].open {
                continue;
            }
            let known = inner.sessions[index]
                .owner_observed
                .iter()
                .copied()
                .collect::<Vec<_>>();
            for (node, tx) in known {
                let Some(result) = inner.applied_fate(node, tx)? else {
                    continue;
                };
                let through = match (result.is_ok(), inner.settled(node, tx)?) {
                    (true, Some(Some(seq))) if seq < LOCAL_BASE => seq,
                    (false, Some(None)) => inner.client.as_ref().expect("owner").known,
                    _ => {
                        return Err(input_error(
                            "owner input fate has no matching authority position",
                        ));
                    }
                };
                let rows = inner.input_rows((node, tx))?;
                let session = &mut inner.sessions[index];
                if result.is_ok() {
                    session.committed.extend(
                        rows.iter()
                            .map(|(table, row, key)| (table.clone(), *row, key.clone(), through)),
                    );
                }
                session.touched.extend(rows);
                session.outbox.push(SyncMessage::FateV1 {
                    node,
                    tx,
                    through,
                    result,
                });
                session.owner_observed.remove(&(node, tx));
                session.owner_acked.remove(&(node, tx));
            }
        }
        Ok(())
    }
}
