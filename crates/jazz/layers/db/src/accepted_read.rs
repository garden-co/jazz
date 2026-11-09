//! Authority-side row bodies paired with the exact commits behind their
//! current state and retained content. Replicas cannot infer these from a
//! sparse local history; forwarding and cached observation are separate APIs.

use super::*;
use crate::ids::NodeUuid;
use jazz_model::tx::TxId;
use jazz_types::time::TxTime;

/// One authority-accepted commit. `sequence` is the authority's ordering
/// coordinate, not a wall-clock timestamp or a local durability receipt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AcceptedVersion {
    pub transaction: TxId,
    pub sequence: u64,
}

/// A row's state and content may come from different accepted commits:
/// deletion and restoration without replacement retain earlier content.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AcceptedRowEvidence {
    pub row: RowUuid,
    pub state: AcceptedVersion,
    pub content: AcceptedVersion,
}

/// One evaluation at an authority coordinate, in result order. An enclosing
/// serializable transaction must still validate this read before its result
/// can authorize a later mutation. This is not a write-acceptance receipt.
#[derive(Debug)]
pub struct AcceptedRead {
    pub through: u64,
    pub rows: Vec<CurrentRow>,
    pub evidence: Vec<AcceptedRowEvidence>,
}

pub(super) fn check_accepted_shape(prepared: &PreparedQuery, schema: &Schema) -> Result<(), Error> {
    let query = &prepared.query;
    if query.aggregate.is_some()
        || query.flat_join.is_some()
        || query.relation.is_some()
        || !query.joins.is_empty()
        || !query.includes.is_empty()
        || !query.array_subqueries.is_empty()
        || !query.reachable.is_empty()
        || !query.inherits.is_empty()
        || crate::schema::history::is_history_table(&query.table)
    {
        return Err(unsupported(format!(
            "accepted row evidence for non-stored-row queries of `{}`",
            query.table
        )));
    }
    let table = schema.table(&query.table)?;
    if !table.branch_by.is_empty() && query.limit != Some(0) {
        return Err(unsupported(format!(
            "accepted row evidence for branched table `{}`",
            query.table
        )));
    }
    Ok(())
}

impl Inner {
    /// Retain admitted names under the handle's authenticated governing policies.
    pub(super) fn governed_accepted_query(
        &self,
        prepared: &PreparedQuery,
        policies: Rc<Schema>,
    ) -> Result<PreparedQuery, Error> {
        let version = prepared
            .schema
            .as_ref()
            .unwrap_or(&self.owner)
            .jazz
            .version_id();
        let schema = self.governed_by(version, policies).ok_or_else(|| {
            Error::new(
                ErrorCode::Schema,
                "accepted-read schema is not admitted by the authority",
            )
        })?;
        let mut prepared = prepared.clone();
        prepared.schema = Some(schema);
        Ok(prepared)
    }
}

impl Db {
    /// Evaluate ordinary, unbranched stored rows on the authority, pairing
    /// each projected body with the origins of its state and retained content.
    /// `through` chooses an authority sequence; None captures the position this authority serves as settled.
    /// This method performs no network requests and refuses replica stores.
    pub fn accepted_read(
        &self,
        prepared: &PreparedQuery,
        through: Option<u64>,
        include_deleted: bool,
        identity: Option<AuthorSubject>,
    ) -> Result<AcceptedRead, Error> {
        let policies = self.view_policies();
        let inner = self.inner();
        if inner.client.is_some() || !inner.authority {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "accepted row evidence requires the authority's complete history",
            ));
        }
        let governed = inner.governed_accepted_query(prepared, policies)?;
        let prepared = &governed;
        let complete = inner.served_through();
        let through = through.unwrap_or(complete);
        if through > complete {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "accepted read coordinate is ahead of authority settlement",
            ));
        }
        let schema = prepared.schema.as_ref().unwrap_or(&inner.owner);
        check_accepted_shape(prepared, schema)?;
        let table = schema.table(&prepared.query.table)?;
        let read = ReadSpec {
            cut: Some(Cut::at(Pos::at(through))),
            include_deleted,
            settled_only: true,
            ..ReadSpec::new(self.governance.clone())
        };
        let rows = inner.run(prepared, identity, &read)?;
        let mut evidence = Vec::with_capacity(rows.len());
        // Each lookup names one returned row and its history boundary. It
        // neither scans unrelated rows nor loads a row's full history.
        let current = quote(&table.stored);
        let history = quote(&format!("{}__h", table.stored));
        let content_sql = format!(
            "SELECT MAX(seq) FROM (\
             SELECT MAX(_clock) AS seq FROM {current} \
             WHERE id=?1 AND _branch=X'' AND _clock<=?2 AND (_state & ?3)=0 \
             UNION ALL SELECT MAX(_clock) AS seq FROM {history} \
             WHERE id=?1 AND _branch=X'' AND _clock<=?2 AND (_state & ?3)=0)"
        );
        let mut content = inner.conn.prepare_cached(&content_sql).map_err(storage)?;
        // Use the writer-qualified metadata the normal sync path ships.
        // `_writer` distinguishes pending versions; accepted ones leave it empty.
        let origin = |seq: u64| {
            if seq == 0 || seq > sync::MAX_SEQ {
                return Err(Error::new(
                    ErrorCode::NotObserved,
                    "row is not authority accepted",
                ));
            }
            let (_, node, tx) = inner
                .tx_ids(&BTreeSet::from([seq]))?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::NotObserved,
                        "accepted writer evidence is missing",
                    )
                })?;
            Ok(AcceptedVersion {
                transaction: TxId::new(TxTime(tx), NodeUuid::from_bytes(node)),
                sequence: seq,
            })
        };
        for row in &rows {
            let content_clock: Option<i64> = content
                .query_row(
                    rusqlite::params![
                        row.id.0.as_bytes().as_slice(),
                        row.seq.seq as i64,
                        STATE_NO_CONTENT | STATE_AS_SENT
                    ],
                    |row| row.get(0),
                )
                .map_err(storage)?;
            let content_clock = content_clock.filter(|seq| *seq > 0).ok_or_else(|| {
                Error::new(
                    ErrorCode::NotObserved,
                    "accepted content witness is missing",
                )
            })? as u64;
            evidence.push(AcceptedRowEvidence {
                row: row.id,
                state: origin(row.seq.seq)?,
                content: origin(content_clock)?,
            });
        }
        Ok(AcceptedRead {
            through,
            rows,
            evidence,
        })
    }
}

/// The identity and claims used by the authority to evaluate one read.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AcceptedReadScope {
    pub author: AuthorSubject,
    pub claims: BTreeMap<String, Value>,
}

/// `jazz.sqlite.accepted-read.v1`: paired origins and ordinary encoded rows
/// from the same authority evaluation. This is neither a write receipt nor
/// an authorization decision that may be reused for a later operation.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EncodedAcceptedRead {
    pub through: u64,
    pub schema: [u8; 16],
    pub scope: AcceptedReadScope,
    pub evidence: Vec<AcceptedRowEvidence>,
    pub rows: Vec<u8>,
}

impl AcceptedRead {
    pub(crate) fn encode(
        self,
        schema: [u8; 16],
        scope: AcceptedReadScope,
    ) -> Result<EncodedAcceptedRead, Error> {
        let rows = crate::binding_codec::encode_rows(&self.rows)
            .map_err(|error| Error::new(ErrorCode::Storage, error.to_string()))?;
        Ok(EncodedAcceptedRead {
            through: self.through,
            schema,
            scope,
            evidence: self.evidence,
            rows,
        })
    }
}

impl EncodedAcceptedRead {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        let bad = || Error::new(ErrorCode::Protocol, "invalid accepted-read-v1 result");
        if self.through > sync::MAX_SEQ {
            return Err(bad());
        }
        let batches = crate::binding_codec::decode_rows(&self.rows).map_err(|_| bad())?;
        let rows: Vec<_> = batches.iter().flat_map(|batch| &batch.rows).collect();
        if rows.len() != self.evidence.len() {
            return Err(bad());
        }
        let mut seen = BTreeSet::new();
        for (row, evidence) in rows.iter().zip(&self.evidence) {
            if row.row_id != evidence.row
                || !seen.insert(evidence.row)
                || evidence.state.sequence == 0
                || evidence.state.sequence > self.through
                || evidence.content.sequence == 0
                || evidence.content.sequence > evidence.state.sequence
            {
                return Err(bad());
            }
        }
        Ok(())
    }
}
