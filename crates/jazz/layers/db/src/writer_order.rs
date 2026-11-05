//! Accepted writer order for each physical row and branch (INV-HIST-23).
//! The frontier joins the row mutation's existing savepoint. Tentative writes
//! and rejected units cannot advance it; another writer cannot erase it.
use super::*;

const PROFILE: &[u8] = b"JAZZ-SQLITE-WRITER-ORDER\0\x01";
const DDL: &str = "\
    CREATE TABLE IF NOT EXISTS _jazz_writer_order_profile_v1 (\
        slot INTEGER PRIMARY KEY CHECK(slot=1), value BLOB NOT NULL);\
    CREATE TABLE IF NOT EXISTS _jazz_writer_order_v1 (\
        t TEXT NOT NULL, id BLOB NOT NULL CHECK(length(id)=16), k BLOB NOT NULL,\
        node BLOB NOT NULL CHECK(length(node)=16), tx BLOB NOT NULL CHECK(length(tx)=8),\
        PRIMARY KEY(t,id,k,node)) WITHOUT ROWID;";

fn profile(conn: &Connection) -> Result<Option<Vec<u8>>, Error> {
    conn.query_row(
        "SELECT value FROM _jazz_writer_order_profile_v1 WHERE slot=1",
        [],
        |row| row.get(0),
    )
    .optional()
    .map_err(storage)
}

fn valid_profile(value: &[u8]) -> Result<(), Error> {
    if value != PROFILE {
        return Err(Error::new(
            ErrorCode::UnsupportedStorageCodecs,
            "unsupported jazz.sqlite.writer-order-profile.v1",
        ));
    }
    Ok(())
}

/// An empty store can establish the profile without reconstructing history.
/// Earlier nonempty stores stay readable, but cannot claim this authority
/// invariant without the missing frontier. No implicit history scan or reset.
pub(super) fn install(conn: &Connection, no_row_history: bool) -> Result<(), Error> {
    conn.execute_batch(DDL).map_err(storage)?;
    if let Some(value) = profile(conn)? {
        return valid_profile(&value);
    }
    if no_row_history {
        let has_receipts: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM _jazz_applied LIMIT 1) \
                 OR EXISTS(SELECT 1 FROM _jazz_settled LIMIT 1) \
                 OR EXISTS(SELECT 1 FROM _jazz_local_commits_v1 LIMIT 1) \
                 OR EXISTS(SELECT 1 FROM _jazz_writer_order_v1 LIMIT 1)",
                [],
                |row| row.get(0),
            )
            .map_err(storage)?;
        if !has_receipts {
            conn.execute(
                "INSERT INTO _jazz_writer_order_profile_v1(slot,value) VALUES(1,?1)",
                [PROFILE],
            )
            .map_err(storage)?;
        }
    }
    Ok(())
}

pub(super) fn require_ready(conn: &Connection) -> Result<(), Error> {
    let value = profile(conn)?.ok_or_else(|| {
        Error::new(
            ErrorCode::UnsupportedStorageCodecs,
            "authority writes require jazz.sqlite.writer-order-profile.v1; \
             this earlier SQLite store has no accepted writer frontier",
        )
    })?;
    valid_profile(&value)
}

fn encode_tx(tx: u64) -> Result<[u8; 8], Error> {
    if tx == 0 || tx >= i64::MAX as u64 {
        return Err(Error::new(
            ErrorCode::Protocol,
            "invalid original transaction for jazz.sqlite.writer-order.v1",
        ));
    }
    Ok(tx.to_be_bytes())
}

fn decode_tx(bytes: &[u8]) -> Result<u64, Error> {
    let invalid = || {
        Error::new(
            ErrorCode::Storage,
            "invalid jazz.sqlite.writer-order.v1 value",
        )
    };
    let fixed: [u8; 8] = bytes.try_into().map_err(|_| invalid())?;
    let tx = u64::from_be_bytes(fixed);
    encode_tx(tx).map_err(|_| invalid())?;
    Ok(tx)
}

impl Inner {
    /// A callback can poll subscriptions or tick while its transaction is open.
    /// Its notification buffer is therefore not a durable list of its writes.
    /// Read only this transaction's exact sequence through each touched table's
    /// existing sequence index; no unrelated rows or history are scanned.
    pub(super) fn record_callback_writer_order(
        &self,
        seq: u64,
        tables: &BTreeSet<String>,
    ) -> Result<(), Error> {
        if self.client.is_some() {
            return Ok(());
        }
        let mut rows = BTreeSet::new();
        for table in tables {
            let mut statement = self
                .conn
                .prepare_cached(&format!("SELECT id,_b FROM {} WHERE _seq=?1", quote(table),))
                .map_err(storage)?;
            let written = statement
                .query_map([seq as i64], |row| {
                    Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .map_err(storage)?;
            for entry in written {
                let (id, key) = entry.map_err(storage)?;
                let id = uuid::Uuid::from_slice(&id).map_err(|error| {
                    Error::new(
                        ErrorCode::Storage,
                        format!("invalid written row id: {error}"),
                    )
                })?;
                rows.insert((table.clone(), RowUuid(id), key));
            }
        }
        let origin = self.local_origin(seq)?;
        self.record_writer_order_rows(*origin.node.as_bytes(), origin.time.0, rows)
    }

    pub(super) fn record_local_writer_order(
        &self,
        seq: u64,
        changed_from: usize,
    ) -> Result<(), Error> {
        // A replica's optimistic publication is not authority acceptance.
        if self.client.is_some() {
            return Ok(());
        }
        let origin = self.local_origin(seq)?;
        self.record_writer_order(*origin.node.as_bytes(), origin.time.0, changed_from)
    }

    /// Called only inside the enclosing commit savepoint, after candidate
    /// permission checks, before fate/allocator publication or notification.
    pub(super) fn record_writer_order(
        &self,
        node: [u8; 16],
        tx: u64,
        changed_from: usize,
    ) -> Result<(), Error> {
        let rows: BTreeSet<_> = self.changed.borrow()[changed_from..]
            .iter()
            .cloned()
            .collect();
        self.record_writer_order_rows(node, tx, rows)
    }

    fn record_writer_order_rows(
        &self,
        node: [u8; 16],
        tx: u64,
        rows: BTreeSet<(String, RowUuid, Vec<u8>)>,
    ) -> Result<(), Error> {
        require_ready(&self.conn)?;
        let encoded = encode_tx(tx)?;
        // Validate the whole touched set before advancing any frontier. All
        // queries address one exact primary key; no row/history walk is used.
        for (table, row, key) in &rows {
            let previous: Option<Vec<u8>> = self.conn.query_row(
                "SELECT tx FROM _jazz_writer_order_v1 WHERE t=?1 AND id=?2 AND k=?3 AND node=?4",
                rusqlite::params![table, row.as_bytes().as_slice(), key, node.as_slice()],
                |row| row.get(0),
            ).optional().map_err(storage)?;
            if previous
                .as_deref()
                .map(decode_tx)
                .transpose()?
                .is_some_and(|prior| tx <= prior)
            {
                return Err(Error::new(
                    ErrorCode::Protocol,
                    "commit arrived after an accepted later write of its writer to the same row",
                ));
            }
        }
        for (table, row, key) in rows {
            self.conn
                .execute(
                    "INSERT INTO _jazz_writer_order_v1(t,id,k,node,tx) VALUES(?1,?2,?3,?4,?5) \
                 ON CONFLICT(t,id,k,node) DO UPDATE SET tx=excluded.tx",
                    rusqlite::params![
                        table,
                        row.as_bytes().as_slice(),
                        key,
                        node.as_slice(),
                        encoded.as_slice()
                    ],
                )
                .map_err(storage)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    /// Alice's frontier stores her original transaction, not an authority seq.
    /// This lower-layer fixture pins metadata bytes with no public row form.
    #[test]
    fn writer_order_v1_pins_profile_and_original_transaction_bytes() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/sqlite_writer_order_v1.json"
        ))
        .unwrap();
        assert_eq!(fixture["role"], "jazz.sqlite.writer-order.v1");
        assert_eq!(fixture["version"], 1);
        assert_eq!(fixture["profile"], hex(PROFILE));
        valid_profile(PROFILE).unwrap();
        for case in fixture["values"].as_array().unwrap() {
            let tx = case["tx"].as_u64().unwrap();
            let encoded = encode_tx(tx).unwrap();
            assert_eq!(hex(&encoded), case["bytes"].as_str().unwrap());
            assert_eq!(decode_tx(&encoded).unwrap(), tx);
        }
    }

    /// Mallory cannot reset Alice's frontier with a truncated, extended or
    /// unsupported encoding. A lower-layer check is needed for corrupt bytes.
    #[test]
    fn writer_order_v1_rejects_malformed_metadata() {
        for bytes in [
            &[][..],
            &[1; 7],
            &[1; 9],
            &[0; 8],
            &[0xff; 8],
            &i64::MAX.to_be_bytes(),
        ] {
            assert_eq!(decode_tx(bytes).unwrap_err().code, ErrorCode::Storage);
        }
        for tx in [0, i64::MAX as u64, u64::MAX] {
            assert_eq!(encode_tx(tx).unwrap_err().code, ErrorCode::Protocol);
        }
        let mut future = PROFILE.to_vec();
        *future.last_mut().unwrap() = 2;
        let mut trailing = PROFILE.to_vec();
        trailing.push(0);
        for bytes in [&[][..], &PROFILE[..PROFILE.len() - 1], &future, &trailing] {
            assert_eq!(
                valid_profile(bytes).unwrap_err().code,
                ErrorCode::UnsupportedStorageCodecs
            );
        }
    }
}
