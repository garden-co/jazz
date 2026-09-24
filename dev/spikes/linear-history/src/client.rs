//! A client: confirmed rows plus a pending transaction log replayed on top.

use std::collections::BTreeMap;

use crate::model::*;

pub struct Client {
    node: u32,
    clock: u64,
    counter: u64,
    schema: Schema,
    confirmed: BTreeMap<(TableId, RowId), RowImage>,
    pending: Vec<Tx>,
}

impl Client {
    pub fn new(node: u32, schema: Schema) -> Self {
        Self {
            node,
            clock: 0,
            counter: 0,
            schema,
            confirmed: BTreeMap::new(),
            pending: Vec::new(),
        }
    }

    pub fn pending(&self) -> &[Tx] {
        &self.pending
    }

    pub fn begin(&mut self) -> TxBuilder<'_> {
        TxBuilder {
            client: self,
            writes: Vec::new(),
        }
    }

    /// Absorb authoritative row images (e.g. from `changes_since`).
    pub fn receive(&mut self, table: TableId, rows: impl IntoIterator<Item = (RowId, RowImage)>) {
        for (row, image) in rows {
            let newest = image
                .cells
                .iter()
                .map(|c| c.stamp.time)
                .chain([image.deleted_stamp.time])
                .max()
                .unwrap_or(0);
            self.clock = self.clock.max(newest);
            self.confirmed.insert((table, row), image);
        }
    }

    /// Drop a decided transaction from the pending log. A rejection needs no
    /// cascade: later pending transactions simply replay without it.
    pub fn settle(&mut self, id: TxId) {
        self.pending.retain(|tx| tx.id != id);
    }

    /// Local view: confirmed image with every pending write replayed in order.
    pub fn view(&self, table: TableId, row: RowId) -> Option<RowImage> {
        self.view_through(table, row, self.pending.len())
    }

    fn view_through(&self, table: TableId, row: RowId, pending: usize) -> Option<RowImage> {
        let mut image = self.confirmed.get(&(table, row)).cloned();
        for tx in &self.pending[..pending] {
            for write in tx
                .writes
                .iter()
                .filter(|w| w.table == table && w.row == row)
            {
                let columns = self.schema.table(table).map_or(0, |t| t.columns.len());
                apply_local(
                    image.get_or_insert_with(|| RowImage::empty(columns)),
                    tx.stamp,
                    write,
                );
            }
        }
        image
    }

    fn row_has_pending(&self, table: TableId, row: RowId) -> bool {
        self.pending
            .iter()
            .flat_map(|tx| &tx.writes)
            .any(|w| w.table == table && w.row == row)
    }
}

fn apply_local(image: &mut RowImage, stamp: Stamp, write: &RowWrite) {
    if let Some(deleted) = write.delete {
        image.deleted = deleted;
        image.deleted_stamp = stamp;
    }
    for (column, cell) in &write.cells {
        let value = match cell {
            CellWrite::Set(value) | CellWrite::ThreeWay { value, .. } => value.clone(),
        };
        image.cells[*column] = Cell { stamp, value };
    }
}

pub struct TxBuilder<'a> {
    client: &'a mut Client,
    writes: Vec<RowWrite>,
}

impl TxBuilder<'_> {
    fn row_write(&mut self, table: TableId, row: RowId) -> &mut RowWrite {
        if let Some(i) = self
            .writes
            .iter()
            .position(|w| w.table == table && w.row == row)
        {
            return &mut self.writes[i];
        }
        self.writes.push(RowWrite {
            table,
            row,
            delete: None,
            cells: Vec::new(),
        });
        self.writes.last_mut().unwrap()
    }

    pub fn set(mut self, table: TableId, row: RowId, column: usize, value: Value) -> Self {
        self.row_write(table, row)
            .cells
            .push((column, CellWrite::Set(value)));
        self
    }

    pub fn delete(mut self, table: TableId, row: RowId, deleted: bool) -> Self {
        self.row_write(table, row).delete = Some(deleted);
        self
    }

    /// Three-way write of `value` over the value this client currently sees.
    /// The base is a history reference when the client sees confirmed state,
    /// and shipped inline when it sees its own unconfirmed write.
    pub fn merge(mut self, table: TableId, row: RowId, column: usize, value: Value) -> Self {
        let client = &*self.client;
        let base = if client.row_has_pending(table, row) {
            BaseRef::Inline(
                client
                    .view(table, row)
                    .and_then(|i| i.cells[column].value.clone()),
            )
        } else {
            BaseRef::AtSeq(client.confirmed.get(&(table, row)).map_or(0, |i| i.seq))
        };
        self.row_write(table, row)
            .cells
            .push((column, CellWrite::ThreeWay { base, value }));
        self
    }

    pub fn commit(self) -> Tx {
        self.commit_as(TxKind::Mergeable)
    }

    pub fn commit_as(self, kind: TxKind) -> Tx {
        let client = self.client;
        client.clock += 1;
        client.counter += 1;
        let tx = Tx {
            id: TxId {
                node: client.node,
                counter: client.counter,
            },
            stamp: Stamp {
                time: client.clock,
                node: client.node,
            },
            kind,
            writes: self.writes,
        };
        client.pending.push(tx.clone());
        tx
    }
}
