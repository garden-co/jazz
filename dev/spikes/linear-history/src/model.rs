use std::collections::BTreeMap;
use std::sync::Arc;

pub type TableId = u32;
pub type RowId = u128;
/// Authority-assigned global sequence. `0` is the empty cut before any
/// accepted transaction.
pub type Seq = u64;
/// A nullable cell value.
pub type Value = Option<Vec<u8>>;

/// Hybrid-logical-clock stamp used for LWW; ties break by node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Stamp {
    pub time: u64,
    pub node: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxId {
    pub node: u32,
    pub counter: u64,
}

/// Three-way merge: `(base, ours, theirs) -> merged`, where `ours` is the
/// authority's current value and `theirs` is the value the author wrote over
/// `base`.
pub type MergeFn = fn(Option<&[u8]>, Option<&[u8]>, Option<&[u8]>) -> Value;

#[derive(Clone, Copy)]
pub enum Strategy {
    Lww,
    ThreeWay(MergeFn),
}

impl std::fmt::Debug for Strategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lww => f.write_str("Lww"),
            Self::ThreeWay(_) => f.write_str("ThreeWay"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ColumnDef {
    pub name: &'static str,
    pub strategy: Strategy,
    pub indexed: bool,
}

#[derive(Clone, Debug)]
pub struct TableDef {
    pub id: TableId,
    pub columns: Vec<ColumnDef>,
}

impl TableDef {
    pub fn new(id: TableId) -> Self {
        Self {
            id,
            columns: Vec::new(),
        }
    }

    pub fn column(mut self, name: &'static str) -> Self {
        self.columns.push(ColumnDef {
            name,
            strategy: Strategy::Lww,
            indexed: false,
        });
        self
    }

    pub fn indexed_column(mut self, name: &'static str) -> Self {
        self.columns.push(ColumnDef {
            name,
            strategy: Strategy::Lww,
            indexed: true,
        });
        self
    }

    pub fn merged_column(mut self, name: &'static str, merge: MergeFn) -> Self {
        self.columns.push(ColumnDef {
            name,
            strategy: Strategy::ThreeWay(merge),
            indexed: false,
        });
        self
    }
}

#[derive(Clone, Debug)]
pub struct Schema {
    tables: Arc<BTreeMap<TableId, TableDef>>,
}

impl Schema {
    pub fn new(tables: impl IntoIterator<Item = TableDef>) -> Self {
        Self {
            tables: Arc::new(tables.into_iter().map(|t| (t.id, t)).collect()),
        }
    }

    pub fn table(&self, id: TableId) -> Option<&TableDef> {
        self.tables.get(&id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cell {
    pub stamp: Stamp,
    pub value: Value,
}

/// A full row image. Deletion is an ordinary stamped register on the row, so a
/// content write never implies restoration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowImage {
    pub seq: Seq,
    pub deleted: bool,
    pub deleted_stamp: Stamp,
    pub cells: Vec<Cell>,
}

impl RowImage {
    pub fn empty(columns: usize) -> Self {
        Self {
            seq: 0,
            deleted: false,
            deleted_stamp: Stamp::default(),
            cells: vec![
                Cell {
                    stamp: Stamp::default(),
                    value: None,
                };
                columns
            ],
        }
    }

    pub fn value(&self, column: usize) -> Option<&[u8]> {
        self.cells.get(column).and_then(|c| c.value.as_deref())
    }

    pub fn visible(&self) -> bool {
        !self.deleted
    }

    pub(crate) fn same_content(&self, other: &Self) -> bool {
        self.deleted == other.deleted
            && self.deleted_stamp == other.deleted_stamp
            && self.cells == other.cells
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BaseRef {
    /// The author's base is the confirmed row image at this cut.
    AtSeq(Seq),
    /// The author's base was its own unconfirmed write; ship it.
    Inline(Value),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CellWrite {
    /// LWW overwrite (also allowed on three-way columns, e.g. inserts).
    Set(Value),
    ThreeWay {
        base: BaseRef,
        value: Value,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowWrite {
    pub table: TableId,
    pub row: RowId,
    pub delete: Option<bool>,
    pub cells: Vec<(usize, CellWrite)>,
}

/// Equality predicate read by an exclusive transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EqPredicate {
    pub table: TableId,
    pub column: usize,
    pub value: Value,
}

impl EqPredicate {
    pub fn matches(&self, image: Option<&RowImage>) -> bool {
        image.is_some_and(|i| i.visible() && i.value(self.column) == self.value.as_deref())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TxKind {
    Mergeable,
    Exclusive {
        base: Seq,
        rows_read: Vec<(TableId, RowId)>,
        predicates: Vec<EqPredicate>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tx {
    pub id: TxId,
    pub stamp: Stamp,
    pub kind: TxKind,
    pub writes: Vec<RowWrite>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectReason {
    UnknownTable,
    UnknownColumn,
    /// A `ThreeWay` write named a column without a merge function.
    StrategyMismatch,
    RowConflict,
    PredicateConflict,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Accepted(Seq),
    Rejected(RejectReason),
}
