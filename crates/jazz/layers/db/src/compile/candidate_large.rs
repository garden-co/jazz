//! Bounded logical-cell questions for the opt-in created-witness executor.
//!
//! Ordinary queries keep SPEC 19.5 cell comparisons. Only a candidate evidence
//! scope records these questions, whose cells are obtained by indexed walks
//! from the captured policy roots before the permission statement runs.
use super::*;

#[derive(Clone, Debug)]
pub(crate) enum Prep {
    Match {
        slot: i64,
        table: String,
        column: String,
        test: MatchTest,
    },
    Class {
        slot: i64,
        columns: Vec<(String, String)>,
    },
}

#[derive(Clone, Debug)]
pub(crate) enum MatchTest {
    Contains(Vec<u8>),
    Cmp(CmpOp, Vec<u8>),
}

impl Prep {
    pub(crate) fn slot(&self) -> i64 {
        match self {
            Self::Match { slot, .. } | Self::Class { slot, .. } => *slot,
        }
    }
}

fn scratch_has(slot: i64, cell: &str) -> String {
    format!("EXISTS (SELECT 1 FROM _jazz_candidate_lv_v2 WHERE slot={slot} AND cell={cell})")
}
fn scratch_value(slot: i64, cell: &str) -> String {
    format!("(SELECT v FROM _jazz_candidate_lv_v2 WHERE slot={slot} AND cell={cell})")
}

impl Ctx<'_> {
    fn candidate_prep(&mut self, make: impl FnOnce(i64) -> Prep) -> i64 {
        // Compiler-walk ordinals are deterministic for one statement and do
        // not merge different questions through a truncated-hash collision.
        let slot = self.preps.len() as i64 + 1;
        self.preps.push(make(slot));
        slot
    }

    fn candidate_match(&mut self, column: &LargeColumn, test: MatchTest) -> i64 {
        let (table, column) = (column.table.clone(), column.column.clone());
        self.candidate_prep(|slot| Prep::Match {
            slot,
            table,
            column,
            test,
        })
    }

    pub(super) fn candidate_large_literal(
        &mut self,
        op: &CmpOp,
        column: &LargeColumn,
        p: &str,
        literal: &[u8],
    ) -> String {
        let slot = self.candidate_match(column, MatchTest::Cmp(op.clone(), literal.to_vec()));
        let c = &column.cell;
        format!(
            "(CASE WHEN {} THEN {} ELSE {} END)",
            crate::large::chunked_sql(c),
            scratch_has(slot, c),
            cmp_plain(op, c, p)
        )
    }

    pub(super) fn candidate_large_pair(
        &mut self,
        op: &CmpOp,
        a: &LargeColumn,
        b: &LargeColumn,
    ) -> String {
        let mut columns = vec![
            (a.table.clone(), a.column.clone()),
            (b.table.clone(), b.column.clone()),
        ];
        columns.sort();
        columns.dedup();
        let slot = self.candidate_prep(|slot| Prep::Class { slot, columns });
        let (ca, cb) = (&a.cell, &b.cell);
        // Missing answers never become a grant through NULL IS NULL. A class
        // comparison requires both exact captured cells to have been answered.
        let ac = crate::large::chunked_sql(ca);
        let bc = crate::large::chunked_sql(cb);
        let compared = cmp_plain(op, &scratch_value(slot, ca), &scratch_value(slot, cb));
        format!(
            "(CASE WHEN {ac} AND {bc} THEN ({} AND {} AND {compared}) WHEN {ac} OR {bc} THEN {} ELSE {} END)",
            scratch_has(slot, ca),
            scratch_has(slot, cb),
            bool_sql(*op == CmpOp::Ne),
            cmp_plain(op, ca, cb)
        )
    }

    pub(super) fn candidate_large_contains(
        &mut self,
        column: &LargeColumn,
        p: &str,
        literal: &[u8],
    ) -> String {
        let slot = self.candidate_match(column, MatchTest::Contains(literal.to_vec()));
        let c = &column.cell;
        format!(
            "(CASE WHEN {} THEN {} ELSE COALESCE(instr(CAST({c} AS BLOB),CAST({p} AS BLOB))>0,0) END)",
            crate::large::chunked_sql(c),
            scratch_has(slot, c)
        )
    }
}
