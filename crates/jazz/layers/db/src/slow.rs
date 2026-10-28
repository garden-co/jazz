//! Slow-query log. A store has one writer connection, so a read that runs
//! long delays every other session it serves. Each query statement is
//! timed; one that takes at least the threshold is recorded under its
//! shape (the compiled SQL, which binds every literal as a parameter),
//! with what SQLite did for it and its query plan, until the host takes
//! the log to show the developer: every host takes it after ticking
//! ([`crate::Db::take_slow_query_report`]), which reports the first slow
//! query right away and then at most every [`REPORT_EVERY`], and writes one
//! line per shape ([`SlowQuery::line`]) where that host reports problems
//! (the server's log, the browser or React Native console, a Rust client's
//! tracing). Nothing is refused or reordered: the log only reports.

use std::collections::HashMap;
use std::time::Duration;

/// Default threshold: about ten server ticks' worth of writer time.
pub const DEFAULT_SLOW_QUERY_THRESHOLD: Duration = Duration::from_millis(50);

/// How often a host reports: each report sums a shape's slow runs since
/// the last, so a hot slow query is one line per window.
pub const REPORT_EVERY: Duration = Duration::from_secs(10);

/// Shapes kept per window; later new shapes only count as dropped.
const MAX_SHAPES: usize = 256;

/// One slow query shape over the window since the log was last taken.
#[derive(Clone, Debug, PartialEq)]
pub struct SlowQuery {
    /// The table the query reads from (as the query names it).
    pub table: String,
    /// The compiled statement; every literal is a bound parameter.
    pub sql: String,
    /// Runs at or over the threshold in this window.
    pub runs: u64,
    /// Writer time those runs took, together.
    pub total: Duration,
    /// The slowest run.
    pub slowest: Duration,
    /// Rows the slowest run returned.
    pub rows: u64,
    /// Rows the slowest run stepped through in full table scans
    /// (`SQLITE_STMTSTATUS_FULLSCAN_STEP`): a large number means no index
    /// served a filter.
    pub scanned: u64,
    /// The slowest run sorted rows no index ordered
    /// (`SQLITE_STMTSTATUS_SORT`).
    pub sorted: bool,
    /// SQLite's query plan, one step per line.
    pub plan: String,
}

impl SlowQuery {
    /// What would most likely make this query fast, read off its plan.
    pub fn hint(&self) -> Option<String> {
        let scans: Vec<&str> = self
            .plan
            .lines()
            .filter_map(|step| {
                let step = step.trim_start_matches([' ', '|', '-', '`']);
                let rest = step.strip_prefix("SCAN ")?;
                // A covering index answers from the index alone; any other
                // scan, by the table or by an index, reads every row.
                if rest.contains(" USING COVERING INDEX ") {
                    return None;
                }
                let alias = rest.split(' ').next()?;
                // The compiled statement names its root table `t`.
                Some(if alias == "t" {
                    self.table.as_str()
                } else {
                    alias
                })
            })
            .collect();
        let mut hints = Vec::new();
        if !scans.is_empty() {
            hints.push(format!(
                "scans all of {}: an index on the columns this query filters or joins on would let it read only the matching rows",
                scans.join(", ")
            ));
        }
        if self.sorted || self.plan.contains("USE TEMP B-TREE FOR ORDER BY") {
            hints.push(
                "sorts its rows after reading them: an index on the ordering columns would return them in order"
                    .to_owned(),
            );
        }
        (!hints.is_empty()).then(|| hints.join("; "))
    }

    /// The report line for this shape, naming `app` when the host knows it.
    pub fn line(&self, app: Option<&str>) -> String {
        let app = app.map(|app| format!(" in app {app}")).unwrap_or_default();
        let ms = |d: Duration| d.as_secs_f64() * 1000.0;
        let mut line = format!(
            "slow query{app} on {}: {} slow run(s), slowest {:.0} ms, {:.0} ms of database time in all \
             (slowest run: {} row(s) returned, {} row(s) scanned{})",
            self.table,
            self.runs,
            ms(self.slowest),
            ms(self.total),
            self.rows,
            self.scanned,
            if self.sorted { ", sorted" } else { "" },
        );
        if let Some(hint) = self.hint() {
            line.push_str(&format!(". It {hint}"));
        }
        line.push_str(&format!(
            ". Plan: {}. SQL: {}",
            self.plan.replace('\n', " | "),
            self.sql
        ));
        line
    }
}

/// What one statement run cost.
#[derive(Clone, Copy, Default)]
pub(crate) struct Run {
    pub elapsed: Duration,
    pub rows: u64,
    pub scanned: u64,
    pub sorted: bool,
}

pub(crate) struct SlowLog {
    threshold: Option<Duration>,
    shapes: HashMap<String, SlowQuery>,
    /// New shapes not kept because the window was full.
    dropped: u64,
    /// Plans already read, by statement: a plan does not change while the
    /// schema does not, and reading one prepares the statement again.
    plans: HashMap<String, String>,
    /// The SQLite schema version `plans` were read under: any DDL (a new
    /// index, a schema change) bumps it and they are read again.
    plans_version: Option<i64>,
    /// When the last report was taken (`None`: none yet, so the first slow
    /// query is reported right away).
    reported: Option<web_time::Instant>,
}

impl Default for SlowLog {
    fn default() -> Self {
        Self {
            threshold: Some(DEFAULT_SLOW_QUERY_THRESHOLD),
            shapes: HashMap::new(),
            dropped: 0,
            plans: HashMap::new(),
            plans_version: None,
            reported: None,
        }
    }
}

impl SlowLog {
    pub fn set_threshold(&mut self, threshold: Option<Duration>) {
        self.threshold = threshold;
    }

    /// Whether a run that took `elapsed` is slow.
    pub fn is_slow(&self, elapsed: Duration) -> bool {
        self.threshold.is_some_and(|threshold| elapsed >= threshold)
    }

    /// Record a slow run of `sql`. `schema_version` is SQLite's schema
    /// version now (`None` if unknown: the plan is read afresh).
    pub fn record(
        &mut self,
        table: &str,
        sql: &str,
        run: Run,
        schema_version: Option<i64>,
        plan: impl FnOnce() -> String,
    ) {
        if !self.shapes.contains_key(sql) && self.shapes.len() >= MAX_SHAPES {
            self.dropped += 1;
            return;
        }
        if schema_version.is_none() || schema_version != self.plans_version {
            self.plans.clear();
            self.plans_version = schema_version;
        }
        if !self.plans.contains_key(sql) {
            if self.plans.len() >= MAX_SHAPES {
                self.plans.clear();
            }
            self.plans.insert(sql.to_owned(), plan());
        }
        let plan = &self.plans[sql];
        let entry = self
            .shapes
            .entry(sql.to_owned())
            .or_insert_with(|| SlowQuery {
                table: table.to_owned(),
                sql: sql.to_owned(),
                runs: 0,
                total: Duration::ZERO,
                slowest: Duration::ZERO,
                rows: 0,
                scanned: 0,
                sorted: false,
                plan: plan.clone(),
            });
        // A plan read after DDL in this window replaces the stale one.
        if entry.plan != *plan {
            entry.plan = plan.clone();
        }
        entry.runs += 1;
        entry.total += run.elapsed;
        if run.elapsed >= entry.slowest {
            entry.slowest = run.elapsed;
            entry.rows = run.rows;
            entry.scanned = run.scanned;
            entry.sorted = run.sorted;
        }
    }

    /// The window's slow shapes, slowest total first, and how many new
    /// shapes did not fit; starts a new window.
    pub fn take(&mut self) -> (Vec<SlowQuery>, u64) {
        let mut shapes: Vec<SlowQuery> = self.shapes.drain().map(|(_, shape)| shape).collect();
        shapes.sort_by(|a, b| b.total.cmp(&a.total));
        (shapes, std::mem::take(&mut self.dropped))
    }

    /// [`SlowLog::take`] when there is something to report and
    /// [`REPORT_EVERY`] passed since the last report, else nothing.
    pub fn take_due(&mut self) -> Option<(Vec<SlowQuery>, u64)> {
        if self.shapes.is_empty() && self.dropped == 0 {
            return None;
        }
        if self
            .reported
            .is_some_and(|reported| reported.elapsed() < REPORT_EVERY)
        {
            return None;
        }
        self.reported = Some(web_time::Instant::now());
        Some(self.take())
    }
}

/// Report lines for a window's shapes and its dropped count.
pub fn report_lines(shapes: &[SlowQuery], dropped: u64, app: Option<&str>) -> Vec<String> {
    let mut lines: Vec<String> = shapes.iter().map(|shape| shape.line(app)).collect();
    if dropped > 0 {
        let app = app.map(|app| format!(" in app {app}")).unwrap_or_default();
        lines.push(format!(
            "{dropped} more slow query shape(s){app} than one report holds"
        ));
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(plan: &str, sorted: bool) -> SlowQuery {
        SlowQuery {
            table: "t".into(),
            sql: "SELECT".into(),
            runs: 1,
            total: Duration::ZERO,
            slowest: Duration::ZERO,
            rows: 0,
            scanned: 0,
            sorted,
            plan: plan.into(),
        }
    }

    #[test]
    fn hints_name_scanned_tables_and_unindexed_sorts() {
        assert_eq!(
            shape("SEARCH docs USING INDEX docs_by_owner (owner=?)", false).hint(),
            None
        );
        assert_eq!(
            shape("SCAN docs USING COVERING INDEX docs_by_owner", false).hint(),
            None
        );
        let hint = shape("SCAN docs\nUSE TEMP B-TREE FOR ORDER BY", false)
            .hint()
            .unwrap();
        assert!(hint.contains("scans all of docs"), "{hint}");
        assert!(hint.contains("ordering columns"), "{hint}");
    }

    #[test]
    fn a_window_aggregates_runs_per_shape_and_keeps_the_slowest() {
        let mut log = SlowLog::default();
        let run = |ms, rows| Run {
            elapsed: Duration::from_millis(ms),
            rows,
            scanned: rows,
            sorted: false,
        };
        let mut plans = 0;
        for (ms, rows) in [(60, 1), (90, 2), (70, 3)] {
            log.record("t", "SELECT a", run(ms, rows), Some(1), || {
                plans += 1;
                "SCAN t".into()
            });
        }
        log.record("u", "SELECT b", run(55, 9), Some(1), || "SCAN u".into());
        let (shapes, dropped) = log.take();
        assert_eq!(plans, 1, "a plan is read once per shape");
        assert_eq!(dropped, 0);
        assert_eq!(shapes.len(), 2);
        assert_eq!(shapes[0].sql, "SELECT a");
        assert_eq!(shapes[0].runs, 3);
        assert_eq!(shapes[0].total, Duration::from_millis(220));
        assert_eq!(shapes[0].slowest, Duration::from_millis(90));
        assert_eq!(shapes[0].rows, 2);
        assert!(log.take().0.is_empty(), "taking starts a new window");
    }

    #[test]
    fn a_schema_change_reads_the_plan_again() {
        let mut log = SlowLog::default();
        let run = Run {
            elapsed: Duration::from_millis(60),
            ..Run::default()
        };
        log.record("t", "SELECT a", run, Some(1), || "SCAN t".into());
        log.record("t", "SELECT a", run, Some(1), || unreachable!("cached"));
        // An index was created: the schema version moved.
        log.record("t", "SELECT a", run, Some(2), || {
            "SEARCH t USING INDEX t_by_a (a=?)".into()
        });
        let (shapes, _) = log.take();
        assert_eq!(shapes[0].plan, "SEARCH t USING INDEX t_by_a (a=?)");
        assert_eq!(shapes[0].hint(), None);
    }

    #[test]
    fn a_line_names_the_app_hint_plan_and_sql() {
        let mut slow = shape("SCAN t\nUSE TEMP B-TREE FOR ORDER BY", true);
        slow.table = "tasks".into();
        slow.slowest = Duration::from_millis(182);
        let line = slow.line(Some("my-app"));
        assert!(
            line.starts_with("slow query in app my-app on tasks: 1 slow run(s), slowest 182 ms"),
            "{line}"
        );
        assert!(line.contains("scans all of tasks"), "{line}");
        assert!(
            line.contains("Plan: SCAN t | USE TEMP B-TREE FOR ORDER BY"),
            "{line}"
        );
        assert!(line.ends_with("SQL: SELECT"), "{line}");
        assert!(!slow.line(None).contains(" in app "));
    }

    #[test]
    fn the_first_slow_query_is_reported_at_once_then_once_per_window() {
        let mut log = SlowLog::default();
        assert!(log.take_due().is_none(), "nothing to report");
        let run = Run {
            elapsed: Duration::from_millis(60),
            ..Run::default()
        };
        log.record("t", "SELECT a", run, Some(1), || "SCAN t".into());
        assert_eq!(log.take_due().unwrap().0.len(), 1);
        log.record("t", "SELECT a", run, Some(1), || "SCAN t".into());
        assert!(log.take_due().is_none(), "within the window");
        assert_eq!(log.take().0[0].runs, 1, "kept for the next report");
    }
}
