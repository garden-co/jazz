//! Result rows (raw measurements) and the boxed-table renderer.

use std::time::Duration;

pub(crate) struct Row {
    pub(crate) topology: &'static str,
    pub(crate) write: Duration,
    pub(crate) rows: usize,
    pub(crate) lookup: Duration,
    /// Durability tier the lookup ran at (`None` for native raw gets).
    pub(crate) tier: Option<&'static str>,
    pub(crate) physical: u64,
    pub(crate) logical: u64,
}

fn fmt_write(d: Duration) -> String {
    let s = d.as_secs_f64();
    if s < 1.0 {
        format!("{s:.3} s")
    } else {
        format!("{s:.1} s")
    }
}

fn fmt_throughput(rows: usize, d: Duration) -> String {
    let r = rows as f64 / d.as_secs_f64().max(1e-9);
    if r >= 1e6 {
        format!("{:.2}M rows/s", r / 1e6)
    } else if r >= 1e3 {
        format!("{:.1}k rows/s", r / 1e3)
    } else {
        format!("{r:.0} rows/s")
    }
}

fn fmt_lookup(d: Duration, tier: Option<&str>) -> String {
    let us = d.as_secs_f64() * 1e6;
    let latency = if us < 1000.0 {
        format!("{us:.1} µs/lookup")
    } else {
        format!("{:.0} ms/lookup", us / 1000.0)
    };
    match tier {
        Some(tier) => format!("{latency} · {tier}"),
        None => latency,
    }
}

fn fmt_size(physical: u64, logical: u64) -> String {
    format!(
        "{:.1} MB ({:.1}× logical)",
        physical as f64 / 1e6,
        physical as f64 / logical.max(1) as f64
    )
}

pub(crate) fn render(rows: &[Row]) {
    let headers = ["topology", "write all", "throughput", "get by id", "size"];
    let table: Vec<Vec<String>> = rows
        .iter()
        .map(|r| {
            vec![
                r.topology.to_string(),
                fmt_write(r.write),
                fmt_throughput(r.rows, r.write),
                fmt_lookup(r.lookup, r.tier),
                fmt_size(r.physical, r.logical),
            ]
        })
        .collect();
    print_box(&headers, &table);
}

/// Render a boxed table. Column count is derived from `headers`, so adding a
/// column only means adding a header and a cell — no widths to keep in lockstep.
fn print_box(headers: &[&str], rows: &[Vec<String>]) {
    let ncol = headers.len();
    let mut w: Vec<usize> = headers.iter().map(|h| h.chars().count()).collect();
    for row in rows {
        for (width, cell) in w.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let border = |l: &str, m: &str, r: &str| {
        let mut s = String::from(l);
        for (i, width) in w.iter().enumerate() {
            s.push_str(&"─".repeat(width + 2));
            s.push_str(if i + 1 < ncol { m } else { r });
        }
        s
    };
    let row_line = |cells: &[String]| {
        let mut s = String::from("│");
        for (&width, cell) in w.iter().zip(cells) {
            s.push_str(&format!(" {cell:<width$} │"));
        }
        s
    };

    println!("{}", border("┌", "┬", "┐"));
    let header_cells: Vec<String> = headers.iter().map(|h| h.to_string()).collect();
    println!("{}", row_line(&header_cells));
    for row in rows {
        println!("{}", border("├", "┼", "┤"));
        println!("{}", row_line(row));
    }
    println!("{}", border("└", "┴", "┘"));
}
