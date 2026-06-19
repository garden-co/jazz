//! Wikipedia article wikitext (CC BY-SA), as JSON Lines of `{title, text}`.
//! The whole object is stored as the record value (large text). Key:
//! `wiki/<title>` (sorted).

use anyhow::{Context, Result};
use serde_json::Value;

pub fn parse(raw: &str, limit: usize) -> Result<Vec<(Vec<u8>, Value)>> {
    let mut rows: Vec<(Vec<u8>, Value)> = Vec::new();
    for line in raw.lines().filter(|l| !l.trim().is_empty()) {
        let value: Value = serde_json::from_str(line).context("wikipedia jsonl line")?;
        let title = value
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        rows.push((format!("wiki/{title}").into_bytes(), value));
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows.truncate(limit);
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = "{\"title\":\"Zebra\",\"text\":\"A zebra is...\"}\n\
        {\"title\":\"Apple\",\"text\":\"An apple is...\"}\n";

    #[test]
    fn parses_sorted_by_title_with_text() {
        let rows = parse(FIXTURE, 10).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, b"wiki/Apple");
        assert_eq!(rows[1].1["text"], "A zebra is...");
    }

    #[test]
    fn respects_limit() {
        assert_eq!(parse(FIXTURE, 1).unwrap().len(), 1);
    }
}
