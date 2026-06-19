//! The Met Open Access dataset (CC0): a CSV with a header row and one museum
//! object per row. The whole row is stored as a JSON object so value sizes stay
//! realistic. Key: `met/<zero-padded Object ID>` (sorted = numeric order).

use anyhow::{Context, Result};
use serde_json::{Map, Value};

pub fn parse(raw: &str, limit: usize) -> Result<Vec<(Vec<u8>, Value)>> {
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(raw.as_bytes());
    let headers = reader.headers().context("met csv header row")?.clone();
    let id_col = headers.iter().position(|h| h == "Object ID");

    let mut rows: Vec<(Vec<u8>, Value)> = Vec::new();
    for record in reader.records() {
        // Tolerate a truncated trailing record (the slice is cut on line count).
        let Ok(record) = record else { continue };
        let mut obj = Map::new();
        for (header, value) in headers.iter().zip(record.iter()) {
            if !value.is_empty() {
                obj.insert(header.to_string(), Value::String(value.to_string()));
            }
        }
        let id = id_col
            .and_then(|i| record.get(i))
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| rows.len().to_string());
        rows.push((format!("met/{id:0>8}").into_bytes(), Value::Object(obj)));
    }

    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows.truncate(limit);
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = "Object ID,Department,Title\n\
        7,Drawings,Study of a Hand\n\
        3,Paintings,Sunrise\n";

    #[test]
    fn parses_sorted_by_object_id_with_fields() {
        let rows = parse(FIXTURE, 10).unwrap();
        assert_eq!(rows.len(), 2);
        // zero-padded keys sort numerically: 3 before 7
        assert_eq!(rows[0].0, b"met/00000003");
        assert_eq!(rows[1].1["Title"], "Study of a Hand");
    }

    #[test]
    fn respects_limit() {
        assert_eq!(parse(FIXTURE, 1).unwrap().len(), 1);
    }
}
