//! `dump-show <dump>`: print a dump's records, long values shortened. It
//! prints row data, so only run it on synthetic dumps or inside the cluster.

use migrate_alpha59_dump::{Reader, Record, Value};

fn short(value: &Value) -> String {
    match value {
        Value::String(text) if text.len() > 40 => {
            format!("String({:?}... {} bytes)", &text[..20], text.len())
        }
        Value::Bytes(bytes) if bytes.len() > 40 => format!("Bytes({} bytes)", bytes.len()),
        Value::Nullable(Some(inner)) => format!("Some({})", short(inner)),
        other => format!("{other:?}"),
    }
}

fn main() {
    let path = std::env::args().nth(1).expect("usage: dump-show <dump>");
    let file = std::io::BufReader::new(std::fs::File::open(path).expect("open dump"));
    let (mut reader, header) = Reader::open(file).expect("read header");
    println!("{header:?}");
    while let Some(record) = reader.next_record().expect("read record") {
        match record {
            Record::Version(mut version) => {
                let cells: Vec<String> = std::mem::take(&mut version.cells)
                    .iter()
                    .map(|(name, value)| format!("{name}={}", short(value)))
                    .collect();
                println!(
                    "Version {} row={:02x?} tx_time={} global={:?} fate={} deletion={:?} by={:?} cells=[{}]",
                    version.table,
                    &version.row[..2],
                    version.tx_time >> 18,
                    version.global_time,
                    version.fate,
                    version.deletion,
                    version.updated_by,
                    cells.join(", ")
                );
            }
            other => println!("{other:?}"),
        }
    }
    println!("{:?}", reader.footer());
}
