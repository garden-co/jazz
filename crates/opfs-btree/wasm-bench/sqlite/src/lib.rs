use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use sqlite_wasm_rs as ffi; // force-links the sqlite3 C symbols
use sqlite_wasm_vfs::sahpool::{install as install_opfs_sahpool, OpfsSAHPoolCfg};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

const RANGE_WINDOW_KEYS: usize = 128;
const RANGE_RESULT_LIMIT: i64 = 64;

fn log(s: &str) {
    web_sys::console::log_1(&JsValue::from_str(s));
}

fn now_ms() -> f64 {
    let g = js_sys::global();
    let perf = js_sys::Reflect::get(&g, &JsValue::from_str("performance")).unwrap();
    let now = js_sys::Reflect::get(&perf, &JsValue::from_str("now")).unwrap();
    let f: js_sys::Function = now.dyn_into().unwrap();
    f.call0(&perf).unwrap().as_f64().unwrap()
}

// ---- .kv/.ops decoder (mirrors opfs-btree/src/bench_dataset.rs) ----

#[derive(PartialEq, Clone, Copy)]
enum PhaseKind {
    LoadAll,
    GetSeq,
    GetIndices,
    RangeStarts,
    UpdateIndices,
    Mixed,
    ColdGetIndices,
}

struct Phase {
    name: String,
    kind: PhaseKind,
    args: Vec<u32>,
}

struct Reader<'a> {
    b: &'a [u8],
    p: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> &'a [u8] {
        let s = &self.b[self.p..self.p + n];
        self.p += n;
        s
    }
    fn u8(&mut self) -> u8 {
        self.take(1)[0]
    }
    fn u16(&mut self) -> u16 {
        let b = self.take(2);
        u16::from_le_bytes([b[0], b[1]])
    }
    fn u32(&mut self) -> u32 {
        let b = self.take(4);
        u32::from_le_bytes([b[0], b[1], b[2], b[3]])
    }
    fn s(&mut self) -> String {
        let n = self.u8() as usize;
        String::from_utf8_lossy(self.take(n)).into_owned()
    }
}

fn decode_kv(bytes: &[u8]) -> (String, Vec<(Vec<u8>, Vec<u8>)>) {
    let mut r = Reader { b: bytes, p: 0 };
    assert_eq!(r.take(6), b"JZKV1\0", "bad kv magic");
    let profile = r.s();
    let _source = r.s();
    let _enc = r.u8();
    let count = r.u32() as usize;
    let mut recs = Vec::with_capacity(count);
    for _ in 0..count {
        let kl = r.u32() as usize;
        let k = r.take(kl).to_vec();
        let vl = r.u32() as usize;
        let v = r.take(vl).to_vec();
        recs.push((k, v));
    }
    (profile, recs)
}

fn decode_ops(bytes: &[u8]) -> Vec<Phase> {
    let mut r = Reader { b: bytes, p: 0 };
    assert_eq!(r.take(6), b"JZOP1\0", "bad ops magic");
    let pc = r.u16() as usize;
    let mut out = Vec::with_capacity(pc);
    for _ in 0..pc {
        let name = r.s();
        let kind = match r.u8() {
            0 => PhaseKind::LoadAll,
            1 => PhaseKind::GetSeq,
            2 => PhaseKind::GetIndices,
            3 => PhaseKind::RangeStarts,
            4 => PhaseKind::UpdateIndices,
            5 => PhaseKind::Mixed,
            6 => PhaseKind::ColdGetIndices,
            other => panic!("bad phase kind {other}"),
        };
        let ac = r.u32() as usize;
        let mut args = Vec::with_capacity(ac);
        for _ in 0..ac {
            args.push(r.u32());
        }
        out.push(Phase { name, kind, args });
    }
    out
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetPhaseResult {
    pub phase: String,
    pub op_count: u32,
    pub elapsed_ms: f64,
    pub ops_per_sec: f64,
    pub checksum: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetRunResult {
    pub engine: String,
    pub profile: String,
    pub record_count: u32,
    pub phases: Vec<DatasetPhaseResult>,
    pub checksum: u64,
}

const DB_PATH: &str = "sqlite.db";

fn open_conn() -> Result<Connection, JsValue> {
    let conn = Connection::open(DB_PATH).map_err(|e| JsValue::from_str(&format!("open: {e}")))?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-32768;",
    )
    .map_err(|e| JsValue::from_str(&format!("pragma: {e}")))?;
    Ok(conn)
}

/// Replays one phase against rusqlite, returning (ops, checksum, conn). Mirrors
/// the opfs-btree replay so per-phase checksums match.
fn replay_phase(
    conn: Connection,
    phase: &Phase,
    keys: &[&[u8]],
    vals: &[&[u8]],
    n: u32,
) -> Result<(u32, u64, Connection), JsValue> {
    let mut checksum: u64 = 0;
    let mut ops: u32 = 0;
    let idx = |raw: u32| -> usize { (raw % n.max(1)) as usize };
    let err = |c: &str, e: rusqlite::Error| JsValue::from_str(&format!("{c}: {e}"));

    let mut conn = conn;
    match phase.kind {
        PhaseKind::LoadAll => {
            conn.execute_batch("BEGIN").map_err(|e| err("begin", e))?;
            {
                let mut st = conn
                    .prepare("INSERT OR REPLACE INTO kv(k,v) VALUES(?1,?2)")
                    .map_err(|e| err("prep", e))?;
                for i in 0..keys.len() {
                    st.execute((keys[i], vals[i])).map_err(|e| err("ins", e))?;
                    ops += 1;
                }
            }
            conn.execute_batch("COMMIT").map_err(|e| err("commit", e))?;
        }
        PhaseKind::GetSeq => {
            conn.execute_batch("BEGIN").map_err(|e| err("begin", e))?;
            {
                let mut st = conn
                    .prepare("SELECT v FROM kv WHERE k=?1")
                    .map_err(|e| err("prep", e))?;
                for i in 0..keys.len() {
                    if let Ok(v) = st.query_row([keys[i]], |r| r.get::<_, Vec<u8>>(0)) {
                        checksum = checksum.wrapping_add(v.first().copied().unwrap_or(0) as u64);
                    }
                    ops += 1;
                }
            }
            conn.execute_batch("COMMIT").map_err(|e| err("commit", e))?;
        }
        PhaseKind::GetIndices | PhaseKind::ColdGetIndices => {
            if phase.kind == PhaseKind::ColdGetIndices {
                drop(conn);
                conn = open_conn()?;
            }
            conn.execute_batch("BEGIN").map_err(|e| err("begin", e))?;
            {
                let mut st = conn
                    .prepare("SELECT v FROM kv WHERE k=?1")
                    .map_err(|e| err("prep", e))?;
                for &raw in &phase.args {
                    if let Ok(v) = st.query_row([keys[idx(raw)]], |r| r.get::<_, Vec<u8>>(0)) {
                        checksum = checksum.wrapping_add(v.first().copied().unwrap_or(0) as u64);
                    }
                    ops += 1;
                }
            }
            conn.execute_batch("COMMIT").map_err(|e| err("commit", e))?;
        }
        PhaseKind::UpdateIndices => {
            conn.execute_batch("BEGIN").map_err(|e| err("begin", e))?;
            {
                let mut st = conn
                    .prepare("INSERT OR REPLACE INTO kv(k,v) VALUES(?1,?2)")
                    .map_err(|e| err("prep", e))?;
                for &raw in &phase.args {
                    let i = idx(raw);
                    st.execute((keys[i], vals[i])).map_err(|e| err("upd", e))?;
                    ops += 1;
                }
            }
            conn.execute_batch("COMMIT").map_err(|e| err("commit", e))?;
        }
        PhaseKind::RangeStarts => {
            conn.execute_batch("BEGIN").map_err(|e| err("begin", e))?;
            {
                let mut st = conn
                    .prepare("SELECT v FROM kv WHERE k>=?1 AND k<?2 ORDER BY k LIMIT ?3")
                    .map_err(|e| err("prep", e))?;
                for &raw in &phase.args {
                    let s = idx(raw);
                    let e = (s + RANGE_WINDOW_KEYS).min(keys.len().saturating_sub(1));
                    let rows = st
                        .query_map(
                            rusqlite::params![keys[s], keys[e], RANGE_RESULT_LIMIT],
                            |_| Ok(()),
                        )
                        .map_err(|er| err("range", er))?
                        .count();
                    checksum = checksum.wrapping_add(rows as u64);
                    ops += 1;
                }
            }
            conn.execute_batch("COMMIT").map_err(|e| err("commit", e))?;
        }
        PhaseKind::Mixed => {
            conn.execute_batch("BEGIN").map_err(|e| err("begin", e))?;
            {
                let mut put = conn
                    .prepare("INSERT OR REPLACE INTO kv(k,v) VALUES(?1,?2)")
                    .map_err(|e| err("prep", e))?;
                let mut del = conn
                    .prepare("DELETE FROM kv WHERE k=?1")
                    .map_err(|e| err("prep", e))?;
                let mut get = conn
                    .prepare("SELECT v FROM kv WHERE k=?1")
                    .map_err(|e| err("prep", e))?;
                for &packed in &phase.args {
                    let op = packed >> 30;
                    let i = idx(packed & 0x3FFF_FFFF);
                    match op {
                        1 => {
                            put.execute((keys[i], vals[i]))
                                .map_err(|e| err("mput", e))?;
                        }
                        2 => {
                            del.execute([keys[i]]).map_err(|e| err("mdel", e))?;
                        }
                        _ => {
                            if let Ok(v) = get.query_row([keys[i]], |r| r.get::<_, Vec<u8>>(0)) {
                                checksum =
                                    checksum.wrapping_add(v.first().copied().unwrap_or(0) as u64);
                            }
                        }
                    }
                    ops += 1;
                }
            }
            conn.execute_batch("COMMIT").map_err(|e| err("commit", e))?;
        }
    }
    Ok((ops, checksum, conn))
}

pub async fn run_sqlite_dataset_result(kv: &[u8], ops: &[u8]) -> Result<DatasetRunResult, JsValue> {
    let (profile, records) = decode_kv(kv);
    let phases = decode_ops(ops);
    let keys: Vec<&[u8]> = records.iter().map(|(k, _)| k.as_slice()).collect();
    let vals: Vec<&[u8]> = records.iter().map(|(_, v)| v.as_slice()).collect();
    let n = keys.len() as u32;

    log(&format!(
        "[sqlite] {profile}: install sahpool + open ({n} records)"
    ));
    install_opfs_sahpool::<ffi::WasmOsCallback>(&OpfsSAHPoolCfg::default(), true)
        .await
        .map_err(|e| JsValue::from_str(&format!("install sahpool: {e:?}")))?;

    let mut conn = open_conn()?;
    conn.execute_batch(
        "DROP TABLE IF EXISTS kv; CREATE TABLE kv(k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID;",
    )
    .map_err(|e| JsValue::from_str(&format!("create: {e}")))?;

    let mut overall = n as u64;
    let mut phase_results = Vec::new();
    for phase in &phases {
        let started = now_ms();
        let (op_count, checksum, c) = replay_phase(conn, phase, &keys, &vals, n)?;
        conn = c;
        let elapsed = now_ms() - started;
        overall = overall.wrapping_add(checksum);
        phase_results.push(DatasetPhaseResult {
            phase: phase.name.clone(),
            op_count,
            elapsed_ms: elapsed,
            ops_per_sec: if elapsed > 0.0 {
                (op_count as f64) / (elapsed / 1000.0)
            } else {
                0.0
            },
            checksum,
        });
    }

    Ok(DatasetRunResult {
        engine: "sqlite_inproc".into(),
        profile,
        record_count: n,
        phases: phase_results,
        checksum: overall,
    })
}
