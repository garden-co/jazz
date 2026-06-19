//! Canonical static-file format shared by the benchmark data tools (producer)
//! and the in-browser engine drivers (consumers). Pure byte (de)serialization;
//! no I/O.

const KV_MAGIC: &[u8; 6] = b"JZKV1\0";
const OPS_MAGIC: &[u8; 6] = b"JZOP1\0";

pub const RANGE_WINDOW_KEYS: u32 = 128;
pub const RANGE_RESULT_LIMIT: u32 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueEncoding {
    Cbor,
    Json,
}

impl ValueEncoding {
    fn to_u8(self) -> u8 {
        match self {
            ValueEncoding::Cbor => 0,
            ValueEncoding::Json => 1,
        }
    }
    fn from_u8(b: u8) -> Result<Self, FormatError> {
        match b {
            0 => Ok(ValueEncoding::Cbor),
            1 => Ok(ValueEncoding::Json),
            other => Err(FormatError::BadEncoding(other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhaseKind {
    LoadAll,
    GetSeq,
    GetIndices,
    RangeStarts,
    UpdateIndices,
    Mixed,
    ColdGetIndices,
}

impl PhaseKind {
    fn to_u8(self) -> u8 {
        match self {
            PhaseKind::LoadAll => 0,
            PhaseKind::GetSeq => 1,
            PhaseKind::GetIndices => 2,
            PhaseKind::RangeStarts => 3,
            PhaseKind::UpdateIndices => 4,
            PhaseKind::Mixed => 5,
            PhaseKind::ColdGetIndices => 6,
        }
    }
    fn from_u8(b: u8) -> Result<Self, FormatError> {
        Ok(match b {
            0 => PhaseKind::LoadAll,
            1 => PhaseKind::GetSeq,
            2 => PhaseKind::GetIndices,
            3 => PhaseKind::RangeStarts,
            4 => PhaseKind::UpdateIndices,
            5 => PhaseKind::Mixed,
            6 => PhaseKind::ColdGetIndices,
            other => return Err(FormatError::BadPhaseKind(other)),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Phase {
    pub name: String,
    pub kind: PhaseKind,
    pub args: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvDataset {
    pub profile: String,
    pub source: String,
    pub encoding: ValueEncoding,
    pub records: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatError {
    BadMagic,
    Truncated,
    BadEncoding(u8),
    BadPhaseKind(u8),
}

impl core::fmt::Display for FormatError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for FormatError {}

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn put_str(out: &mut Vec<u8>, s: &str) {
    out.push(s.len() as u8);
    out.extend_from_slice(s.as_bytes());
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}
impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], FormatError> {
        let end = self.pos.checked_add(n).ok_or(FormatError::Truncated)?;
        let slice = self.buf.get(self.pos..end).ok_or(FormatError::Truncated)?;
        self.pos = end;
        Ok(slice)
    }
    fn u8(&mut self) -> Result<u8, FormatError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, FormatError> {
        let b = self.take(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }
    fn u32(&mut self) -> Result<u32, FormatError> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn str(&mut self) -> Result<String, FormatError> {
        let len = self.u8()? as usize;
        let bytes = self.take(len)?;
        Ok(String::from_utf8_lossy(bytes).into_owned())
    }
}

pub fn encode_kv(
    profile: &str,
    source: &str,
    encoding: ValueEncoding,
    records: &[(Vec<u8>, Vec<u8>)],
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(KV_MAGIC);
    put_str(&mut out, profile);
    put_str(&mut out, source);
    out.push(encoding.to_u8());
    put_u32(&mut out, records.len() as u32);
    for (k, v) in records {
        put_u32(&mut out, k.len() as u32);
        out.extend_from_slice(k);
        put_u32(&mut out, v.len() as u32);
        out.extend_from_slice(v);
    }
    out
}

pub fn decode_kv(bytes: &[u8]) -> Result<KvDataset, FormatError> {
    let mut r = Reader::new(bytes);
    if r.take(6)? != KV_MAGIC {
        return Err(FormatError::BadMagic);
    }
    let profile = r.str()?;
    let source = r.str()?;
    let encoding = ValueEncoding::from_u8(r.u8()?)?;
    let count = r.u32()? as usize;
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let klen = r.u32()? as usize;
        let key = r.take(klen)?.to_vec();
        let vlen = r.u32()? as usize;
        let val = r.take(vlen)?.to_vec();
        records.push((key, val));
    }
    Ok(KvDataset {
        profile,
        source,
        encoding,
        records,
    })
}

pub fn encode_ops(phases: &[Phase]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(OPS_MAGIC);
    out.extend_from_slice(&(phases.len() as u16).to_le_bytes());
    for p in phases {
        put_str(&mut out, &p.name);
        out.push(p.kind.to_u8());
        put_u32(&mut out, p.args.len() as u32);
        for &a in &p.args {
            put_u32(&mut out, a);
        }
    }
    out
}

pub fn decode_ops(bytes: &[u8]) -> Result<Vec<Phase>, FormatError> {
    let mut r = Reader::new(bytes);
    if r.take(6)? != OPS_MAGIC {
        return Err(FormatError::BadMagic);
    }
    let phase_count = r.u16()? as usize;
    let mut phases = Vec::with_capacity(phase_count);
    for _ in 0..phase_count {
        let name = r.str()?;
        let kind = PhaseKind::from_u8(r.u8()?)?;
        let arg_count = r.u32()? as usize;
        let mut args = Vec::with_capacity(arg_count);
        for _ in 0..arg_count {
            args.push(r.u32()?);
        }
        phases.push(Phase { name, kind, args });
    }
    Ok(phases)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kv_round_trips() {
        let records = vec![
            (b"k0".to_vec(), b"v0".to_vec()),
            (b"k1".to_vec(), vec![0u8; 300]),
        ];
        let bytes = encode_kv("people", "openaddresses", ValueEncoding::Cbor, &records);
        let decoded = decode_kv(&bytes).expect("decode kv");
        assert_eq!(decoded.profile, "people");
        assert_eq!(decoded.source, "openaddresses");
        assert_eq!(decoded.encoding, ValueEncoding::Cbor);
        assert_eq!(decoded.records, records);
    }

    #[test]
    fn ops_round_trips() {
        let phases = vec![
            Phase {
                name: "load".into(),
                kind: PhaseKind::LoadAll,
                args: vec![],
            },
            Phase {
                name: "get_skewed".into(),
                kind: PhaseKind::GetIndices,
                args: vec![3, 1, 3, 0],
            },
        ];
        let bytes = encode_ops(&phases);
        let decoded = decode_ops(&bytes).expect("decode ops");
        assert_eq!(decoded, phases);
    }

    #[test]
    fn decode_rejects_bad_magic() {
        assert!(decode_kv(b"nope").is_err());
        assert!(decode_ops(b"nope").is_err());
    }
}
