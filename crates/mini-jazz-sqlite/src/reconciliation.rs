use crate::protocol::{
    ReconcileAlgorithm, ReconcileParameters, ReconcileSet, ReconcileSymbol, ReconciliationSketch,
    RowHeadItem,
};
use crate::{Error, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const DEFAULT_RATLESS_INITIAL_SYMBOLS: u32 = 64;
const DEFAULT_RATLESS_MORE_SYMBOLS: u32 = 1024;
pub(crate) const DEFAULT_RATLESS_MAX_SYMBOLS: u32 = 65_536;
const DEFAULT_RATLESS_TARGET_DEGREE: u8 = 8;
const RATLESS_SEED: u64 = 0x726f_7768_6561_6473;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RowHeadDifference {
    pub server_only: BTreeSet<RowHeadItem>,
    pub client_only: BTreeSet<RowHeadItem>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RatelessDecode {
    Complete(RowHeadDifference),
    NeedMore {
        next_symbol_index: u32,
        requested_symbols: u32,
    },
    Failed,
}

pub(crate) fn exact_sketch(row_heads: Vec<RowHeadItem>) -> ReconciliationSketch {
    ReconciliationSketch {
        set: ReconcileSet::RowHeads,
        algorithm: ReconcileAlgorithm::Exact,
        parameters: None,
        symbols: Vec::new(),
        row_heads,
    }
}

pub(crate) fn rateless_sketch(row_heads: &[RowHeadItem]) -> ReconciliationSketch {
    let parameters = ReconcileParameters {
        seed: RATLESS_SEED,
        estimated_items: row_heads.len() as u64,
        target_degree: DEFAULT_RATLESS_TARGET_DEGREE,
        symbol_count: DEFAULT_RATLESS_MAX_SYMBOLS,
    };
    let initial_symbols = DEFAULT_RATLESS_INITIAL_SYMBOLS.min(parameters.symbol_count);
    ReconciliationSketch {
        set: ReconcileSet::RowHeads,
        algorithm: ReconcileAlgorithm::Rateless,
        parameters: Some(parameters.clone()),
        symbols: rateless_symbols(row_heads, &parameters, 0, initial_symbols),
        row_heads: Vec::new(),
    }
}

pub(crate) fn rateless_symbols(
    row_heads: &[RowHeadItem],
    parameters: &ReconcileParameters,
    start_index: u32,
    count: u32,
) -> Vec<ReconcileSymbol> {
    let mut row_heads = row_heads.to_vec();
    row_heads.sort();
    (start_index..start_index.saturating_add(count))
        .map(|index| {
            let mut symbol = SymbolAccumulator::new(index);
            for row_head in &row_heads {
                let item_bytes = canonical_row_head_bytes(row_head);
                let item_hash = item_hash(&item_bytes);
                if item_participates(parameters, index, &item_hash) {
                    symbol.xor_item(1, &item_bytes, &item_hash);
                }
            }
            symbol.to_protocol()
        })
        .collect()
}

pub(crate) fn exact_difference(
    client_heads: impl IntoIterator<Item = RowHeadItem>,
    server_heads: impl IntoIterator<Item = RowHeadItem>,
) -> RowHeadDifference {
    let client_heads = client_heads.into_iter().collect::<BTreeSet<_>>();
    let server_heads = server_heads.into_iter().collect::<BTreeSet<_>>();
    RowHeadDifference {
        server_only: server_heads.difference(&client_heads).cloned().collect(),
        client_only: client_heads.difference(&server_heads).cloned().collect(),
    }
}

pub(crate) fn decode_rateless_difference(
    client_symbols: &[ReconcileSymbol],
    server_heads: &[RowHeadItem],
    parameters: &ReconcileParameters,
) -> Result<RatelessDecode> {
    if parameters.target_degree == 0 {
        return Ok(RatelessDecode::Failed);
    }
    if client_symbols.is_empty() {
        return Ok(request_more_or_fail(0, parameters));
    }

    let symbol_indices = client_symbols
        .iter()
        .map(|symbol| symbol.index)
        .collect::<Vec<_>>();
    let next_symbol_index = symbol_indices
        .iter()
        .copied()
        .max()
        .and_then(|index| index.checked_add(1))
        .unwrap_or(0);

    let mut accumulators = BTreeMap::new();
    let mut sorted_server_heads = server_heads.to_vec();
    sorted_server_heads.sort();
    for index in &symbol_indices {
        if *index >= parameters.symbol_count {
            return Ok(RatelessDecode::Failed);
        }
        let mut server_symbol = SymbolAccumulator::new(*index);
        for row_head in &sorted_server_heads {
            let item_bytes = canonical_row_head_bytes(row_head);
            let item_hash = item_hash(&item_bytes);
            if item_participates(parameters, *index, &item_hash) {
                server_symbol.xor_item(1, &item_bytes, &item_hash);
            }
        }
        if accumulators.insert(*index, server_symbol).is_some() {
            return Ok(RatelessDecode::Failed);
        }
    }

    for client_symbol in client_symbols {
        let Some(accumulator) = accumulators.get_mut(&client_symbol.index) else {
            return Ok(RatelessDecode::Failed);
        };
        accumulator.subtract_protocol(client_symbol)?;
    }

    let mut server_only = BTreeSet::new();
    let mut client_only = BTreeSet::new();
    loop {
        let singleton = accumulators
            .values()
            .find_map(SymbolAccumulator::decoded_singleton);
        let Some((sign, item)) = singleton else {
            break;
        };
        let item_bytes = canonical_row_head_bytes(&item);
        let item_hash = item_hash(&item_bytes);
        if sign > 0 {
            server_only.insert(item.clone());
        } else {
            client_only.insert(item.clone());
        }
        for (index, accumulator) in accumulators.iter_mut() {
            if item_participates(parameters, *index, &item_hash) {
                accumulator.xor_item(-sign, &item_bytes, &item_hash);
            }
        }
    }

    if accumulators.values().all(SymbolAccumulator::is_zero) {
        Ok(RatelessDecode::Complete(RowHeadDifference {
            server_only,
            client_only,
        }))
    } else {
        Ok(request_more_or_fail(next_symbol_index, parameters))
    }
}

fn request_more_or_fail(
    next_symbol_index: u32,
    parameters: &ReconcileParameters,
) -> RatelessDecode {
    let remaining = parameters.symbol_count.saturating_sub(next_symbol_index);
    if remaining == 0 {
        RatelessDecode::Failed
    } else {
        RatelessDecode::NeedMore {
            next_symbol_index,
            requested_symbols: DEFAULT_RATLESS_MORE_SYMBOLS.min(remaining),
        }
    }
}

#[derive(Clone, Debug)]
struct SymbolAccumulator {
    index: u32,
    count: i64,
    item_len_xor: u64,
    item_bytes_xor: Vec<u8>,
    item_hash_xor: [u8; 16],
}

impl SymbolAccumulator {
    fn new(index: u32) -> Self {
        Self {
            index,
            count: 0,
            item_len_xor: 0,
            item_bytes_xor: Vec::new(),
            item_hash_xor: [0; 16],
        }
    }

    fn subtract_protocol(&mut self, symbol: &ReconcileSymbol) -> Result<()> {
        self.count -= symbol.count;
        self.item_len_xor ^= symbol.item_len_xor;
        let bytes = STANDARD
            .decode(symbol.item_bytes_xor.as_bytes())
            .map_err(|error| Error::new(format!("invalid reconciliation symbol bytes: {error}")))?;
        xor_bytes(&mut self.item_bytes_xor, &bytes);
        let hash = STANDARD
            .decode(symbol.item_hash_xor.as_bytes())
            .map_err(|error| Error::new(format!("invalid reconciliation symbol hash: {error}")))?;
        if hash.len() != 16 {
            return Err(Error::new("invalid reconciliation symbol hash length"));
        }
        for (target, source) in self.item_hash_xor.iter_mut().zip(hash) {
            *target ^= source;
        }
        Ok(())
    }

    fn xor_item(&mut self, count_delta: i64, item_bytes: &[u8], item_hash: &[u8; 16]) {
        self.count += count_delta;
        self.item_len_xor ^= item_bytes.len() as u64;
        xor_bytes(&mut self.item_bytes_xor, item_bytes);
        for (target, source) in self.item_hash_xor.iter_mut().zip(item_hash) {
            *target ^= *source;
        }
    }

    fn to_protocol(mut self) -> ReconcileSymbol {
        trim_trailing_zeroes(&mut self.item_bytes_xor);
        ReconcileSymbol {
            index: self.index,
            count: self.count,
            item_len_xor: self.item_len_xor,
            item_bytes_xor: STANDARD.encode(self.item_bytes_xor),
            item_hash_xor: STANDARD.encode(self.item_hash_xor),
        }
    }

    fn decoded_singleton(&self) -> Option<(i64, RowHeadItem)> {
        if self.count.abs() != 1 {
            return None;
        }
        let item_len = usize::try_from(self.item_len_xor).ok()?;
        if item_len == 0 || self.item_bytes_xor.len() < item_len {
            return None;
        }
        if self.item_bytes_xor[item_len..]
            .iter()
            .any(|byte| *byte != 0)
        {
            return None;
        }
        let item_bytes = &self.item_bytes_xor[..item_len];
        if item_hash(item_bytes) != self.item_hash_xor {
            return None;
        }
        Some((self.count.signum(), parse_row_head_bytes(item_bytes)?))
    }

    fn is_zero(&self) -> bool {
        self.count == 0
            && self.item_len_xor == 0
            && self.item_hash_xor.iter().all(|byte| *byte == 0)
            && self.item_bytes_xor.iter().all(|byte| *byte == 0)
    }
}

fn canonical_row_head_bytes(row_head: &RowHeadItem) -> Vec<u8> {
    let mut bytes = vec![1];
    push_string(&mut bytes, &row_head.branch_id);
    push_string(&mut bytes, &row_head.table);
    push_string(&mut bytes, &row_head.row_id);
    push_string(&mut bytes, &row_head.head_tx_id);
    bytes
}

fn parse_row_head_bytes(bytes: &[u8]) -> Option<RowHeadItem> {
    let (version, mut rest) = bytes.split_first()?;
    if *version != 1 {
        return None;
    }
    let branch_id = take_string(&mut rest)?;
    let table = take_string(&mut rest)?;
    let row_id = take_string(&mut rest)?;
    let head_tx_id = take_string(&mut rest)?;
    if !rest.is_empty() {
        return None;
    }
    Some(RowHeadItem {
        branch_id,
        table,
        row_id,
        head_tx_id,
    })
}

fn push_string(bytes: &mut Vec<u8>, value: &str) {
    let len = u32::try_from(value.len()).expect("row-head field length fits in u32");
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(value.as_bytes());
}

fn take_string(rest: &mut &[u8]) -> Option<String> {
    let len_bytes = rest.get(..4)?;
    let len = u32::from_le_bytes(len_bytes.try_into().ok()?) as usize;
    *rest = &rest[4..];
    let value_bytes = rest.get(..len)?;
    let value = std::str::from_utf8(value_bytes).ok()?.to_owned();
    *rest = &rest[len..];
    Some(value)
}

fn item_hash(item_bytes: &[u8]) -> [u8; 16] {
    let hash = blake3::hash(item_bytes);
    let mut short = [0; 16];
    short.copy_from_slice(&hash.as_bytes()[..16]);
    short
}

fn item_participates(
    parameters: &ReconcileParameters,
    symbol_index: u32,
    item_hash: &[u8; 16],
) -> bool {
    let target_degree = u32::from(parameters.target_degree.max(1));
    let level = (symbol_index % target_degree).min(30) + 1;
    let divisor = 1_u64 << level;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"mini-jazz-sqlite-rateless-symbol-v1");
    hasher.update(&parameters.seed.to_le_bytes());
    hasher.update(&symbol_index.to_le_bytes());
    hasher.update(item_hash);
    let hash = hasher.finalize();
    let mut bytes = [0; 8];
    bytes.copy_from_slice(&hash.as_bytes()[..8]);
    u64::from_le_bytes(bytes) % divisor == 0
}

fn xor_bytes(target: &mut Vec<u8>, source: &[u8]) {
    if target.len() < source.len() {
        target.resize(source.len(), 0);
    }
    for (target, source) in target.iter_mut().zip(source) {
        *target ^= *source;
    }
}

fn trim_trailing_zeroes(bytes: &mut Vec<u8>) {
    while bytes.last() == Some(&0) {
        bytes.pop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str, tx: &str) -> RowHeadItem {
        RowHeadItem {
            branch_id: "main".to_owned(),
            table: "todos".to_owned(),
            row_id: id.to_owned(),
            head_tx_id: tx.to_owned(),
        }
    }

    #[test]
    fn row_head_canonical_bytes_are_deterministic_and_parseable() {
        let item = row("todo-1", "tx-1");
        let bytes = canonical_row_head_bytes(&item);

        assert_eq!(canonical_row_head_bytes(&item), bytes);
        assert_eq!(parse_row_head_bytes(&bytes), Some(item));
        assert_ne!(canonical_row_head_bytes(&row("todo-1", "tx-2")), bytes);
    }

    #[test]
    fn rateless_decode_recovers_insert_delete_and_update_differences() {
        let client = vec![row("todo-1", "tx-old"), row("todo-client", "tx-client")];
        let server = vec![row("todo-1", "tx-new"), row("todo-server", "tx-server")];
        let parameters = ReconcileParameters {
            seed: 7,
            estimated_items: 2,
            target_degree: 8,
            symbol_count: 256,
        };
        let client_symbols = rateless_symbols(&client, &parameters, 0, 256);

        let decoded = decode_rateless_difference(&client_symbols, &server, &parameters).unwrap();

        let RatelessDecode::Complete(diff) = decoded else {
            panic!("expected complete decode, got {decoded:?}");
        };
        assert_eq!(
            diff.server_only,
            BTreeSet::from([row("todo-1", "tx-new"), row("todo-server", "tx-server")])
        );
        assert_eq!(
            diff.client_only,
            BTreeSet::from([row("todo-1", "tx-old"), row("todo-client", "tx-client")])
        );
    }
}
