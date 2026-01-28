const MIN_MATCH_LEN: usize = 4;
const MAX_MATCH_LEN: usize = 15 + 3;
const MAX_LITERALS: usize = 15;
const HASH_LOG: u32 = 16;
const HASH_TABLE_SIZE: usize = 1 << HASH_LOG;

fn hash(data: &[u8]) -> usize {
    const KNUTH_MULT_PRIME: u32 = 2654435761;
    let val = u32::from_le_bytes(data.try_into().unwrap());
    ((val.wrapping_mul(KNUTH_MULT_PRIME)) >> (32 - HASH_LOG)) as usize
}

#[derive(Debug, PartialEq)]
pub enum DecompressionError {
    InvalidToken,
    UnexpectedEof,
}

pub fn decompress(input: &[u8]) -> Result<Vec<u8>, DecompressionError> {
    let mut decompressed = Vec::with_capacity(input.len() * 2);
    let mut i = 0;

    while i < input.len() {
        let token = input[i];
        i += 1;

        let literal_len = (token >> 4) as usize;
        let match_len_token = (token & 0x0F) as usize;

        if i + literal_len > input.len() {
            return Err(DecompressionError::UnexpectedEof);
        }
        decompressed.extend_from_slice(&input[i..i + literal_len]);
        i += literal_len;

        if match_len_token > 0 {
            if i + 2 > input.len() {
                return Err(DecompressionError::UnexpectedEof);
            }

            let offset = u16::from_le_bytes([input[i], input[i + 1]]) as usize;
            i += 2;

            if offset == 0 || offset > decompressed.len() {
                return Err(DecompressionError::InvalidToken);
            }

            let match_len = match_len_token + 3;
            let match_start = decompressed.len() - offset;

            for k in 0..match_len {
                decompressed.push(decompressed[match_start + k]);
            }
        }
    }

    Ok(decompressed)
}

pub fn compress(input: &[u8]) -> Vec<u8> {
    let mut compressor = Compressor::new();
    compressor.compress_chunk(input)
}

fn emit_sequence(out: &mut Vec<u8>, mut literals: &[u8], match_len: usize, offset: u16) {
    while literals.len() > MAX_LITERALS {
        let token = (MAX_LITERALS as u8) << 4;
        out.push(token);
        out.extend_from_slice(&literals[..MAX_LITERALS]);
        literals = &literals[MAX_LITERALS..];
    }

    let lit_len_token = literals.len() as u8;
    let match_len_token = if match_len > 0 {
        (match_len - 3) as u8
    } else {
        0
    };

    let token = lit_len_token << 4 | match_len_token;
    out.push(token);
    out.extend_from_slice(literals);

    if match_len > 0 {
        out.extend_from_slice(&offset.to_le_bytes());
    }
}

pub struct Compressor {
    hash_table: Vec<u32>,
    history: Vec<u8>,
}

impl Compressor {
    pub fn new() -> Self {
        Self {
            hash_table: vec![0; HASH_TABLE_SIZE],
            history: Vec::new(),
        }
    }

    pub fn compress_chunk(&mut self, chunk: &[u8]) -> Vec<u8> {
        let mut compressed_chunk = Vec::new();

        let chunk_start_cursor = self.history.len();
        self.history.extend_from_slice(chunk);

        let mut cursor = chunk_start_cursor;
        let mut literal_anchor = chunk_start_cursor;

        while cursor < self.history.len() {
            let mut best_match: Option<(u16, usize)> = None;

            if self.history.len() - cursor >= MIN_MATCH_LEN {
                let h = hash(&self.history[cursor..cursor + 4]);
                let match_pos = self.hash_table[h] as usize;

                if match_pos < cursor && cursor - match_pos < u16::MAX as usize {
                    if self.history.get(match_pos..match_pos + MIN_MATCH_LEN)
                        == Some(&self.history[cursor..cursor + MIN_MATCH_LEN])
                    {
                        let mut match_len = MIN_MATCH_LEN;
                        while cursor + match_len < self.history.len()
                            && match_len < MAX_MATCH_LEN
                            && self.history.get(match_pos + match_len)
                                == self.history.get(cursor + match_len)
                        {
                            match_len += 1;
                        }
                        best_match = Some(((cursor - match_pos) as u16, match_len));
                    }
                }
                self.hash_table[h] = cursor as u32;
            }

            if let Some((offset, match_len)) = best_match {
                let literals = &self.history[literal_anchor..cursor];
                emit_sequence(&mut compressed_chunk, literals, match_len, offset);
                cursor += match_len;
                literal_anchor = cursor;
            } else {
                cursor += 1;
            }
        }

        if literal_anchor < cursor {
            let literals = &self.history[literal_anchor..cursor];
            emit_sequence(&mut compressed_chunk, literals, 0, 0);
        }

        compressed_chunk
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_roundtrip() {
        let data = b"hello world, hello people";
        let compressed = compress(data);
        println!(
            "Compressed '{}': {:x?}",
            std::str::from_utf8(data).unwrap(),
            compressed
        );
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data, decompressed.as_slice());
    }

    #[test]
    fn test_long_literals() {
        let data = b"abcdefghijklmnopqrstuvwxyz";
        let compressed = compress(data);
        println!(
            "Compressed '{}': {:x?}",
            std::str::from_utf8(data).unwrap(),
            compressed
        );
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data, decompressed.as_slice());
    }

    #[test]
    fn test_decompress_empty() {
        let data = b"";
        let compressed = compress(data);
        assert!(compressed.is_empty());
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data, decompressed.as_slice());
    }

    #[test]
    fn test_overlapping_match() {
        let data = b"abcdeabcdeabcdeabcde"; // repeating sequence
        let compressed = compress(data);
        println!(
            "Compressed '{}': {:x?}",
            std::str::from_utf8(data).unwrap(),
            compressed
        );
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data, decompressed.as_slice());

        let data2 = b"abababababababababab";
        let compressed2 = compress(data2);
        println!(
            "Compressed '{}': {:x?}",
            std::str::from_utf8(data2).unwrap(),
            compressed2
        );
        let decompressed2 = decompress(&compressed2).unwrap();
        assert_eq!(data2, decompressed2.as_slice());
    }

    #[test]
    fn test_json_roundtrip() {
        let data = std::fs::read("data/compression_66k_JSON.txt").unwrap();
        let compressed = compress(&data);
        std::fs::write("compressed_66k.lzy", &compressed).unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data, decompressed.as_slice());
    }

    mod crdt_helpers {
        use serde::{Deserialize, Serialize};

        #[derive(Serialize, Deserialize, Debug)]
        #[serde(rename_all = "camelCase")]
        pub struct After {
            pub session_id: String,
            pub tx_index: u32,
            pub change_idx: u32,
        }

        #[derive(Serialize, Deserialize, Debug)]
        pub struct Transaction {
            pub op: String,
            pub value: String,
            pub after: After,
        }

        pub fn generate_transactions(text: &str, session_id: &str) -> Vec<String> {
            let mut transactions = Vec::new();
            for (i, c) in text.chars().enumerate() {
                let tx = Transaction {
                    op: "app".to_string(),
                    value: c.to_string(),
                    after: After {
                        session_id: session_id.to_string(),
                        tx_index: i as u32,
                        change_idx: 0,
                    },
                };
                transactions.push(serde_json::to_string(&tx).unwrap());
            }
            transactions
        }

        pub fn generate_shorthand_transactions(text: &str) -> Vec<String> {
            let mut transactions = Vec::new();
            for c in text.chars() {
                transactions.push(serde_json::to_string(&c.to_string()).unwrap());
            }
            transactions
        }
    }

    #[test]
    fn test_crdt_transaction_generation() {
        let sample_text = "This is a sample text for our CRDT simulation. \
        It should be long enough to see some interesting compression results later on. \
        Let's add another sentence to make it a bit more substantial.";

        let session_id = "co_zRtnoNffeMHge9wvyL5mK1RWbdz_session_zKvAVFSV5cqW";
        let transactions = crdt_helpers::generate_transactions(sample_text, session_id);

        println!("--- Generated CRDT Transactions ---");
        for tx in &transactions {
            println!("{}", tx);
        }
        println!("--- End of CRDT Transactions ---");

        assert!(!transactions.is_empty());
        assert_eq!(transactions.len(), sample_text.chars().count());
    }

    #[test]
    fn test_crdt_chunked_compression() {
        let sample_text = "This is a sample text for our CRDT simulation. \
        It should be long enough to see some interesting compression results later on. \
        Let's add another sentence to make it a bit more substantial.";

        let session_id = "co_zRtnoNffeMHge9wvyL5mK1RWbdz_session_zKvAVFSV5cqW";
        let transactions_json = crdt_helpers::generate_transactions(sample_text, session_id);

        let mut compressor = Compressor::new();
        let mut compressed_log = Vec::new();
        let mut total_json_len = 0;

        for tx_json in &transactions_json {
            let compressed_chunk = compressor.compress_chunk(tx_json.as_bytes());
            compressed_log.extend_from_slice(&compressed_chunk);
            total_json_len += tx_json.len();
        }

        let decompressed = decompress(&compressed_log).unwrap();

        // Verify roundtrip
        let original_log_concatenated = transactions_json.join("");
        assert_eq!(decompressed, original_log_concatenated.as_bytes());

        let plaintext_len = sample_text.len();
        let compressed_len = compressed_log.len();

        let compression_ratio = compressed_len as f64 / total_json_len as f64;
        let overhead_ratio = compressed_len as f64 / plaintext_len as f64;

        println!("\n--- CRDT Chunked Compression Test ---");
        println!("Plaintext size: {} bytes", plaintext_len);
        println!("Total JSON size: {} bytes", total_json_len);
        println!("Compressed log size: {} bytes", compressed_len);
        println!(
            "Compression ratio (compressed/json): {:.4}",
            compression_ratio
        );
        println!(
            "Overhead ratio (compressed/plaintext): {:.4}",
            overhead_ratio
        );
        println!("--- End of Test ---");
    }

    #[test]
    fn test_crdt_shorthand_compression() {
        let sample_text = "This is a sample text for our CRDT simulation. \
        It should be long enough to see some interesting compression results later on. \
        Let's add another sentence to make it a bit more substantial.";

        let transactions_json = crdt_helpers::generate_shorthand_transactions(sample_text);

        let mut compressor = Compressor::new();
        let mut compressed_log = Vec::new();
        let mut total_json_len = 0;

        for tx_json in &transactions_json {
            let compressed_chunk = compressor.compress_chunk(tx_json.as_bytes());
            compressed_log.extend_from_slice(&compressed_chunk);
            total_json_len += tx_json.len();
        }

        let decompressed = decompress(&compressed_log).unwrap();

        // Verify roundtrip
        let original_log_concatenated = transactions_json.join("");
        assert_eq!(decompressed, original_log_concatenated.as_bytes());

        let plaintext_len = sample_text.len();
        let compressed_len = compressed_log.len();

        let compression_ratio = compressed_len as f64 / total_json_len as f64;
        let overhead_ratio = compressed_len as f64 / plaintext_len as f64;

        println!("\n--- CRDT Shorthand Compression Test ---");
        println!("Plaintext size: {} bytes", plaintext_len);
        println!("Total JSON size: {} bytes", total_json_len);
        println!("Compressed log size: {} bytes", compressed_len);
        println!(
            "Compression ratio (compressed/json): {:.4}",
            compression_ratio
        );
        println!(
            "Overhead ratio (compressed/plaintext): {:.4}",
            overhead_ratio
        );
        println!("--- End of Test ---");
    }
}
