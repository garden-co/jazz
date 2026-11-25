//! Hand-built zstd blocks inside the channel profile, decoded through
//! `StreamDecoder` with whichever zstd decoder is compiled in.
#![cfg(any(feature = "zstd", feature = "zrip"))]
use jazz_compression::stream::{Codec, StreamDecoder};

/// Profile frame header; descriptor 0 is a 1KiB window, 0x30 is 64KiB.
fn header(window_descriptor: u8) -> Vec<u8> {
    vec![0x28, 0xb5, 0x2f, 0xfd, 0, window_descriptor]
}
fn block(kind: u32, body: &[u8], size: usize) -> Vec<u8> {
    let header = ((size as u32) << 3) | (kind << 1);
    let mut block = header.to_le_bytes()[..3].to_vec();
    block.extend_from_slice(body);
    block
}
fn reject(window_descriptor: u8, block: &[u8]) {
    let mut bytes = header(window_descriptor);
    bytes.extend_from_slice(block);
    let mut decoder = StreamDecoder::new(Codec::Zstd).unwrap();
    let error = decoder.decode(&bytes, &mut [0; 65536]).unwrap_err();
    assert!(error.contains("channel codec"), "{error}");
    assert!(decoder.finish().is_err());
}

#[test]
fn flushed_block_is_delivered_before_frame_end() {
    let mut bytes = header(0x30);
    bytes.extend(block(0, b"small flushed message", 21));
    let mut decoder = StreamDecoder::new(Codec::Zstd).unwrap();
    let mut output = [0; 64];
    let p = decoder.decode(&bytes, &mut output).unwrap();
    assert_eq!(&output[..p.written], b"small flushed message");
    assert_eq!(p.consumed, bytes.len());
    assert!(!p.finished);
    assert!(decoder.finish().is_err());
}

#[test]
fn raw_and_rle_blocks_cannot_exceed_the_window() {
    reject(0, &block(0, &[7; 1025], 1025));
    reject(0, &block(1, &[7], 1025));
}

#[test]
fn literal_and_sequence_counts_cannot_exceed_the_window() {
    // A compressed block with a two-byte RLE literal header advertising 2048
    // literals, one literal byte, zero sequences. Only four encoded bytes.
    reject(0, &block(2, &[5, 128, 7, 0], 4));
    // Zero literals; 127 sequences with a mode byte and no bitstream.
    reject(0, &block(2, &[0, 127, 0], 3));
}

#[test]
fn zero_offset_is_rejected() {
    // Zero literals, one sequence, all three symbol tables RLE. ll=0,
    // of-code=1 plus extra bit 1 yields repeat offset 3, ml=3. With the
    // initial offset history [1,4,8], repeat-offset-3 means 1-1 = 0.
    reject(0x30, &block(2, &[0, 1, 0x54, 0, 1, 0, 3], 7));
}

#[test]
fn huffman_literal_output_cannot_exceed_its_declared_size() {
    for four_streams in [false, true] {
        // Direct Huffman weights for two one-bit symbols. Each sixteen-byte bit
        // stream decodes 127 symbols, but the literal header advertises one.
        let mut literals = vec![128, 0x10];
        if four_streams {
            literals.extend_from_slice(&[16, 0, 16, 0, 16, 0]);
            literals.extend_from_slice(&[0xff; 64]);
        } else {
            literals.extend_from_slice(&[0xff; 16]);
        }
        let header = ((literals.len() as u32) << 14) | (1 << 4) | if four_streams { 6 } else { 2 };
        let mut body = header.to_le_bytes()[..3].to_vec();
        body.extend(literals);
        body.push(0); // no sequences
        reject(0x30, &block(2, &body, body.len()));
    }
}
