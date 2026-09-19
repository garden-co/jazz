#![cfg(feature = "ruzstd")]
use ruzstd::decoding::{BlockDecodingStrategy, FrameDecoder};

fn decoder(limit: usize) -> FrameDecoder {
    let mut decoder = FrameDecoder::new();
    decoder
        .init(&[0x28, 0xb5, 0x2f, 0xfd, 0, 0x30][..])
        .unwrap();
    decoder.set_max_block_output(limit).unwrap();
    decoder
}
fn block(kind: u32, body: &[u8], decoded: usize) -> Vec<u8> {
    let header = ((decoded as u32) << 3) | (kind << 1);
    let mut block = header.to_le_bytes()[..3].to_vec();
    block.extend_from_slice(body);
    block
}
#[test]
fn streaming_read_delivers_window_bytes_without_discarding_history() {
    let mut d = decoder(65536);
    let raw = block(0, b"small flushed message", 21);
    d.decode_blocks(raw.as_slice(), BlockDecodingStrategy::UptoBlocks(1))
        .unwrap();
    let mut output = [0; 64];
    let n = d.read_streaming(&mut output);
    assert_eq!(&output[..n], b"small flushed message");
    assert_eq!(d.streaming_available(), 0);
    assert!(!d.is_finished());
    assert_eq!(d.read_streaming(&mut output), 0);
}
#[test]
fn raw_and_rle_output_limits_are_checked_before_expansion() {
    for b in [block(0, &[7; 65], 65), block(1, &[7], 65)] {
        let error = decoder(64)
            .decode_blocks(b.as_slice(), BlockDecodingStrategy::UptoBlocks(1))
            .unwrap_err();
        assert!(format!("{error:?}").contains("OutputTooLarge { requested: 65, limit: 64 }"));
    }
}
#[test]
fn literals_and_sequence_counts_are_checked_before_allocation() {
    // Compressed block with a two-byte RLE literal header advertising 2048
    // literals, one literal byte, zero sequences. Only four encoded bytes.
    let literals = block(2, &[5, 128, 7, 0], 4);
    let error = decoder(1024)
        .decode_blocks(literals.as_slice(), BlockDecodingStrategy::UptoBlocks(1))
        .unwrap_err();
    assert!(format!("{error:?}").contains("OutputTooLarge { requested: 2048, limit: 1024 }"));
    // Zero literals; 127 sequences with a mode byte. Reject the count before
    // the sequence decoder can reserve its table or inspect entropy data.
    let sequences = block(2, &[0, 127, 0], 3);
    let error = decoder(64)
        .decode_blocks(sequences.as_slice(), BlockDecodingStrategy::UptoBlocks(1))
        .unwrap_err();
    assert!(format!("{error:?}").contains("OutputTooLarge { requested: 127, limit: 21 }"));
}
#[test]
fn match_output_limit_is_checked_before_sequence_execution() {
    // A valid native frame with a tiny compressed block that expands to 64KiB.
    let frame = zstd::bulk::compress(&vec![42; 65536], 3).unwrap();
    let mut source = frame.as_slice();
    let mut d = FrameDecoder::new();
    d.init(&mut source).unwrap();
    d.set_max_block_output(1024).unwrap();
    let error = d
        .decode_blocks(source, BlockDecodingStrategy::UptoBlocks(1))
        .unwrap_err();
    assert!(format!("{error:?}").contains("OutputTooLarge { requested: 65536, limit: 1024 }"));
    assert_eq!(
        d.streaming_available(),
        0,
        "reject before adding any regenerated output"
    );
}

#[test]
fn zero_offset_is_rejected_without_repetition_loop() {
    // Zero literals, one sequence, all three symbol tables RLE. ll=0,
    // of-code=1 plus extra bit 1 yields repeat offset 3, ml=3. With the
    // initial offset history [1,4,8], repeat-offset-3 means 1-1 = 0.
    let invalid = block(2, &[0, 1, 0x54, 0, 1, 0, 3], 7);
    let error = decoder(64)
        .decode_blocks(invalid.as_slice(), BlockDecodingStrategy::UptoBlocks(1))
        .unwrap_err();
    assert!(format!("{error:?}").contains("ZeroOffset"), "{error:?}");
}
