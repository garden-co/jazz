//! Byte-level receipt for the terminal child occurrence key v1 wire contract.
//! Public query results cannot expose these opaque addresses, so pin them here.

use groove::ivm::terminal_occurrence_key;

#[test]
fn terminal_child_occurrence_key_v1_corpus() {
    // Typed UUID tag (10), followed by the UUID's sixteen bytes in network order.
    let row_key = vec![10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
    let corpus = [
        (0, "0a000102030405060708090a0b0c0d0e0f"),
        (1, "0a000102030405060708090a0b0c0d0e0fff0000000000000001"),
        (256, "0a000102030405060708090a0b0c0d0e0fff0000000000000100"),
        (
            u64::MAX,
            "0a000102030405060708090a0b0c0d0e0fffffffffffffffffff",
        ),
    ];
    for (ordinal, expected) in corpus {
        let encoded = terminal_occurrence_key(row_key.clone(), ordinal);
        let actual = encoded
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(actual, expected, "repeat ordinal {ordinal}");
    }
}
