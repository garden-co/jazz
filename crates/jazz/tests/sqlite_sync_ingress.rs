//! A semantic frame has exactly one canonical encoding.
use jazz::db::{SyncMessage, decode_message};

/// Mallory cannot give Alice's unsubscribe a second varint spelling or append
/// another payload to its frame. The independent bytes pin ordinary traffic.
#[test]
fn semantic_decoder_refuses_noncanonical_varints_and_suffixes() {
    let valid = [1, 128, 1]; // Unsubscribe { id: 128 }.
    assert!(matches!(
        decode_message(&valid).unwrap(),
        SyncMessage::Unsubscribe { id: 128 }
    ));
    for invalid in [
        &[129, 0, 128, 1][..],
        &[1, 128, 129, 0],
        &[1, 128, 1, 0],
        &[],
        &[255],
        &[1, 128],
    ] {
        assert!(
            decode_message(invalid).is_err(),
            "accepted malformed bytes: {invalid:?}"
        );
    }
    assert!(decode_message(&valid).is_ok());
}
