use jazz_types::identity::DeviceAccountSecret;

#[path = "support/zeroized_allocation.rs"]
mod zeroized_allocation;

/// Alice's owned device-account text is wiped when parsing succeeds or fails.
/// The allocator observes the owned buffer because the API consumes the string.
#[test]
fn owned_device_account_text_is_wiped_on_success_and_failure() {
    use zeroized_allocation::assert_zeroized;

    let secret = DeviceAccountSecret::from_bytes([5; 32]);
    let text = secret.format().as_str().to_owned();
    assert_eq!(
        assert_zeroized(text, DeviceAccountSecret::parse_owned),
        Ok(secret)
    );
    assert!(assert_zeroized("invalid".to_owned(), DeviceAccountSecret::parse_owned).is_err());
}

/// A secret in the earlier hexadecimal form, or without its prefix, is
/// refused with the expected format, never echoing the text; a hexadecimal
/// one names the likely package and build version mismatch.
#[test]
fn a_secret_in_another_format_is_refused_with_the_expected_format() {
    use jazz_types::identity::{LEGACY_HEX_SECRET, UNPREFIXED_SECRET};

    let hex = "ab".repeat(32);
    assert_eq!(DeviceAccountSecret::parse(&hex), Err(LEGACY_HEX_SECRET));
    assert!(LEGACY_HEX_SECRET.contains("jazz-auth-v1:"));
    assert!(LEGACY_HEX_SECRET.contains("matching versions"));
    assert!(!LEGACY_HEX_SECRET.contains(&hex));
    for text in [
        "",
        "ab",
        &"ab".repeat(31),
        "jazz-auth-v2:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
    ] {
        assert_eq!(
            DeviceAccountSecret::parse(text),
            Err(UNPREFIXED_SECRET),
            "{text}"
        );
    }
    // The prefixed form keeps its plain refusal.
    assert_eq!(
        DeviceAccountSecret::parse("jazz-auth-v1:short"),
        Err("invalid device account secret")
    );
}
