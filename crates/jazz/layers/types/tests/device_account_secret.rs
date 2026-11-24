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
