//! Stateless native account crypto; no database, registry, or network ownership.

use crate::{
    JazzNativeRelayBytes, JazzNativeRelayStatus, bounded_bytes_arg, ffi_call, require_arg,
    reset_out, write_bytes_out,
};
use jazz::tools::identity::{LOCAL_FIRST_ISSUER, mint_jazz_self_signed_token_at};

/// Generate a 32-byte local-first signing root using the operating system RNG.
/// The caller must retain it securely before opening a context.
///
/// # Safety
/// `out` must be null or writable. A successful result must be released through
/// `jazz_native_relay_bytes_free`; no input or output may alias an active buffer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_account_secret(
    out: *mut JazzNativeRelayBytes,
) -> JazzNativeRelayStatus {
    ffi_call(|| {
        unsafe { reset_out(out, JazzNativeRelayBytes::EMPTY)? };
        let mut seed = vec![0; 32];
        getrandom::fill(&mut seed).map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        unsafe { write_bytes_out(out, seed) };
        Ok(())
    })
}

/// Mint the same local-first proof as WASM/NAPI, without loading a database.
/// This proves key possession only; external registration/linking stays in the
/// shared account helper and the authoritative core registry.
///
/// # Safety
/// Input pointers must be readable for their lengths and `out` writable;
/// pointers must not alias. Output ownership matches `account_secret` above.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_mint_local_first_token(
    seed: *const u8,
    seed_len: usize,
    audience: *const u8,
    audience_len: usize,
    ttl_seconds: u64,
    now_seconds: u64,
    out: *mut JazzNativeRelayBytes,
) -> JazzNativeRelayStatus {
    ffi_call(|| {
        unsafe { reset_out(out, JazzNativeRelayBytes::EMPTY)? };
        require_arg(!seed.is_null() && seed_len == 32)?;
        let audience = unsafe { bounded_bytes_arg(audience, audience_len, 16 * 1024)? };
        require_arg(ttl_seconds != 0 && now_seconds.checked_add(ttl_seconds).is_some())?;
        let seed: &[u8; 32] = unsafe { &*seed.cast::<[u8; 32]>() };
        let audience =
            std::str::from_utf8(audience).map_err(|_| JazzNativeRelayStatus::InvalidArgument)?;
        let token = mint_jazz_self_signed_token_at(
            seed,
            LOCAL_FIRST_ISSUER,
            audience,
            ttl_seconds,
            now_seconds,
        )
        .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        unsafe { write_bytes_out(out, token.into_bytes()) };
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jazz_native_relay_bytes_free;
    use jazz::tools::identity::verify_jazz_self_signed_proof_with_max_ttl_at;

    #[test]
    fn native_account_crypto_mints_a_portable_proof_and_rejects_malformed_inputs() {
        let mut seed = JazzNativeRelayBytes::EMPTY;
        assert_eq!(
            unsafe { jazz_native_relay_account_secret(&mut seed) },
            JazzNativeRelayStatus::Ok
        );
        assert_eq!(seed.len, 32);
        let audience = b"native-account-crypto-test";
        let mut token = JazzNativeRelayBytes::EMPTY;
        assert_eq!(
            unsafe {
                jazz_native_relay_mint_local_first_token(
                    seed.data,
                    seed.len,
                    audience.as_ptr(),
                    audience.len(),
                    3600,
                    1000,
                    &mut token,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        let token_text =
            std::str::from_utf8(unsafe { std::slice::from_raw_parts(token.data, token.len) })
                .unwrap();
        let verified = verify_jazz_self_signed_proof_with_max_ttl_at(
            token_text,
            "native-account-crypto-test",
            3600,
            1001,
        )
        .unwrap();
        assert_eq!(verified.issuer, LOCAL_FIRST_ISSUER);
        assert!(
            verify_jazz_self_signed_proof_with_max_ttl_at(token_text, "other-app", 3600, 1001)
                .is_err()
        );
        unsafe { jazz_native_relay_bytes_free(&mut token) };
        for (seed_len, ttl, now) in [(31, 3600, 1000), (32, 0, 1000), (32, 2, u64::MAX)] {
            assert_eq!(
                unsafe {
                    jazz_native_relay_mint_local_first_token(
                        seed.data,
                        seed_len,
                        audience.as_ptr(),
                        audience.len(),
                        ttl,
                        now,
                        &mut token,
                    )
                },
                JazzNativeRelayStatus::InvalidArgument
            );
            assert!(token.data.is_null());
            assert_eq!(token.len, 0);
        }
        unsafe { jazz_native_relay_bytes_free(&mut seed) };
    }
}
