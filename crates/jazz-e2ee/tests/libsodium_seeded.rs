//! With libsodium's random source replaced by a fixed byte sequence,
//! randomised Ed25519 key generation produces identical keys on both sides.
//! Own test binary, because the random source is process-wide and must be
//! installed before `sodium_init`.

mod common;

use std::collections::VecDeque;
use std::ffi::{c_char, c_int, c_void};
use std::sync::Mutex;

use common::{FixedRandom, TestRandom};
use jazz_e2ee::primitives::sign;
use libsodium_sys as ffi;

static BYTES: Mutex<VecDeque<u8>> = Mutex::new(VecDeque::new());

unsafe extern "C" fn name() -> *const c_char {
    c"fixed".as_ptr()
}

unsafe extern "C" fn random() -> u32 {
    let mut bytes = [0u8; 4];
    // SAFETY: a 4-byte writable buffer.
    unsafe { buf(bytes.as_mut_ptr().cast(), 4) };
    u32::from_le_bytes(bytes)
}

unsafe extern "C" fn stir() {}

unsafe extern "C" fn uniform(upper: u32) -> u32 {
    // SAFETY: no preconditions.
    unsafe { random() % upper.max(1) }
}

unsafe extern "C" fn buf(dest: *mut c_void, size: usize) {
    // Initialisation draws bytes before any are supplied; those are zeros.
    let mut bytes = BYTES.lock().unwrap();
    for index in 0..size {
        // SAFETY: libsodium passes a buffer of `size` writable bytes.
        unsafe { *dest.cast::<u8>().add(index) = bytes.pop_front().unwrap_or(0) };
    }
}

unsafe extern "C" fn close() -> c_int {
    0
}

static IMPLEMENTATION: ffi::randombytes_implementation = ffi::randombytes_implementation {
    implementation_name: Some(name),
    random: Some(random),
    stir: Some(stir),
    uniform: Some(uniform),
    buf: Some(buf),
    close: Some(close),
};

fn supply(bytes: &[u8]) {
    let mut queue = BYTES.lock().unwrap();
    assert!(
        queue.is_empty(),
        "libsodium did not use every supplied byte"
    );
    queue.extend(bytes);
}

fn consumed() {
    assert!(
        BYTES.lock().unwrap().is_empty(),
        "libsodium did not use every supplied byte"
    );
}

#[test]
fn randomised_operations_match_byte_for_byte() {
    // SAFETY: set once, before initialisation; the implementation is static.
    unsafe {
        assert_eq!(ffi::randombytes_set_implementation(&IMPLEMENTATION), 0);
        assert!(ffi::sodium_init() >= 0);
    }
    let mut rng = TestRandom(21);
    for _ in 0..100 {
        // Signing keypair.
        let seed = rng.bytes(32);
        supply(&seed);
        let (mut public, mut secret) = ([0u8; 32], [0u8; 64]);
        // SAFETY: Ed25519-sized buffers.
        assert_eq!(
            unsafe { ffi::crypto_sign_keypair(public.as_mut_ptr(), secret.as_mut_ptr()) },
            0
        );
        consumed();
        let ours = sign::SigningKeyPair::generate(&mut FixedRandom(seed)).unwrap();
        assert_eq!((ours.public_key, *ours.secret_key), (public, secret));
    }
}
