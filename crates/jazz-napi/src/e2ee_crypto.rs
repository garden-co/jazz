//! Native libsodium primitives. Envelope framing and context derivation live in TypeScript.
use std::sync::OnceLock;

use libsodium_sys::{
    crypto_aead_xchacha20poly1305_ietf_decrypt, crypto_aead_xchacha20poly1305_ietf_encrypt,
    crypto_box_keypair, crypto_box_seal, crypto_box_seal_open, crypto_generichash,
    crypto_sign_detached, crypto_sign_keypair, crypto_sign_seed_keypair,
    crypto_sign_verify_detached, randombytes_buf, sodium_init, sodium_memzero,
};
use libsodium_sys::{
    crypto_secretstream_xchacha20poly1305_init_pull,
    crypto_secretstream_xchacha20poly1305_init_push, crypto_secretstream_xchacha20poly1305_pull,
    crypto_secretstream_xchacha20poly1305_push, crypto_secretstream_xchacha20poly1305_state,
};
use napi::bindgen_prelude::{Error, Result, Uint8Array};
use napi_derive::napi;

/// Owned native stream state, never serialised into JavaScript memory.
#[napi]
pub struct E2eeSodiumStream {
    state: Option<Box<crypto_secretstream_xchacha20poly1305_state>>,
    header: Vec<u8>,
    encrypting: bool,
}

/// One authenticated stream record.
#[napi(object)]
pub struct E2eeSodiumStreamRecord {
    pub message: Uint8Array,
    pub final_record: bool,
}

#[napi]
impl E2eeSodiumStream {
    /// Without a header starts encryption; a supplied header starts decryption.
    #[napi(constructor)]
    pub fn new(key: Uint8Array, header: Option<Uint8Array>) -> Result<Self> {
        if key.len() != 32 || header.as_ref().is_some_and(|value| value.len() != 24) {
            return Err(Error::from_reason("Invalid E2EE stream key or header"));
        }
        initialise()?;
        let encrypting = header.is_none();
        let mut stream = Self {
            state: Some(Box::new(crypto_secretstream_xchacha20poly1305_state {
                k: [0; 32],
                nonce: [0; 12],
                _pad: [0; 8],
            })),
            header: header.map_or_else(|| vec![0; 24], |value| value.to_vec()),
            encrypting,
        };
        let state = stream
            .state
            .as_deref_mut()
            .ok_or_else(|| Error::from_reason("Closed E2EE stream"))?;
        // SAFETY: state is uniquely owned; key and header lengths were checked above.
        let result = unsafe {
            if encrypting {
                crypto_secretstream_xchacha20poly1305_init_push(
                    state,
                    stream.header.as_mut_ptr(),
                    key.as_ptr(),
                )
            } else {
                crypto_secretstream_xchacha20poly1305_init_pull(
                    state,
                    stream.header.as_ptr(),
                    key.as_ptr(),
                )
            }
        };
        if result != 0 {
            return Err(Error::from_reason("E2EE stream initialisation failed"));
        }
        Ok(stream)
    }

    /// Public, non-secret stream header.
    #[napi(getter)]
    pub fn header(&self) -> Uint8Array {
        self.header.clone().into()
    }

    /// Encrypts one bounded record; final records must be empty.
    #[napi]
    pub fn push(
        &mut self,
        message: Uint8Array,
        context: Uint8Array,
        final_record: bool,
    ) -> Result<Uint8Array> {
        if !self.encrypting || message.len() > 65_536 || (final_record && !message.is_empty()) {
            return Err(Error::from_reason("Invalid E2EE stream record"));
        }
        let state = self
            .state
            .as_deref_mut()
            .ok_or_else(|| Error::from_reason("Closed E2EE stream"))?;
        let mut output = vec![0; message.len() + 17];
        let mut length = 0;
        // SAFETY: state is uniquely owned and output includes the 17-byte authentication overhead.
        let result = unsafe {
            crypto_secretstream_xchacha20poly1305_push(
                state,
                output.as_mut_ptr(),
                &mut length,
                message.as_ptr(),
                message.len() as u64,
                context.as_ptr(),
                context.len() as u64,
                if final_record { 3 } else { 0 },
            )
        };
        if result != 0 || length != output.len() as u64 {
            self.dispose();
            return Err(Error::from_reason("E2EE stream encryption failed"));
        }
        if final_record {
            self.dispose();
        }
        Ok(output.into())
    }

    /// Returns plaintext only after authenticating the record.
    #[napi]
    pub fn pull(
        &mut self,
        ciphertext: Uint8Array,
        context: Uint8Array,
    ) -> Result<E2eeSodiumStreamRecord> {
        if self.encrypting || !(17..=65_553).contains(&ciphertext.len()) {
            return Err(Error::from_reason("Invalid E2EE stream record"));
        }
        let state = self
            .state
            .as_deref_mut()
            .ok_or_else(|| Error::from_reason("Closed E2EE stream"))?;
        let mut message = vec![0; ciphertext.len() - 17];
        let mut length = 0;
        let mut tag = 0;
        // SAFETY: ciphertext includes the authentication overhead; message has sufficient capacity.
        let result = unsafe {
            crypto_secretstream_xchacha20poly1305_pull(
                state,
                message.as_mut_ptr(),
                &mut length,
                &mut tag,
                ciphertext.as_ptr(),
                ciphertext.len() as u64,
                context.as_ptr(),
                context.len() as u64,
            )
        };
        if result != 0
            || length != message.len() as u64
            || !matches!(tag, 0 | 3)
            || (tag == 3 && !message.is_empty())
        {
            // SAFETY: the vector owns this writable buffer, which must not escape on failure.
            unsafe {
                sodium_memzero(message.as_mut_ptr().cast(), message.len());
            }
            self.dispose();
            return Err(Error::from_reason("E2EE stream authentication failed"));
        }
        if tag == 3 {
            self.dispose();
        }
        Ok(E2eeSodiumStreamRecord {
            message: message.into(),
            final_record: tag == 3,
        })
    }

    /// Wipes stream keys immediately; also called on drop.
    #[napi]
    pub fn dispose(&mut self) {
        if let Some(mut state) = self.state.take() {
            // SAFETY: the box exclusively owns this correctly sized state allocation.
            unsafe {
                sodium_memzero(
                    (&mut *state as *mut crypto_secretstream_xchacha20poly1305_state).cast(),
                    std::mem::size_of_val(&*state),
                );
            }
        }
    }
}

impl Drop for E2eeSodiumStream {
    fn drop(&mut self) {
        self.dispose();
    }
}

/// Owned libsodium device encryption keys.
#[napi(object)]
pub struct SodiumDeviceKeyPair {
    pub public_key: Uint8Array,
    pub private_key: Uint8Array,
}

/// Generates a device keypair using libsodium's system RNG.
#[napi(js_name = "e2eeSodiumKeyPair")]
pub fn e2ee_sodium_key_pair() -> Result<SodiumDeviceKeyPair> {
    initialise()?;
    let mut public_key = vec![0; 32];
    let mut private_key = vec![0; 32];
    // SAFETY: disjoint output buffers each contain the required 32 writable bytes.
    let result = unsafe { crypto_box_keypair(public_key.as_mut_ptr(), private_key.as_mut_ptr()) };
    if result != 0 {
        private_key.fill(0);
        return Err(Error::from_reason("E2EE device key generation failed"));
    }
    Ok(SodiumDeviceKeyPair {
        public_key: public_key.into(),
        private_key: private_key.into(),
    })
}

/// Seals plaintext to a device; this does not authenticate a sender identity.
#[napi(js_name = "e2eeSodiumSeal")]
pub fn e2ee_sodium_seal(public_key: Uint8Array, plaintext: Uint8Array) -> Result<Uint8Array> {
    if public_key.len() != 32 {
        return Err(Error::from_reason("Invalid E2EE device key length"));
    }
    initialise()?;
    let length = plaintext
        .len()
        .checked_add(48)
        .ok_or_else(|| Error::from_reason("E2EE plaintext too large"))?;
    let mut output = vec![0; length];
    // SAFETY: public key length is checked; output includes the 48-byte sealed-box overhead.
    let result = unsafe {
        crypto_box_seal(
            output.as_mut_ptr(),
            plaintext.as_ptr(),
            plaintext.len() as u64,
            public_key.as_ptr(),
        )
    };
    if result != 0 {
        return Err(Error::from_reason("E2EE device encryption failed"));
    }
    Ok(output.into())
}

/// Opens a sealed box, returning plaintext only after successful authentication.
#[napi(js_name = "e2eeSodiumOpen")]
pub fn e2ee_sodium_open(
    public_key: Uint8Array,
    private_key: Uint8Array,
    ciphertext: Uint8Array,
) -> Result<Uint8Array> {
    if public_key.len() != 32 || private_key.len() != 32 {
        return Err(Error::from_reason("Invalid E2EE device key length"));
    }
    initialise()?;
    let length = ciphertext
        .len()
        .checked_sub(48)
        .ok_or_else(|| Error::from_reason("Invalid E2EE sealed box length"))?;
    let mut output = vec![0; length];
    // SAFETY: key lengths are checked, ciphertext contains the overhead, and output
    // covers its plaintext. No plaintext is returned after authentication failure.
    let result = unsafe {
        crypto_box_seal_open(
            output.as_mut_ptr(),
            ciphertext.as_ptr(),
            ciphertext.len() as u64,
            public_key.as_ptr(),
            private_key.as_ptr(),
        )
    };
    if result != 0 {
        output.fill(0);
        return Err(Error::from_reason("E2EE authentication failed"));
    }
    Ok(output.into())
}

/// Generates independent Ed25519 keys, or reconstructs them from an exact 32-byte seed.
#[napi(js_name = "e2eeSodiumSigningKeyPair")]
pub fn e2ee_sodium_signing_key_pair(seed: Option<Uint8Array>) -> Result<SodiumDeviceKeyPair> {
    if seed.as_ref().is_some_and(|value| value.len() != 32) {
        return Err(Error::from_reason("Invalid E2EE signing seed"));
    }
    initialise()?;
    let mut public_key = vec![0; 32];
    let mut private_key = vec![0; 64];
    // SAFETY: output buffers have Ed25519 sizes; the optional seed length is checked.
    let result = unsafe {
        match seed {
            Some(seed) => crypto_sign_seed_keypair(
                public_key.as_mut_ptr(),
                private_key.as_mut_ptr(),
                seed.as_ptr(),
            ),
            None => crypto_sign_keypair(public_key.as_mut_ptr(), private_key.as_mut_ptr()),
        }
    };
    if result != 0 {
        // SAFETY: the vector owns its full writable allocation.
        unsafe { sodium_memzero(private_key.as_mut_ptr().cast(), private_key.len()) };
        return Err(Error::from_reason("E2EE signing key generation failed"));
    }
    Ok(SodiumDeviceKeyPair {
        public_key: public_key.into(),
        private_key: private_key.into(),
    })
}

/// Produces a detached Ed25519 signature; rejects inconsistent secret-key material.
#[napi(js_name = "e2eeSodiumSign")]
pub fn e2ee_sodium_sign(private_key: Uint8Array, message: Uint8Array) -> Result<Uint8Array> {
    if private_key.len() != 64 {
        return Err(Error::from_reason("Invalid E2EE signing key"));
    }
    initialise()?;
    let mut public = [0; 32];
    let mut checked = [0; 64];
    // SAFETY: the input contains a 32-byte seed and both outputs have Ed25519 sizes.
    let generated = unsafe {
        crypto_sign_seed_keypair(
            public.as_mut_ptr(),
            checked.as_mut_ptr(),
            private_key.as_ptr(),
        )
    };
    let mut signature = vec![0; 64];
    let valid = generated == 0 && checked.as_slice() == private_key.as_ref();
    let result = if valid {
        // SAFETY: key and output sizes are fixed; message covers its stated length.
        unsafe {
            crypto_sign_detached(
                signature.as_mut_ptr(),
                std::ptr::null_mut(),
                message.as_ptr(),
                message.len() as u64,
                checked.as_ptr(),
            )
        }
    } else {
        -1
    };
    // SAFETY: checked is a writable 64-byte temporary secret, never returned.
    unsafe { sodium_memzero(checked.as_mut_ptr().cast(), checked.len()) };
    if result != 0 {
        return Err(Error::from_reason("Invalid E2EE signing key"));
    }
    Ok(signature.into())
}

/// Verifies a detached Ed25519 signature; malformed sizes return false.
#[napi(js_name = "e2eeSodiumVerify")]
pub fn e2ee_sodium_verify(
    public_key: Uint8Array,
    message: Uint8Array,
    signature: Uint8Array,
) -> Result<bool> {
    if public_key.len() != 32 || signature.len() != 64 {
        return Ok(false);
    }
    initialise()?;
    // SAFETY: public key and signature lengths are checked; message covers its length.
    Ok(unsafe {
        crypto_sign_verify_detached(
            signature.as_ptr(),
            message.as_ptr(),
            message.len() as u64,
            public_key.as_ptr(),
        )
    } == 0)
}

fn initialise() -> Result<()> {
    static READY: OnceLock<bool> = OnceLock::new();
    // SAFETY: sodium_init has no arguments and is safe to call once before all primitives.
    if *READY.get_or_init(|| unsafe { sodium_init() >= 0 }) {
        Ok(())
    } else {
        Err(Error::from_reason("E2EE crypto initialisation failed"))
    }
}

fn check_key_nonce(key: &[u8], nonce: &[u8]) -> Result<()> {
    if key.len() != 32 || nonce.len() != 24 {
        return Err(Error::from_reason("Invalid E2EE key or nonce length"));
    }
    initialise()
}

/// Returns a fresh 24-byte XChaCha20 nonce from libsodium's system RNG.
#[napi(js_name = "e2eeSodiumNonce")]
pub fn e2ee_sodium_nonce() -> Result<Uint8Array> {
    initialise()?;
    let mut nonce = vec![0; 24];
    // SAFETY: nonce owns 24 writable bytes and sodium is initialised.
    unsafe { randombytes_buf(nonce.as_mut_ptr().cast(), nonce.len()) };
    Ok(nonce.into())
}

/// Computes the protocol's 32-byte keyed BLAKE2b digest with a 32-byte key.
#[napi(js_name = "e2eeSodiumHash")]
pub fn e2ee_sodium_hash(key: Uint8Array, input: Uint8Array) -> Result<Uint8Array> {
    if key.len() != 32 {
        return Err(Error::from_reason("Invalid E2EE key length"));
    }
    initialise()?;
    let mut output = vec![0; 32];
    // SAFETY: every pointer covers its supplied length; output and inputs are disjoint.
    let result = unsafe {
        crypto_generichash(
            output.as_mut_ptr(),
            output.len(),
            input.as_ptr(),
            input.len() as u64,
            key.as_ptr(),
            key.len(),
        )
    };
    if result != 0 {
        return Err(Error::from_reason("E2EE key derivation failed"));
    }
    Ok(output.into())
}

/// Encrypts with XChaCha20-Poly1305-IETF, returning ciphertext and its appended tag.
#[napi(js_name = "e2eeSodiumEncrypt")]
pub fn e2ee_sodium_encrypt(
    key: Uint8Array,
    nonce: Uint8Array,
    aad: Uint8Array,
    plaintext: Uint8Array,
) -> Result<Uint8Array> {
    check_key_nonce(&key, &nonce)?;
    let length = plaintext
        .len()
        .checked_add(16)
        .ok_or_else(|| Error::from_reason("E2EE plaintext too large"))?;
    let mut output = vec![0; length];
    // SAFETY: key/nonce lengths are checked; output has room for the message and tag.
    // Optional output length and unused secret nonce pointers may be null.
    let result = unsafe {
        crypto_aead_xchacha20poly1305_ietf_encrypt(
            output.as_mut_ptr(),
            std::ptr::null_mut(),
            plaintext.as_ptr(),
            plaintext.len() as u64,
            aad.as_ptr(),
            aad.len() as u64,
            std::ptr::null(),
            nonce.as_ptr(),
            key.as_ptr(),
        )
    };
    if result != 0 {
        return Err(Error::from_reason("E2EE encryption failed"));
    }
    Ok(output.into())
}

/// Authenticates before returning plaintext; rejects short or invalid ciphertext.
#[napi(js_name = "e2eeSodiumDecrypt")]
pub fn e2ee_sodium_decrypt(
    key: Uint8Array,
    nonce: Uint8Array,
    aad: Uint8Array,
    ciphertext: Uint8Array,
) -> Result<Uint8Array> {
    check_key_nonce(&key, &nonce)?;
    let length = ciphertext
        .len()
        .checked_sub(16)
        .ok_or_else(|| Error::from_reason("Invalid E2EE ciphertext length"))?;
    let mut output = vec![0; length];
    // SAFETY: input includes the tag, output covers the plaintext, and key/nonce
    // lengths are checked. No output bytes are returned unless authentication succeeds.
    let result = unsafe {
        crypto_aead_xchacha20poly1305_ietf_decrypt(
            output.as_mut_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            ciphertext.as_ptr(),
            ciphertext.len() as u64,
            aad.as_ptr(),
            aad.len() as u64,
            nonce.as_ptr(),
            key.as_ptr(),
        )
    };
    if result != 0 {
        output.fill(0);
        return Err(Error::from_reason("E2EE authentication failed"));
    }
    Ok(output.into())
}
