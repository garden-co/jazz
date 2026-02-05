use ed25519_dalek::SigningKey;
use lru::LruCache;
use salsa20::{cipher::Key, XSalsa20};
use std::{cell::RefCell, num::NonZero};

use crate::core::{CoJsonCoreError, KeySecret, SignerSecret};

#[derive(Debug, Clone)]
pub struct CryptoCache {
    xsalsa20_key_cache: RefCell<LruCache<KeySecret, Key<XSalsa20>>>,
    ed25519_signing_key_cache: RefCell<LruCache<SignerSecret, SigningKey>>,
}

impl CryptoCache {
    pub fn new() -> Self {
        Self {
            xsalsa20_key_cache: RefCell::new(LruCache::new(NonZero::new(2).unwrap())),
            ed25519_signing_key_cache: RefCell::new(LruCache::new(NonZero::new(2).unwrap())),
        }
    }

    /// Get or derive the XSalsa20 key from a KeySecret, using the cache.
    /// This avoid to run bs58 decoding multiple times for the same key.
    pub fn get_xsalsa20_key(
        &self,
        key_secret: &KeySecret,
    ) -> Result<Key<XSalsa20>, CoJsonCoreError> {
        let mut cache = self.xsalsa20_key_cache.borrow_mut();
        if let Some(key) = cache.get(key_secret) {
            return Ok(*key);
        }

        let bytes: [u8; 32] = key_secret.try_into()?;
        let key: Key<XSalsa20> = bytes.into();
        cache.put(key_secret.to_owned(), key);
        Ok(key)
    }

    /// Get or derive the Ed25519 SigningKey from a SignerSecret, using the cache.
    /// This avoid to run bs58 decoding multiple times for the same key.
    pub fn get_ed25519_signing_key(
        &self,
        signer_secret: &SignerSecret,
    ) -> Result<SigningKey, CoJsonCoreError> {
        let mut cache = self.ed25519_signing_key_cache.borrow_mut();
        if let Some(signing_key) = cache.get(signer_secret) {
            return Ok(signing_key.clone());
        }

        let signing_key: SigningKey = signer_secret.try_into()?;
        cache.put(signer_secret.to_owned(), signing_key.clone());
        Ok(signing_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crypto_cache_different_keys() {
        let crypto_cache = CryptoCache::new();
        let key_secret = KeySecret(String::from(
            "signer_z3FdM2ucYXUkbJQgPRf8R4Di6exd2sNPVaHaJHhQ8WAqi",
        ));
        let key = crypto_cache.get_xsalsa20_key(&key_secret).unwrap();
        let key2 = crypto_cache.get_xsalsa20_key(&key_secret).unwrap();
        assert_eq!(key, key2);
    }

    #[test]
    fn test_crypto_cache_different_signer_secrets() {
        let crypto_cache = CryptoCache::new();
        let signer_secret = SignerSecret(String::from(
            "signer_z3FdM2ucYXUkbJQgPRf8R4Di6exd2sNPVaHaJHhQ8WAqi",
        ));
        let signing_key = crypto_cache
            .get_ed25519_signing_key(&signer_secret)
            .unwrap();
        let signing_key2 = crypto_cache
            .get_ed25519_signing_key(&signer_secret)
            .unwrap();
        assert_eq!(signing_key, signing_key2);
    }
}
