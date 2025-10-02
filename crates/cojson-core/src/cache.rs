use ed25519_dalek::SigningKey;
use lru::LruCache;
use salsa20::{cipher::Key, XSalsa20};
use std::{cell::RefCell, num::NonZero};

use crate::{KeySecret, SignerSecret};

#[derive(Debug, Clone)]
pub struct CryptoCache {
    xsalsa20_key_cache: RefCell<LruCache<KeySecret, Key<XSalsa20>>>,
    ed25519_signing_key_cache: RefCell<LruCache<SignerSecret, SigningKey>>,
}

impl CryptoCache {
    pub fn new() -> Self {
        Self {
            xsalsa20_key_cache: RefCell::new(LruCache::new(NonZero::new(1000).unwrap())),
            ed25519_signing_key_cache: RefCell::new(LruCache::new(NonZero::new(1000).unwrap())),
        }
    }

    /// Get or derive the XSalsa20 key from a KeySecret, using the cache.
    pub fn get_xsalsa20_key(&self, key_secret: &KeySecret) -> Key<XSalsa20> {
        let mut cache = self.xsalsa20_key_cache.borrow_mut();
        if let Some(key) = cache.get(key_secret) {
            return key.clone();
        }

        let bytes: [u8; 32] = key_secret.into();
        let key: Key<XSalsa20> = bytes.into();
        cache.put(key_secret.to_owned(), key.clone());
        key
    }

    /// Get or derive the Ed25519 SigningKey from a SignerSecret, using the cache.
    pub fn get_ed25519_signing_key(&self, signer_secret: &SignerSecret) -> SigningKey {
        let mut cache = self.ed25519_signing_key_cache.borrow_mut();
        if let Some(signing_key) = cache.get(signer_secret) {
            return signing_key.clone();
        }

        let signing_key: SigningKey = signer_secret.into();
        cache.put(signer_secret.to_owned(), signing_key.clone());
        signing_key
    }
}
