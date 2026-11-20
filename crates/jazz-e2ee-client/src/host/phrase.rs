//! The recovery phrase: a 32-byte recovery secret as 24 words of the BIP-39
//! English word list (256 bits and an 8-bit checksum, 11 bits a word), the
//! same words the TypeScript client's `RecoveryPhrase` makes, so a phrase
//! moves between platforms. The runtime's recovery secret is exactly these
//! 32 bytes (`E2EE_PROTOCOL.md` §2.9).
//!
//! Both a phrase and its bytes are the secret, so every buffer that holds
//! either (the bits, a lowercased copy, the words, the bytes) is zeroized
//! when dropped, and the phrase is written into one buffer reserved up front,
//! so it never grows and leaves a copy behind.

use jazz_e2ee::Zeroizing;
use sha2::{Digest, Sha256};

/// The 2048 words, in order.
const LIST: &str = include_str!("english.txt");

/// Why a phrase is not a recovery phrase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhraseError {
    /// Not 24 words.
    InvalidLength,
    /// A word that is not in the list.
    InvalidWord,
    /// The words are in the list but the checksum is wrong (a typo, or words
    /// out of order).
    InvalidChecksum,
}

impl PhraseError {
    /// The code the TypeScript client's `RecoveryPhraseError` gives it.
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidLength => "invalid-length",
            Self::InvalidWord => "invalid-word",
            Self::InvalidChecksum => "invalid-checksum",
        }
    }
}

fn words() -> Vec<&'static str> {
    LIST.lines().collect()
}

/// The longest phrase: 24 words of at most 8 letters, and the spaces between.
const MAX_PHRASE: usize = 24 * 8 + 23;

/// The 24-word phrase of `secret`.
pub fn from_secret(secret: &[u8; 32]) -> Zeroizing<String> {
    let words = words();
    let mut bits = Zeroizing::new([0u8; 33]);
    bits[..32].copy_from_slice(secret);
    bits[32] = Sha256::digest(secret)[0];
    let mut phrase = Zeroizing::new(String::with_capacity(MAX_PHRASE));
    for word in 0..24 {
        let mut index = 0usize;
        for bit in 0..11 {
            let at = word * 11 + bit;
            index = (index << 1) | usize::from((bits[at / 8] >> (7 - at % 8)) & 1);
        }
        if word > 0 {
            phrase.push(' ');
        }
        phrase.push_str(words[index]);
    }
    phrase
}

/// The 32 secret bytes of `phrase`: words separated by any white space, in
/// any letter case.
pub fn to_secret(phrase: &str) -> Result<Zeroizing<[u8; 32]>, PhraseError> {
    let phrase = Zeroizing::new(phrase.to_lowercase());
    if phrase.split_whitespace().count() != 24 {
        return Err(PhraseError::InvalidLength);
    }
    let words = words();
    let mut bits = Zeroizing::new([0u8; 33]);
    for (word, text) in phrase.split_whitespace().enumerate() {
        let index = words
            .iter()
            .position(|known| *known == text)
            .ok_or(PhraseError::InvalidWord)?;
        for bit in 0..11 {
            if (index >> (10 - bit)) & 1 == 1 {
                let at = word * 11 + bit;
                bits[at / 8] |= 1 << (7 - at % 8);
            }
        }
    }
    let mut secret = Zeroizing::new([0u8; 32]);
    secret.copy_from_slice(&bits[..32]);
    if Sha256::digest(*secret)[0] != bits[32] {
        return Err(PhraseError::InvalidChecksum);
    }
    Ok(secret)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Contract: the phrase is BIP-39's, the one the TypeScript client makes:
    /// the standard vectors for the all-zero and all-ones secrets.
    #[test]
    fn a_phrase_is_the_standard_one() {
        let zero = format!("{}art", "abandon ".repeat(23));
        assert_eq!(*from_secret(&[0; 32]), zero);
        assert_eq!(to_secret(&zero).map(|secret| *secret), Ok([0; 32]));
        let ones = format!("{}vote", "zoo ".repeat(23));
        assert_eq!(*from_secret(&[0xff; 32]), ones);
        assert_eq!(
            to_secret(&ones.to_uppercase()).map(|secret| *secret),
            Ok([0xff; 32])
        );
    }

    /// Contract: the embedded list is BIP-39's English list, byte for byte
    /// (its published SHA-256), so a phrase written down today reads back on
    /// every platform that shares it.
    #[test]
    fn the_word_list_is_the_published_one() {
        let digest: String = Sha256::digest(LIST.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_eq!(
            digest,
            "2f5eed53a4727b4bf8880d8f3f199efc90e58503646d9ff8eff3a2ed3b24dbda"
        );
        assert_eq!(words().len(), 2048);
    }

    /// Contract: a phrase that is not exactly a recovery phrase is refused
    /// with the code the TypeScript client gives.
    #[test]
    fn a_wrong_phrase_is_refused_by_name() {
        let right = from_secret(&[7; 32]);
        assert_eq!(to_secret(&right).map(|secret| *secret), Ok([7; 32]));
        let short: Vec<&str> = right.split(' ').take(23).collect();
        assert_eq!(to_secret(&short.join(" ")), Err(PhraseError::InvalidLength));
        assert_eq!(
            to_secret(&right.replacen(right.split(' ').next().unwrap(), "notaword", 1)),
            Err(PhraseError::InvalidWord)
        );
        assert_eq!(
            to_secret(&format!("{}abandon", "abandon ".repeat(23))),
            Err(PhraseError::InvalidChecksum)
        );
    }
}
