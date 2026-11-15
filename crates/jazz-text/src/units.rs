//! Plain-text units, shared by the merge and the text index's analyzer.

use crate::TextFormat;
use crate::merge::{token_len, word_token};

/// `text` cut into the units a [`TextFormat::Plain`] merge keeps whole: runs
/// of word characters (letters and digits of the scripts written with
/// spaces, with their marks), and every other character on its own (with
/// the marks, joiners and modifiers that complete it), including each
/// ideograph, kana and Thai letter. The units concatenate back to `text`.
///
/// The character tables are fixed in this crate, not taken from the
/// toolchain's Unicode version, so every build cuts a value the same way.
pub fn plain_units(text: &str) -> PlainUnits<'_> {
    PlainUnits { rest: text }
}

/// Iterator returned by [`plain_units`].
#[derive(Clone, Debug)]
pub struct PlainUnits<'a> {
    rest: &'a str,
}

impl<'a> Iterator for PlainUnits<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        if self.rest.is_empty() {
            return None;
        }
        // Plain text has no line-start tokens; the flag only matters for
        // Markdown list markers.
        let len = token_len(TextFormat::Plain, self.rest, false);
        let (unit, rest) = self.rest.split_at(len);
        self.rest = rest;
        Some(unit)
    }
}

/// Whether a unit of [`plain_units`] is a run of word characters (a word or
/// number), not a single other character.
pub fn is_word_unit(unit: &str) -> bool {
    word_token(unit)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The units of a value are exactly its merge tokens, cut the same way.
    #[test]
    fn units_concatenate_back_and_classify() {
        let text = "Don't stop—3.14 東京 👍🏽 naïve";
        let units: Vec<&str> = plain_units(text).collect();
        assert_eq!(units.concat(), text);
        let words: Vec<&str> = units.iter().copied().filter(|u| is_word_unit(u)).collect();
        assert_eq!(words, ["Don", "t", "stop", "3", "14", "naïve"]);
        assert!(units.contains(&"東") && units.contains(&"👍🏽"));
    }
}
