//! Text primitives shared by Jazz's authority and its clients.
//!
//! - [`merge3`]: the authority's merge of a text-merging column (SPEC 4 §4.3).
//! - [`Anchor`] and [`Range`]: a position or span in a text value that any
//!   holder of a newer value can find again, with no history: each carries
//!   the text around it (W3C text quote selectors). Tools and LLMs can author
//!   them from the current value alone.
//! - [`plain_units`]: the units a plain-text merge never splits (words,
//!   numbers, and every other character with its marks), which the text
//!   index's analyzer (SPEC 20.10.1) also cuts words with.
//!
//!   Positions count UTF-16 code units (JavaScript, Kotlin), and the `_utf8`
//!   methods take and return UTF-8 byte offsets (`str` indices) instead, as
//!   Rust and Go count them. Either way the anchor itself is the same, so
//!   hosts that count differently share them.
//!
//! Neither depends on storage, so both also build for WASM and native hosts.

mod anchor;
mod merge;
mod range;
mod units;

pub use anchor::{Anchor, CONTEXT_UNITS, Resolved, Side};
pub use merge::{MERGE_WORK_BUDGET, merge3, merge3_within, try_merge3, try_merge3_within};
pub use range::{Range, ResolvedRange};
pub use units::{PlainUnits, is_word_unit, plain_units};

/// A UTF-8 byte offset given to a `_utf8` method that is not on a char
/// boundary of the text: inside a code point, or past its end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NotCharBoundary {
    /// The offset refused.
    pub offset: usize,
}

impl std::fmt::Display for NotCharBoundary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UTF-8 offset {} is not on a char boundary of the text",
            self.offset
        )
    }
}

impl std::error::Error for NotCharBoundary {}

/// The UTF-16 offset of the UTF-8 byte offset `byte` of `text`.
pub(crate) fn utf8_to_utf16(text: &str, byte: usize) -> Result<usize, NotCharBoundary> {
    if !text.is_char_boundary(byte) {
        return Err(NotCharBoundary { offset: byte });
    }
    Ok(text[..byte].encode_utf16().count())
}

/// The UTF-8 byte offset of the UTF-16 offset `units` of `text`, rounded
/// down to a char boundary and clamped to its end.
pub(crate) fn utf16_to_utf8(text: &str, units: usize) -> usize {
    let mut counted = 0;
    for (byte, character) in text.char_indices() {
        counted += character.len_utf16();
        if counted > units {
            return byte;
        }
    }
    text.len()
}

/// How a text-merging column's values are cut into the units a merge never
/// splits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextFormat {
    /// Words, whitespace and other characters.
    Plain,
    /// Plain units, plus delimiter runs, list markers and link targets.
    Markdown,
    /// Plain units, plus tags and entities.
    Html,
}
