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
//! Neither depends on storage, so both also build for WASM and native hosts.

mod anchor;
mod merge;
mod range;
mod units;

pub use anchor::{Anchor, CONTEXT_UNITS, Resolved, Side};
pub use merge::{MERGE_WORK_BUDGET, merge3, merge3_within, try_merge3, try_merge3_within};
pub use range::{Range, ResolvedRange};
pub use units::{PlainUnits, is_word_unit, plain_units};

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
