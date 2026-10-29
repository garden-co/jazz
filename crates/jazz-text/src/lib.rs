//! Text primitives shared by Jazz's authority and its clients.
//!
//! - [`merge3`]: the authority's merge of a text-merging column (SPEC 4 §4.3).
//! - [`Anchor`] and [`Range`]: a position or span in a text value that any
//!   holder of a newer value can find again, with no history: each carries
//!   the text around it (W3C text quote selectors). Tools and LLMs can author
//!   them from the current value alone.
//!
//! Neither depends on storage, so both also build for WASM and native hosts.

mod anchor;
mod merge;
mod range;

pub use anchor::{Anchor, CONTEXT_UNITS, Resolved, Side};
pub use merge::{MERGE_WORK_BUDGET, merge3, merge3_within};
pub use range::{Range, ResolvedRange};

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
