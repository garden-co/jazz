//! Built-in block-granular markdown rung-3 merge strategy.
//!
//! This v1 strategy recognizes only coarse markdown blocks: headings,
//! paragraphs, list items, and fenced code blocks. It deliberately does not
//! reconcile inline formatting spans. Op-stream edits on format-declared
//! columns and N-head strategy inputs are staging limitations documented on
//! [`crate::merge_strategy::MergeStrategy`].

use crate::merge_strategy::{MergeStrategy, MergeStrategyInput, MergeStrategyOutput};
use crate::text_merge::{EventId, TextEvent, TextEventGraph, TextMergeError, TieBreak, diff};
use crate::tx::TxId;

/// Stable id for the built-in simple markdown strategy.
pub const STRATEGY_ID: &str = "builtin.simple-markdown";
/// Current simple markdown strategy version.
pub const STRATEGY_VERSION: u32 = 1;

/// Built-in block-granular markdown strategy.
#[derive(Clone, Copy, Debug, Default)]
pub struct SimpleMarkdownStrategy;

#[derive(Clone, Debug, Eq, PartialEq)]
struct Block {
    bytes: Vec<u8>,
    start: usize,
    end: usize,
}

#[derive(Clone, Debug, Default)]
struct Projection {
    before: Vec<Vec<Vec<u8>>>,
    replacements: Vec<Option<Vec<u8>>>,
    tail: Vec<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ChangeRange {
    start: usize,
    end: usize,
}

impl MergeStrategy for SimpleMarkdownStrategy {
    fn id(&self) -> &str {
        STRATEGY_ID
    }

    fn version(&self) -> u32 {
        STRATEGY_VERSION
    }

    fn structural_proximity(&self, input: &MergeStrategyInput) -> bool {
        let base_blocks = parse_blocks(&input.base);
        let left = touched_blocks(
            &base_blocks,
            &change_ranges(&input.base, &input.left.materialized),
        );
        let right = touched_blocks(
            &base_blocks,
            &change_ranges(&input.base, &input.right.materialized),
        );
        left.iter().any(|l| {
            right
                .iter()
                .any(|r| l.abs_diff(*r) <= 1 || base_blocks.len() <= 1)
        })
    }

    fn merge(&self, input: &MergeStrategyInput) -> Result<MergeStrategyOutput, TextMergeError> {
        let merged = merge_markdown(input)?;
        Ok(MergeStrategyOutput {
            op_against_base: diff(&input.base, &merged),
            strategy_id: self.id().to_owned(),
            strategy_version: self.version(),
        })
    }
}

fn merge_markdown(input: &MergeStrategyInput) -> Result<Vec<u8>, TextMergeError> {
    let base_blocks = parse_blocks(&input.base);
    let left_blocks = parse_blocks(&input.left.materialized);
    let right_blocks = parse_blocks(&input.right.materialized);
    let left = project_side(&base_blocks, &left_blocks);
    let right = project_side(&base_blocks, &right_blocks);
    let left_ranges = change_ranges(&input.base, &input.left.materialized);
    let right_ranges = change_ranges(&input.base, &input.right.materialized);
    let mut out = Vec::new();

    for (idx, base) in base_blocks.iter().enumerate() {
        append_insertions(
            &mut out,
            &left.before[idx],
            input.left.head,
            &right.before[idx],
            input.right.head,
        );

        match (&left.replacements[idx], &right.replacements[idx]) {
            (None, None) => out.extend_from_slice(&base.bytes),
            (Some(bytes), None) | (None, Some(bytes)) => out.extend_from_slice(bytes),
            (Some(left_bytes), Some(right_bytes)) if left_bytes == right_bytes => {
                out.extend_from_slice(left_bytes);
            }
            (Some(left_bytes), Some(right_bytes)) => {
                let left_changed = range_touches_block(&left_ranges, base);
                let right_changed = range_touches_block(&right_ranges, base);
                if !ranges_overlap(&left_changed, &right_changed) {
                    out.extend(char_merge_block(
                        &base.bytes,
                        left_bytes,
                        right_bytes,
                        input.left.head,
                        input.right.head,
                    )?);
                } else if input.left.head > input.right.head {
                    out.extend_from_slice(left_bytes);
                } else {
                    out.extend_from_slice(right_bytes);
                }
            }
        }
    }
    append_insertions(
        &mut out,
        &left.tail,
        input.left.head,
        &right.tail,
        input.right.head,
    );
    Ok(out)
}

fn append_insertions(
    out: &mut Vec<u8>,
    left: &[Vec<u8>],
    left_head: TxId,
    right: &[Vec<u8>],
    right_head: TxId,
) {
    let (first, second) = if left_head <= right_head {
        (left, right)
    } else {
        (right, left)
    };
    for bytes in first.iter().chain(second.iter()) {
        out.extend_from_slice(bytes);
    }
}

fn project_side(base: &[Block], side: &[Block]) -> Projection {
    let mut projection = Projection {
        before: vec![Vec::new(); base.len()],
        replacements: vec![None; base.len()],
        tail: Vec::new(),
    };
    let mut side_idx = 0usize;
    for (base_idx, base_block) in base.iter().enumerate() {
        if side_idx < side.len() && side[side_idx].bytes == base_block.bytes {
            side_idx += 1;
            continue;
        }
        if let Some(found) = side[side_idx..]
            .iter()
            .position(|candidate| candidate.bytes == base_block.bytes)
        {
            projection.before[base_idx] = side[side_idx..side_idx + found]
                .iter()
                .map(|block| block.bytes.clone())
                .collect();
            side_idx += found + 1;
        } else if side_idx < side.len() {
            projection.replacements[base_idx] = Some(side[side_idx].bytes.clone());
            side_idx += 1;
        } else {
            projection.replacements[base_idx] = Some(Vec::new());
        }
    }
    projection.tail = side[side_idx..]
        .iter()
        .map(|block| block.bytes.clone())
        .collect();
    projection
}

fn char_merge_block(
    base: &[u8],
    left: &[u8],
    right: &[u8],
    left_head: TxId,
    right_head: TxId,
) -> Result<Vec<u8>, TextMergeError> {
    let mut graph = TextEventGraph::default();
    graph.insert(TextEvent {
        id: EventId(0),
        parents: Vec::new(),
        op: diff(base, base),
        tie_break: TieBreak(0),
    })?;
    graph.insert(TextEvent {
        id: EventId(1),
        parents: vec![EventId(0)],
        op: diff(base, left),
        tie_break: TieBreak(tx_tie_break(left_head)),
    })?;
    graph.insert(TextEvent {
        id: EventId(2),
        parents: vec![EventId(0)],
        op: diff(base, right),
        tie_break: TieBreak(tx_tie_break(right_head)),
    })?;
    graph.merge_heads(EventId(0), base, &[EventId(1), EventId(2)])
}

fn tx_tie_break(tx: TxId) -> u64 {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&tx.time.0.to_be_bytes());
    bytes.extend_from_slice(tx.node.0.as_bytes());
    let hash = blake3::hash(&bytes);
    u64::from_be_bytes(hash.as_bytes()[0..8].try_into().expect("hash slice length"))
}

fn parse_blocks(input: &[u8]) -> Vec<Block> {
    let mut blocks = Vec::new();
    let mut pos = 0usize;
    while pos < input.len() {
        let start = pos;
        let line_end = next_line_end(input, pos);
        let line = &input[pos..line_end];
        if is_blank_line(line) {
            pos = line_end;
            continue;
        }
        if fence_marker(line).is_some() {
            pos = line_end;
            while pos < input.len() {
                let end = next_line_end(input, pos);
                if fence_marker(&input[pos..end]).is_some() {
                    pos = end;
                    break;
                }
                pos = end;
            }
            pos = consume_blank_lines(input, pos);
            blocks.push(Block {
                bytes: input[start..pos].to_vec(),
                start,
                end: pos,
            });
            continue;
        }
        if is_heading(line) || is_list_item(line) {
            pos = line_end;
            pos = consume_blank_lines(input, pos);
            blocks.push(Block {
                bytes: input[start..pos].to_vec(),
                start,
                end: pos,
            });
            continue;
        }
        pos = line_end;
        while pos < input.len() {
            let end = next_line_end(input, pos);
            let next = &input[pos..end];
            if is_blank_line(next)
                || is_heading(next)
                || is_list_item(next)
                || fence_marker(next).is_some()
            {
                break;
            }
            pos = end;
        }
        pos = consume_blank_lines(input, pos);
        blocks.push(Block {
            bytes: input[start..pos].to_vec(),
            start,
            end: pos,
        });
    }
    blocks
}

fn next_line_end(input: &[u8], start: usize) -> usize {
    input[start..]
        .iter()
        .position(|byte| *byte == b'\n')
        .map_or(input.len(), |offset| start + offset + 1)
}

fn consume_blank_lines(input: &[u8], mut pos: usize) -> usize {
    while pos < input.len() {
        let end = next_line_end(input, pos);
        if !is_blank_line(&input[pos..end]) {
            break;
        }
        pos = end;
    }
    pos
}

fn trim_line(line: &[u8]) -> &[u8] {
    let mut start = 0usize;
    let mut end = line.len();
    while start < end && matches!(line[start], b' ' | b'\t') {
        start += 1;
    }
    while end > start && matches!(line[end - 1], b' ' | b'\t' | b'\r' | b'\n') {
        end -= 1;
    }
    &line[start..end]
}

fn is_blank_line(line: &[u8]) -> bool {
    trim_line(line).is_empty()
}

fn is_heading(line: &[u8]) -> bool {
    let trimmed = trim_line(line);
    let hashes = trimmed.iter().take_while(|byte| **byte == b'#').count();
    (1..=6).contains(&hashes) && trimmed.get(hashes) == Some(&b' ')
}

fn is_list_item(line: &[u8]) -> bool {
    let trimmed = trim_line(line);
    trimmed.starts_with(b"- ")
        || trimmed.starts_with(b"* ")
        || trimmed
            .iter()
            .position(|byte| *byte == b'.')
            .is_some_and(|dot| dot > 0 && trimmed.get(dot + 1) == Some(&b' '))
}

fn fence_marker(line: &[u8]) -> Option<u8> {
    let trimmed = trim_line(line);
    if trimmed.starts_with(b"```") {
        Some(b'`')
    } else if trimmed.starts_with(b"~~~") {
        Some(b'~')
    } else {
        None
    }
}

fn change_ranges(base: &[u8], side: &[u8]) -> Vec<ChangeRange> {
    if base == side {
        return Vec::new();
    }
    let prefix = base
        .iter()
        .zip(side.iter())
        .take_while(|(left, right)| left == right)
        .count();
    let mut base_suffix = base.len();
    let mut side_suffix = side.len();
    while base_suffix > prefix
        && side_suffix > prefix
        && base[base_suffix - 1] == side[side_suffix - 1]
    {
        base_suffix -= 1;
        side_suffix -= 1;
    }
    vec![ChangeRange {
        start: prefix,
        end: base_suffix,
    }]
}

fn touched_blocks(blocks: &[Block], ranges: &[ChangeRange]) -> Vec<usize> {
    let mut touched = Vec::new();
    for range in ranges {
        for (idx, block) in blocks.iter().enumerate() {
            if range.start <= block.end && range.end >= block.start {
                touched.push(idx);
            }
        }
    }
    touched.sort_unstable();
    touched.dedup();
    touched
}

fn range_touches_block(ranges: &[ChangeRange], block: &Block) -> Vec<ChangeRange> {
    ranges
        .iter()
        .filter(|range| range.start <= block.end && range.end >= block.start)
        .copied()
        .collect()
}

fn ranges_overlap(left: &[ChangeRange], right: &[ChangeRange]) -> bool {
    left.iter()
        .any(|l| right.iter().any(|r| l.start < r.end && r.start < l.end))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge_strategy::testing::run_exact_intention_case;
    use crate::schema::TextMergeSpec;

    fn spec() -> TextMergeSpec {
        TextMergeSpec::new(STRATEGY_ID, STRATEGY_VERSION, Vec::new())
    }

    fn case(base: &str, left: &str, right: &str, expected: &str) {
        run_exact_intention_case(&SimpleMarkdownStrategy, base, left, right, spec(), expected)
            .unwrap();
    }

    #[test]
    fn disjoint_block_edits_both_apply() {
        case(
            "# Title\n\nFirst paragraph.\n\nSecond paragraph.\n",
            "# New Title\n\nFirst paragraph.\n\nSecond paragraph.\n",
            "# Title\n\nFirst paragraph.\n\nSecond paragraph changed.\n",
            "# New Title\n\nFirst paragraph.\n\nSecond paragraph changed.\n",
        );
    }

    #[test]
    fn same_block_disjoint_char_edits_both_apply() {
        case(
            "alpha beta gamma\n",
            "alpha BETA gamma\n",
            "alpha beta GAMMA\n",
            "alpha BETA GAMMA\n",
        );
    }

    #[test]
    fn same_block_conflicting_edits_use_lww_by_tx_id() {
        case(
            "alpha beta\n",
            "alpha LEFT\n",
            "alpha RIGHT\n",
            "alpha RIGHT\n",
        );
    }

    #[test]
    fn block_insertion_vs_adjacent_block_edit_preserves_both() {
        case(
            "One.\n\nTwo.\n",
            "One.\n\nInserted.\n\nTwo.\n",
            "One changed.\n\nTwo.\n",
            "One changed.\n\nInserted.\n\nTwo.\n",
        );
    }

    #[test]
    fn list_item_add_vs_sibling_edit_preserves_both() {
        case(
            "- one\n- two\n",
            "- one\n- added\n- two\n",
            "- one edited\n- two\n",
            "- one edited\n- added\n- two\n",
        );
    }

    #[test]
    fn code_fence_edit_vs_outside_edit_preserves_fence_integrity() {
        case(
            "Intro.\n\n```\nlet a = 1;\n```\n\nTail.\n",
            "Intro.\n\n```\nlet a = 2;\n```\n\nTail.\n",
            "Intro changed.\n\n```\nlet a = 1;\n```\n\nTail.\n",
            "Intro changed.\n\n```\nlet a = 2;\n```\n\nTail.\n",
        );
    }

    #[test]
    fn heading_edit_vs_section_body_edit_preserves_both() {
        case(
            "# Old\n\nBody text.\n",
            "# New\n\nBody text.\n",
            "# Old\n\nBody changed.\n",
            "# New\n\nBody changed.\n",
        );
    }

    #[test]
    fn malformed_unclosed_code_fence_does_not_error() {
        case("```\nbase\n", "```\nleft\n", "```\nright\n", "```\nright\n");
    }
}
