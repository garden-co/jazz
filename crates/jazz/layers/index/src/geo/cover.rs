//! Covering grid rectangles with few ranges of cells.
//!
//! A rectangle of grid cells is snapped outward to the finest quadtree level
//! at which it spans at most [`SNAP_SIDE`] blocks per axis. The aligned
//! blocks inside the snapped rectangle are exactly a set of cell ranges
//! (every aligned quadtree block is contiguous on the curve). Those ranges
//! are then merged down to at most [`MAX_RANGES`] by filling the smallest
//! gaps, which for a given set of ranges reads the fewest extra cells.
//!
//! Finer snapping never reads more: its ranges are a subset of the coarser
//! level's, so any merge of the coarser ranges also covers them.

/// Most ranges in a cover (SPEC 20.10.3).
pub const MAX_RANGES: usize = 16;

/// Most snapped blocks per rectangle side. Bounds the work of a cover to a
/// few thousand quadtree nodes; past 64 the read amplification barely moves
/// (measured in `tests::amplification`).
pub(crate) const SNAP_SIDE: u32 = 64;

/// Bits per axis.
pub(crate) const BITS: u32 = 31;

/// An inclusive rectangle on the 31-bit grid: `u` is longitude, `v` latitude.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Rect {
    pub u0: u32,
    pub u1: u32,
    pub v0: u32,
    pub v1: u32,
}

/// How a curve numbers an aligned block: `key(x, y, k)` is the first cell
/// index of the block of side `2^k` whose lower corner is `(x, y)` (both
/// multiples of `2^k`, on the full grid). Its cells are
/// `key .. key + 4^k`.
pub(crate) trait Curve {
    fn block_start(x: u32, y: u32, k: u32) -> i64;
}

pub(crate) struct Morton;

impl Curve for Morton {
    fn block_start(x: u32, y: u32, _k: u32) -> i64 {
        // The low 2k bits of an aligned corner's code are zero already.
        interleave(x, y)
    }
}

/// Spreads the 31 bits of `x` to the even bit positions.
fn spread(x: u32) -> u64 {
    let mut v = x as u64;
    v = (v | (v << 16)) & 0x0000_FFFF_0000_FFFF;
    v = (v | (v << 8)) & 0x00FF_00FF_00FF_00FF;
    v = (v | (v << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
    v = (v | (v << 2)) & 0x3333_3333_3333_3333;
    v = (v | (v << 1)) & 0x5555_5555_5555_5555;
    v
}

/// The Z-order code: bit `2i` is bit `i` of `u` (longitude), bit `2i + 1`
/// is bit `i` of `v` (latitude).
pub(crate) fn interleave(u: u32, v: u32) -> i64 {
    (spread(u) | (spread(v) << 1)) as i64
}

/// Covers the union of `rects` with at most [`MAX_RANGES`] inclusive ranges,
/// sorted and disjoint.
pub(crate) fn cover<C: Curve>(rects: &[Rect]) -> Vec<(i64, i64)> {
    let mut ranges = exact_ranges::<C>(rects, snap_level(rects));
    ranges.sort_unstable();
    merge_touching(&mut ranges);
    limit(ranges, MAX_RANGES)
}

/// The finest level `s` at which every rectangle spans at most
/// [`SNAP_SIDE`] blocks of side `2^s` per axis.
pub(crate) fn snap_level(rects: &[Rect]) -> u32 {
    let mut s = 0;
    while s < BITS
        && rects.iter().any(|r| {
            (r.u1 >> s) - (r.u0 >> s) + 1 > SNAP_SIDE || (r.v1 >> s) - (r.v0 >> s) + 1 > SNAP_SIDE
        })
    {
        s += 1;
    }
    s
}

/// The ranges of all blocks of side `2^s` that meet one of `rects`, unsorted.
pub(crate) fn exact_ranges<C: Curve>(rects: &[Rect], s: u32) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    for r in rects {
        let snapped = Rect {
            u0: r.u0 >> s,
            u1: r.u1 >> s,
            v0: r.v0 >> s,
            v1: r.v1 >> s,
        };
        walk::<C>(&snapped, s, 0, 0, BITS - s, &mut out);
    }
    out
}

/// Visits the quadtree node at `(x, y)` (snapped coordinates) of side `2^k`.
fn walk<C: Curve>(r: &Rect, s: u32, x: u32, y: u32, k: u32, out: &mut Vec<(i64, i64)>) {
    let last = (1u32 << k) - 1;
    if x > r.u1 || x + last < r.u0 || y > r.v1 || y + last < r.v0 {
        return;
    }
    if x >= r.u0 && x + last <= r.u1 && y >= r.v0 && y + last <= r.v1 {
        let level = k + s;
        let start = C::block_start(x << s, y << s, level);
        out.push((start, start + ((1i64 << (2 * level)) - 1)));
        return;
    }
    let h = 1u32 << (k - 1);
    walk::<C>(r, s, x, y, k - 1, out);
    walk::<C>(r, s, x + h, y, k - 1, out);
    walk::<C>(r, s, x, y + h, k - 1, out);
    walk::<C>(r, s, x + h, y + h, k - 1, out);
}

/// Merges sorted ranges that overlap or touch.
pub(crate) fn merge_touching(ranges: &mut Vec<(i64, i64)>) {
    let mut out: Vec<(i64, i64)> = Vec::with_capacity(ranges.len());
    for &(a, b) in ranges.iter() {
        match out.last_mut() {
            Some(last) if a <= last.1.saturating_add(1) => last.1 = last.1.max(b),
            _ => out.push((a, b)),
        }
    }
    *ranges = out;
}

/// Merges sorted, disjoint ranges down to `max` by filling the smallest
/// gaps; among equal gaps the later one is filled first.
pub(crate) fn limit(ranges: Vec<(i64, i64)>, max: usize) -> Vec<(i64, i64)> {
    if ranges.len() <= max {
        return ranges;
    }
    let mut gaps: Vec<(i64, usize)> = ranges
        .windows(2)
        .enumerate()
        .map(|(i, w)| (w[1].0 - w[0].1 - 1, i))
        .collect();
    // Largest gaps first; among equal gaps, earlier first.
    gaps.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    let mut keep = vec![false; ranges.len() - 1];
    for &(_, i) in gaps.iter().take(max - 1) {
        keep[i] = true;
    }
    let mut out = Vec::with_capacity(max);
    let mut cur = ranges[0];
    for (i, &next) in ranges.iter().enumerate().skip(1) {
        if keep[i - 1] {
            out.push(cur);
            cur = next;
        } else {
            cur.1 = next.1;
        }
    }
    out.push(cur);
    out
}
