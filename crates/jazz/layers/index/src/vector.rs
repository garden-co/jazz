//! The `vector` index kind (SPEC 20.10.2).
//!
//! Pure functions over `f32` vectors: validation and cosine normalization
//! ([`prepare`]), scores ([`score`]), the `f32le/1` cell encoding and a
//! bounded top-k ([`TopK`]). Every node computes the same bits for the same
//! inputs (`INV-IX-1`):
//!
//! - normalization divides in `f64` and rounds once to `f32`;
//! - scores accumulate `f32` inputs widened to `f64`, one product at a time,
//!   in index order, with a separate multiply and add (never `mul_add`), so
//!   no target may fuse or reassociate them. Rust never contracts `a * b + c`
//!   into an FMA on its own, and never reorders floating-point additions.
//!
//! Inputs are finite and at most [`MAX_DIMENSIONS`] long, so every product is
//! at most `(f32::MAX)^2 ≈ 1.2e77` and every sum stays far below `f64::MAX`:
//! scores are always finite and compare totally.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Name of the cell and shadow-row encoding: `n` IEEE-754 binary32 values,
/// little-endian, concatenated, no header (SPEC 20.1).
pub const ENCODING: &str = "f32le/1";

/// Largest dimension a `vector` column may declare (SPEC 20.1).
pub const MAX_DIMENSIONS: u32 = 4096;

/// Distance metric of a vector index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Metric {
    /// `−Σ aᵢ bᵢ` over L2-normalized vectors.
    Cosine,
    /// Squared Euclidean distance `Σ (aᵢ − bᵢ)²`.
    L2,
    /// `−Σ aᵢ bᵢ`.
    Dot,
}

impl Metric {
    /// The spelling used in `jazz.app.json` (`"metric": "cosine"`).
    pub fn as_str(self) -> &'static str {
        match self {
            Metric::Cosine => "cosine",
            Metric::L2 => "l2",
            Metric::Dot => "dot",
        }
    }

    /// Parses the `jazz.app.json` spelling.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "cosine" => Some(Metric::Cosine),
            "l2" => Some(Metric::L2),
            "dot" => Some(Metric::Dot),
            _ => None,
        }
    }
}

/// Why a vector or its bytes were refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VectorError {
    /// Element `index` is NaN or infinite.
    NonFinite { index: usize },
    /// A cosine vector has L2 norm zero, so it has no direction.
    ZeroVector,
    /// The vector has `actual` elements where `expected` were declared.
    WrongLength { expected: usize, actual: usize },
    /// `f32le/1` bytes whose length is not `4 * expected`.
    WrongByteLength { expected: usize, actual: usize },
}

impl std::fmt::Display for VectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VectorError::NonFinite { index } => {
                write!(f, "vector element {index} is NaN or infinite")
            }
            VectorError::ZeroVector => {
                write!(f, "a zero vector has no direction for the cosine metric")
            }
            VectorError::WrongLength { expected, actual } => {
                write!(f, "vector has {actual} elements, expected {expected}")
            }
            VectorError::WrongByteLength { expected, actual } => write!(
                f,
                "f32le/1 value has {actual} bytes, expected {} ({expected} elements)",
                expected * 4
            ),
        }
    }
}

impl std::error::Error for VectorError {}

/// Returns the index of the first NaN or infinite element, if any.
pub fn check_finite(v: &[f32]) -> Result<(), VectorError> {
    match v.iter().position(|x| !x.is_finite()) {
        Some(index) => Err(VectorError::NonFinite { index }),
        None => Ok(()),
    }
}

/// Validates a vector for `metric` and returns the form its index stores and
/// scores: for `cosine` the vector divided by its L2 norm, otherwise a copy.
///
/// Refuses NaN and infinities for every metric and the zero vector for
/// `cosine`. The norm is `sqrt(Σ xᵢ²)` accumulated sequentially in `f64`;
/// each element is divided in `f64` and rounded once to `f32`. A vector
/// whose norm underflows `f64` cannot occur: the smallest nonzero `f32`
/// squared is `≈ 2e-90`, well inside `f64`'s normal range.
///
/// An error never fails a write: index maintenance is total (`INV-IX-7`),
/// so the drain leaves a stored vector `prepare` refuses unindexed (it never
/// matches). For a query vector the error is a typed validation error.
pub fn prepare(metric: Metric, v: &[f32]) -> Result<Vec<f32>, VectorError> {
    check_finite(v)?;
    match metric {
        Metric::L2 | Metric::Dot => Ok(v.to_vec()),
        Metric::Cosine => {
            let mut sum = 0.0f64;
            for &x in v {
                let x = f64::from(x);
                let sq = x * x;
                sum += sq;
            }
            if sum == 0.0 {
                return Err(VectorError::ZeroVector);
            }
            let norm = sum.sqrt();
            Ok(v.iter().map(|&x| (f64::from(x) / norm) as f32).collect())
        }
    }
}

/// Score of `stored` against `query`, smaller is better. Both are the
/// outputs of [`prepare`] for the same metric and have the same length.
///
/// | metric   | score              |
/// | -------- | ------------------ |
/// | `l2`     | `Σ (aᵢ − bᵢ)²`      |
/// | `cosine` | `−Σ aᵢ bᵢ`          |
/// | `dot`    | `−Σ aᵢ bᵢ`          |
///
/// Each term is computed in `f64` from the widened `f32` inputs (the
/// difference and product of two `f32` widened to `f64` are exact or
/// correctly rounded on every IEEE-754 target) and added to the running sum
/// in index order.
///
/// # Panics
///
/// If the lengths differ: callers validate dimensions first.
pub fn score(metric: Metric, query: &[f32], stored: &[f32]) -> f64 {
    assert_eq!(
        query.len(),
        stored.len(),
        "vector score over different dimensions"
    );
    match metric {
        Metric::L2 => {
            let mut sum = 0.0f64;
            for (&a, &b) in query.iter().zip(stored) {
                let d = f64::from(a) - f64::from(b);
                let sq = d * d;
                sum += sq;
            }
            sum
        }
        Metric::Cosine | Metric::Dot => {
            let mut sum = 0.0f64;
            for (&a, &b) in query.iter().zip(stored) {
                let p = f64::from(a) * f64::from(b);
                sum += p;
            }
            -sum
        }
    }
}

/// Encodes `v` as `f32le/1`: each element's IEEE-754 bits, little-endian.
pub fn encode_f32le(v: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 4);
    for x in v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

/// Decodes `f32le/1` bytes of a `vector(dimensions)` value. Refuses a wrong
/// length; does not check finiteness (see [`decode_f32le_checked`]).
pub fn decode_f32le(bytes: &[u8], dimensions: usize) -> Result<Vec<f32>, VectorError> {
    if bytes.len() != dimensions * 4 {
        return Err(VectorError::WrongByteLength {
            expected: dimensions,
            actual: bytes.len(),
        });
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect())
}

/// [`decode_f32le`], then refuses NaN and infinities: what admission checks
/// for a `vector(dimensions)` cell.
pub fn decode_f32le_checked(bytes: &[u8], dimensions: usize) -> Result<Vec<f32>, VectorError> {
    let v = decode_f32le(bytes, dimensions)?;
    check_finite(&v)?;
    Ok(v)
}

/// Checks that `bytes` are a valid `vector(dimensions)` cell without
/// allocating: length `4 * dimensions` and every element finite.
pub fn validate_f32le(bytes: &[u8], dimensions: usize) -> Result<(), VectorError> {
    if bytes.len() != dimensions * 4 {
        return Err(VectorError::WrongByteLength {
            expected: dimensions,
            actual: bytes.len(),
        });
    }
    for (index, c) in bytes.chunks_exact(4).enumerate() {
        if !f32::from_le_bytes([c[0], c[1], c[2], c[3]]).is_finite() {
            return Err(VectorError::NonFinite { index });
        }
    }
    Ok(())
}

/// One candidate kept by [`TopK`], ordered by `(score, id)`.
#[derive(Clone, Debug)]
struct Entry<I, P> {
    score: f64,
    id: I,
    payload: P,
}

impl<I: Ord, P> Entry<I, P> {
    fn key_cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl<I: Ord, P> PartialEq for Entry<I, P> {
    fn eq(&self, other: &Self) -> bool {
        self.key_cmp(other) == Ordering::Equal
    }
}
impl<I: Ord, P> Eq for Entry<I, P> {}
impl<I: Ord, P> PartialOrd for Entry<I, P> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<I: Ord, P> Ord for Entry<I, P> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key_cmp(other)
    }
}

/// The `k` best candidates by `(score, id)`, smaller first, in `O(k)` memory.
///
/// `id` is the row's stable identity (its object id), never a node-local
/// `rid`, so every node breaks ties the same way. `payload` rides along
/// (e.g. the local `rid`) and does not take part in ordering. Pushing the
/// same candidates in any order gives the same result.
#[derive(Clone, Debug)]
pub struct TopK<I: Ord, P = ()> {
    k: usize,
    /// Max-heap: the worst kept candidate is on top.
    heap: BinaryHeap<Entry<I, P>>,
}

impl<I: Ord, P> TopK<I, P> {
    pub fn new(k: usize) -> Self {
        Self {
            k,
            heap: BinaryHeap::with_capacity(k.saturating_add(1).min(1024)),
        }
    }

    /// Offers a candidate; keeps it only if it is among the `k` best so far.
    pub fn push(&mut self, score: f64, id: I, payload: P) {
        if self.k == 0 {
            return;
        }
        let entry = Entry { score, id, payload };
        if self.heap.len() < self.k {
            self.heap.push(entry);
        } else if let Some(mut worst) = self.heap.peek_mut()
            && entry < *worst
        {
            *worst = entry;
        }
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// The kept candidates, best first, as `(score, id, payload)`.
    pub fn into_sorted(self) -> Vec<(f64, I, P)> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|e| (e.score, e.id, e.payload))
            .collect()
    }
}

#[cfg(test)]
mod tests;
