//! Unit tests for the pure vector kind. These are lower-level than the
//! guidelines prefer because this crate has no client API: the scores and
//! bytes pinned here are the contract every node must reproduce
//! (`INV-IX-1`, `INV-IX-5`), and the `jazz-db` integration tests build on it.

use super::*;

const FIXTURE: &str = include_str!("../../../../SPEC/fixtures/ix/vector-f32le-v1.json");

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn metric(name: &str) -> Metric {
    Metric::parse(name).unwrap()
}

fn decode_any(hex: &str) -> Vec<f32> {
    let bytes = unhex(hex);
    decode_f32le(&bytes, bytes.len() / 4).unwrap()
}

/// `prepare` reproduces the independent reference's bytes for every metric,
/// and `f32le/1` round-trips them exactly.
#[test]
fn prepare_matches_fixture_bytes() {
    let fixture: serde_json::Value = serde_json::from_str(FIXTURE).unwrap();
    assert_eq!(fixture["encoding"], ENCODING);
    let cases = fixture["prepare"].as_array().unwrap();
    assert!(cases.len() >= 20);
    for case in cases {
        let input_bytes = unhex(case["input"].as_str().unwrap());
        let input = decode_f32le(&input_bytes, input_bytes.len() / 4).unwrap();
        assert_eq!(encode_f32le(&input), input_bytes, "round trip {case}");
        let got = prepare(metric(case["metric"].as_str().unwrap()), &input);
        match case["prepared"].as_str() {
            Some(expected) => assert_eq!(encode_f32le(&got.unwrap()), unhex(expected), "{case}"),
            None => assert_eq!(got, Err(VectorError::ZeroVector), "{case}"),
        }
    }
}

/// `score` gives the reference's exact binary64 bits.
#[test]
fn score_matches_fixture_bits() {
    let fixture: serde_json::Value = serde_json::from_str(FIXTURE).unwrap();
    let cases = fixture["score"].as_array().unwrap();
    assert!(cases.len() >= 12);
    for case in cases {
        let q = decode_any(case["query_prepared"].as_str().unwrap());
        let s = decode_any(case["stored_prepared"].as_str().unwrap());
        let expected = u64::from_str_radix(case["score_f64_be"].as_str().unwrap(), 16).unwrap();
        let got = score(metric(case["metric"].as_str().unwrap()), &q, &s);
        assert_eq!(got.to_bits(), expected, "{case}: got {got}");
        assert!(got.is_finite());
    }
}

/// Worked examples from SPEC 20.10.2, readable without the fixture.
#[test]
fn worked_examples() {
    assert_eq!(
        prepare(Metric::Cosine, &[3.0, 4.0]).unwrap(),
        vec![0.6, 0.8]
    );
    assert_eq!(score(Metric::L2, &[1.0, 2.0], &[4.0, 6.0]), 25.0);
    assert_eq!(score(Metric::Dot, &[1.0, 2.0], &[4.0, 6.0]), -16.0);
    let a = prepare(Metric::Cosine, &[1.0, 0.0]).unwrap();
    let b = prepare(Metric::Cosine, &[0.0, 5.0]).unwrap();
    assert_eq!(score(Metric::Cosine, &a, &b), 0.0);
    assert_eq!(score(Metric::Cosine, &a, &a), -1.0);
    assert_eq!(
        encode_f32le(&[1.0, -2.0]),
        [0, 0, 0x80, 0x3f, 0, 0, 0, 0xc0]
    );
}

/// The order of accumulation is fixed: a sum that reassociation would change
/// gives the sequential result.
#[test]
fn accumulation_is_sequential() {
    // 1e30*1 + 1*1 + (-1e30)*1, left to right in f64: (1e30 + 1) rounds to
    // 1e30, minus 1e30 is 0. Pairing the big terms first would give 1.
    let q = [1e30f32, 1.0, -1e30];
    let s = [1.0f32, 1.0, 1.0];
    let big = f64::from(1e30f32);
    let sequential = -((big + 1.0) + -big);
    assert_eq!(score(Metric::Dot, &q, &s), sequential);
    assert_eq!(sequential, 0.0);
}

/// The summation runs in index order: `[1, 1e30, -1e30]·[1, 1, 1]` is
/// `(1 + 1e30) - 1e30 = 0` in order, but `1` summed in reverse, so a
/// reversed or pairwise implementation fails here.
#[test]
fn accumulation_is_in_index_order() {
    let q = [1.0f32, 1e30, -1e30];
    let s = [1.0f32, 1.0, 1.0];
    assert_eq!(score(Metric::Dot, &q, &s), -0.0);
    assert_eq!(score(Metric::Dot, &q, &s).to_bits(), (-0.0f64).to_bits());
    let reversed_q: Vec<f32> = q.iter().rev().copied().collect();
    assert_eq!(
        score(Metric::Dot, &reversed_q, &s),
        -1.0,
        "reversed order differs"
    );
}

/// `l2` squares and adds separately: with a fused multiply-add the last
/// bit of this sum differs (`...cc45` instead of `...cc44`, found against
/// an FMA reference).
#[test]
fn l2_is_not_fused() {
    let bits = |b: [u32; 2]| b.map(f32::from_bits);
    let a = bits([0xbf1cce38, 0x3cbc2b07]);
    let b = bits([0x3f9e9c51, 0x3fbb967d]);
    let got = score(Metric::L2, &a, &b);
    assert_eq!(got.to_bits(), 0x401609e1f8c6cc44);
    let fused = a.iter().zip(&b).fold(0.0f64, |sum, (&x, &y)| {
        let d = f64::from(x) - f64::from(y);
        d.mul_add(d, sum)
    });
    assert_eq!(
        fused.to_bits(),
        0x401609e1f8c6cc45,
        "the case separates FMA"
    );
}

/// Non-finite values are refused for every metric, the zero vector only for
/// cosine, with the offending position named.
#[test]
fn prepare_refusals() {
    for m in [Metric::Cosine, Metric::L2, Metric::Dot] {
        assert_eq!(
            prepare(m, &[1.0, f32::NAN]),
            Err(VectorError::NonFinite { index: 1 })
        );
        assert_eq!(
            prepare(m, &[f32::INFINITY]),
            Err(VectorError::NonFinite { index: 0 })
        );
        assert_eq!(
            prepare(m, &[0.0, f32::NEG_INFINITY]),
            Err(VectorError::NonFinite { index: 1 })
        );
    }
    assert_eq!(
        prepare(Metric::Cosine, &[0.0, -0.0]),
        Err(VectorError::ZeroVector)
    );
    assert_eq!(prepare(Metric::L2, &[0.0, 0.0]).unwrap(), vec![0.0, 0.0]);
    assert_eq!(prepare(Metric::Dot, &[0.0]).unwrap(), vec![0.0]);
    // The smallest subnormal still has a direction.
    assert_eq!(
        prepare(Metric::Cosine, &[1e-45, 0.0]).unwrap(),
        vec![1.0, 0.0]
    );
}

/// Cell validation refuses a wrong byte length and non-finite elements.
#[test]
fn f32le_validation() {
    let ok = encode_f32le(&[1.0, 2.0, 3.0]);
    assert_eq!(validate_f32le(&ok, 3), Ok(()));
    assert_eq!(decode_f32le_checked(&ok, 3).unwrap(), vec![1.0, 2.0, 3.0]);
    assert_eq!(
        validate_f32le(&ok, 2),
        Err(VectorError::WrongByteLength {
            expected: 2,
            actual: 12
        })
    );
    assert_eq!(
        decode_f32le(&ok[..11], 3),
        Err(VectorError::WrongByteLength {
            expected: 3,
            actual: 11
        })
    );
    let nan = encode_f32le(&[1.0, f32::NAN]);
    assert_eq!(
        validate_f32le(&nan, 2),
        Err(VectorError::NonFinite { index: 1 })
    );
    assert_eq!(
        decode_f32le_checked(&nan, 2),
        Err(VectorError::NonFinite { index: 1 })
    );
    assert!(decode_f32le(&nan, 2).unwrap()[1].is_nan());
}

/// `TopK` keeps the k smallest by `(score, id)`, best first, whatever order
/// candidates arrive in; equal scores are ordered by id.
#[test]
fn top_k_by_score_then_id() {
    let candidates: Vec<(f64, u32)> = vec![
        (0.5, 7),
        (0.1, 9),
        (0.5, 3),
        (-1.0, 4),
        (0.1, 2),
        (2.0, 1),
        (0.5, 5),
    ];
    let expected = vec![(-1.0, 4), (0.1, 2), (0.1, 9), (0.5, 3)];
    let mut rev = candidates.clone();
    rev.reverse();
    let mut sorted = candidates.clone();
    sorted.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    for order in [candidates, rev, sorted] {
        let mut top = TopK::new(4);
        for (s, id) in order {
            top.push(s, id, id * 10);
        }
        let got = top.into_sorted();
        assert_eq!(
            got.iter().map(|(s, id, _)| (*s, *id)).collect::<Vec<_>>(),
            expected
        );
        assert!(got.iter().all(|(_, id, payload)| *payload == id * 10));
    }
}

/// `k = 0` keeps nothing, `k` above the candidate count keeps all, and
/// `-0.0` sorts before `0.0` (total order), so equal-looking scores never
/// fall back to an unspecified order.
#[test]
fn top_k_edges() {
    let mut zero = TopK::<u8>::new(0);
    zero.push(0.0, 1, ());
    assert!(zero.is_empty());
    let mut wide = TopK::<u8>::new(10);
    wide.push(1.0, 2, ());
    wide.push(1.0, 1, ());
    assert_eq!(wide.len(), 2);
    assert_eq!(
        wide.into_sorted()
            .into_iter()
            .map(|(_, id, _)| id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    let mut z = TopK::<u8>::new(1);
    z.push(0.0, 1, ());
    z.push(-0.0, 2, ());
    assert_eq!(z.into_sorted()[0].1, 2);
}
