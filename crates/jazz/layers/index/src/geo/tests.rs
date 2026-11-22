//! Tests of the geo kind's pure functions.
//!
//! These are white-box unit tests on purpose: `jazz-index` has no storage or
//! client API, and what they pin (cover supersets, byte encodings,
//! cross-target bits) is only observable here. End-to-end geo queries are
//! tested in `jazz-db` through the public query API.

use super::*;

/// A small deterministic PRNG (xorshift64*), so the tests need no
/// dependency and every run checks the same cases.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    /// Uniform in `[0, 1)`.
    fn f(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn range(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (hi - lo) * self.f()
    }
    fn pick<T: Copy>(&mut self, xs: &[T]) -> T {
        xs[(self.next() % xs.len() as u64) as usize]
    }
}

fn inside(ranges: &[(i64, i64)], c: i64) -> bool {
    ranges.iter().any(|&(a, b)| a <= c && c <= b)
}

fn assert_well_formed(ranges: &[(i64, i64)]) {
    assert!(!ranges.is_empty());
    assert!(ranges.len() <= MAX_RANGES, "{} ranges", ranges.len());
    for &(a, b) in ranges {
        assert!(0 <= a && a <= b && b < 1i64 << 62);
    }
    for w in ranges.windows(2) {
        assert!(
            w[0].1 + 1 < w[1].0,
            "ranges not sorted and disjoint: {ranges:?}"
        );
    }
}

/// A latitude drawn to hit the interesting places: poles, the equator,
/// exact edges, and anywhere.
fn some_lat(rng: &mut Rng) -> f64 {
    match rng.next() % 6 {
        0 => rng.pick(&[-90.0, 90.0, 0.0, -0.0, 89.999_999_9, -89.999_999_9]),
        1 => rng.range(80.0, 90.0),
        2 => rng.range(-90.0, -80.0),
        _ => rng.range(-90.0, 90.0),
    }
}

/// A longitude drawn to hit the antimeridian and prime meridian often.
fn some_lng(rng: &mut Rng) -> f64 {
    match rng.next() % 6 {
        0 => rng.pick(&[-180.0, 180.0, 0.0, -0.0, 179.999_999_9, -179.999_999_9]),
        1 => rng.range(170.0, 180.0),
        2 => rng.range(-180.0, -170.0),
        _ => rng.range(-180.0, 180.0),
    }
}

/// A size in degrees from sub-centimetre to most of the globe.
fn some_span(rng: &mut Rng, max: f64) -> f64 {
    let e = rng.range(-9.0, max.log10());
    match rng.next() % 8 {
        0 => 0.0,
        1 => max,
        _ => 10f64.powf(e),
    }
}

/// The trigonometry stays within about one ulp of the platform's libm, so
/// the deterministic version is also an accurate one.
#[test]
fn trig_matches_libm_closely() {
    let mut rng = Rng(1);
    for i in 0..200_000 {
        let d = if i < 361 {
            i as f64 - 180.0
        } else {
            rng.range(-180.0, 180.0)
        };
        let (s, c) = trig::sin_cos_deg(d);
        let r = d.to_radians();
        // libm sees `r` already rounded, off by up to half an ulp of `r`.
        let tol = 2.3e-16 + r.abs() * 1.2e-16;
        assert!((s - r.sin()).abs() <= tol, "sin {d}: {s} vs {}", r.sin());
        assert!((c - r.cos()).abs() <= tol, "cos {d}: {c} vs {}", r.cos());
    }
    // Exact values where the reduction makes them exact.
    assert_eq!(trig::sin_cos_deg(90.0), (1.0, -0.0));
    assert_eq!(trig::sin_cos_deg(0.0), (0.0, 1.0));
    assert_eq!(trig::sin_cos_deg(-180.0).1, -1.0);
}

/// The cell is the Z-order code of the 31-bit steps, longitude in the even
/// bits, latitude in the odd bits.
#[test]
fn cell_layout() {
    assert_eq!(cell(-90.0, -180.0), Ok(0));
    assert_eq!(cell(90.0, 180.0), Ok((1i64 << 62) - 1));
    // Longitude just east of -180 by one step sets bit 0; latitude bit 1.
    let lng_step = 360.0 / (1u64 << 31) as f64;
    let lat_step = 180.0 / (1u64 << 31) as f64;
    assert_eq!(cell(-90.0, -180.0 + lng_step), Ok(1));
    assert_eq!(cell(-90.0 + lat_step, -180.0), Ok(2));
    // (0, 0) is the middle step of both axes: the top bit of each.
    assert_eq!(cell(0.0, 0.0), Ok(0b11 << 60));
    let mut rng = Rng(2);
    for _ in 0..10_000 {
        let (lat, lng) = (some_lat(&mut rng), some_lng(&mut rng));
        let c = cell(lat, lng).unwrap();
        let (mut u, mut v) = (0u32, 0u32);
        for i in 0..31 {
            u |= (((c >> (2 * i)) & 1) as u32) << i;
            v |= (((c >> (2 * i + 1)) & 1) as u32) << i;
        }
        let u_lng = -180.0 + u as f64 * lng_step;
        let v_lat = -90.0 + v as f64 * lat_step;
        assert!(
            u_lng <= lng && lng <= u_lng + lng_step + 1e-12,
            "{lng} → {u}"
        );
        assert!(
            v_lat <= lat && lat <= v_lat + lat_step + 1e-12,
            "{lat} → {v}"
        );
    }
}

/// Unit vectors have length 1 and point where SPEC 20.10.3 says.
#[test]
fn unit_vectors() {
    assert_eq!(unit(90.0, 0.0).unwrap()[2], 1.0);
    assert_eq!(unit(0.0, 0.0).unwrap(), [1.0, 0.0, 0.0]);
    assert_eq!(unit(0.0, 90.0).unwrap()[1], 1.0);
    let mut rng = Rng(3);
    for _ in 0..10_000 {
        let p = unit(some_lat(&mut rng), some_lng(&mut rng)).unwrap();
        let n = p[0] * p[0] + p[1] * p[1] + p[2] * p[2];
        assert!((n - 1.0).abs() < 1e-15);
    }
}

/// The chord threshold matches great-circle distance: two points `d` meters
/// apart along a meridian are within `chord2_for_meters(d)`.
#[test]
fn chord_threshold_matches_distance() {
    assert_eq!(chord2_for_meters(0.0), Ok(0.0));
    for &(lat, d) in &[
        (0.0, 1.0),
        (45.0, 1000.0),
        (-60.0, 250_000.0),
        (10.0, 1_000_000.0),
    ] {
        let dlat = d / EARTH_RADIUS_M * RAD_TO_DEG;
        let c2 = chord2(unit(lat, 3.0).unwrap(), unit(lat + dlat, 3.0).unwrap());
        let t = chord2_for_meters(d).unwrap();
        assert!((c2 / t - 1.0).abs() < 1e-6, "{d} m: {c2} vs {t}");
    }
}

/// Out-of-range and non-finite inputs are refused with a typed error.
#[test]
fn validation() {
    assert_eq!(cell(90.1, 0.0), Err(GeoError::Latitude(90.1)));
    assert_eq!(cell(0.0, -180.5), Err(GeoError::Longitude(-180.5)));
    assert!(matches!(unit(f64::NAN, 0.0), Err(GeoError::Latitude(_))));
    assert!(matches!(
        cell(0.0, f64::INFINITY),
        Err(GeoError::Longitude(_))
    ));
    assert_eq!(
        cover_radius(0.0, 0.0, 1_000_000.1),
        Err(GeoError::Radius(1_000_000.1))
    );
    assert_eq!(cover_radius(0.0, 0.0, -1.0), Err(GeoError::Radius(-1.0)));
    assert!(matches!(
        chord2_for_meters(f64::NAN),
        Err(GeoError::Radius(_))
    ));
    assert_eq!(
        cover_box(10.0, 0.0, 5.0, 1.0),
        Err(GeoError::BoxInverted {
            south: 10.0,
            north: 5.0
        })
    );
    assert!(cover_radius(0.0, 0.0, MAX_RADIUS_M).is_ok());
    assert!(cover_box(-90.0, -180.0, 90.0, 180.0).is_ok());
}

/// Every point inside a box, edges included, has its cell in the cover.
///
/// Boxes are drawn across the antimeridian, at the poles, from
/// sub-centimetre to the whole globe; points are drawn inside, on the
/// edges and at the corners.
#[test]
fn box_cover_is_a_superset() {
    let mut rng = Rng(4);
    for _ in 0..20_000 {
        let south = some_lat(&mut rng);
        let north = (south + some_span(&mut rng, 180.0)).min(90.0);
        let west = some_lng(&mut rng);
        let mut east = west + some_span(&mut rng, 360.0);
        if east > 180.0 {
            east -= 360.0;
        }
        let ranges = cover_box(south, west, north, east).unwrap();
        assert_well_formed(&ranges);
        let width = if west <= east {
            east - west
        } else {
            360.0 - west + east
        };
        for i in 0..40 {
            let lat = match i {
                0 | 1 => south,
                2 | 3 => north,
                _ => rng.range(south, north),
            };
            let lng = match i {
                0 | 2 => west,
                1 | 3 => east,
                _ => {
                    let l = west + width * rng.f();
                    if l > 180.0 { l - 360.0 } else { l }
                }
            };
            let in_box = (south..=north).contains(&lat)
                && if west <= east {
                    (west..=east).contains(&lng)
                } else {
                    lng >= west || lng <= east
                };
            if !in_box {
                continue;
            }
            let c = cell(lat, lng).unwrap();
            assert!(
                inside(&ranges, c),
                "({lat}, {lng}) outside cover of {south},{west},{north},{east}"
            );
        }
    }
}

/// The point `meters` from `(lat, lng)` on `bearing` (degrees), with the
/// platform's trigonometry (test-only, so exactness doesn't matter).
fn destination(lat: f64, lng: f64, bearing: f64, meters: f64) -> (f64, f64) {
    let (p, l, b, d) = (
        lat.to_radians(),
        lng.to_radians(),
        bearing.to_radians(),
        meters / EARTH_RADIUS_M,
    );
    let p2 = (p.sin() * d.cos() + p.cos() * d.sin() * b.cos())
        .clamp(-1.0, 1.0)
        .asin();
    let l2 = l + (b.sin() * d.sin() * p.cos()).atan2(d.cos() - p.sin() * p2.sin());
    let mut lng2 = l2.to_degrees();
    while lng2 > 180.0 {
        lng2 -= 360.0;
    }
    while lng2 < -180.0 {
        lng2 += 360.0;
    }
    (p2.to_degrees().clamp(-90.0, 90.0), lng2)
}

/// Every point that passes the exact chord test has its cell in the cover.
///
/// Centers are drawn on the antimeridian and near the poles, radii from 0 to
/// 1000 km; points are drawn on and just around the circle (where rounding
/// decides), and anywhere inside.
#[test]
fn radius_cover_is_a_superset() {
    let mut rng = Rng(5);
    let mut tested = 0u64;
    for _ in 0..20_000 {
        let (lat, lng) = (some_lat(&mut rng), some_lng(&mut rng));
        let meters = match rng.next() % 6 {
            0 => 0.0,
            1 => MAX_RADIUS_M,
            _ => 10f64.powf(rng.range(-3.0, 6.0)),
        };
        let q = radius_query(lat, lng, meters).unwrap();
        assert_well_formed(&q.ranges);
        for i in 0..40 {
            let (plat, plng) = match i {
                0 => (lat, lng),
                _ => {
                    let d = if i % 2 == 0 {
                        meters * rng.range(0.999_999, 1.000_001)
                    } else {
                        meters * rng.f()
                    };
                    destination(lat, lng, rng.range(0.0, 360.0), d)
                }
            };
            let p = point(plat, plng).unwrap();
            if chord2(p.unit, q.center) <= q.chord2 {
                tested += 1;
                assert!(
                    inside(&q.ranges, p.cell),
                    "({plat}, {plng}) passes the chord test but is outside the cover of {lat},{lng} r={meters}"
                );
            }
        }
    }
    assert!(tested > 400_000, "too few points inside: {tested}");
}

/// The cover stays the same function of its inputs: equal inputs, equal
/// ranges, and `-0.0` behaves as `0.0`.
#[test]
fn covers_are_deterministic() {
    assert_eq!(
        cover_radius(0.0, 0.0, 500.0),
        cover_radius(-0.0, -0.0, 500.0)
    );
    assert_eq!(cover_box(1.0, 2.0, 3.0, 4.0), cover_box(1.0, 2.0, 3.0, 4.0));
}

// ---------------------------------------------------------------------------
// Fixtures (SPEC 20.2): `crates/jazz/SPEC/fixtures/ix/geo/`.
//
// `points-v1.json` is written by `generate.py` beside it, an independent
// Python reference (exact rational cells, the SPEC's trigonometry checked
// against mpmath), never by this code. `covers.txt` is a snapshot of this
// code's covers (a cover only chooses which cells are read, so it is not an
// encoding); `JAZZ_BLESS=1` rewrites it after a deliberate change.

const FIXTURE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../SPEC/fixtures/ix/geo");
const POINTS_FIXTURE: &str = include_str!("../../../../SPEC/fixtures/ix/geo/points-v1.json");

fn hex(v: f64) -> String {
    format!("{:016x}", v.to_bits())
}

fn f64_hex(value: &serde_json::Value) -> f64 {
    f64::from_bits(u64::from_str_radix(value.as_str().unwrap(), 16).unwrap())
}

/// Cells and unit vectors reproduce the independent reference bit for bit,
/// including points on and next to quantization boundaries, where a rounded
/// evaluation of `floor((lng + 180) / 360 · 2³¹)` (in either association)
/// gives a different cell (`INV-IX-1`, `INV-IX-5`).
#[test]
fn points_match_the_independent_reference() {
    let fixture: serde_json::Value = serde_json::from_str(POINTS_FIXTURE).unwrap();
    assert_eq!(fixture["encoding"], ENCODING);
    let cases = fixture["points"].as_array().unwrap();
    assert!(cases.len() >= 150, "{}", cases.len());
    for case in cases {
        let (lat, lng) = (f64_hex(&case["lat"]), f64_hex(&case["lng"]));
        let p = point(lat, lng).unwrap();
        let expected: i64 = case["cell"].as_str().unwrap().parse().unwrap();
        assert_eq!(p.cell, expected, "cell of ({lat:e}, {lng:e})");
        let unit = [
            f64_hex(&case["x"]),
            f64_hex(&case["y"]),
            f64_hex(&case["z"]),
        ];
        assert_eq!(
            p.unit.map(f64::to_bits),
            unit.map(f64::to_bits),
            "unit of ({lat:e}, {lng:e})"
        );
    }
    for case in fixture["chord_thresholds"].as_array().unwrap() {
        let meters = f64_hex(&case["meters"]);
        assert_eq!(
            chord2_for_meters(meters).unwrap().to_bits(),
            f64_hex(&case["chord2"]).to_bits(),
            "{meters} m"
        );
    }
}

fn covers_fixture() -> String {
    let mut s = format!(
        "# {ENCODING} covers: `radius lat lng meters` or `box south west north east` (f64 bits), \
         then `chord2` (f64 bits, radius only) and the inclusive ranges `lo-hi` (decimal)\n"
    );
    let radii: &[(f64, f64, f64)] = &[
        (37.774_929, -122.419_416, 1000.0),
        (51.477_928, -0.001_545, 250.0),
        (0.0, 179.9999, 5000.0),
        (89.9, 10.0, 20_000.0),
        (-33.868_820, 151.209_296, 0.0),
        (60.0, 25.0, 1_000_000.0),
    ];
    for &(lat, lng, m) in radii {
        let q = radius_query(lat, lng, m).unwrap();
        s += &format!(
            "radius {} {} {} {}",
            hex(lat),
            hex(lng),
            hex(m),
            hex(q.chord2)
        );
        for (a, b) in q.ranges {
            s += &format!(" {a}-{b}");
        }
        s += "\n";
    }
    let boxes: &[(f64, f64, f64, f64)] = &[
        (37.70, -122.52, 37.83, -122.35),
        (-10.0, 170.0, 10.0, -170.0),
        (80.0, -180.0, 90.0, 180.0),
        (0.0, 0.0, 0.0, 0.0),
    ];
    for &(so, we, no, ea) in boxes {
        s += &format!("box {} {} {} {}", hex(so), hex(we), hex(no), hex(ea));
        for (a, b) in cover_box(so, we, no, ea).unwrap() {
            s += &format!(" {a}-{b}");
        }
        s += "\n";
    }
    s
}

fn check_fixture(name: &str, actual: String) {
    let path = format!("{FIXTURE_DIR}/{name}");
    if std::env::var_os("JAZZ_BLESS").is_some() {
        std::fs::create_dir_all(FIXTURE_DIR).unwrap();
        std::fs::write(&path, &actual).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    assert!(
        expected == actual,
        "{path} differs from the encoding; see the module comment on fixtures"
    );
}

/// Covers stay what they were: a change shows up as a reviewed diff.
#[test]
fn covers_snapshot() {
    check_fixture("covers.txt", covers_fixture());
}

/// Points that pass the exact chord test only through rounding: each lies
/// just past the circle's edge, on a quadtree block boundary, so a cover
/// without the bounding-box pad (`pad = 0`) or without the cap test's slack
/// misses it. Found by searching with those mutants; with the real code each
/// must be in its cover. They sit at a latitude edge, near both poles and on
/// the antimeridian.
#[test]
fn points_admitted_by_rounding_are_in_the_cover() {
    // (center lat, center lng, meters, point lat, point lng)
    let cases: &[(f64, f64, f64, f64, f64)] = &[
        // Killed `pad = 0`.
        (
            0.3755333718231392,
            -73.04843142742129,
            83.21466835371496,
            0.37628173828125,
            -73.04843142742129,
        ),
        (
            1.9575128805689832,
            -142.21443062525412,
            776.1324622597966,
            1.9644927978515625,
            -142.21443062525412,
        ),
        (
            -6.989551281485651,
            -129.91900069881896,
            1776.8422451507347,
            -6.97357177734375,
            -129.91900069881896,
        ),
        (
            8.439329652757987,
            -116.61372423370446,
            3919.5381407690834,
            8.474578857421875,
            -116.61372423370446,
        ),
        (0.0, 179.99999937935942, 0.06901217877278167, 0.0, -180.0),
        // Killed the cap test without slack.
        (
            14.29101860620539,
            79.77751209907905,
            14.938825094193772,
            14.291152954101563,
            79.77751209907905,
        ),
        (
            -41.103254167914876,
            -55.91188735877118,
            110.9051960529292,
            -41.102256774902344,
            -55.91188735877118,
        ),
        (
            18.867833629733777,
            50.534274789539325,
            169.93362646651806,
            18.869361877441406,
            50.534274789539325,
        ),
        (
            28.92939814650216,
            -23.485785943974662,
            0.7856933379881635,
            28.929405212402344,
            -23.485785943974662,
        ),
        (0.0, 179.99992850883748, 7.949465551872666, 0.0, -180.0),
        (
            84.73583647527434,
            33.86014848840307,
            9.070157075913336,
            84.73591804504395,
            33.86014848840307,
        ),
        (
            -88.58534553638447,
            11.890164099022314,
            0.7721225326562623,
            -88.5853385925293,
            11.890164099022314,
        ),
        (
            -85.48544019016232,
            -50.85820818346478,
            5.401116553366116,
            -85.48539161682129,
            -50.85820818346478,
        ),
        (
            85.83446387519132,
            -153.03190848446948,
            0.128059387640437,
            85.83446502685547,
            -153.03190848446948,
        ),
    ];
    for &(lat, lng, meters, plat, plng) in cases {
        let q = radius_query(lat, lng, meters).unwrap();
        let p = point(plat, plng).unwrap();
        assert!(
            chord2(p.unit, q.center) <= q.chord2,
            "({plat}, {plng}) should pass the chord test"
        );
        assert!(
            inside(&q.ranges, p.cell),
            "({plat}, {plng}) passes the chord test of ({lat}, {lng}, {meters} m) but is outside the cover"
        );
    }
}
