//! The `geo` index kind (SPEC 20.10.3).
//!
//! A point `(lat, lng)` in degrees (WGS84) is indexed as:
//!
//! - a **cell**: the 62-bit Z-order (Morton) code of the point quantized to
//!   31 bits per axis on the equirectangular grid, `u` from longitude −180,
//!   `v` from latitude −90; bit `2i` of the code is bit `i` of `u`, bit
//!   `2i + 1` is bit `i` of `v` (encoding [`ENCODING`]);
//! - a **unit vector** `[x, y, z]` on the unit sphere, `x` towards
//!   `(0°, 0°)`, `y` towards `(0°, 90°E)`, `z` towards the north pole.
//!
//! A query region is covered by at most [`MAX_RANGES`] inclusive ranges of
//! cells ([`cover_box`], [`cover_radius`]). The cover only chooses which
//! shadow rows to read; the exact test decides: for a radius,
//! [`chord2`]`(row, center) <= `[`chord2_for_meters`]; for a box, the
//! stored `lat`/`lng` against the box bounds. Every cover is a superset of
//! its region under that exact test, rounding included.
//!
//! Every function here returns the same bits on every target (`INV-IX-1`):
//! only IEEE-754 basic operations and `sqrt` are used, never the platform's
//! `sin`/`cos` (see `trig`).

mod cover;
mod trig;

#[cfg(test)]
mod tests;

pub use cover::MAX_RANGES;
use cover::{BITS, Morton, Rect};

/// The name of this module's byte-level choices: cell layout, unit vector
/// formula and trigonometry. Changing any of them is a new encoding.
pub const ENCODING: &str = "jazz-ix-geo-z62/1";

/// Mean Earth radius in meters (IUGG R₁).
pub const EARTH_RADIUS_M: f64 = 6_371_008.8;

/// Largest radius a search may use, in meters (SPEC 20.4).
pub const MAX_RADIUS_M: f64 = 1_000_000.0;

/// `2^31`, the number of grid steps per axis.
const STEPS: f64 = (1u64 << BITS) as f64;
const MAX_STEP: u32 = (1u32 << BITS) - 1;

/// `180 / π`, rounded to nearest.
const RAD_TO_DEG: f64 = 180.0 / core::f64::consts::PI;

/// Invalid geo input. Carries no row data (SPEC 20.11).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum GeoError {
    /// Latitude not a finite number in `[-90, 90]`.
    Latitude(f64),
    /// Longitude not a finite number in `[-180, 180]`.
    Longitude(f64),
    /// Radius not a finite number in `[0, MAX_RADIUS_M]`.
    Radius(f64),
    /// A box whose `south` is north of its `north`.
    BoxInverted { south: f64, north: f64 },
}

impl core::fmt::Display for GeoError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            GeoError::Latitude(v) => write!(f, "latitude {v} is not in [-90, 90]"),
            GeoError::Longitude(v) => write!(f, "longitude {v} is not in [-180, 180]"),
            GeoError::Radius(v) => {
                write!(f, "radius {v} m is not in [0, {MAX_RADIUS_M}] m")
            }
            GeoError::BoxInverted { south, north } => {
                write!(f, "box south {south} is north of its north {north}")
            }
        }
    }
}

impl std::error::Error for GeoError {}

fn check_lat(lat: f64) -> Result<f64, GeoError> {
    if (-90.0..=90.0).contains(&lat) {
        Ok(lat)
    } else {
        Err(GeoError::Latitude(lat))
    }
}

fn check_lng(lng: f64) -> Result<f64, GeoError> {
    if (-180.0..=180.0).contains(&lng) {
        Ok(lng)
    } else {
        Err(GeoError::Longitude(lng))
    }
}

fn check_meters(meters: f64) -> Result<f64, GeoError> {
    if (0.0..=MAX_RADIUS_M).contains(&meters) {
        Ok(meters)
    } else {
        Err(GeoError::Radius(meters))
    }
}

/// Grid step of a value `offset` degrees from the axis origin, out of
/// `span`. Monotonic in `offset`, so a value inside a range of degrees never
/// lands outside that range's steps.
fn step(offset: f64, span: f64) -> u32 {
    let s = (offset / span * STEPS).floor();
    if s >= MAX_STEP as f64 {
        MAX_STEP
    } else if s <= 0.0 {
        0
    } else {
        s as u32
    }
}

fn step_lng(lng: f64) -> u32 {
    step(lng + 180.0, 360.0)
}

fn step_lat(lat: f64) -> u32 {
    step(lat + 90.0, 180.0)
}

/// The cell of a point (encoding [`ENCODING`]): a non-negative 62-bit code.
pub fn cell(lat: f64, lng: f64) -> Result<i64, GeoError> {
    let (lat, lng) = (check_lat(lat)?, check_lng(lng)?);
    Ok(cover::interleave(step_lng(lng), step_lat(lat)))
}

/// The point on the unit sphere: `[cos φ cos λ, cos φ sin λ, sin φ]`.
pub fn unit(lat: f64, lng: f64) -> Result<[f64; 3], GeoError> {
    let (lat, lng) = (check_lat(lat)?, check_lng(lng)?);
    let (sin_lat, cos_lat) = trig::sin_cos_deg(lat);
    let (sin_lng, cos_lng) = trig::sin_cos_deg(lng);
    Ok([cos_lat * cos_lng, cos_lat * sin_lng, sin_lat])
}

/// What a geo index stores for one row.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GeoPoint {
    pub cell: i64,
    pub unit: [f64; 3],
}

/// [`cell`] and [`unit`] of a point.
pub fn point(lat: f64, lng: f64) -> Result<GeoPoint, GeoError> {
    Ok(GeoPoint {
        cell: cell(lat, lng)?,
        unit: unit(lat, lng)?,
    })
}

/// Squared chord distance between two unit vectors, evaluated exactly as the
/// SQL does: `(x-qx)*(x-qx)+(y-qy)*(y-qy)+(z-qz)*(z-qz)`, left to right.
pub fn chord2(a: [f64; 3], b: [f64; 3]) -> f64 {
    let dx = a[0] - b[0];
    let dy = a[1] - b[1];
    let dz = a[2] - b[2];
    dx * dx + dy * dy + dz * dz
}

/// The squared chord of a great-circle distance on a sphere of radius
/// [`EARTH_RADIUS_M`]: `(2 sin(meters / 2R))²`.
pub fn chord2_for_meters(meters: f64) -> Result<f64, GeoError> {
    let meters = check_meters(meters)?;
    // At most 1000 km, so the half angle is below 0.08 < π/4.
    let c = 2.0 * trig::kernel_sin(meters / (2.0 * EARTH_RADIUS_M));
    Ok(c * c)
}

/// Covers the box from `south` to `north` and from `west` eastwards to
/// `east`; `west > east` crosses the antimeridian. Inclusive on all sides:
/// every point with `south <= lat <= north` and `west <= lng <= east` (or,
/// crossing, `lng >= west || lng <= east`) has its cell in a range.
pub fn cover_box(
    south: f64,
    west: f64,
    north: f64,
    east: f64,
) -> Result<Vec<(i64, i64)>, GeoError> {
    let (south, north) = (check_lat(south)?, check_lat(north)?);
    let (west, east) = (check_lng(west)?, check_lng(east)?);
    if south > north {
        return Err(GeoError::BoxInverted { south, north });
    }
    let (v0, v1) = (step_lat(south), step_lat(north));
    let rects: Vec<Rect> = if west <= east {
        vec![Rect {
            u0: step_lng(west),
            u1: step_lng(east),
            v0,
            v1,
        }]
    } else {
        vec![
            Rect {
                u0: 0,
                u1: step_lng(east),
                v0,
                v1,
            },
            Rect {
                u0: step_lng(west),
                u1: MAX_STEP,
                v0,
                v1,
            },
        ]
    };
    Ok(cover::cover::<Morton>(&rects, &cover::keep_all))
}

/// The grid rectangles of a radius's bounding box, padded so that every
/// point passing the exact chord test is inside despite rounding.
fn radius_rects(lat: f64, lng: f64, meters: f64) -> Vec<Rect> {
    let delta = meters / EARTH_RADIUS_M; // radians, at most 0.157
    let delta_deg = delta * RAD_TO_DEG;
    // Covers the rounding of every step here and of the stored vectors
    // (about 1e-15 rad), with a wide margin; 1e-9° is about 0.1 mm.
    let pad = 1e-9 + delta_deg * 1e-9;
    let south = lat - delta_deg - pad;
    let north = lat + delta_deg + pad;
    let all_lng = |south: f64, north: f64| {
        vec![Rect {
            u0: 0,
            u1: MAX_STEP,
            v0: step_lat(south.max(-90.0)),
            v1: step_lat(north.min(90.0)),
        }]
    };
    if south <= -90.0 || north >= 90.0 {
        // The cap reaches a pole: it spans every longitude.
        return all_lng(south, north);
    }
    // sin Δλ = sin δ / cos φ, and asin t ≤ t / √(1 − t²), sin δ ≤ δ.
    let (_, cos_lat) = trig::sin_cos_deg(lat);
    let t = delta / cos_lat;
    if t >= 1.0 {
        return all_lng(south, north);
    }
    let dlng = t / (1.0 - t * t).sqrt() * RAD_TO_DEG * (1.0 + 1e-9) + pad;
    if dlng >= 180.0 {
        return all_lng(south, north);
    }
    let (v0, v1) = (step_lat(south), step_lat(north));
    let (west, east) = (lng - dlng, lng + dlng);
    if west < -180.0 {
        vec![
            Rect {
                u0: 0,
                u1: step_lng(east),
                v0,
                v1,
            },
            Rect {
                u0: step_lng(west + 360.0),
                u1: MAX_STEP,
                v0,
                v1,
            },
        ]
    } else if east > 180.0 {
        vec![
            Rect {
                u0: 0,
                u1: step_lng(east - 360.0),
                v0,
                v1,
            },
            Rect {
                u0: step_lng(west),
                u1: MAX_STEP,
                v0,
                v1,
            },
        ]
    } else {
        vec![Rect {
            u0: step_lng(west),
            u1: step_lng(east),
            v0,
            v1,
        }]
    }
}

/// Covers every point within `meters` (great-circle, on a sphere of radius
/// [`EARTH_RADIUS_M`]) of `(lat, lng)` with the blocks that meet the cap.
/// The cap's bounding box (split at the antimeridian, spanning all
/// longitudes when the cap is near a pole) is only the snapping frame.
pub fn cover_radius(lat: f64, lng: f64, meters: f64) -> Result<Vec<(i64, i64)>, GeoError> {
    let (lat, lng, meters) = (check_lat(lat)?, check_lng(lng)?, check_meters(meters)?);
    let cap = Cap::new(lat, lng, meters);
    let keep = |x: u32, y: u32, k: u32| cap.meets(x, y, k);
    Ok(cover::cover::<Morton>(
        &radius_rects(lat, lng, meters),
        &keep,
    ))
}

/// Grid step `i` of an axis spanning `span` degrees from `origin`, in degrees.
fn edge(origin: f64, span: f64, i: u64) -> f64 {
    origin + i as f64 / STEPS * span
}

/// `sin²(d/2)` for `0 ≤ d ≤ 180` degrees: the haversine.
fn hav(d: f64) -> f64 {
    let (s, _) = trig::sin_cos_deg(d * 0.5);
    s * s
}

/// A spherical cap, for testing which quadtree blocks it meets.
struct Cap {
    lat: f64,
    lng: f64,
    cos_lat: f64,
    /// `hav` of the padded angular radius, with slack for rounding.
    limit: f64,
}

impl Cap {
    fn new(lat: f64, lng: f64, meters: f64) -> Cap {
        let delta_deg = meters / EARTH_RADIUS_M * RAD_TO_DEG;
        // The same padding as the bounding box, then slack for the rounding
        // of the bound itself (a few ulps of values at most 1).
        let padded = (delta_deg + 1e-9 + delta_deg * 1e-9).min(180.0);
        Cap {
            lat,
            lng,
            cos_lat: trig::sin_cos_deg(lat).1,
            limit: hav(padded) * (1.0 + 1e-9) + 1e-30,
        }
    }

    /// Whether the block of side `2^k` at grid corner `(x, y)` may hold a
    /// point of the cap. With the haversine formula
    /// `hav d = hav Δφ + cos φ₀ cos φ hav Δλ`, each term is bounded below
    /// separately over the block: the latitude gap to the block, the
    /// smallest `cos φ` in it, the longitude gap (around the globe) to it.
    /// So a rejected block holds no point within the radius.
    fn meets(&self, x: u32, y: u32, k: u32) -> bool {
        let side = 1u64 << k;
        let (w, e) = (
            edge(-180.0, 360.0, x as u64),
            edge(-180.0, 360.0, x as u64 + side),
        );
        let (s, n) = (
            edge(-90.0, 180.0, y as u64),
            edge(-90.0, 180.0, y as u64 + side),
        );
        let dlat = if self.lat < s {
            s - self.lat
        } else if self.lat > n {
            self.lat - n
        } else {
            0.0
        };
        let dlng = if w <= self.lng && self.lng <= e {
            0.0
        } else {
            let gap = |a: f64| {
                let d = (a - self.lng).abs();
                if d > 180.0 { 360.0 - d } else { d }
            };
            gap(w).min(gap(e))
        };
        // cos φ is smallest at the block's latitude farthest from the equator.
        let cos_min = trig::sin_cos_deg(s.abs().max(n.abs()).min(90.0)).1.max(0.0);
        let bound = hav(dlat.min(180.0)) + self.cos_lat * cos_min * hav(dlng.min(180.0));
        bound <= self.limit
    }
}

/// Everything SQL needs for a radius search (SPEC 20.5): read the `ranges`,
/// keep rows with `chord2(row, center) <= chord2`, order by that value.
#[derive(Clone, Debug, PartialEq)]
pub struct RadiusQuery {
    pub ranges: Vec<(i64, i64)>,
    pub center: [f64; 3],
    pub chord2: f64,
}

/// [`cover_radius`], [`unit`] of the center and [`chord2_for_meters`].
pub fn radius_query(lat: f64, lng: f64, meters: f64) -> Result<RadiusQuery, GeoError> {
    Ok(RadiusQuery {
        ranges: cover_radius(lat, lng, meters)?,
        center: unit(lat, lng)?,
        chord2: chord2_for_meters(meters)?,
    })
}
