//! Spherical geometry support for point-only spatial queries.
//!
//! This module is the only place where Groove depends on the upstream S2
//! implementation. Persisted values and query bindings use Groove-owned
//! longitude/latitude coordinates and unsigned S2 cell IDs.

use std::f64::consts::{PI, TAU};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use s2rst::s1::Angle;
use s2rst::s2::edge_distances;
use s2rst::s2::region_coverer::RegionCoverer;
use s2rst::s2::{Cap, CellId, LatLng, Loop, Point as S2Point, Polygon, Region};
use thiserror::Error;

/// Mean Earth radius used by Jazz's point query contract.
pub const EARTH_RADIUS_METERS: f64 = 6_371_008.8;
/// Stable identifier for the derived S2 point key encoding.
pub const S2_POINT_KEY_CONTRACT: &str = "groove.s2-point-key.v1";
/// Maximum cells requested from S2 for one predicate covering.
pub const MAX_COVERING_CELLS: usize = 64;
/// Hard bound on ordered scans after covering cells have been converted to
/// leaf-cell intervals and merged. `MAX_COVERING_CELLS` is a target per
/// component, not a total MultiPolygon limit.
pub const MAX_SCAN_RANGES: usize = 4_096;
/// Maximum total vertices accepted in one GeoJSON area.
pub const MAX_AREA_VERTICES: usize = 4_096;
const MAX_CELL_LEVEL: u8 = 30;
const ANTIPODAL_EDGE_TOLERANCE_RADIANS: f64 = 32.0 * f64::EPSILON;

/// WGS84 longitude/latitude in degrees, in that order.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GeoPoint {
    pub longitude: f64,
    pub latitude: f64,
}

impl GeoPoint {
    pub fn new(longitude: f64, latitude: f64) -> Result<Self, SpatialError> {
        if !longitude.is_finite()
            || !latitude.is_finite()
            || !(-180.0..=180.0).contains(&longitude)
            || !(-90.0..=90.0).contains(&latitude)
        {
            return Err(SpatialError::InvalidPoint);
        }
        Ok(Self {
            longitude,
            latitude,
        })
    }

    /// Return the canonical level-30 S2 leaf cell ID for this position.
    pub fn leaf_cell_id(self) -> u64 {
        CellId::from_point(&self.to_s2()).id()
    }

    /// Spherical distance in metres using the shared mean Earth radius.
    pub fn distance_meters(self, other: Self) -> f64 {
        self.to_s2().distance(other.to_s2()).radians() * EARTH_RADIUS_METERS
    }

    fn to_s2(self) -> S2Point {
        // All longitudes identify the same point at a pole. Canonicalizing
        // them avoids tiny trigonometric differences selecting adjacent cells.
        let longitude = if self.latitude.abs() == 90.0 {
            0.0
        } else if self.longitude == 180.0 {
            -180.0
        } else {
            self.longitude
        };
        LatLng::from_degrees(self.latitude, longitude).to_point()
    }
}

/// GeoJSON Polygon or MultiPolygon coordinates. Ring positions are
/// `[longitude, latitude]`; the first and last positions must be equal.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", content = "coordinates")]
pub enum GeoArea {
    Polygon(Vec<Vec<[f64; 2]>>),
    MultiPolygon(Vec<Vec<Vec<[f64; 2]>>>),
}

impl GeoArea {
    /// Parse a GeoJSON Polygon or MultiPolygon object. Feature wrappers and
    /// every other geometry type are rejected.
    pub fn from_geojson(value: &serde_json::Value) -> Result<Self, SpatialError> {
        let object = value.as_object().ok_or(SpatialError::InvalidArea)?;
        let kind = object
            .get("type")
            .and_then(serde_json::Value::as_str)
            .ok_or(SpatialError::InvalidArea)?;
        let coordinates = object.get("coordinates").ok_or(SpatialError::InvalidArea)?;
        match kind {
            "Polygon" => Ok(Self::Polygon(parse_polygon_coordinates(coordinates)?)),
            "MultiPolygon" => {
                let polygons = coordinates
                    .as_array()
                    .ok_or(SpatialError::InvalidArea)?
                    .iter()
                    .map(parse_polygon_coordinates)
                    .collect::<Result<Vec<_>, _>>()?;
                if polygons.is_empty() {
                    return Err(SpatialError::InvalidArea);
                }
                Ok(Self::MultiPolygon(polygons))
            }
            _ => Err(SpatialError::InvalidArea),
        }
    }

    /// Validate and compile this area once for use across query evaluation and
    /// subscription updates.
    pub fn compile(&self) -> Result<CompiledGeoArea, SpatialError> {
        let total_vertices: usize = match self {
            Self::Polygon(rings) => rings.iter().map(Vec::len).sum(),
            Self::MultiPolygon(polygons) => polygons
                .iter()
                .flat_map(|rings| rings.iter())
                .map(Vec::len)
                .sum(),
        };
        if total_vertices > MAX_AREA_VERTICES {
            return Err(SpatialError::VertexLimitExceeded);
        }
        let polygons = match self {
            Self::Polygon(rings) => vec![compile_polygon(rings)?],
            Self::MultiPolygon(polygons) => polygons
                .iter()
                .map(|rings| compile_polygon(rings))
                .collect::<Result<Vec<_>, _>>()?,
        };
        if polygons.is_empty() {
            return Err(SpatialError::InvalidArea);
        }

        // Build one temporary polygon to let S2's full validator reject
        // crossing or shared edges, then compare actual component interiors.
        // A component inside another component's hole is valid and must not
        // be rejected merely because one shell vertex lies within the other
        // component's exterior ring.
        if polygons.len() > 1 {
            let loops = polygons
                .iter()
                .flat_map(|polygon| polygon.loops().iter().cloned())
                .collect::<Vec<_>>();
            let combined = Polygon::from_loops(loops);
            if combined.find_validation_error().is_some() {
                return Err(SpatialError::InvalidAreaTopology);
            }
            for (index, polygon) in polygons.iter().enumerate() {
                for other in polygons.iter().skip(index + 1) {
                    if polygon.intersects_polygon(other) {
                        return Err(SpatialError::InvalidAreaTopology);
                    }
                }
            }
        }

        let mut ranges = Vec::new();
        for polygon in &polygons {
            let polygon_ranges = covering_ranges(polygon)?;
            ranges.extend(polygon_ranges);
        }
        let ranges = normalize_ranges(ranges)?;
        Ok(CompiledGeoArea {
            polygons,
            ranges,
            vertex_count: total_vertices,
        })
    }
}

/// One half-open interval of leaf-cell IDs: `[start, end_exclusive)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct S2CellRange {
    pub start: u64,
    pub end_exclusive: u64,
}

/// Validated S2 geometry and its conservative durable-index ranges.
#[derive(Clone, Debug)]
pub struct CompiledGeoArea {
    polygons: Vec<Polygon>,
    ranges: Vec<S2CellRange>,
    vertex_count: usize,
}

/// Compiled spatial predicate shared by query graph nodes and live updates.
/// Equality and hashing use an explicit versioned identity, not upstream S2
/// implementation details.
#[derive(Clone, Debug)]
pub struct SpatialPredicate {
    kind: SpatialPredicateKind,
    ranges: Vec<S2CellRange>,
    identity: Arc<[u8]>,
}

#[derive(Clone, Debug)]
enum SpatialPredicateKind {
    Near(NearQuery),
    Within(Arc<CompiledGeoArea>),
    Intersects(Arc<CompiledGeoArea>),
}

impl SpatialPredicate {
    /// Construct and validate a bounded distance predicate.
    pub fn near(
        center: GeoPoint,
        min_distance_meters: Option<f64>,
        max_distance_meters: f64,
    ) -> Result<Self, SpatialError> {
        let query = NearQuery::new(center, min_distance_meters, max_distance_meters)?;
        let ranges = query.ranges()?;
        let mut identity = b"groove.spatial-predicate.v1\0near".to_vec();
        put_f64(&mut identity, center.longitude);
        put_f64(&mut identity, center.latitude);
        put_f64(&mut identity, query.min_distance_meters);
        put_f64(&mut identity, query.max_distance_meters);
        Ok(Self {
            kind: SpatialPredicateKind::Near(query),
            ranges,
            identity: identity.into(),
        })
    }

    /// Construct and validate a strict-interior area predicate.
    pub fn within(area: &GeoArea) -> Result<Self, SpatialError> {
        Self::area_predicate(area, false)
    }

    /// Construct and validate an inclusive-boundary area predicate.
    pub fn intersects(area: &GeoArea) -> Result<Self, SpatialError> {
        Self::area_predicate(area, true)
    }

    fn area_predicate(area: &GeoArea, intersects: bool) -> Result<Self, SpatialError> {
        let compiled = Arc::new(area.compile()?);
        let ranges = compiled.ranges().to_vec();
        let mut identity = if intersects {
            b"groove.spatial-predicate.v1\0intersects".to_vec()
        } else {
            b"groove.spatial-predicate.v1\0within".to_vec()
        };
        let encoded = postcard::to_allocvec(area).map_err(|_| SpatialError::InvalidArea)?;
        identity.extend_from_slice(&encoded);
        Ok(Self {
            kind: if intersects {
                SpatialPredicateKind::Intersects(compiled)
            } else {
                SpatialPredicateKind::Within(compiled)
            },
            ranges,
            identity: identity.into(),
        })
    }

    /// Conservative leaf-cell ranges used to access the durable index.
    pub fn ranges(&self) -> &[S2CellRange] {
        &self.ranges
    }

    /// Number of supplied GeoJSON positions used by this predicate. Near
    /// predicates contribute no vertices.
    pub fn vertex_count(&self) -> usize {
        match &self.kind {
            SpatialPredicateKind::Near(_) => 0,
            SpatialPredicateKind::Within(area) | SpatialPredicateKind::Intersects(area) => {
                area.vertex_count
            }
        }
    }

    /// Whether this is a distance predicate.
    pub fn is_near(&self) -> bool {
        matches!(self.kind, SpatialPredicateKind::Near(_))
    }

    /// Exact predicate membership for one validated stored point.
    pub fn matches(&self, point: GeoPoint) -> bool {
        match &self.kind {
            SpatialPredicateKind::Near(query) => query.matches(point),
            SpatialPredicateKind::Within(area) => area.contains_strict(point),
            SpatialPredicateKind::Intersects(area) => area.intersects(point),
        }
    }

    /// Spherical distance for ranking a near candidate.
    pub fn distance_meters(&self, point: GeoPoint) -> Option<f64> {
        match &self.kind {
            SpatialPredicateKind::Near(query) => Some(query.center.distance_meters(point)),
            SpatialPredicateKind::Within(_) | SpatialPredicateKind::Intersects(_) => None,
        }
    }
}

impl PartialEq for SpatialPredicate {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity
    }
}

impl Eq for SpatialPredicate {}

impl Hash for SpatialPredicate {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.identity.hash(state);
    }
}

impl PartialOrd for SpatialPredicate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SpatialPredicate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.identity.cmp(&other.identity)
    }
}

fn put_f64(output: &mut Vec<u8>, value: f64) {
    // Treat both signed zero encodings as the same coordinate/distance.
    let bits = if value == 0.0 { 0 } else { value.to_bits() };
    output.extend_from_slice(&bits.to_be_bytes());
}

impl CompiledGeoArea {
    /// Conservative S2 leaf-cell ranges for indexed candidate lookup.
    pub fn ranges(&self) -> &[S2CellRange] {
        &self.ranges
    }

    /// True when the point is strictly inside the area, excluding every
    /// exterior and hole boundary.
    pub fn contains_strict(&self, point: GeoPoint) -> bool {
        let point = point.to_s2();
        self.polygons
            .iter()
            .any(|polygon| !polygon_on_boundary(polygon, point) && polygon.contains_point(&point))
    }

    /// True when the point is inside the area or on any exterior or hole
    /// boundary.
    pub fn intersects(&self, point: GeoPoint) -> bool {
        let point = point.to_s2();
        self.polygons
            .iter()
            .any(|polygon| polygon_on_boundary(polygon, point) || polygon.contains_point(&point))
    }
}

/// Bounded spherical distance query parameters.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct NearQuery {
    pub center: GeoPoint,
    pub min_distance_meters: f64,
    pub max_distance_meters: f64,
}

impl NearQuery {
    pub fn new(
        center: GeoPoint,
        min_distance_meters: Option<f64>,
        max_distance_meters: f64,
    ) -> Result<Self, SpatialError> {
        let min_distance_meters = min_distance_meters.unwrap_or(0.0);
        if !max_distance_meters.is_finite()
            || !min_distance_meters.is_finite()
            || min_distance_meters < 0.0
            || max_distance_meters < 0.0
            || min_distance_meters > max_distance_meters
        {
            return Err(SpatialError::InvalidDistanceBounds);
        }
        Ok(Self {
            center,
            min_distance_meters,
            max_distance_meters,
        })
    }

    pub fn matches(self, point: GeoPoint) -> bool {
        let distance = self.center.distance_meters(point);
        distance >= self.min_distance_meters && distance <= self.max_distance_meters
    }

    /// Conservative S2 covering of the radius. Distances larger than the
    /// sphere's half-circumference cover the whole sphere.
    pub fn ranges(self) -> Result<Vec<S2CellRange>, SpatialError> {
        let angle = (self.max_distance_meters / EARTH_RADIUS_METERS).min(PI);
        let cap = Cap::from_center_angle(self.center.to_s2(), Angle::from_radians(angle));
        let coverer = query_coverer();
        let cells = coverer.covering(&cap);
        normalize_ranges(cells.iter().copied().map(cell_range).collect())
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum SpatialError {
    #[error("invalid WGS84 point; expected finite [longitude, latitude] within geographic bounds")]
    InvalidPoint,
    #[error("invalid GeoJSON Polygon or MultiPolygon")]
    InvalidArea,
    #[error("invalid polygon topology")]
    InvalidAreaTopology,
    #[error("area exceeds the 4096 vertex budget")]
    VertexLimitExceeded,
    #[error("spatial query exceeds the 4096 scan range budget")]
    RangeLimitExceeded,
    #[error("near query requires finite non-negative distance bounds with minimum <= maximum")]
    InvalidDistanceBounds,
}

fn parse_polygon_coordinates(
    value: &serde_json::Value,
) -> Result<Vec<Vec<[f64; 2]>>, SpatialError> {
    let rings = value.as_array().ok_or(SpatialError::InvalidArea)?;
    if rings.is_empty() {
        return Err(SpatialError::InvalidArea);
    }
    rings
        .iter()
        .map(|ring| {
            ring.as_array()
                .ok_or(SpatialError::InvalidArea)?
                .iter()
                .map(|position| {
                    let values = position.as_array().ok_or(SpatialError::InvalidArea)?;
                    if values.len() != 2 {
                        return Err(SpatialError::InvalidArea);
                    }
                    let longitude = values[0].as_f64().ok_or(SpatialError::InvalidArea)?;
                    let latitude = values[1].as_f64().ok_or(SpatialError::InvalidArea)?;
                    GeoPoint::new(longitude, latitude)?;
                    Ok([longitude, latitude])
                })
                .collect()
        })
        .collect()
}

fn compile_polygon(rings: &[Vec<[f64; 2]>]) -> Result<Polygon, SpatialError> {
    if rings.is_empty() {
        return Err(SpatialError::InvalidArea);
    }
    let vertex_count = rings.iter().map(Vec::len).sum::<usize>();
    if vertex_count > MAX_AREA_VERTICES {
        return Err(SpatialError::VertexLimitExceeded);
    }
    let mut loops = Vec::with_capacity(rings.len());
    for ring in rings {
        if ring.len() < 4 || ring.first() != ring.last() {
            return Err(SpatialError::InvalidArea);
        }
        let vertices = ring[..ring.len() - 1]
            .iter()
            .map(|[longitude, latitude]| GeoPoint::new(*longitude, *latitude).map(GeoPoint::to_s2))
            .collect::<Result<Vec<_>, SpatialError>>()?;
        if vertices.iter().enumerate().any(|(index, start)| {
            let end = vertices[(index + 1) % vertices.len()];
            (start.distance(end).radians() - PI).abs() <= ANTIPODAL_EDGE_TOLERANCE_RADIANS
        }) {
            // S2's loop validator accepts antipodal endpoints even though they
            // leave two equally short candidate arcs. Jazz rejects that
            // ambiguity before asking S2 to classify or cover the polygon.
            return Err(SpatialError::InvalidAreaTopology);
        }
        let mut loop_ = Loop::new(vertices);
        loop_
            .validate()
            .map_err(|_| SpatialError::InvalidAreaTopology)?;
        if (loop_.area() - 2.0 * PI).abs() <= 16.0 * f64::EPSILON * TAU {
            return Err(SpatialError::InvalidAreaTopology);
        }
        loop_.normalize();
        loops.push(loop_);
    }

    // GeoJSON assigns ring roles by position: the first ring is the exterior
    // and every later ring is a hole of that exterior. S2's `from_loops`
    // infers roles from containment, which would otherwise turn an outside
    // hole into a second exterior and a nested hole into a filled island.
    let exterior = &loops[0];
    for hole in loops.iter().skip(1) {
        if !exterior.contains_loop(hole) {
            return Err(SpatialError::InvalidAreaTopology);
        }
    }
    for (index, first_hole) in loops.iter().enumerate().skip(1) {
        for second_hole in loops.iter().skip(index + 1) {
            // Holes must be direct, disjoint children of the exterior. The
            // full polygon validator below additionally rejects crossing or
            // shared edges; this check catches a hole nested inside a hole.
            if first_hole.contains_loop(second_hole)
                || second_hole.contains_loop(first_hole)
                || first_hole.intersects_loop(second_hole)
            {
                return Err(SpatialError::InvalidAreaTopology);
            }
        }
    }
    let polygon = Polygon::from_loops(loops);
    if polygon.find_validation_error().is_some() {
        return Err(SpatialError::InvalidAreaTopology);
    }
    Ok(polygon)
}

fn polygon_on_boundary(polygon: &Polygon, point: S2Point) -> bool {
    // The edge distance uses S2's projection predicates. The threshold is
    // limited to floating-point construction error on unit vectors; it is
    // unrelated to map-scale distance and does not widen candidate ranges.
    let construction_error = 32.0 * f64::EPSILON;
    polygon.loops().iter().any(|ring| {
        (0..ring.num_vertices()).any(|index| {
            let start = ring.vertex(index);
            let end = ring.vertex((index + 1) % ring.num_vertices());
            edge_distances::distance_from_segment(point, start, end).radians() <= construction_error
        })
    })
}

fn covering_ranges(region: &dyn s2rst::s2::Region) -> Result<Vec<S2CellRange>, SpatialError> {
    let coverer = query_coverer();
    let cells = coverer.covering(region);
    Ok(cells.iter().copied().map(cell_range).collect())
}

fn query_coverer() -> RegionCoverer {
    RegionCoverer::new()
        .min_level(0_u8)
        .max_level(MAX_CELL_LEVEL)
        .level_mod(1)
        .max_cells(MAX_COVERING_CELLS)
}

fn cell_range(cell: CellId) -> S2CellRange {
    let start = cell.range_min().id();
    let end_exclusive = cell
        .range_max()
        .id()
        .checked_add(1)
        .expect("S2 cell ranges do not end at u64::MAX");
    S2CellRange {
        start,
        end_exclusive,
    }
}

fn normalize_ranges(mut ranges: Vec<S2CellRange>) -> Result<Vec<S2CellRange>, SpatialError> {
    ranges.sort_unstable();
    let mut normalized: Vec<S2CellRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if let Some(previous) = normalized.last_mut()
            && range.start <= previous.end_exclusive
        {
            previous.end_exclusive = previous.end_exclusive.max(range.end_exclusive);
        } else {
            normalized.push(range);
        }
        if normalized.len() > MAX_SCAN_RANGES {
            return Err(SpatialError::RangeLimitExceeded);
        }
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point(longitude: f64, latitude: f64) -> GeoPoint {
        GeoPoint::new(longitude, latitude).unwrap()
    }

    fn square() -> GeoArea {
        GeoArea::Polygon(vec![vec![
            [9.0, 19.0],
            [9.0, 21.0],
            [11.0, 21.0],
            [11.0, 19.0],
            [9.0, 19.0],
        ]])
    }

    /// This lower-level test pins the S2 adapter's coordinate order and leaf
    /// cell encoding; Jazz integration tests exercise the public read API.
    #[test]
    fn point_coordinates_map_to_s2_leaf_cell_and_normalize_equivalents() {
        let origin = point(0.0, 0.0);
        assert_eq!(origin.leaf_cell_id(), 0x1000_0000_0000_0001);
        assert_eq!(
            point(180.0, 10.0).leaf_cell_id(),
            point(-180.0, 10.0).leaf_cell_id()
        );
        assert_eq!(
            point(10.0, 90.0).leaf_cell_id(),
            point(-45.0, 90.0).leaf_cell_id()
        );
    }

    /// This direct geometry test covers the declared strict/intersects
    /// boundary contract, including a point on an edge rather than only a
    /// vertex; user-visible query behavior is covered at the Jazz layer.
    #[test]
    fn area_predicates_distinguish_interior_edge_and_exterior() {
        let area = square().compile().unwrap();
        assert!(area.contains_strict(point(10.0, 20.0)));
        assert!(!area.contains_strict(point(9.0, 20.0)));
        assert!(area.intersects(point(9.0, 20.0)));
        assert!(!area.intersects(point(8.0, 20.0)));
        assert!(!area.ranges().is_empty());
    }

    /// This unit test pins hole and dateline geometry at the adapter boundary;
    /// Jazz integration tests exercise the same classification through reads.
    #[test]
    fn area_predicates_preserve_holes_and_short_great_circle_edges() {
        let with_hole = GeoArea::Polygon(vec![
            vec![
                [0.0, 0.0],
                [10.0, 0.0],
                [10.0, 10.0],
                [0.0, 10.0],
                [0.0, 0.0],
            ],
            vec![[2.0, 2.0], [8.0, 2.0], [8.0, 8.0], [2.0, 8.0], [2.0, 2.0]],
        ])
        .compile()
        .unwrap();
        assert!(with_hole.contains_strict(point(1.0, 1.0)));
        assert!(!with_hole.contains_strict(point(5.0, 5.0)));
        assert!(!with_hole.intersects(point(5.0, 5.0)));
        assert!(!with_hole.contains_strict(point(2.0, 5.0)));
        assert!(with_hole.intersects(point(2.0, 5.0)));

        let dateline = GeoArea::Polygon(vec![vec![
            [170.0, -10.0],
            [-170.0, -10.0],
            [-170.0, 10.0],
            [170.0, 10.0],
            [170.0, -10.0],
        ]])
        .compile()
        .unwrap();
        assert!(dateline.contains_strict(point(180.0, 0.0)));
        assert!(!dateline.intersects(point(0.0, 0.0)));
    }

    /// This adapter-level regression is needed because public query tests only
    /// observe the compiled area result: they cannot establish that malformed
    /// GeoJSON ring roles are rejected before S2 infers a different shape.
    #[test]
    fn polygon_rejects_holes_outside_shell_and_nested_holes() {
        let outside_hole = GeoArea::Polygon(vec![
            vec![[0.0, 0.0], [4.0, 0.0], [4.0, 4.0], [0.0, 4.0], [0.0, 0.0]],
            vec![
                [10.0, 10.0],
                [11.0, 10.0],
                [11.0, 11.0],
                [10.0, 11.0],
                [10.0, 10.0],
            ],
        ]);
        assert_eq!(
            outside_hole.compile().unwrap_err(),
            SpatialError::InvalidAreaTopology
        );

        let nested_holes = GeoArea::Polygon(vec![
            vec![
                [0.0, 0.0],
                [10.0, 0.0],
                [10.0, 10.0],
                [0.0, 10.0],
                [0.0, 0.0],
            ],
            vec![[2.0, 2.0], [8.0, 2.0], [8.0, 8.0], [2.0, 8.0], [2.0, 2.0]],
            vec![[3.0, 3.0], [4.0, 3.0], [4.0, 4.0], [3.0, 4.0], [3.0, 3.0]],
        ]);
        assert_eq!(
            nested_holes.compile().unwrap_err(),
            SpatialError::InvalidAreaTopology
        );
    }

    /// This direct covering test proves the spatial index candidate ranges are
    /// conservative; result-level integration tests cannot see a point omitted
    /// before exact predicate evaluation.
    #[test]
    fn area_and_radius_coverings_include_every_exact_match() {
        let area = square().compile().unwrap();
        for longitude in [9.0, 9.5, 10.0, 10.5, 11.0] {
            for latitude in [19.0, 19.5, 20.0, 20.5, 21.0] {
                let candidate = point(longitude, latitude);
                if area.intersects(candidate) {
                    let leaf = candidate.leaf_cell_id();
                    assert!(
                        area.ranges()
                            .iter()
                            .any(|range| range.start <= leaf && leaf < range.end_exclusive)
                    );
                }
            }
        }

        let radius = NearQuery::new(point(179.0, 0.0), None, 500_000.0).unwrap();
        let ranges = radius.ranges().unwrap();
        for candidate in [point(180.0, 0.0), point(-179.0, 1.0), point(170.0, 0.0)] {
            if radius.matches(candidate) {
                let leaf = candidate.leaf_cell_id();
                assert!(
                    ranges
                        .iter()
                        .any(|range| range.start <= leaf && leaf < range.end_exclusive)
                );
            }
        }
    }

    /// MultiPolygon is an accepted public GeoJSON geometry, so test its
    /// separated component membership directly as well as Polygon behavior.
    #[test]
    fn multipolygon_classifies_components_and_rejects_overlapping_components() {
        let component = |west: f64, east: f64| {
            vec![vec![
                [west, 0.0],
                [east, 0.0],
                [east, 2.0],
                [west, 2.0],
                [west, 0.0],
            ]]
        };
        let area = GeoArea::MultiPolygon(vec![component(0.0, 2.0), component(4.0, 6.0)])
            .compile()
            .unwrap();
        assert!(area.contains_strict(point(1.0, 1.0)));
        assert!(area.contains_strict(point(5.0, 1.0)));
        assert!(!area.intersects(point(3.0, 1.0)));

        let overlapping = GeoArea::MultiPolygon(vec![component(0.0, 3.0), component(2.0, 4.0)]);
        assert_eq!(
            overlapping.compile().unwrap_err(),
            SpatialError::InvalidAreaTopology
        );

        let shell_with_hole = vec![
            vec![
                [0.0, 0.0],
                [10.0, 0.0],
                [10.0, 10.0],
                [0.0, 10.0],
                [0.0, 0.0],
            ],
            vec![[2.0, 2.0], [8.0, 2.0], [8.0, 8.0], [2.0, 8.0], [2.0, 2.0]],
        ];
        let island_in_hole = vec![vec![
            [3.0, 3.0],
            [4.0, 3.0],
            [4.0, 4.0],
            [3.0, 4.0],
            [3.0, 3.0],
        ]];
        let disjoint = GeoArea::MultiPolygon(vec![shell_with_hole, island_in_hole])
            .compile()
            .unwrap();
        assert!(disjoint.contains_strict(point(3.5, 3.5)));
        assert!(!disjoint.intersects(point(5.0, 5.0)));
    }

    /// S2 loop normalization must accept either vertex winding while keeping
    /// the smaller spherical exterior, including a cap that encloses a pole.
    #[test]
    fn area_predicates_accept_reversed_winding_and_polar_rings() {
        let clockwise = GeoArea::Polygon(vec![vec![
            [9.0, 19.0],
            [9.0, 21.0],
            [11.0, 21.0],
            [11.0, 19.0],
            [9.0, 19.0],
        ]])
        .compile()
        .unwrap();
        let reversed = GeoArea::Polygon(vec![vec![
            [9.0, 19.0],
            [11.0, 19.0],
            [11.0, 21.0],
            [9.0, 21.0],
            [9.0, 19.0],
        ]])
        .compile()
        .unwrap();
        assert!(clockwise.contains_strict(point(10.0, 20.0)));
        assert!(reversed.contains_strict(point(10.0, 20.0)));
        assert!(!reversed.contains_strict(point(0.0, 0.0)));

        let polar = GeoArea::Polygon(vec![vec![
            [-135.0, 80.0],
            [-45.0, 80.0],
            [45.0, 80.0],
            [135.0, 80.0],
            [-135.0, 80.0],
        ]])
        .compile()
        .unwrap();
        assert!(polar.contains_strict(point(0.0, 89.0)));
        assert!(!polar.intersects(point(0.0, 0.0)));
    }

    /// S2's topology validator is checked here directly because malformed
    /// query geometry must fail before a query plan can scan an index.
    #[test]
    fn area_compilation_rejects_open_and_self_intersecting_rings() {
        let open = GeoArea::Polygon(vec![vec![[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0]]]);
        assert_eq!(open.compile().unwrap_err(), SpatialError::InvalidArea);

        let bow_tie = GeoArea::Polygon(vec![vec![
            [0.0, 0.0],
            [2.0, 2.0],
            [0.0, 2.0],
            [2.0, 0.0],
            [0.0, 0.0],
        ]]);
        assert_eq!(
            bow_tie.compile().unwrap_err(),
            SpatialError::InvalidAreaTopology
        );

        let antipodal_edge = GeoArea::Polygon(vec![vec![
            [0.0, 0.0],
            [180.0, 0.0],
            [90.0, 10.0],
            [0.0, 0.0],
        ]]);
        assert_eq!(
            antipodal_edge.compile().unwrap_err(),
            SpatialError::InvalidAreaTopology
        );

        let hemispherical = GeoArea::Polygon(vec![vec![
            [0.0, 0.0],
            [90.0, 0.0],
            [180.0, 0.0],
            [-90.0, 0.0],
            [0.0, 0.0],
        ]]);
        assert_eq!(
            hemispherical.compile().unwrap_err(),
            SpatialError::InvalidAreaTopology
        );
    }

    /// This byte-adjacent range test is direct because the durable index uses
    /// half-open U64 scans and must include every row sharing the final cell.
    #[test]
    fn covering_ranges_are_sorted_merged_and_half_open() {
        let ranges = normalize_ranges(vec![
            S2CellRange {
                start: 20,
                end_exclusive: 30,
            },
            S2CellRange {
                start: 5,
                end_exclusive: 20,
            },
            S2CellRange {
                start: 40,
                end_exclusive: 45,
            },
        ])
        .unwrap();
        assert_eq!(
            ranges,
            vec![
                S2CellRange {
                    start: 5,
                    end_exclusive: 30,
                },
                S2CellRange {
                    start: 40,
                    end_exclusive: 45,
                },
            ]
        );
    }

    /// This direct admission test covers the bounded metric contract before
    /// query lowering can turn it into an index range.
    #[test]
    fn near_bounds_are_inclusive_and_validate_distance_parameters() {
        let center = point(-69.9312, 18.4861);
        let same = NearQuery::new(center, None, 0.0).unwrap();
        assert!(same.matches(center));
        assert!(NearQuery::new(center, Some(-1.0), 5.0).is_err());
        assert!(NearQuery::new(center, Some(5.0), 4.0).is_err());
        assert!(NearQuery::new(center, None, f64::INFINITY).is_err());
    }
}
