//! Declarative fixture and write-stream generation for Jazz benchmark scenarios.

use std::collections::BTreeMap;

use jazz::ids::{AuthorSubject, RowUuid};
use jazz::records::Value;

use crate::distributions::{Lcg, Zipf};

/// Declarative fixture builder.
#[derive(Clone, Debug, Default)]
pub struct FixtureBuilder {
    entities: Vec<EntitySet>,
    edges: Vec<EdgeSet>,
}

impl FixtureBuilder {
    /// Construct an empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an entity set.
    pub fn entity_set(mut self, set: EntitySet) -> Self {
        self.entities.push(set);
        self
    }

    /// Add a junction/edge set.
    pub fn edge_set(mut self, set: EdgeSet) -> Self {
        self.edges.push(set);
        self
    }

    /// Generate a deterministic fixture.
    pub fn build(&self, seed: u64) -> Fixture {
        let mut rng = Lcg::new(seed);
        let mut fixture = Fixture::default();
        let mut set_indices = BTreeMap::<String, Vec<usize>>::new();

        for entity in &self.entities {
            let mut rows = Vec::with_capacity(entity.count);
            let mut indices = Vec::with_capacity(entity.count);
            for idx in 0..entity.count {
                let row_uuid = deterministic_row_uuid(seed, &entity.name, idx);
                let mut cells = BTreeMap::new();
                for generator in &entity.cells {
                    cells.insert(
                        generator.column.clone(),
                        generator.value(&fixture, &mut rng, idx),
                    );
                }
                let commit_idx = fixture.commits.len();
                fixture.commits.push(FixtureCommit {
                    table: entity.table.clone(),
                    row_uuid,
                    cells,
                });
                rows.push(row_uuid);
                indices.push(commit_idx);
            }
            if entity.authors {
                fixture.authors_by_set.insert(
                    entity.name.clone(),
                    rows.iter()
                        .map(|row| {
                            AuthorSubject::authenticated("urn:jazz:sim", &row.0.to_string())
                                .expect("simulation issuer is external")
                        })
                        .collect::<Vec<_>>(),
                );
            }
            fixture.rows_by_set.insert(entity.name.clone(), rows);
            set_indices.insert(entity.name.clone(), indices);
        }

        for edge in &self.edges {
            let left_rows = fixture
                .rows_by_set
                .get(&edge.left_set)
                .unwrap_or_else(|| panic!("unknown left set {}", edge.left_set))
                .clone();
            let right_rows = fixture
                .rows_by_set
                .get(&edge.right_set)
                .unwrap_or_else(|| panic!("unknown right set {}", edge.right_set))
                .clone();
            let zipf = matches!(edge.right_distribution, RefDistribution::Zipf { .. })
                .then(|| edge.right_distribution.zipf(right_rows.len()));
            for (left_idx, left) in left_rows.iter().copied().enumerate() {
                let count = rng.range_usize(edge.min_per_left, edge.max_per_left);
                for edge_idx in 0..count {
                    let right_idx =
                        edge.right_distribution
                            .sample(&mut rng, right_rows.len(), zipf.as_ref());
                    let mut cells = BTreeMap::new();
                    cells.insert(edge.left_column.clone(), Value::Uuid(left.0));
                    cells.insert(
                        edge.right_column.clone(),
                        Value::Uuid(right_rows[right_idx].0),
                    );
                    for generator in &edge.cells {
                        cells.insert(
                            generator.column.clone(),
                            generator.value(&fixture, &mut rng, edge_idx),
                        );
                    }
                    fixture.commits.push(FixtureCommit {
                        table: edge.table.clone(),
                        row_uuid: deterministic_row_uuid(
                            seed,
                            &edge.name,
                            left_idx * edge.max_per_left + edge_idx,
                        ),
                        cells,
                    });
                }
            }
        }

        fixture
    }
}

/// One generated mergeable fixture commit.
#[derive(Clone, Debug, PartialEq)]
pub struct FixtureCommit {
    /// Target table.
    pub table: String,
    /// Stable generated row id.
    pub row_uuid: RowUuid,
    /// User cells keyed by table column name.
    pub cells: BTreeMap<String, Value>,
}

/// Generated fixture output consumed by scenarios.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Fixture {
    /// Commit payloads in deterministic insertion order.
    pub commits: Vec<FixtureCommit>,
    /// Row ids by declared set name.
    pub rows_by_set: BTreeMap<String, Vec<RowUuid>>,
    /// Optional author ids by declared set name.
    pub authors_by_set: BTreeMap<String, Vec<AuthorSubject>>,
}

impl Fixture {
    /// Insert every generated row into `db`, in one transaction per `batch`
    /// rows.
    pub fn insert_into(&self, db: &jazz::db::Db, batch: usize) {
        for chunk in self.commits.chunks(batch.max(1)) {
            let tx = jazz::db::block_on(db.mergeable_tx()).expect("open fixture transaction");
            for commit in chunk {
                jazz::db::block_on(tx.insert(
                    &commit.table,
                    commit.cells.clone(),
                    jazz::db::InsertOptions {
                        row_id: Some(commit.row_uuid),
                        ..Default::default()
                    },
                ))
                .expect("insert fixture row");
            }
            jazz::db::block_on(tx.commit()).expect("commit fixture rows");
        }
    }

    /// Count generated rows for one table.
    pub fn table_count(&self, table: &str) -> usize {
        self.commits
            .iter()
            .filter(|commit| commit.table == table)
            .count()
    }

    /// Stable hash of generated payloads for reproducibility checks.
    pub fn stable_hash(&self) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for commit in &self.commits {
            mix_str(&mut hash, &commit.table);
            mix_bytes(&mut hash, commit.row_uuid.as_bytes());
            for (key, value) in &commit.cells {
                mix_str(&mut hash, key);
                mix_str(&mut hash, &format!("{value:?}"));
            }
        }
        hash
    }
}

/// Entity-set declaration.
#[derive(Clone, Debug)]
pub struct EntitySet {
    /// Logical set name for references.
    pub name: String,
    /// Target Jazz table.
    pub table: String,
    /// Number of rows to generate.
    pub count: usize,
    /// Cell generators.
    pub cells: Vec<CellGen>,
    /// Whether generated row ids are also author ids.
    pub authors: bool,
}

impl EntitySet {
    /// Construct an entity set.
    pub fn new(name: impl Into<String>, table: impl Into<String>, count: usize) -> Self {
        Self {
            name: name.into(),
            table: table.into(),
            count,
            cells: Vec::new(),
            authors: false,
        }
    }

    /// Add a cell generator.
    pub fn cell(mut self, column: impl Into<String>, value_gen: CellValueGen) -> Self {
        self.cells.push(CellGen {
            column: column.into(),
            value_gen,
        });
        self
    }

    /// Mark this set's row ids as usable authors.
    pub fn as_authors(mut self) -> Self {
        self.authors = true;
        self
    }
}

/// Junction/edge-set declaration.
#[derive(Clone, Debug)]
pub struct EdgeSet {
    /// Logical edge set name.
    pub name: String,
    /// Target Jazz table.
    pub table: String,
    /// Referenced left set.
    pub left_set: String,
    /// Left reference column.
    pub left_column: String,
    /// Referenced right set.
    pub right_set: String,
    /// Right reference column.
    pub right_column: String,
    /// Minimum edges per left row.
    pub min_per_left: usize,
    /// Maximum edges per left row.
    pub max_per_left: usize,
    /// Distribution used to choose right rows.
    pub right_distribution: RefDistribution,
    /// Additional cell generators.
    pub cells: Vec<CellGen>,
}

impl EdgeSet {
    /// Construct an edge set.
    pub fn new(
        name: impl Into<String>,
        table: impl Into<String>,
        left_set: impl Into<String>,
        left_column: impl Into<String>,
        right_set: impl Into<String>,
        right_column: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            table: table.into(),
            left_set: left_set.into(),
            left_column: left_column.into(),
            right_set: right_set.into(),
            right_column: right_column.into(),
            min_per_left: 1,
            max_per_left: 1,
            right_distribution: RefDistribution::Uniform,
            cells: Vec::new(),
        }
    }

    /// Set inclusive edges-per-left bounds.
    pub fn per_left(mut self, min: usize, max: usize) -> Self {
        assert!(min <= max, "invalid edge count bounds");
        self.min_per_left = min;
        self.max_per_left = max;
        self
    }

    /// Set the right-side reference distribution.
    pub fn right_distribution(mut self, distribution: RefDistribution) -> Self {
        self.right_distribution = distribution;
        self
    }
}

/// One named cell generator.
#[derive(Clone, Debug)]
pub struct CellGen {
    /// Target column.
    pub column: String,
    /// Value generator.
    pub value_gen: CellValueGen,
}

impl CellGen {
    fn value(&self, fixture: &Fixture, rng: &mut Lcg, row_idx: usize) -> Value {
        self.value_gen.value(fixture, rng, row_idx)
    }
}

/// Cell generator variants.
#[derive(Clone, Debug)]
pub enum CellValueGen {
    /// Deterministic string from a prefix and row index.
    StringPool { prefix: String, pool: usize },
    /// Uniform integer range.
    U64Range { start: u64, end: u64 },
    /// Enum discriminant from a weighted choice.
    EnumWeighted { weights: Vec<u64> },
    /// Reference to a generated row id from another set.
    UuidRef {
        set: String,
        distribution: RefDistribution,
    },
    /// Constant typed value.
    Constant(Value),
}

impl CellValueGen {
    fn value(&self, fixture: &Fixture, rng: &mut Lcg, row_idx: usize) -> Value {
        match self {
            Self::StringPool { prefix, pool } => {
                Value::String(format!("{prefix}-{}", row_idx % (*pool).max(1)))
            }
            Self::U64Range { start, end } => {
                assert!(start <= end, "invalid u64 range");
                Value::U64(*start + (rng.next_u64() % (*end - *start + 1)))
            }
            Self::EnumWeighted { weights } => Value::EnumTag(rng.weighted_index(weights) as u8),
            Self::UuidRef { set, distribution } => {
                let rows = fixture
                    .rows_by_set
                    .get(set)
                    .unwrap_or_else(|| panic!("unknown referenced set {set}"));
                let zipf = matches!(distribution, RefDistribution::Zipf { .. })
                    .then(|| distribution.zipf(rows.len()));
                Value::Uuid(rows[distribution.sample(rng, rows.len(), zipf.as_ref())].0)
            }
            Self::Constant(value) => value.clone(),
        }
    }
}

/// Reference-selection distribution.
#[derive(Clone, Debug)]
pub enum RefDistribution {
    /// Uniform selection.
    Uniform,
    /// Zipf-ranked selection.
    Zipf { s: f64 },
}

impl RefDistribution {
    fn zipf(&self, n: usize) -> Zipf {
        match self {
            Self::Uniform => Zipf::new(n, 1.0),
            Self::Zipf { s } => Zipf::new(n, *s),
        }
    }

    fn sample(&self, rng: &mut Lcg, n: usize, zipf: Option<&Zipf>) -> usize {
        match self {
            Self::Uniform => rng.usize(n),
            Self::Zipf { .. } => zipf.expect("zipf sampler").sample(rng),
        }
    }
}

/// One generated ongoing edit.
#[derive(Clone, Debug, PartialEq)]
pub struct WriteStep {
    /// Simulated time in milliseconds.
    pub at_ms: u64,
    /// Target row.
    pub row_uuid: RowUuid,
    /// Acting author.
    pub author: AuthorSubject,
}

/// Rate-driven ongoing edit stream.
#[derive(Clone, Debug)]
pub struct WriteStream {
    steps: Vec<WriteStep>,
}

impl WriteStream {
    /// Build a deterministic write stream.
    pub fn new(
        seed: u64,
        edits_per_sec: u64,
        steps: usize,
        rows: &[RowUuid],
        authors: &[AuthorSubject],
        row_zipf_s: f64,
        author_zipf_s: f64,
    ) -> Self {
        assert!(!rows.is_empty(), "write stream rows must be non-empty");
        assert!(
            !authors.is_empty(),
            "write stream authors must be non-empty"
        );
        assert!(
            edits_per_sec > 0,
            "write stream edits per second must be positive"
        );
        let mut rng = Lcg::new(seed);
        let row_zipf = Zipf::new(rows.len(), row_zipf_s);
        let author_zipf = Zipf::new(authors.len(), author_zipf_s);
        let steps = (0..steps)
            .map(|idx| WriteStep {
                at_ms: u64::try_from(
                    (u128::try_from(idx).expect("write stream step index must fit in u128")
                        * 1_000)
                        / u128::from(edits_per_sec),
                )
                .expect("write stream timestamp must fit in u64"),
                row_uuid: rows[row_zipf.sample(&mut rng)],
                author: authors[author_zipf.sample(&mut rng)],
            })
            .collect();
        Self { steps }
    }

    /// Generated steps.
    pub fn steps(&self) -> &[WriteStep] {
        &self.steps
    }
}

fn deterministic_row_uuid(seed: u64, set: &str, idx: usize) -> RowUuid {
    let mut hash = seed ^ 0xa076_1d64_78bd_642f;
    mix_str(&mut hash, set);
    hash ^= idx as u64;
    let mut bytes = [0_u8; 16];
    bytes[0..8].copy_from_slice(&hash.to_be_bytes());
    bytes[8..16].copy_from_slice(&idx.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn mix_str(hash: &mut u64, value: &str) {
    mix_bytes(hash, value.as_bytes());
}

fn mix_bytes(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_builder_is_reproducible() {
        let builder = FixtureBuilder::new()
            .entity_set(EntitySet::new("users", "users", 10).cell(
                "name",
                CellValueGen::StringPool {
                    prefix: "user".to_owned(),
                    pool: 5,
                },
            ))
            .entity_set(EntitySet::new("issues", "issues", 25).cell(
                "assignee",
                CellValueGen::UuidRef {
                    set: "users".to_owned(),
                    distribution: RefDistribution::Zipf { s: 1.2 },
                },
            ));
        assert_eq!(
            builder.build(3).stable_hash(),
            builder.build(3).stable_hash()
        );
        assert_ne!(
            builder.build(3).stable_hash(),
            builder.build(4).stable_hash()
        );
    }

    #[test]
    fn write_stream_repeats_for_same_seed() {
        let rows = (0..8)
            .map(|idx| deterministic_row_uuid(1, "rows", idx))
            .collect::<Vec<_>>();
        let authors = rows
            .iter()
            .map(|row| {
                AuthorSubject::authenticated("urn:jazz:sim", &row.0.to_string())
                    .expect("simulation issuer is external")
            })
            .collect::<Vec<_>>();
        let a = WriteStream::new(7, 10, 16, &rows, &authors, 1.1, 1.1);
        let b = WriteStream::new(7, 10, 16, &rows, &authors, 1.1, 1.1);
        assert_eq!(a.steps(), b.steps());
    }

    #[test]
    fn write_stream_uses_fractional_millisecond_cadence_at_600_per_second() {
        let rows = vec![deterministic_row_uuid(1, "rows", 0)];
        let authors = vec![
            AuthorSubject::authenticated("urn:jazz:sim", &rows[0].0.to_string())
                .expect("simulation issuer is external"),
        ];
        let stream = WriteStream::new(7, 600, 6, &rows, &authors, 1.1, 1.1);

        assert_eq!(
            stream
                .steps()
                .iter()
                .map(|step| step.at_ms)
                .collect::<Vec<_>>(),
            vec![0, 1, 3, 5, 6, 8]
        );
    }

    #[test]
    fn write_stream_repeats_milliseconds_at_2000_per_second() {
        let rows = vec![deterministic_row_uuid(1, "rows", 0)];
        let authors = vec![
            AuthorSubject::authenticated("urn:jazz:sim", &rows[0].0.to_string())
                .expect("simulation issuer is external"),
        ];
        let stream = WriteStream::new(7, 2_000, 8, &rows, &authors, 1.1, 1.1);

        assert_eq!(
            stream
                .steps()
                .iter()
                .map(|step| step.at_ms)
                .collect::<Vec<_>>(),
            vec![0, 0, 1, 1, 2, 2, 3, 3]
        );
    }

    #[test]
    #[should_panic(expected = "write stream edits per second must be positive")]
    fn write_stream_rejects_zero_rate() {
        let rows = vec![deterministic_row_uuid(1, "rows", 0)];
        let authors = vec![
            AuthorSubject::authenticated("urn:jazz:sim", &rows[0].0.to_string())
                .expect("simulation issuer is external"),
        ];

        let _ = WriteStream::new(7, 0, 1, &rows, &authors, 1.1, 1.1);
    }
}
