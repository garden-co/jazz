//! Projection benchmark for project_row.
//!
//! Measures the performance of projecting rows with different column configurations.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use jazz::query_manager::encoding::{encode_row, project_row};
use jazz::query_manager::types::{ColumnDescriptor, ColumnType, RowDescriptor, Value};

/// Mixed columns: 3 fixed + 2 variable
fn project_mixed_5cols(c: &mut Criterion) {
    let mut group = c.benchmark_group("project_row/mixed_5cols");

    let src_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("active", ColumnType::Boolean),
        ColumnDescriptor::new("bio", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::BigInt),
    ]);

    let dst_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("name_out", ColumnType::Text),
        ColumnDescriptor::new("score_out", ColumnType::BigInt),
        ColumnDescriptor::new("id_out", ColumnType::Integer),
    ]);

    let src_values = vec![
        Value::Integer(42),
        Value::Text("Alice Johnson".into()),
        Value::Boolean(true),
        Value::Text("Software engineer from San Francisco".into()),
        Value::BigInt(98765),
    ];

    let src_encoded = encode_row(&src_desc, &src_values).unwrap();
    let mapping = [(1, 0), (4, 1), (0, 2)]; // name->0, score->1, id->2

    group.throughput(Throughput::Elements(1));
    group.bench_function("project", |b| {
        b.iter(|| project_row(&src_desc, &src_encoded, &dst_desc, &mapping).unwrap());
    });

    group.finish();
}

/// All fixed columns (5 fixed)
fn project_all_fixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("project_row/all_fixed");

    let src_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("a", ColumnType::Integer),
        ColumnDescriptor::new("b", ColumnType::BigInt),
        ColumnDescriptor::new("c", ColumnType::Boolean),
        ColumnDescriptor::new("d", ColumnType::Timestamp),
        ColumnDescriptor::new("e", ColumnType::Integer),
    ]);

    let dst_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("x", ColumnType::BigInt),
        ColumnDescriptor::new("y", ColumnType::Integer),
    ]);

    let src_values = vec![
        Value::Integer(42),
        Value::BigInt(123456789),
        Value::Boolean(true),
        Value::Timestamp(9999999),
        Value::Integer(100),
    ];

    let src_encoded = encode_row(&src_desc, &src_values).unwrap();
    let mapping = [(1, 0), (4, 1)]; // bigint->0, int->1

    group.throughput(Throughput::Elements(1));
    group.bench_function("project", |b| {
        b.iter(|| project_row(&src_desc, &src_encoded, &dst_desc, &mapping).unwrap());
    });

    group.finish();
}

/// Large text column (10KB)
fn project_large_text(c: &mut Criterion) {
    let mut group = c.benchmark_group("project_row/large_text");

    let large_text = "x".repeat(10_000);

    let src_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("content", ColumnType::Text),
        ColumnDescriptor::new("meta", ColumnType::Text),
    ]);

    let dst_desc = RowDescriptor::new(vec![ColumnDescriptor::new("content_out", ColumnType::Text)]);

    let src_values = vec![
        Value::Integer(1),
        Value::Text(large_text),
        Value::Text("small metadata".into()),
    ];

    let src_encoded = encode_row(&src_desc, &src_values).unwrap();
    let mapping = [(1, 0)]; // content->0

    group.throughput(Throughput::Bytes(10_000));
    group.bench_function("project", |b| {
        b.iter(|| project_row(&src_desc, &src_encoded, &dst_desc, &mapping).unwrap());
    });

    group.finish();
}

/// Multiple variable columns reordered
fn project_variable_reorder(c: &mut Criterion) {
    let mut group = c.benchmark_group("project_row/variable_reorder");

    let src_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("first", ColumnType::Text),
        ColumnDescriptor::new("second", ColumnType::Text),
        ColumnDescriptor::new("third", ColumnType::Text),
        ColumnDescriptor::new("fourth", ColumnType::Text),
    ]);

    let dst_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("was_fourth", ColumnType::Text),
        ColumnDescriptor::new("was_second", ColumnType::Text),
        ColumnDescriptor::new("was_first", ColumnType::Text),
    ]);

    let src_values = vec![
        Value::Text("First value here".into()),
        Value::Text("Second value here".into()),
        Value::Text("Third value here".into()),
        Value::Text("Fourth value here".into()),
    ];

    let src_encoded = encode_row(&src_desc, &src_values).unwrap();
    let mapping = [(3, 0), (1, 1), (0, 2)]; // fourth->0, second->1, first->2

    group.throughput(Throughput::Elements(1));
    group.bench_function("project", |b| {
        b.iter(|| project_row(&src_desc, &src_encoded, &dst_desc, &mapping).unwrap());
    });

    group.finish();
}

/// Nullable columns with null values
fn project_nullable(c: &mut Criterion) {
    let mut group = c.benchmark_group("project_row/nullable");

    let src_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("name", ColumnType::Text).nullable(),
        ColumnDescriptor::new("score", ColumnType::Integer).nullable(),
        ColumnDescriptor::new("bio", ColumnType::Text).nullable(),
    ]);

    let dst_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("name_out", ColumnType::Text).nullable(),
        ColumnDescriptor::new("id_out", ColumnType::Integer),
        ColumnDescriptor::new("bio_out", ColumnType::Text).nullable(),
    ]);

    let src_values = vec![
        Value::Integer(42),
        Value::Null,
        Value::Integer(100),
        Value::Text("Some bio text here".into()),
    ];

    let src_encoded = encode_row(&src_desc, &src_values).unwrap();
    let mapping = [(1, 0), (0, 1), (3, 2)]; // name->0, id->1, bio->2

    group.throughput(Throughput::Elements(1));
    group.bench_function("project", |b| {
        b.iter(|| project_row(&src_desc, &src_encoded, &dst_desc, &mapping).unwrap());
    });

    group.finish();
}

criterion_group!(
    benches,
    project_mixed_5cols,
    project_all_fixed,
    project_large_text,
    project_variable_reorder,
    project_nullable,
);
criterion_main!(benches);
