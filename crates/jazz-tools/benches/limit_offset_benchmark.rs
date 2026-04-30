//! Benchmarks for paginated live-query settling.
//!
//! The growing-prefix case mirrors a query subscription receiving many existing
//! rows incrementally: each settle sees a longer ordered input and recomputes the
//! current page. This used to amplify provenance into every window tuple, making
//! repeated settles get progressively more expensive.

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use jazz_tools::metadata::RowProvenance;
use jazz_tools::object::{BranchName, ObjectId};
use jazz_tools::query_manager::encoding::encode_row;
use jazz_tools::query_manager::graph_nodes::LimitOffsetNode;
use jazz_tools::query_manager::types::{
    ColumnDescriptor, ColumnType, RowDescriptor, Tuple, TupleDescriptor, TupleElement, Value,
};
use jazz_tools::row_histories::BatchId;

fn descriptor() -> RowDescriptor {
    RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("name", ColumnType::Text),
    ])
}

fn make_tuple(id: ObjectId, index: usize, descriptor: &RowDescriptor) -> Tuple {
    let content = encode_row(
        descriptor,
        &[
            Value::Integer(index as i32),
            Value::Text(format!("Organization {}", index)),
        ],
    )
    .expect("row should encode");

    Tuple::new(vec![TupleElement::Row {
        id,
        content: content.into(),
        batch_id: BatchId([index as u8; 16]),
        row_provenance: RowProvenance::for_insert("jazz:bench", index as u64),
    }])
    .with_provenance([(id, BranchName::new("main"))].into_iter().collect())
}

fn make_tuples(count: usize) -> Vec<Tuple> {
    let descriptor = descriptor();
    (0..count)
        .map(|index| make_tuple(ObjectId::new(), index, &descriptor))
        .collect()
}

fn settle_growing_prefix(c: &mut Criterion) {
    let mut group = c.benchmark_group("limit_offset/growing_prefix");

    for row_count in [1_800usize] {
        group.throughput(Throughput::Elements(row_count as u64));
        group.bench_with_input(
            BenchmarkId::new("limit_50_offset_0", row_count),
            &row_count,
            |b, &row_count| {
                let tuples = make_tuples(row_count);
                let tuple_descriptor =
                    TupleDescriptor::single_with_materialization("", descriptor(), true);

                b.iter(|| {
                    let mut node = LimitOffsetNode::with_tuple_descriptor(
                        tuple_descriptor.clone(),
                        Some(50),
                        0,
                    );

                    for len in 1..=row_count {
                        black_box(node.process_with_ordered_input(black_box(&tuples[..len])));
                    }

                    black_box(node.windowed_tuples().len());
                });
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = settle_growing_prefix
}
criterion_main!(benches);
