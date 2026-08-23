fn main() {
    divan::main();
}

#[divan::bench(args = [(4, 128), (16, 512)])]
fn indexed_fixture_count((tenant_count, records_per_tenant): (usize, usize)) -> usize {
    jazz_example_benchmark_smoke::indexed_fixture_count(
        divan::black_box(tenant_count),
        divan::black_box(records_per_tenant),
    )
}
