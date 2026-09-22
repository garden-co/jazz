//! Small, intentionally self-contained fixture used to prove the example
//! benchmark runner. Product examples own their own schema, fixture, and
//! workload code; this crate is only a discovery smoke test.

pub fn indexed_fixture_count(tenant_count: usize, records_per_tenant: usize) -> usize {
    (0..tenant_count)
        .map(|tenant| {
            (0..records_per_tenant)
                .filter(|record| record % tenant_count == tenant)
                .count()
        })
        .sum()
}
