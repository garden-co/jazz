// Keep the allocator at the executable boundary: the scenario is also
// included in a combined correctness-test executable with its own allocator.
#[path = "local_batch_phases.rs"]
mod scenario;

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    scenario::main();
}
