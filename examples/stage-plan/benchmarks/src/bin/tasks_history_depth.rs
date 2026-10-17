#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    jazz_example_stage_plan_benchmark::tasks::history_depth::main();
}
