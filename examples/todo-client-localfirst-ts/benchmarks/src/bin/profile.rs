#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;
fn main() {
    jazz_example_todo_benchmark::profile_main();
}
