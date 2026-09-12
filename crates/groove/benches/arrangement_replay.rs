#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn main() {
    println!("allocator={}", jazz_benchmark_guard::ALLOCATOR_NAME);
    groove::replay_captured_arrangements();
}
