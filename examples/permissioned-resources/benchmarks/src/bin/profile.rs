use jazz_example_permissioned_resources_benchmark::SelectedAllocator;
#[global_allocator]
static ALLOCATOR: SelectedAllocator = SelectedAllocator;
fn main() {
    jazz_example_permissioned_resources_benchmark::profile_main();
}
