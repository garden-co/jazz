use opfs_btree_bench_harness::app::App;

fn main() {
    yew::Renderer::<App>::with_root(
        web_sys::window()
            .and_then(|window| window.document())
            .and_then(|document| document.get_element_by_id("app"))
            .expect("missing #app root"),
    )
    .render();
}
