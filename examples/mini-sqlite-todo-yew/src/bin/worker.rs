#[cfg(target_arch = "wasm32")]
fn main() {
    use mini_sqlite_todo_yew::browser_worker::BrowserRuntimeWorker;
    use mini_sqlite_todo_yew::worker_bridge::register_worker;
    use std::{cell::RefCell, rc::Rc};

    console_error_panic_hook::set_once();
    let worker = Rc::new(RefCell::new(BrowserRuntimeWorker::new()));
    register_worker(move |input, responder| {
        BrowserRuntimeWorker::handle_shared(worker.clone(), input, responder);
    });
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {}
