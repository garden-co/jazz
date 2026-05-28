use gloo_worker::Registrable;
use mini_sqlite_todo_yew::browser_worker::BrowserRuntimeWorker;
use mini_sqlite_todo_yew::worker_codec::JsonCodec;

fn main() {
    console_error_panic_hook::set_once();
    BrowserRuntimeWorker::registrar()
        .encoding::<JsonCodec>()
        .register();
}
