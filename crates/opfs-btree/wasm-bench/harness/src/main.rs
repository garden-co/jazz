use gloo_worker::{Spawnable, WorkerBridge};
use opfs_btree_bench_harness::btree_worker::BtreeWorker;
use opfs_btree_bench_harness::sqlite_worker::SqliteWorker;
use opfs_btree_bench_harness::types::{RunProfile, WorkerSmokeResult};
use yew::prelude::*;

enum Msg {
    RunSmoke,
    BtreeDone(WorkerSmokeResult),
    SqliteDone(WorkerSmokeResult),
}

struct App {
    btree: WorkerBridge<BtreeWorker>,
    sqlite: WorkerBridge<SqliteWorker>,
    rows: Vec<WorkerSmokeResult>,
}

impl Component for App {
    type Message = Msg;
    type Properties = ();

    fn create(ctx: &Context<Self>) -> Self {
        let link = ctx.link().clone();
        let mut btree_spawner = BtreeWorker::spawner();
        btree_spawner.callback(move |result| link.send_message(Msg::BtreeDone(result)));
        let btree = btree_spawner.spawn("/btree-worker.js");
        let link = ctx.link().clone();
        let mut sqlite_spawner = SqliteWorker::spawner();
        sqlite_spawner.callback(move |result| link.send_message(Msg::SqliteDone(result)));
        let sqlite = sqlite_spawner.spawn("/sqlite-worker.js");
        Self {
            btree,
            sqlite,
            rows: Vec::new(),
        }
    }

    fn update(&mut self, _ctx: &Context<Self>, msg: Self::Message) -> bool {
        match msg {
            Msg::RunSmoke => {
                let request = RunProfile {
                    profile: "objects".to_string(),
                };
                self.btree.send(request.clone());
                self.sqlite.send(request);
                false
            }
            Msg::BtreeDone(result) | Msg::SqliteDone(result) => {
                self.rows.push(result);
                true
            }
        }
    }

    fn view(&self, ctx: &Context<Self>) -> Html {
        html! {
            <>
                <h1>{"opfs-btree storage benchmark"}</h1>
                <button type="button" onclick={ctx.link().callback(|_| Msg::RunSmoke)}>{"Run"}</button>
                <ul>
                    { for self.rows.iter().map(|row| html! {
                        <li>{format!("{} {}", row.engine, row.profile)}</li>
                    }) }
                </ul>
            </>
        }
    }
}

fn main() {
    yew::Renderer::<App>::with_root(
        web_sys::window()
            .and_then(|window| window.document())
            .and_then(|document| document.get_element_by_id("app"))
            .expect("missing #app root"),
    )
    .render();
}
