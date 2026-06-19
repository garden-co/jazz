use std::collections::{BTreeMap, VecDeque};

use gloo_worker::{Spawnable, WorkerBridge};
use wasm_bindgen::JsValue;
use yew::prelude::*;

use crate::btree_worker::BtreeWorker;
use crate::sqlite_worker::SqliteWorker;
use crate::types::{
    AutomationResult, BenchmarkFailure, EngineRunResult, ProfileComparison, RunProfile,
    WorkerResult,
};

pub enum Msg {
    Run,
    BtreeDone(WorkerResult),
    SqliteDone(WorkerResult),
}

#[derive(Default)]
struct PendingProfile {
    btree: Option<EngineRunResult>,
    sqlite: Option<EngineRunResult>,
}

pub struct App {
    btree: WorkerBridge<BtreeWorker>,
    sqlite: WorkerBridge<SqliteWorker>,
    configured_profiles: Vec<String>,
    profiles: VecDeque<String>,
    pending: BTreeMap<String, PendingProfile>,
    results: Vec<ProfileComparison>,
    error: Option<BenchmarkFailure>,
    running: bool,
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

        let configured_profiles = profiles_from_query();
        let app = Self {
            btree,
            sqlite,
            profiles: VecDeque::from(configured_profiles.clone()),
            configured_profiles,
            pending: BTreeMap::new(),
            results: Vec::new(),
            error: None,
            running: false,
        };
        if autorun_from_query() {
            ctx.link().send_message(Msg::Run);
        }
        app
    }

    fn update(&mut self, _ctx: &Context<Self>, msg: Self::Message) -> bool {
        match msg {
            Msg::Run => {
                self.results.clear();
                self.pending.clear();
                self.error = None;
                self.profiles = VecDeque::from(self.configured_profiles.clone());
                self.running = true;
                self.dispatch_next_profile();
                true
            }
            Msg::BtreeDone(result) => {
                self.record_worker_result("opfs_btree", result);
                true
            }
            Msg::SqliteDone(result) => {
                self.record_worker_result("sqlite_inproc", result);
                true
            }
        }
    }

    fn view(&self, ctx: &Context<Self>) -> Html {
        html! {
            <main>
                <h1>{"opfs-btree vs SQLite"}</h1>
                <button type="button" disabled={self.running} onclick={ctx.link().callback(|_| Msg::Run)}>
                    {"Run"}
                </button>
                { self.view_status() }
                { self.view_results() }
            </main>
        }
    }
}

impl App {
    fn dispatch_next_profile(&mut self) {
        if self.error.is_some() {
            self.finish();
            return;
        }

        let Some(profile) = self.profiles.pop_front() else {
            self.finish();
            return;
        };

        self.pending
            .insert(profile.clone(), PendingProfile::default());
        let request = RunProfile { profile };
        self.btree.send(request.clone());
        self.sqlite.send(request);
    }

    fn record_worker_result(&mut self, expected_engine: &str, result: WorkerResult) {
        if self.error.is_some() || !self.running {
            return;
        }

        let result = match result {
            WorkerResult::Ok(result) => result,
            WorkerResult::Err(error) => {
                self.error = Some(BenchmarkFailure {
                    profile: Some(error.profile),
                    error: format!("{} worker failed: {}", error.engine, error.error),
                });
                self.finish();
                return;
            }
        };

        if result.engine != expected_engine {
            self.error = Some(BenchmarkFailure {
                profile: Some(result.profile),
                error: format!("expected {expected_engine}, got {}", result.engine),
            });
            self.finish();
            return;
        }

        let profile = result.profile.clone();
        let pending = self.pending.entry(profile.clone()).or_default();
        match expected_engine {
            "opfs_btree" => pending.btree = Some(result),
            "sqlite_inproc" => pending.sqlite = Some(result),
            other => {
                self.error = Some(BenchmarkFailure {
                    profile: Some(profile),
                    error: format!("unknown engine {other}"),
                });
                self.finish();
                return;
            }
        }

        if let Some(comparison) = self.take_complete_profile(&profile) {
            if comparison.btree.checksum != comparison.sqlite.checksum {
                self.error = Some(BenchmarkFailure {
                    profile: Some(profile),
                    error: format!(
                        "checksum mismatch: opfs-btree={} sqlite={}",
                        comparison.btree.checksum, comparison.sqlite.checksum
                    ),
                });
                self.finish();
                return;
            }
            self.results.push(comparison);
            self.dispatch_next_profile();
        }
    }

    fn take_complete_profile(&mut self, profile: &str) -> Option<ProfileComparison> {
        let pending = self.pending.get(profile)?;
        let btree = pending.btree.clone()?;
        let sqlite = pending.sqlite.clone()?;
        self.pending.remove(profile);
        Some(ProfileComparison {
            profile: profile.to_string(),
            btree,
            sqlite,
        })
    }

    fn finish(&mut self) {
        self.running = false;
        export_automation_result(AutomationResult {
            ok: self.error.is_none(),
            results: self.results.clone(),
            error: self.error.clone(),
        });
    }

    fn view_status(&self) -> Html {
        let text = if let Some(error) = &self.error {
            format!("Failed: {}", error.error)
        } else if self.running {
            "Running".to_string()
        } else {
            "Idle".to_string()
        };
        html! { <p>{text}</p> }
    }

    fn view_results(&self) -> Html {
        html! {
            <table>
                <thead>
                    <tr>
                        <th>{"Profile"}</th>
                        <th>{"Phase"}</th>
                        <th>{"opfs-btree ms"}</th>
                        <th>{"SQLite ms"}</th>
                        <th>{"opfs-btree ops/s"}</th>
                        <th>{"SQLite ops/s"}</th>
                    </tr>
                </thead>
                <tbody>
                    { for self.results.iter().flat_map(result_rows) }
                </tbody>
            </table>
        }
    }
}

fn result_rows(comparison: &ProfileComparison) -> Vec<Html> {
    comparison
        .btree
        .phases
        .iter()
        .map(|btree_phase| {
            let sqlite_phase = comparison
                .sqlite
                .phases
                .iter()
                .find(|phase| phase.phase == btree_phase.phase);
            html! {
                <tr>
                    <td>{comparison.profile.clone()}</td>
                    <td>{btree_phase.phase.clone()}</td>
                    <td>{format!("{:.2}", btree_phase.elapsed_ms)}</td>
                    <td>{sqlite_phase.map(|p| format!("{:.2}", p.elapsed_ms)).unwrap_or_default()}</td>
                    <td>{format!("{:.0}", btree_phase.ops_per_sec)}</td>
                    <td>{sqlite_phase.map(|p| format!("{:.0}", p.ops_per_sec)).unwrap_or_default()}</td>
                </tr>
            }
        })
        .collect()
}

fn profiles_from_query() -> Vec<String> {
    let Some(window) = web_sys::window() else {
        return default_profiles();
    };
    let search = window.location().search().unwrap_or_default();
    let params = web_sys::UrlSearchParams::new_with_str(&search).ok();
    params
        .and_then(|params| params.get("profiles"))
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|profile| !profile.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .filter(|profiles| !profiles.is_empty())
        .unwrap_or_else(default_profiles)
}

fn default_profiles() -> Vec<String> {
    vec!["objects".to_string(), "wikipedia".to_string()]
}

fn autorun_from_query() -> bool {
    let Some(window) = web_sys::window() else {
        return false;
    };
    let search = window.location().search().unwrap_or_default();
    web_sys::UrlSearchParams::new_with_str(&search)
        .ok()
        .and_then(|params| params.get("autorun"))
        .as_deref()
        == Some("1")
}

fn export_automation_result(result: AutomationResult) {
    let value = serde_wasm_bindgen::to_value(&result).unwrap_or(JsValue::NULL);
    if let Some(window) = web_sys::window() {
        let _ = js_sys::Reflect::set(&window, &JsValue::from_str("__benchDone"), &JsValue::TRUE);
        let _ = js_sys::Reflect::set(&window, &JsValue::from_str("__benchResult"), &value);
    }
}
