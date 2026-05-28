#[cfg(target_arch = "wasm32")]
mod todo_runtime;

#[cfg(target_arch = "wasm32")]
mod app {
    use super::todo_runtime::{Todo, TodoRuntime, TodoState};
    use web_sys::{Event, HtmlInputElement, SubmitEvent};
    use yew::prelude::*;

    #[function_component(App)]
    fn app() -> Html {
        let state = use_state(TodoState::default);
        let runtime = use_mut_ref(|| None::<TodoRuntime>);
        let title_ref = use_node_ref();

        {
            let state = state.clone();
            let runtime = runtime.clone();
            use_effect_with((), move |_| {
                let on_state = Callback::from({
                    let state = state.clone();
                    move |next| state.set(next)
                });
                match TodoRuntime::open(on_state) {
                    Ok(opened) => *runtime.borrow_mut() = Some(opened),
                    Err(error) => state.set(TodoState::with_error(error)),
                }

                move || {
                    *runtime.borrow_mut() = None;
                }
            });
        }

        let disabled = controls_locked(&state);
        let onsubmit = {
            let state = state.clone();
            let runtime = runtime.clone();
            let title_ref = title_ref.clone();
            Callback::from(move |event: SubmitEvent| {
                event.prevent_default();
                if controls_locked(&state) {
                    return;
                }
                let Some(input) = title_ref.cast::<HtmlInputElement>() else {
                    return;
                };
                let title = input.value().trim().to_owned();
                if title.is_empty() {
                    return;
                }
                if let Some(runtime) = runtime.borrow().as_ref() {
                    runtime.add(title);
                    input.set_value("");
                }
            })
        };
        let on_generate = {
            let state = state.clone();
            let runtime = runtime.clone();
            Callback::from(move |_| {
                if controls_locked(&state) {
                    return;
                }
                if let Some(runtime) = runtime.borrow().as_ref() {
                    runtime.generate_100k();
                }
            })
        };

        html! {
            <section class="todo-app">
                <header class="app-header">
                    <div>
                        <p class="eyebrow">{ "mini-jazz-sqlite Yew" }</p>
                        <h1>{ "Todos" }</h1>
                    </div>
                    <p class="status" role="status">{ status_text(&state) }</p>
                </header>
                <form class="todo-form" {onsubmit}>
                    <input ref={title_ref} type="text" autocomplete="off" placeholder="Add a task" required=true disabled={disabled} />
                    <button type="submit" disabled={disabled}>{ "Add" }</button>
                </form>
                <button class="generate" type="button" onclick={on_generate} disabled={disabled}>{ "Generate 100k todos" }</button>
                if !state.error.is_empty() {
                    <p class="error-message" role="alert">{ &state.error }</p>
                }
                <ul class="todo-list">
                    { for state.todos.iter().map(|todo| todo_item(todo, disabled, &runtime)) }
                </ul>
                if state.todos.is_empty() {
                    <p class="empty-state">{ "No todos in the synced page." }</p>
                }
                <p class="summary">{ summary_text(&state) }</p>
            </section>
        }
    }

    fn todo_item(
        todo: &Todo,
        disabled: bool,
        runtime: &std::rc::Rc<std::cell::RefCell<Option<TodoRuntime>>>,
    ) -> Html {
        let id = todo.id.clone();
        let checked = todo.done;
        let runtime_for_toggle = runtime.clone();
        let onchange = Callback::from(move |event: Event| {
            if let Some(runtime) = runtime_for_toggle.borrow().as_ref() {
                let input: HtmlInputElement = event.target_unchecked_into();
                runtime.toggle(id.clone(), input.checked());
            }
        });

        let id = todo.id.clone();
        let runtime_for_delete = runtime.clone();
        let onclick = Callback::from(move |_| {
            if let Some(runtime) = runtime_for_delete.borrow().as_ref() {
                runtime.delete(id.clone());
            }
        });

        html! {
            <li class={classes!("todo-item", todo.done.then_some("done"))}>
                <label class="todo-label">
                    <input type="checkbox" data-role="toggle" checked={checked} disabled={disabled} {onchange} />
                    <span>{ &todo.title }</span>
                </label>
                <button type="button" disabled={disabled} {onclick}>{ "Delete" }</button>
            </li>
        }
    }

    fn controls_locked(state: &TodoState) -> bool {
        !state.ready || state.generating || state.syncing
    }

    fn status_text(state: &TodoState) -> String {
        if !state.error.is_empty() {
            "Error".to_owned()
        } else if state.generating {
            format!(
                "Generating {} / {} in main memory...",
                format_count(state.generated),
                format_count(state.total_to_generate)
            )
        } else if state.syncing {
            "Syncing main memory to OPFS worker...".to_owned()
        } else if state.ready {
            "Main memory runtime synced with OPFS worker".to_owned()
        } else {
            "Opening runtimes...".to_owned()
        }
    }

    fn summary_text(state: &TodoState) -> String {
        let mut parts = vec![
            format!("{} OPFS current rows", format_count(state.current_rows)),
            format!("main query {:.2} ms", state.main_query_ms),
            format!("export {:.2} ms", state.export_ms),
            format!("OPFS apply {:.2} ms", state.worker_apply_ms),
            format!("OPFS query {:.2} ms", state.worker_query_ms),
            format!("round trip {:.2} ms", state.worker_round_trip_ms),
        ];
        if state.generate_ms > 0.0 {
            parts.push(format!("generate {:.2} s", state.generate_ms / 1000.0));
        }
        format!("{}.", parts.join(". "))
    }

    fn format_count(value: u64) -> String {
        let text = value.to_string();
        let mut out = String::new();
        for (index, ch) in text.chars().rev().enumerate() {
            if index > 0 && index % 3 == 0 {
                out.push(',');
            }
            out.push(ch);
        }
        out.chars().rev().collect()
    }

    pub fn run() {
        console_error_panic_hook::set_once();
        yew::Renderer::<App>::new().render();
    }
}

#[cfg(target_arch = "wasm32")]
fn main() {
    app::run();
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {}
