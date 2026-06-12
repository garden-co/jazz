#[cfg(target_arch = "wasm32")]
mod todo_runtime;

#[cfg(target_arch = "wasm32")]
mod app {
    use super::todo_runtime::{Todo, TodoRuntime, TodoState};
    use js_sys::Date;
    use mini_sqlite_todo_yew::todo_display::{self, TodoDisplayState};
    use mini_sqlite_todo_yew::todo_query::{TodoDoneFilter, TodoSortDirection, TodoSortField};
    use wasm_bindgen::JsValue;
    use web_sys::{Event, HtmlInputElement, HtmlSelectElement, InputEvent, SubmitEvent};
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
        let on_search = {
            let runtime = runtime.clone();
            Callback::from(move |event: InputEvent| {
                if let Some(runtime) = runtime.borrow().as_ref() {
                    let input: HtmlInputElement = event.target_unchecked_into();
                    runtime.set_title_search(input.value());
                }
            })
        };
        let on_done_filter = {
            let runtime = runtime.clone();
            Callback::from(move |event: Event| {
                if let Some(runtime) = runtime.borrow().as_ref() {
                    let select: HtmlSelectElement = event.target_unchecked_into();
                    runtime.set_done_filter(TodoDoneFilter::from_value(&select.value()));
                }
            })
        };
        let on_sort_field = {
            let runtime = runtime.clone();
            Callback::from(move |event: Event| {
                if let Some(runtime) = runtime.borrow().as_ref() {
                    let select: HtmlSelectElement = event.target_unchecked_into();
                    runtime.set_sort_field(TodoSortField::from_value(&select.value()));
                }
            })
        };
        let on_sort_direction = {
            let runtime = runtime.clone();
            Callback::from(move |event: Event| {
                if let Some(runtime) = runtime.borrow().as_ref() {
                    let select: HtmlSelectElement = event.target_unchecked_into();
                    runtime.set_sort_direction(TodoSortDirection::from_value(&select.value()));
                }
            })
        };
        let on_previous_page = {
            let runtime = runtime.clone();
            Callback::from(move |_| {
                if let Some(runtime) = runtime.borrow().as_ref() {
                    runtime.previous_page();
                }
            })
        };
        let on_next_page = {
            let runtime = runtime.clone();
            Callback::from(move |_| {
                if let Some(runtime) = runtime.borrow().as_ref() {
                    runtime.next_page();
                }
            })
        };
        let previous_disabled = disabled || state.query.page == 0;
        let next_disabled = disabled || !state.has_next_page;

        html! {
            <section class="todo-app">
                <header class="app-header">
                    <div>
                        <p class="eyebrow">{ "mini-jazz-sqlite Yew" }</p>
                        <h1>{ "Todos" }</h1>
                    </div>
                    <div class="header-actions">
                        <button class="generate" type="button" onclick={on_generate} disabled={disabled}>{ "Generate 100k todos" }</button>
                        <p class="status" role="status">{ status_text(&state) }</p>
                    </div>
                </header>
                <form class="todo-form" {onsubmit}>
                    <input ref={title_ref} type="text" autocomplete="off" placeholder="Add a task" required=true disabled={disabled} />
                    <button type="submit" disabled={disabled}>{ "Add" }</button>
                </form>
                <div class="filter-bar">
                    <input
                        class="search-input"
                        type="search"
                        autocomplete="off"
                        placeholder="Search title"
                        value={state.query.title_search.clone()}
                        oninput={on_search}
                        disabled={disabled}
                    />
                    <select class="done-filter" onchange={on_done_filter} disabled={disabled} aria-label="Done filter">
                        <option value="all" selected={state.query.done_filter == TodoDoneFilter::All}>{ "All" }</option>
                        <option value="open" selected={state.query.done_filter == TodoDoneFilter::Open}>{ "Open" }</option>
                        <option value="done" selected={state.query.done_filter == TodoDoneFilter::Done}>{ "Done" }</option>
                    </select>
                    <select class="sort-field" onchange={on_sort_field} disabled={disabled} aria-label="Sort field">
                        <option value="date" selected={state.query.sort_field == TodoSortField::Date}>{ "Date" }</option>
                        <option value="title" selected={state.query.sort_field == TodoSortField::Title}>{ "Title" }</option>
                    </select>
                    <select class="sort-direction" onchange={on_sort_direction} disabled={disabled} aria-label="Sort direction">
                        <option value="desc" selected={state.query.sort_direction == TodoSortDirection::Desc}>{ "Desc" }</option>
                        <option value="asc" selected={state.query.sort_direction == TodoSortDirection::Asc}>{ "Asc" }</option>
                    </select>
                </div>
                if !state.error.is_empty() {
                    <p class="error-message" role="alert">{ &state.error }</p>
                }
                <ul class="todo-list">
                    { for state.todos.iter().map(|todo| todo_item(todo, disabled, &runtime)) }
                </ul>
                if state.todos.is_empty() {
                    <p class="empty-state">{ "No todos in the synced page." }</p>
                }
                <nav class="pagination" aria-label="Todo pages">
                    <button type="button" onclick={on_previous_page} disabled={previous_disabled}>{ "Previous" }</button>
                    <span>{ format!("Page {}", state.query.page + 1) }</span>
                    <button type="button" onclick={on_next_page} disabled={next_disabled}>{ "Next" }</button>
                </nav>
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
                    <span class="todo-text">
                        <span class="todo-title">{ &todo.title }</span>
                        <time class="todo-created">{ format_created_at(todo.created_at) }</time>
                    </span>
                </label>
                <button type="button" disabled={disabled} {onclick}>{ "Delete" }</button>
            </li>
        }
    }

    fn format_created_at(created_at: i64) -> String {
        if created_at <= 0 {
            return "Created --".to_owned();
        }
        let date = Date::new(&JsValue::from_f64(created_at as f64));
        format!(
            "{:04}-{:02}-{:02} {:02}:{:02}",
            date.get_full_year(),
            date.get_month() + 1,
            date.get_date(),
            date.get_hours(),
            date.get_minutes()
        )
    }

    fn controls_locked(state: &TodoState) -> bool {
        todo_display::controls_locked(&display_state(state))
    }

    fn status_text(state: &TodoState) -> String {
        todo_display::status_text(&display_state(state))
    }

    fn display_state(state: &TodoState) -> TodoDisplayState {
        TodoDisplayState {
            ready: state.ready,
            generating: state.generating,
            syncing: state.syncing,
            error: state.error.clone(),
            generated: state.generated,
            total_to_generate: state.total_to_generate,
        }
    }

    fn summary_text(state: &TodoState) -> String {
        let parts = vec![
            format!("{} OPFS current rows", format_count(state.current_rows)),
            format!("page {}", state.query.page + 1),
        ];
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
