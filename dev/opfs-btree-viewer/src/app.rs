use std::collections::HashMap;

use gloo_timers::future::TimeoutFuture;
use js_sys::Uint8Array;
use opfs_btree::{BTreeOptions, MemoryFile, OpfsBTree, RawPageSummary};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::{JsFuture, spawn_local};
use web_sys::{DragEvent, Event, File, HtmlElement, HtmlInputElement, InputEvent};
use yew::html::Scope;
use yew::prelude::*;

use crate::bundle::{StorageBundle, StorageBundleFile, decode_storage_bundle};
use crate::format::{PreviewMode, bytes_to_hex, format_bytes, format_value, page_kind_label};

const ENTRY_SCAN_BATCH_SIZE: usize = 250;
const PAGE_SCAN_BATCH_SIZE: usize = 250;
const ENTRY_PAGE_SIZE: usize = 100;
const PHYSICAL_PAGE_SIZE: usize = 100;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadedBundle {
    name: String,
    size: u64,
    bundle: StorageBundle,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawEntry {
    key: String,
    key_bytes: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanStatus {
    Loading,
    Ready,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct EntryScanState {
    status: ScanStatus,
    entries: Vec<RawEntry>,
    message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PageScanState {
    status: ScanStatus,
    pages: Vec<RawPageSummary>,
    total_pages: u64,
    page_size: usize,
    message: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActiveTab {
    Entries,
    Pages,
}

pub struct App {
    input_ref: NodeRef,
    loaded: Option<LoadedBundle>,
    selected_path: Option<String>,
    entry_scans: HashMap<String, EntryScanState>,
    page_scans: HashMap<String, PageScanState>,
    active_tab: ActiveTab,
    preview_mode: PreviewMode,
    filter: String,
    entry_page: usize,
    physical_page: usize,
    error: Option<String>,
    copy_status: Option<String>,
    is_opening: bool,
}

pub enum Msg {
    ClickOpen,
    OpenFile(File),
    BundleLoaded(Result<LoadedBundle, String>),
    SelectPath(String),
    SetTab(ActiveTab),
    SetPreviewMode(PreviewMode),
    SetFilter(String),
    SetEntryPage(usize),
    SetPhysicalPage(usize),
    EntryBatch {
        path: String,
        entries: Vec<RawEntry>,
        done: bool,
    },
    EntryScanFailed {
        path: String,
        message: String,
    },
    PageBatch {
        path: String,
        pages: Vec<RawPageSummary>,
        total_pages: u64,
        page_size: usize,
        done: bool,
    },
    PageScanFailed {
        path: String,
        message: String,
    },
    CopyValue(String),
    CopyFinished(String),
}

impl Component for App {
    type Message = Msg;
    type Properties = ();

    fn create(_ctx: &Context<Self>) -> Self {
        Self {
            input_ref: NodeRef::default(),
            loaded: None,
            selected_path: None,
            entry_scans: HashMap::new(),
            page_scans: HashMap::new(),
            active_tab: ActiveTab::Entries,
            preview_mode: PreviewMode::Utf8,
            filter: String::new(),
            entry_page: 0,
            physical_page: 0,
            error: None,
            copy_status: None,
            is_opening: false,
        }
    }

    fn update(&mut self, ctx: &Context<Self>, msg: Self::Message) -> bool {
        match msg {
            Msg::ClickOpen => {
                if let Some(input) = self.input_ref.cast::<HtmlInputElement>()
                    && let Some(element) = input.dyn_ref::<HtmlElement>()
                {
                    element.click();
                }
                false
            }
            Msg::OpenFile(file) => {
                self.is_opening = true;
                self.error = None;
                self.copy_status = None;
                read_browser_file(file, ctx.link().clone());
                true
            }
            Msg::BundleLoaded(result) => {
                self.is_opening = false;
                match result {
                    Ok(loaded) => {
                        self.selected_path =
                            loaded.bundle.files.first().map(|file| file.path.clone());
                        self.loaded = Some(loaded);
                        self.entry_scans.clear();
                        self.page_scans.clear();
                        self.filter.clear();
                        self.entry_page = 0;
                        self.physical_page = 0;
                        self.error = None;
                        self.ensure_scans_for_selected(ctx);
                    }
                    Err(message) => {
                        self.loaded = None;
                        self.selected_path = None;
                        self.entry_scans.clear();
                        self.page_scans.clear();
                        self.error = Some(message);
                    }
                }
                true
            }
            Msg::SelectPath(path) => {
                self.selected_path = Some(path);
                self.entry_page = 0;
                self.physical_page = 0;
                self.copy_status = None;
                self.ensure_scans_for_selected(ctx);
                true
            }
            Msg::SetTab(tab) => {
                self.active_tab = tab;
                true
            }
            Msg::SetPreviewMode(mode) => {
                self.preview_mode = mode;
                true
            }
            Msg::SetFilter(filter) => {
                self.filter = filter;
                self.entry_page = 0;
                true
            }
            Msg::SetEntryPage(page) => {
                self.entry_page = page;
                true
            }
            Msg::SetPhysicalPage(page) => {
                self.physical_page = page;
                true
            }
            Msg::EntryBatch {
                path,
                entries,
                done,
            } => {
                if let Some(scan) = self.entry_scans.get_mut(&path) {
                    scan.entries.extend(entries);
                    scan.status = if done {
                        ScanStatus::Ready
                    } else {
                        ScanStatus::Loading
                    };
                }
                true
            }
            Msg::EntryScanFailed { path, message } => {
                self.entry_scans.insert(
                    path,
                    EntryScanState {
                        status: ScanStatus::Error,
                        entries: Vec::new(),
                        message: Some(message),
                    },
                );
                true
            }
            Msg::PageBatch {
                path,
                pages,
                total_pages,
                page_size,
                done,
            } => {
                if let Some(scan) = self.page_scans.get_mut(&path) {
                    scan.pages.extend(pages);
                    scan.total_pages = total_pages;
                    scan.page_size = page_size;
                    scan.status = if done {
                        ScanStatus::Ready
                    } else {
                        ScanStatus::Loading
                    };
                }
                true
            }
            Msg::PageScanFailed { path, message } => {
                self.page_scans.insert(
                    path,
                    PageScanState {
                        status: ScanStatus::Error,
                        pages: Vec::new(),
                        total_pages: 0,
                        page_size: 0,
                        message: Some(message),
                    },
                );
                true
            }
            Msg::CopyValue(text) => {
                copy_to_clipboard(text, ctx.link().clone());
                false
            }
            Msg::CopyFinished(status) => {
                self.copy_status = Some(status);
                true
            }
        }
    }

    fn view(&self, ctx: &Context<Self>) -> Html {
        html! {
            <main class="app-shell">
                <header class="topbar">
                    <div>
                        <h1>{"OPFS B-tree Viewer"}</h1>
                        <p>{"Open a Jazz storage bundle and inspect raw opfs-btree entries and pages."}</p>
                    </div>
                    <button type="button" class="primary-action" onclick={ctx.link().callback(|_| Msg::ClickOpen)}>
                        {"Open bundle"}
                    </button>
                    <input
                        ref={self.input_ref.clone()}
                        aria-label="Open storage bundle"
                        class="file-input"
                        type="file"
                        accept=".jazz-opfs-bundle,application/vnd.jazz.opfs-btree-bundle"
                        onchange={ctx.link().callback(|event: Event| {
                            let input: HtmlInputElement = event.target_unchecked_into();
                            let file = input.files().and_then(|files| files.get(0));
                            input.set_value("");
                            match file {
                                Some(file) => Msg::OpenFile(file),
                                None => Msg::CopyFinished("No file selected.".to_string()),
                            }
                        })}
                    />
                </header>

                <section
                    class={classes!("drop-zone", self.is_opening.then_some("is-loading"))}
                    ondragover={Callback::from(|event: DragEvent| event.prevent_default())}
                    ondrop={ctx.link().callback(|event: DragEvent| {
                        event.prevent_default();
                        let file = event
                            .data_transfer()
                            .and_then(|transfer| transfer.files())
                            .and_then(|files| files.get(0));
                        match file {
                            Some(file) => Msg::OpenFile(file),
                            None => Msg::CopyFinished("No dropped file found.".to_string()),
                        }
                    })}
                >
                    <span>{if self.is_opening { "Opening bundle..." } else { "Drop a .jazz-opfs-bundle file here" }}</span>
                </section>

                {self.view_error()}
                {self.view_workspace(ctx)}
            </main>
        }
    }
}

impl App {
    fn selected_file(&self) -> Option<StorageBundleFile> {
        let loaded = self.loaded.as_ref()?;
        let selected_path = self.selected_path.as_ref()?;
        loaded
            .bundle
            .files
            .iter()
            .find(|file| &file.path == selected_path)
            .cloned()
    }

    fn ensure_scans_for_selected(&mut self, ctx: &Context<Self>) {
        let Some(file) = self.selected_file() else {
            return;
        };

        if !self.entry_scans.contains_key(&file.path) {
            self.entry_scans.insert(
                file.path.clone(),
                EntryScanState {
                    status: ScanStatus::Loading,
                    entries: Vec::new(),
                    message: None,
                },
            );
            start_entry_scan(ctx.link().clone(), file.path.clone(), file.bytes.clone());
        }

        if !self.page_scans.contains_key(&file.path) {
            self.page_scans.insert(
                file.path.clone(),
                PageScanState {
                    status: ScanStatus::Loading,
                    pages: Vec::new(),
                    total_pages: 0,
                    page_size: 0,
                    message: None,
                },
            );
            start_page_scan(ctx.link().clone(), file.path.clone(), file.bytes);
        }
    }

    fn view_error(&self) -> Html {
        match &self.error {
            Some(error) => html! { <p class="error-banner">{error}</p> },
            None => Html::default(),
        }
    }

    fn view_workspace(&self, ctx: &Context<Self>) -> Html {
        let Some(loaded) = &self.loaded else {
            return html! {
                <section class="empty-state">
                    <h2>{"No bundle open"}</h2>
                    <p>{"Paste the README snippet in the app origin console, then open the downloaded file here."}</p>
                </section>
            };
        };

        html! {
            <section class="workspace">
                <aside class="sidebar" aria-label="Bundle files">
                    <div class="bundle-summary">
                        <span class="eyebrow">{"Bundle"}</span>
                        <strong>{&loaded.name}</strong>
                        <span>{format_bytes(loaded.size)}</span>
                    </div>
                    <pre class="metadata">{&loaded.bundle.metadata_text}</pre>
                    <div class="file-list">
                        {for loaded.bundle.files.iter().map(|file| self.view_file_button(ctx, file))}
                    </div>
                </aside>

                <section class="entry-panel">
                    {self.view_toolbar(ctx)}
                    {self.copy_status.as_ref().map(|status| html! { <p class="status-line">{status}</p> }).unwrap_or_default()}
                    {
                        match self.active_tab {
                            ActiveTab::Entries => self.view_entries(ctx),
                            ActiveTab::Pages => self.view_pages(ctx),
                        }
                    }
                </section>
            </section>
        }
    }

    fn view_file_button(&self, ctx: &Context<Self>, file: &StorageBundleFile) -> Html {
        let path = file.path.clone();
        let is_selected = self.selected_path.as_deref() == Some(file.path.as_str());
        html! {
            <button
                type="button"
                class={classes!(is_selected.then_some("is-selected"))}
                onclick={ctx.link().callback(move |_| Msg::SelectPath(path.clone()))}
            >
                <span>{&file.path}</span>
                <small>{format_bytes(file.bytes.len() as u64)}</small>
            </button>
        }
    }

    fn view_toolbar(&self, ctx: &Context<Self>) -> Html {
        let selected_path = self
            .selected_path
            .as_deref()
            .unwrap_or("No file selected")
            .to_string();

        html! {
            <div class="entry-toolbar">
                <div>
                    <span class="eyebrow">{"Raw storage"}</span>
                    <h2>{selected_path}</h2>
                </div>
                <div class="toolbar-controls">
                    <div class="segmented tabs" aria-label="Viewer mode">
                        {self.view_tab_button(ctx, ActiveTab::Entries, "Entries")}
                        {self.view_tab_button(ctx, ActiveTab::Pages, "Pages")}
                    </div>
                    {
                        if self.active_tab == ActiveTab::Entries {
                            html! {
                                <>
                                    <input
                                        aria-label="Filter entries"
                                        type="search"
                                        placeholder="Filter key or hex"
                                        value={self.filter.clone()}
                                        oninput={ctx.link().callback(|event: InputEvent| {
                                            let input: HtmlInputElement = event.target_unchecked_into();
                                            Msg::SetFilter(input.value())
                                        })}
                                    />
                                    <div class="segmented" aria-label="Preview encoding">
                                        {for PreviewMode::all().into_iter().map(|mode| self.view_preview_button(ctx, mode))}
                                    </div>
                                </>
                            }
                        } else {
                            Html::default()
                        }
                    }
                </div>
            </div>
        }
    }

    fn view_tab_button(&self, ctx: &Context<Self>, tab: ActiveTab, label: &'static str) -> Html {
        html! {
            <button
                type="button"
                class={classes!((self.active_tab == tab).then_some("is-selected"))}
                onclick={ctx.link().callback(move |_| Msg::SetTab(tab))}
            >
                {label}
            </button>
        }
    }

    fn view_preview_button(&self, ctx: &Context<Self>, mode: PreviewMode) -> Html {
        html! {
            <button
                type="button"
                class={classes!((self.preview_mode == mode).then_some("is-selected"))}
                onclick={ctx.link().callback(move |_| Msg::SetPreviewMode(mode))}
            >
                {mode.label()}
            </button>
        }
    }

    fn view_entries(&self, ctx: &Context<Self>) -> Html {
        let Some(selected_path) = &self.selected_path else {
            return html! { <p class="loading-state">{"No file selected."}</p> };
        };
        let Some(scan) = self.entry_scans.get(selected_path) else {
            return html! { <p class="loading-state">{"Scanning opfs-btree file..."}</p> };
        };
        if scan.status == ScanStatus::Error {
            return html! { <p class="error-banner">{scan.message.as_deref().unwrap_or("Failed to scan file")}</p> };
        }
        if scan.entries.is_empty() {
            let message = if scan.status == ScanStatus::Loading {
                "Scanning opfs-btree file..."
            } else {
                "No raw entries found."
            };
            return html! { <p class="loading-state">{message}</p> };
        }

        let query = self.filter.trim().to_lowercase();
        let filtered = scan
            .entries
            .iter()
            .filter(|entry| {
                if query.is_empty() {
                    return true;
                }
                entry.key.to_lowercase().contains(&query)
                    || bytes_to_hex(&entry.key_bytes, usize::MAX)
                        .to_lowercase()
                        .contains(&query)
            })
            .collect::<Vec<_>>();

        if filtered.is_empty() {
            return html! { <p class="loading-state">{"No entries match the current filter."}</p> };
        }

        let page_count = page_count(filtered.len(), ENTRY_PAGE_SIZE);
        let page_index = self.entry_page.min(page_count - 1);
        let start = page_index * ENTRY_PAGE_SIZE;
        let end = (start + ENTRY_PAGE_SIZE).min(filtered.len());

        html! {
            <>
                <p class="status-line">
                    {if scan.status == ScanStatus::Loading { "Scanning..." } else { "Scanned" }}
                    {" "}
                    {scan.entries.len()}
                    {" raw entries"}
                    {if filtered.len() != scan.entries.len() { format!(", {} matching filter", filtered.len()) } else { String::new() }}
                    {"."}
                </p>
                {self.view_pagination(ctx, page_index, page_count, true)}
                <div class="entry-table" role="table" aria-label="Raw opfs-btree entries">
                    <div class="entry-row entry-heading" role="row">
                        <span role="columnheader">{"Key"}</span>
                        <span role="columnheader">{"Key bytes"}</span>
                        <span role="columnheader">{"Value bytes"}</span>
                        <span role="columnheader">{"Value preview"}</span>
                        <span role="columnheader">{"Actions"}</span>
                    </div>
                    {for filtered[start..end].iter().enumerate().map(|(index, entry)| self.view_entry_row(ctx, entry, start + index))}
                </div>
                {self.view_pagination(ctx, page_index, page_count, true)}
            </>
        }
    }

    fn view_entry_row(&self, ctx: &Context<Self>, entry: &RawEntry, index: usize) -> Html {
        let key_title = bytes_to_hex(&entry.key_bytes, usize::MAX);
        let key = if entry.key.is_empty() {
            bytes_to_hex(&entry.key_bytes, 48)
        } else {
            entry.key.clone()
        };
        let value = entry.value.clone();
        let mode = self.preview_mode;

        html! {
            <div class="entry-row" role="row" key={format!("{key_title}:{index}")}>
                <code role="cell" title={key_title}>{key}</code>
                <span role="cell">{format_bytes(entry.key_bytes.len() as u64)}</span>
                <span role="cell">{format_bytes(entry.value.len() as u64)}</span>
                <code role="cell" class="value-preview">{format_value(&entry.value, self.preview_mode, 320)}</code>
                <span role="cell">
                    <button
                        type="button"
                        class="text-action"
                        onclick={ctx.link().callback(move |_| Msg::CopyValue(format_value(&value, mode, usize::MAX)))}
                    >
                        {"Copy value"}
                    </button>
                </span>
            </div>
        }
    }

    fn view_pages(&self, ctx: &Context<Self>) -> Html {
        let Some(selected_path) = &self.selected_path else {
            return html! { <p class="loading-state">{"No file selected."}</p> };
        };
        let Some(scan) = self.page_scans.get(selected_path) else {
            return html! { <p class="loading-state">{"Scanning opfs-btree pages..."}</p> };
        };
        if scan.status == ScanStatus::Error {
            return html! { <p class="error-banner">{scan.message.as_deref().unwrap_or("Failed to scan pages")}</p> };
        }
        if scan.pages.is_empty() {
            return html! { <p class="loading-state">{"Scanning opfs-btree pages..."}</p> };
        }

        let Some(file) = self.selected_file() else {
            return html! { <p class="loading-state">{"No file selected."}</p> };
        };
        let page_count = page_count(scan.pages.len(), PHYSICAL_PAGE_SIZE);
        let page_index = self.physical_page.min(page_count - 1);
        let start = page_index * PHYSICAL_PAGE_SIZE;
        let end = (start + PHYSICAL_PAGE_SIZE).min(scan.pages.len());

        html! {
            <>
                <p class="status-line">
                    {if scan.status == ScanStatus::Loading { "Scanning pages..." } else { "Scanned pages" }}
                    {" "}
                    {scan.pages.len()}
                    {" of "}
                    {scan.total_pages}
                    {" page slots"}
                    {if scan.page_size > 0 { format!(" at {} each", format_bytes(scan.page_size as u64)) } else { String::new() }}
                    {"."}
                </p>
                {self.view_pagination(ctx, page_index, page_count, false)}
                <div class="page-table" role="table" aria-label="Physical opfs-btree pages">
                    <div class="page-row entry-heading" role="row">
                        <span role="columnheader">{"Page"}</span>
                        <span role="columnheader">{"Kind"}</span>
                        <span role="columnheader">{"Items"}</span>
                        <span role="columnheader">{"Next"}</span>
                        <span role="columnheader">{"Role"}</span>
                        <span role="columnheader">{"Header bytes"}</span>
                    </div>
                    {for scan.pages[start..end].iter().map(|page| self.view_page_row(&file, page))}
                </div>
                {self.view_pagination(ctx, page_index, page_count, false)}
            </>
        }
    }

    fn view_page_row(&self, file: &StorageBundleFile, page: &RawPageSummary) -> Html {
        let role = page_role(page);
        let next = page
            .next_page_id
            .map(|page_id| page_id.to_string())
            .unwrap_or_else(|| "-".to_string());
        let page_bytes = page_bytes(file, page);
        let preview = if let Some(error) = &page.error {
            error.clone()
        } else {
            bytes_to_hex(page_bytes, 48)
        };

        html! {
            <div class="page-row" role="row" key={page.page_id}>
                <code role="cell">{page.page_id}</code>
                <span role="cell">{page_kind_label(page.kind)}</span>
                <span role="cell">{page.item_count}</span>
                <span role="cell">{next}</span>
                <span role="cell">{role}</span>
                <code role="cell" class="value-preview">{preview}</code>
            </div>
        }
    }

    fn view_pagination(
        &self,
        ctx: &Context<Self>,
        page_index: usize,
        page_count: usize,
        entries: bool,
    ) -> Html {
        if page_count <= 1 {
            return Html::default();
        }

        let previous = page_index.saturating_sub(1);
        let next = (page_index + 1).min(page_count - 1);
        html! {
            <div class="pagination">
                <button
                    type="button"
                    class="text-action"
                    disabled={page_index == 0}
                    onclick={ctx.link().callback(move |_| {
                        if entries {
                            Msg::SetEntryPage(previous)
                        } else {
                            Msg::SetPhysicalPage(previous)
                        }
                    })}
                >
                    {"Previous"}
                </button>
                <span>{format!("Page {} of {}", page_index + 1, page_count)}</span>
                <button
                    type="button"
                    class="text-action"
                    disabled={page_index + 1 >= page_count}
                    onclick={ctx.link().callback(move |_| {
                        if entries {
                            Msg::SetEntryPage(next)
                        } else {
                            Msg::SetPhysicalPage(next)
                        }
                    })}
                >
                    {"Next"}
                </button>
            </div>
        }
    }
}

fn read_browser_file(file: File, link: Scope<App>) {
    let name = file.name();
    let size = file.size() as u64;
    spawn_local(async move {
        let result = async {
            let buffer = JsFuture::from(file.array_buffer())
                .await
                .map_err(js_error_to_string)?;
            let bytes = Uint8Array::new(&buffer).to_vec();
            let bundle = decode_storage_bundle(&bytes)?;
            Ok(LoadedBundle { name, size, bundle })
        }
        .await;
        link.send_message(Msg::BundleLoaded(result));
    });
}

fn start_entry_scan(link: Scope<App>, path: String, bytes: Vec<u8>) {
    spawn_local(async move {
        let result = async {
            let file = MemoryFile::from_bytes(bytes);
            let mut tree = OpfsBTree::open(file, BTreeOptions::default())
                .map_err(|err| format!("open opfs-btree bytes: {err}"))?;
            let mut cursor = None;

            loop {
                let batch = tree
                    .raw_entries_batch(cursor, ENTRY_SCAN_BATCH_SIZE)
                    .map_err(|err| format!("scan opfs-btree entries: {err}"))?;
                let done = batch.done;
                cursor = batch.next_cursor;
                let entries = batch
                    .entries
                    .into_iter()
                    .map(|(key_bytes, value)| RawEntry {
                        key: String::from_utf8_lossy(&key_bytes).into_owned(),
                        key_bytes,
                        value,
                    })
                    .collect();
                link.send_message(Msg::EntryBatch {
                    path: path.clone(),
                    entries,
                    done,
                });
                if done {
                    break;
                }
                TimeoutFuture::new(0).await;
            }
            Ok::<(), String>(())
        }
        .await;

        if let Err(message) = result {
            link.send_message(Msg::EntryScanFailed { path, message });
        }
    });
}

fn start_page_scan(link: Scope<App>, path: String, bytes: Vec<u8>) {
    spawn_local(async move {
        let result = async {
            let file = MemoryFile::from_bytes(bytes);
            let mut tree = OpfsBTree::open(file, BTreeOptions::default())
                .map_err(|err| format!("open opfs-btree bytes: {err}"))?;
            let total_pages = tree.total_pages();
            let page_size = tree.page_size();
            let mut next_page_id = Some(0);

            while let Some(start_page_id) = next_page_id {
                let batch = tree
                    .raw_page_summaries_batch(start_page_id, PAGE_SCAN_BATCH_SIZE)
                    .map_err(|err| format!("scan opfs-btree pages: {err}"))?;
                next_page_id = batch.next_page_id;
                let done = batch.done;
                link.send_message(Msg::PageBatch {
                    path: path.clone(),
                    pages: batch.pages,
                    total_pages,
                    page_size,
                    done,
                });
                if done {
                    break;
                }
                TimeoutFuture::new(0).await;
            }
            Ok::<(), String>(())
        }
        .await;

        if let Err(message) = result {
            link.send_message(Msg::PageScanFailed { path, message });
        }
    });
}

fn copy_to_clipboard(text: String, link: Scope<App>) {
    spawn_local(async move {
        let status = async {
            let window = web_sys::window().ok_or("Clipboard access is not available.")?;
            let promise = window.navigator().clipboard().write_text(&text);
            JsFuture::from(promise).await.map_err(js_error_to_string)?;
            Ok::<String, String>("Copied value.".to_string())
        }
        .await
        .unwrap_or_else(|message| message);

        link.send_message(Msg::CopyFinished(status));
    });
}

fn page_count(item_count: usize, page_size: usize) -> usize {
    item_count.div_ceil(page_size).max(1)
}

fn page_bytes<'a>(file: &'a StorageBundleFile, page: &RawPageSummary) -> &'a [u8] {
    let Ok(start) = usize::try_from(page.byte_offset) else {
        return &[];
    };
    let end = start.saturating_add(page.byte_len).min(file.bytes.len());
    file.bytes.get(start..end).unwrap_or(&[])
}

fn page_role(page: &RawPageSummary) -> String {
    let mut roles = Vec::new();
    if page.is_active {
        roles.push("active");
    }
    if page.is_root {
        roles.push("root");
    }
    if page.is_free {
        roles.push("free");
    }
    if roles.is_empty() {
        if matches!(
            page.kind,
            opfs_btree::RawPageKind::SuperblockA | opfs_btree::RawPageKind::SuperblockB
        ) {
            "inactive".to_string()
        } else {
            "allocated".to_string()
        }
    } else {
        roles.join(", ")
    }
}

fn js_error_to_string(value: JsValue) -> String {
    value
        .as_string()
        .unwrap_or_else(|| "Browser operation failed.".to_string())
}
