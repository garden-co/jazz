use gloo_net::http::Request;
use gloo_timers::callback::Interval;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use wasm_bindgen_futures::spawn_local;
use web_sys::{HtmlInputElement, HtmlSelectElement};
use yew::prelude::*;

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct FlowRow {
    timestamp: String,
    service_name: String,
    span_name: String,
    #[serde(default)]
    thread: String,
    #[serde(default)]
    fields: String,
    #[serde(default)]
    payload: String,
    #[serde(default)]
    payload_json: String,
    #[serde(default)]
    peer_kind: String,
    #[serde(default)]
    peer_id: String,
    #[serde(default)]
    tier: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct FlowAttrs {
    payload: String,
    peer_kind: String,
    peer_id: String,
    tier: String,
    payload_json: String,
}

#[derive(Clone, Debug, Deserialize)]
struct SqlEnvelope {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
}

#[derive(Clone, Debug, Deserialize)]
struct SqlErrorEnvelope {
    error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct SqlRequest {
    query: String,
}

#[function_component(App)]
fn app() -> Html {
    let minutes = use_state(|| 30_u32);

    html! {
        <div style={APP_STYLE}>
            <header style={HEADER_STYLE}>
                <h1 style={TITLE_STYLE}>{"Sync Flow"}</h1>
                <label style={TIME_WINDOW_STYLE}>
                    <span style={FIELD_LABEL_STYLE}>{"Window (min)"}</span>
                    <input
                        type="number"
                        min="1"
                        value={minutes.to_string()}
                        oninput={{
                            let minutes = minutes.clone();
                            Callback::from(move |event: InputEvent| {
                                let input: HtmlInputElement = event.target_unchecked_into();
                                let value = input.value().parse::<u32>().unwrap_or(1).max(1);
                                minutes.set(value);
                            })
                        }}
                        style={SMALL_INPUT_STYLE}
                    />
                </label>
            </header>
            <FlowList minutes={*minutes} />
        </div>
    }
}

#[derive(Properties, PartialEq)]
struct FlowListProps {
    minutes: u32,
}

#[function_component(FlowList)]
fn flow_list(props: &FlowListProps) -> Html {
    let limit = use_state(|| 5_000_u32);
    let payload_filter = use_state(String::new);
    let layer_filter = use_state(String::new);
    let expanded_rows = use_state(Vec::<String>::new);
    let rows = use_state(Vec::<FlowRow>::new);
    let error = use_state(|| None::<String>);

    {
        let rows = rows.clone();
        let error = error.clone();
        let minutes = props.minutes;
        let limit_value = *limit;
        let payload_value = (*payload_filter).clone();
        use_effect_with((minutes, limit_value, payload_value), move |deps| {
            fetch_flow(deps.0, deps.1, deps.2.clone(), rows.clone(), error.clone());
            let rows = rows.clone();
            let error = error.clone();
            let deps = deps.clone();
            let interval = Interval::new(3_000, move || {
                fetch_flow(deps.0, deps.1, deps.2.clone(), rows.clone(), error.clone());
            });
            move || drop(interval)
        });
    }

    let filtered_rows = rows
        .iter()
        .filter(|row| layer_filter.is_empty() || layer_label(row) == *layer_filter)
        .cloned()
        .collect::<Vec<_>>();

    html! {
        <div>
            <section style={CONTROLS_STYLE}>
                <Field label="Limit">
                    <input
                        type="number"
                        min="1"
                        value={limit.to_string()}
                        oninput={{
                            let limit = limit.clone();
                            Callback::from(move |event: InputEvent| {
                                let input: HtmlInputElement = event.target_unchecked_into();
                                limit.set(input.value().parse::<u32>().unwrap_or(100).max(1));
                            })
                        }}
                        style={INPUT_STYLE}
                    />
                </Field>
                <Field label="Payload">
                    <input
                        type="text"
                        placeholder="any"
                        value={(*payload_filter).clone()}
                        oninput={{
                            let payload_filter = payload_filter.clone();
                            Callback::from(move |event: InputEvent| {
                                let input: HtmlInputElement = event.target_unchecked_into();
                                payload_filter.set(input.value());
                            })
                        }}
                        style={INPUT_STYLE}
                    />
                </Field>
                <Field label="Layer">
                    <select
                        value={(*layer_filter).clone()}
                        onchange={{
                            let layer_filter = layer_filter.clone();
                            Callback::from(move |event: Event| {
                                let select: HtmlSelectElement = event.target_unchecked_into();
                                layer_filter.set(select.value());
                            })
                        }}
                        style={INPUT_STYLE}
                    >
                        <option value="">{"any"}</option>
                        <option value="browser/main">{"browser/main"}</option>
                        <option value="browser/worker">{"browser/worker"}</option>
                        <option value="server">{"server"}</option>
                    </select>
                </Field>
            </section>

            if let Some(message) = &*error {
                <div style={ERROR_STYLE}>{"error: "}{message}</div>
            }

            <table style={TABLE_STYLE}>
                <thead>
                    <tr>
                        <th style={TH_STYLE}>{"time"}</th>
                        <th style={TH_STYLE}>{"layer"}</th>
                        <th style={TH_STYLE}>{"dir"}</th>
                        <th style={TH_STYLE}>{"payload"}</th>
                        <th style={TH_STYLE}>{"peer"}</th>
                        <th style={TH_STYLE}>{"tier"}</th>
                    </tr>
                </thead>
                <tbody>
                    {for filtered_rows.iter().enumerate().map(|(index, row)| {
                        let row_key = format!("{}-{index}", row.timestamp);
                        let toggle_key = row_key.clone();
                        let expanded = expanded_rows.contains(&row_key);
                        html! {
                            <FlowTableRows
                                key={row_key.clone()}
                                row={row.clone()}
                                expanded={expanded}
                                on_toggle={{
                                    let expanded_rows = expanded_rows.clone();
                                    Callback::from(move |_| {
                                        let mut next = (*expanded_rows).clone();
                                        if let Some(index) = next.iter().position(|key| key == &toggle_key) {
                                            next.remove(index);
                                        } else {
                                            next.push(toggle_key.clone());
                                        }
                                        expanded_rows.set(next);
                                    })
                                }}
                            />
                        }
                    })}
                    if filtered_rows.is_empty() && error.is_none() {
                        <tr>
                            <td colspan="6" style={EMPTY_STYLE}>{"No sync messages in the selected window."}</td>
                        </tr>
                    }
                </tbody>
            </table>
        </div>
    }
}

#[derive(Properties, PartialEq)]
struct FieldProps {
    label: &'static str,
    children: Children,
}

#[function_component(Field)]
fn field(props: &FieldProps) -> Html {
    html! {
        <label style={FIELD_STYLE}>
            <span style={FIELD_LABEL_STYLE}>{props.label}</span>
            {for props.children.iter()}
        </label>
    }
}

#[derive(Properties, PartialEq)]
struct FlowTableRowsProps {
    row: FlowRow,
    expanded: bool,
    on_toggle: Callback<()>,
}

#[function_component(FlowTableRows)]
fn flow_table_rows(props: &FlowTableRowsProps) -> Html {
    let attrs = resolve_flow_attrs(&props.row);
    let can_expand = !attrs.payload.is_empty() || !attrs.payload_json.is_empty();
    let layer = layer_label(&props.row);
    let direction = if props.row.span_name == "sync.send" {
        "send"
    } else {
        "recv"
    };
    let onclick = {
        let on_toggle = props.on_toggle.clone();
        Callback::from(move |_| {
            if can_expand {
                on_toggle.emit(());
            }
        })
    };
    let row_style = format!(
        "{TR_STYLE}; background: {}; cursor: {};",
        row_background(&layer),
        if can_expand { "pointer" } else { "default" }
    );

    html! {
        <>
            <tr style={row_style} {onclick}>
                <td style={TD_STYLE}>{format_time(&props.row.timestamp)}</td>
                <td style={TD_STYLE}>{layer}</td>
                <td style={direction_style(&props.row.span_name)}>{direction}</td>
                <td style={PAYLOAD_CELL_STYLE}>{if attrs.payload.is_empty() { "-" } else { &attrs.payload }}</td>
                <td style={TD_STYLE}>{short_peer(&attrs.peer_kind, &attrs.peer_id)}</td>
                <td style={TD_STYLE}>{if attrs.tier.is_empty() { "-" } else { &attrs.tier }}</td>
            </tr>
            if props.expanded && can_expand {
                <tr>
                    <td colspan="6" style={EXPANDED_STYLE}>
                        if !attrs.payload.is_empty() {
                            <PayloadDetail label="payload" value={attrs.payload.clone()} is_json={false} />
                        }
                        if !attrs.payload_json.is_empty() {
                            <PayloadDetail label="payload_json" value={attrs.payload_json.clone()} is_json={true} />
                        }
                    </td>
                </tr>
            }
        </>
    }
}

#[derive(Properties, PartialEq)]
struct PayloadDetailProps {
    label: &'static str,
    value: String,
    is_json: bool,
}

#[function_component(PayloadDetail)]
fn payload_detail(props: &PayloadDetailProps) -> Html {
    html! {
        <div style={PAYLOAD_ROW_STYLE}>
            <span style={PAYLOAD_LABEL_STYLE}>{props.label}</span>
            if props.is_json {
                <pre style={PRE_STYLE}>{pretty_json(&props.value)}</pre>
            } else {
                <code style={CODE_STYLE}>{&props.value}</code>
            }
        </div>
    }
}

fn fetch_flow(
    minutes: u32,
    limit: u32,
    payload_filter: String,
    rows: UseStateHandle<Vec<FlowRow>>,
    error: UseStateHandle<Option<String>>,
) {
    spawn_local(async move {
        match run_query(&build_flow_sql(minutes, limit, &payload_filter)).await {
            Ok(next_rows) => {
                error.set(None);
                rows.set(next_rows);
            }
            Err(message) => error.set(Some(message)),
        }
    });
}

async fn run_query(sql: &str) -> Result<Vec<FlowRow>, String> {
    let response = Request::post("/sql")
        .header("content-type", "application/json")
        .body(
            serde_json::to_string(&SqlRequest {
                query: sql.trim().to_string(),
            })
            .map_err(|err| err.to_string())?,
        )
        .map_err(|err| err.to_string())?
        .send()
        .await
        .map_err(|err| err.to_string())?;

    let status = response.status();
    let body = response.text().await.map_err(|err| err.to_string())?;
    if !(200..300).contains(&status) {
        return Err(parse_error(&body).unwrap_or_else(|| format!("sql endpoint returned {status}")));
    }

    let envelope: SqlEnvelope = serde_json::from_str(&body).map_err(|err| err.to_string())?;
    let mut out = Vec::with_capacity(envelope.rows.len());
    for row in envelope.rows {
        let mut object = serde_json::Map::new();
        for (index, column) in envelope.columns.iter().enumerate() {
            object.insert(
                column.clone(),
                row.get(index).cloned().unwrap_or(Value::Null),
            );
        }
        out.push(serde_json::from_value(Value::Object(object)).map_err(|err| err.to_string())?);
    }
    Ok(out)
}

fn parse_error(body: &str) -> Option<String> {
    serde_json::from_str::<SqlErrorEnvelope>(body)
        .ok()
        .and_then(|envelope| envelope.error)
        .or_else(|| (!body.is_empty()).then(|| body.to_string()))
}

fn build_flow_sql(minutes: u32, limit: u32, payload_filter: &str) -> String {
    let minutes = minutes.max(1);
    let limit = limit.max(1);
    let now_ms = js_sys::Date::now() as u64;
    let cutoff_ns = now_ms.saturating_sub(minutes as u64 * 60_000) * 1_000_000;
    let mut where_parts = vec![
        format!("start_time_unix_nano > {cutoff_ns}"),
        "service_name IN ('jazz-browser', 'jazz-dev-server', 'jazz-server')".to_string(),
        "name IN ('sync.send', 'sync.recv')".to_string(),
    ];
    let payload = payload_filter.trim();
    if !payload.is_empty() {
        where_parts.push(format!(
            "{} = '{}'",
            attr("payload"),
            escape_sql_string(payload)
        ));
    }

    format!(
        "
        SELECT
          strftime(to_timestamp(start_time_unix_nano / 1e9), '%Y-%m-%dT%H:%M:%S.%gZ') AS Timestamp,
          service_name AS ServiceName,
          name AS SpanName,
          {thread_attr} AS thread,
          {fields_attr} AS fields,
          {payload_attr} AS payload,
          {payload_json_attr} AS payload_json,
          {peer_kind_attr} AS peer_kind,
          {peer_id_attr} AS peer_id,
          {tier_attr} AS tier
        FROM spans
        WHERE {where_clause}
        ORDER BY start_time_unix_nano DESC
        LIMIT {limit}
        ",
        thread_attr = attr("jazz.runtime_thread"),
        fields_attr = attr("jazz.span.fields"),
        payload_attr = attr("payload"),
        payload_json_attr = attr("payload_json"),
        peer_kind_attr = attr("peer_kind"),
        peer_id_attr = attr("peer_id"),
        tier_attr = attr("tier"),
        where_clause = where_parts.join(" AND "),
    )
}

fn attr(key: &str) -> String {
    format!(
        "(
            SELECT COALESCE(
              json_extract_string(a, '$.value.stringValue'),
              CAST(json_extract(a, '$.value.intValue') AS VARCHAR),
              CAST(json_extract(a, '$.value.doubleValue') AS VARCHAR),
              CAST(json_extract(a, '$.value.boolValue') AS VARCHAR)
            )
            FROM UNNEST(attributes::JSON[]) AS u(a)
            WHERE json_extract_string(a, '$.key') = '{}'
            LIMIT 1
        )",
        escape_sql_string(key)
    )
}

fn resolve_flow_attrs(row: &FlowRow) -> FlowAttrs {
    let mut attrs = FlowAttrs {
        payload: row.payload.clone(),
        peer_kind: row.peer_kind.clone(),
        peer_id: row.peer_id.clone(),
        tier: row.tier.clone(),
        payload_json: row.payload_json.clone(),
    };

    if (!attrs.payload.is_empty() && !attrs.peer_kind.is_empty() && !attrs.payload_json.is_empty())
        || row.fields.is_empty()
    {
        return attrs;
    }

    if let Ok(fields) = serde_json::from_str::<serde_json::Map<String, Value>>(&row.fields) {
        if attrs.payload.is_empty() {
            attrs.payload = string_field(&fields, "payload");
        }
        if attrs.peer_kind.is_empty() {
            attrs.peer_kind = string_field(&fields, "peer_kind");
        }
        if attrs.peer_id.is_empty() {
            attrs.peer_id = string_field(&fields, "peer_id");
        }
        if attrs.tier.is_empty() {
            attrs.tier = string_field(&fields, "tier");
        }
        if attrs.payload_json.is_empty() {
            attrs.payload_json = string_field(&fields, "payload_json");
        }
    }
    attrs
}

fn string_field(fields: &serde_json::Map<String, Value>, key: &str) -> String {
    fields
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn layer_label(row: &FlowRow) -> String {
    if row.service_name == "jazz-dev-server" || row.service_name == "jazz-server" {
        "server".to_string()
    } else if row.thread == "worker" {
        "browser/worker".to_string()
    } else if row.thread == "main" {
        "browser/main".to_string()
    } else {
        row.service_name.clone()
    }
}

fn row_background(layer: &str) -> &'static str {
    match layer {
        "server" => "#f5f0ff",
        "browser/worker" => "#f0fff5",
        "browser/main" => "#fff8f0",
        _ => "white",
    }
}

fn direction_style(span_name: &str) -> String {
    let color = if span_name == "sync.send" {
        "#047857"
    } else {
        "#1d4ed8"
    };
    format!("{TD_STYLE}; color: {color}; font-weight: 600;")
}

fn short_peer(kind: &str, id: &str) -> String {
    if id.is_empty() {
        kind.to_string()
    } else {
        format!(
            "{}:{}",
            if kind.is_empty() { "?" } else { kind },
            &id[..id.len().min(8)]
        )
    }
}

fn format_time(value: &str) -> String {
    if value.len() >= 23 {
        value[11..23].to_string()
    } else {
        value.to_string()
    }
}

fn pretty_json(value: &str) -> String {
    serde_json::from_str::<Value>(value)
        .and_then(|json| serde_json::to_string_pretty(&json))
        .unwrap_or_else(|_| value.to_string())
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn main() {
    yew::Renderer::<App>::with_root(
        gloo_utils::document()
            .get_element_by_id("root")
            .expect("root element"),
    )
    .render();
}

const APP_STYLE: &str = "font-family: ui-sans-serif, system-ui, -apple-system, sans-serif; padding: 16px; max-width: 1400px; margin: 0 auto; color: #1f2937;";
const HEADER_STYLE: &str = "display: flex; align-items: center; gap: 24px; flex-wrap: wrap; border-bottom: 1px solid #e5e7eb; padding-bottom: 8px; margin-bottom: 16px;";
const TITLE_STYLE: &str = "font-size: 18px; margin: 0; margin-right: auto;";
const TIME_WINDOW_STYLE: &str = "display: flex; align-items: center; gap: 8px;";
const FIELD_STYLE: &str = "display: flex; flex-direction: column; gap: 2px;";
const FIELD_LABEL_STYLE: &str = "font-size: 11px; color: #6b7280; text-transform: uppercase;";
const SMALL_INPUT_STYLE: &str = "padding: 4px 8px; border: 1px solid #d1d5db; border-radius: 4px; font-size: 14px; width: 70px;";
const CONTROLS_STYLE: &str = "display: flex; gap: 12px; flex-wrap: wrap; margin: 0 0 12px; padding: 10px; background: #fafafa; border: 1px solid #eee; border-radius: 6px; align-items: flex-end;";
const INPUT_STYLE: &str = "padding: 4px 8px; border: 1px solid #d1d5db; border-radius: 4px; font-size: 14px; width: 130px;";
const ERROR_STYLE: &str =
    "background: #fee2e2; color: #991b1b; padding: 8px; border-radius: 4px; margin: 8px 0;";
const TABLE_STYLE: &str = "width: 100%; border-collapse: collapse; font-family: ui-monospace, SFMono-Regular, monospace; font-size: 12px;";
const TH_STYLE: &str = "text-align: left; padding: 6px 8px; border-bottom: 2px solid #ddd; font-size: 11px; color: #4b5563; text-transform: uppercase;";
const TR_STYLE: &str = "border-bottom: 1px solid #f0f0f0";
const TD_STYLE: &str = "padding: 4px 8px; vertical-align: top; white-space: nowrap;";
const PAYLOAD_CELL_STYLE: &str =
    "padding: 4px 8px; vertical-align: top; white-space: nowrap; font-weight: 500;";
const EMPTY_STYLE: &str = "padding: 24px; text-align: center; color: #6b7280;";
const EXPANDED_STYLE: &str = "padding: 12px; background: #fafafa;";
const PAYLOAD_ROW_STYLE: &str = "display: grid; grid-template-columns: 120px minmax(0, 1fr); gap: 12px; align-items: start; margin-bottom: 8px;";
const PAYLOAD_LABEL_STYLE: &str =
    "color: #6b7280; font-size: 11px; text-transform: uppercase; padding-top: 2px;";
const CODE_STYLE: &str = "font-family: ui-monospace, SFMono-Regular, monospace; font-size: 11px; white-space: pre-wrap; overflow-wrap: anywhere;";
const PRE_STYLE: &str = "margin: 0; font-size: 11px; overflow-x: auto; white-space: pre;";
