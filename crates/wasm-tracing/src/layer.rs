use std::sync::atomic::AtomicUsize;

use tracing::{Level, Subscriber};
#[cfg(feature = "tracing-log")]
use tracing_log::NormalizeEvent as _;
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};

use crate::{
    debug1, debug4, error1, error4, log1, log4, mark, mark_name, measure, prelude::*,
    recorder::StringRecorder, thread_display_suffix, warn1, warn4,
};

#[doc = r#"
Implements [tracing_subscriber::layer::Layer] which uses [wasm_bindgen] for marking and measuring via `window.performance` and `window.console`

If composing a subscriber, provide `WasmLayer` as such:

```notest
use tracing_subscriber::prelude::*;
use tracing::Subscriber;

pub struct MySubscriber {
    // ...
}

impl Subscriber for MySubscriber {
    // ...
}

let subscriber = MySubscriber::new()
    .with(WasmLayer::default());

tracing::subscriber::set_global_default(subscriber);
```
"#]
pub struct WasmLayer {
    last_event_id: AtomicUsize,
    config: WasmLayerConfig,
}

impl WasmLayer {
    /// Create a new [Layer] with the provided config
    pub fn new(config: WasmLayerConfig) -> Self {
        WasmLayer {
            last_event_id: AtomicUsize::new(0),
            config,
        }
    }
}

impl Default for WasmLayer {
    fn default() -> Self {
        WasmLayer::new(WasmLayerConfig::default())
    }
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for WasmLayer {
    fn enabled(&self, metadata: &tracing::Metadata<'_>, _: Context<'_, S>) -> bool {
        let level = metadata.level();
        level <= &self.config.max_level
    }

    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::Id,
        ctx: Context<'_, S>,
    ) {
        let mut new_debug_record = StringRecorder::new(self.config.show_fields);
        attrs.record(&mut new_debug_record);

        if let Some(span_ref) = ctx.span(id) {
            span_ref
                .extensions_mut()
                .insert::<StringRecorder>(new_debug_record);
        }
    }

    fn on_record(&self, id: &tracing::Id, values: &tracing::span::Record<'_>, ctx: Context<'_, S>) {
        if let Some(span_ref) = ctx.span(id) {
            if let Some(debug_record) = span_ref.extensions_mut().get_mut::<StringRecorder>() {
                values.record(debug_record);
            }
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        if !self.config.enabled {
            return;
        }

        let mut recorder = StringRecorder::new(self.config.show_fields);
        event.record(&mut recorder);
        #[cfg(feature = "tracing-log")]
        let normalized_meta = event.normalized_metadata();
        #[cfg(feature = "tracing-log")]
        let meta = normalized_meta.as_ref().unwrap_or_else(|| event.metadata());
        #[cfg(not(feature = "tracing-log"))]
        let meta = event.metadata();
        let level = meta.level();

        if self.config.report_logs_in_timings {
            let mark_name = format!(
                "c{:x}",
                self.last_event_id
                    .fetch_add(1, core::sync::atomic::Ordering::Relaxed)
            );
            // mark and measure so you can see a little blip in the profile
            mark(&mark_name);
            let _ = measure(
                format!(
                    "{} {}{} {}",
                    level,
                    meta.module_path().unwrap_or("..."),
                    thread_display_suffix(),
                    recorder,
                ),
                mark_name,
            );
        }

        let origin = if self.config.show_origin {
            meta.file()
                .and_then(|file| {
                    meta.line().map(|ln| {
                        format!(
                            "{}{}:{}",
                            self.config.origin_base_url.as_deref().unwrap_or_default(),
                            file,
                            ln
                        )
                    })
                })
                .unwrap_or_default()
        } else {
            String::new()
        };

        let fields = ctx
            .lookup_current()
            .and_then(|span| {
                span.extensions()
                    .get::<StringRecorder>()
                    .map(|span_recorder| {
                        span_recorder
                            .fields
                            .iter()
                            .map(|(key, value)| format!("\n\t{key}: {value}"))
                            .collect::<Vec<_>>()
                            .join("")
                    })
            })
            .unwrap_or_default();
        if self.config.color {
            log_with_color(
                format!(
                    "%c{}%c {}{}%c{}{}",
                    level,
                    origin,
                    thread_display_suffix(),
                    recorder,
                    fields
                ),
                level,
                self.config.use_console_methods,
            );
        } else {
            log(
                format!(
                    "{} {}{} {}{}",
                    level,
                    origin,
                    thread_display_suffix(),
                    recorder,
                    fields
                ),
                level,
                self.config.use_console_methods,
            );
        }
    }

    fn on_enter(&self, id: &tracing::Id, _ctx: Context<'_, S>) {
        if self.config.report_logs_in_timings {
            mark(&mark_name(id));
        }
    }

    fn on_exit(&self, id: &tracing::Id, ctx: Context<'_, S>) {
        if !self.config.report_logs_in_timings {
            return;
        }

        if let Some(span_ref) = ctx.span(id) {
            let meta = span_ref.metadata();
            if let Some(debug_record) = span_ref.extensions().get::<StringRecorder>() {
                let _ = measure(
                    format!(
                        "\"{}\"{} {} {}",
                        meta.name(),
                        thread_display_suffix(),
                        meta.module_path().unwrap_or("..."),
                        debug_record,
                    ),
                    mark_name(id),
                );
            } else {
                let _ = measure(
                    format!(
                        "\"{}\"{} {}",
                        meta.name(),
                        thread_display_suffix(),
                        meta.module_path().unwrap_or("..."),
                    ),
                    mark_name(id),
                );
            }
        }
    }
}

fn log(message: String, level: &Level, use_console_methods: bool) {
    if use_console_methods {
        match *level {
            Level::TRACE | Level::DEBUG => debug1(message),
            Level::INFO => log1(message),
            Level::WARN => warn1(message),
            Level::ERROR => error1(message),
        }
    } else {
        log1(message)
    }
}

fn log_with_color(message: String, level: &Level, use_console_methods: bool) {
    let level_log = if use_console_methods {
        match *level {
            Level::TRACE | Level::DEBUG => debug4,
            Level::INFO => log4,
            Level::WARN => warn4,
            Level::ERROR => error4,
        }
    } else {
        log4
    };
    level_log(
        message,
        level.color(),
        "color: gray; font-style: italic",
        "color: inherit",
    );
}

trait LevelExt {
    fn color(&self) -> &'static str;
}

impl LevelExt for Level {
    fn color(&self) -> &'static str {
        match *self {
            Level::TRACE => "color: dodgerblue; background: #444",
            Level::DEBUG => "color: lawngreen; background: #444",
            Level::INFO => "color: whitesmoke; background: #444",
            Level::WARN => "color: orange; background: #444",
            Level::ERROR => "color: red; background: #444",
        }
    }
}
