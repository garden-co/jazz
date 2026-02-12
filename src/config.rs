///Configuration parameters for the [WasmLayer](crate::prelude::WasmLayer).
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct WasmLayerConfig {
    /// determines if any reporting should occur.
    ///
    /// `true` by default.
    pub enabled: bool,
    /// In dev-tools, report timings of traces.
    ///
    /// `true` by default.
    pub report_logs_in_timings: bool,

    /// emits the traces in either colorful or colorless fashion.
    ///
    /// `true` by default.
    pub color: bool,

    /// Enabling this value will emit traces to their corresponding
    /// console method.
    ///
    /// `false` by default.
    ///
    /// | Level | Console Method |
    /// | ----- | -------------- |
    /// | `TRACE` | `console.debug` |
    /// | `DEBUG` | `console.debug` |
    /// | `INFO` | `console.log` |
    /// | `WARN` | `console.warn` |
    pub use_console_methods: bool,
    /// Maximum log level
    pub max_level: tracing::Level,
    /// Show/hide fields of types
    pub show_fields: bool,
    /// Show origin (line number, source)
    pub show_origin: bool,
    /// Optional URL to prepend to origins. E.g. to allow for showing full file paths that can be navigated when logged in the browser console.
    pub origin_base_url: Option<String>,
}

impl Default for WasmLayerConfig {
    fn default() -> Self {
        WasmLayerConfig {
            enabled: true,
            report_logs_in_timings: true,
            use_console_methods: false,
            color: true,
            max_level: tracing::Level::TRACE,
            show_fields: true,
            show_origin: true,
            origin_base_url: None,
        }
    }
}

impl WasmLayerConfig {
    /// Create a default [WasmLayerConfig]
    pub fn new() -> WasmLayerConfig {
        WasmLayerConfig::default()
    }

    pub fn disable(mut self) -> Self {
        self.enabled = false;
        self
    }

    /// Disables events from appearing in performance timings
    pub fn remove_timings(mut self) -> Self {
        self.report_logs_in_timings = false;
        self
    }

    /// Removes color from the logs
    pub fn with_colorless_logs(mut self) -> Self {
        self.color = false;
        self
    }

    /// Set the maximal level on which events should be displayed
    pub fn with_max_level(mut self, max_level: tracing::Level) -> Self {
        self.max_level = max_level;
        self
    }

    /// Removes the line number and source from the logs
    pub fn remove_origin(mut self) -> Self {
        self.show_origin = false;
        self
    }

    /// Removes the fields of types from the logs
    pub fn remove_fields(mut self) -> Self {
        self.show_fields = false;
        self
    }

    /// Set the base URL for origins. This can be used to show full file paths in the browser console.
    pub fn with_origin_base_url(mut self, origin_base_url: impl ToString) -> Self {
        self.origin_base_url = Some(origin_base_url.to_string());
        self
    }
}

#[test]
fn test_default_built_config() {
    let config = WasmLayerConfig::new();

    assert_eq!(
        config,
        WasmLayerConfig {
            enabled: true,
            report_logs_in_timings: true,
            color: true,
            use_console_methods: false,
            max_level: tracing::Level::TRACE,
            show_fields: true,
            show_origin: true,
            origin_base_url: None
        }
    )
}

#[test]
fn test_set_report_logs_in_timings() {
    let config = WasmLayerConfig::new().remove_timings();

    assert!(!config.report_logs_in_timings);
}

#[test]
fn test_set_console_config_no_reporting() {
    let config = WasmLayerConfig::new().disable();

    assert!(!config.enabled);
}

#[test]
fn test_set_console_config_without_color() {
    let config = WasmLayerConfig::new().with_colorless_logs();
    assert!(!config.color);
}

#[test]
fn test_set_config_log_level_warn() {
    let config = WasmLayerConfig::new().with_max_level(tracing::Level::WARN);

    assert_eq!(config.max_level, tracing::Level::WARN);
}
