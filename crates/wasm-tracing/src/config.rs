///Configuration parameters for the [WasmLayer](crate::prelude::WasmLayer).
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct WasmLayerConfig {
    /// determines if any reporting should occur.
    ///
    /// `true` by default.
    pub enabled: bool,
    /// Maximum log level
    pub max_level: tracing::Level,
    /// Log span enter/exit as `console.groupCollapsed` / `console.groupEnd`.
    ///
    /// `false` by default.
    pub console_group_spans: bool,
}

impl Default for WasmLayerConfig {
    fn default() -> Self {
        WasmLayerConfig {
            enabled: true,
            max_level: tracing::Level::TRACE,
            console_group_spans: false,
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

    /// Set the maximal level on which events should be displayed
    pub fn with_max_level(mut self, max_level: tracing::Level) -> Self {
        self.max_level = max_level;
        self
    }

    /// Log span enter/exit as `console.groupCollapsed` / `console.groupEnd`.
    pub fn with_console_group_spans(mut self) -> Self {
        self.console_group_spans = true;
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
            max_level: tracing::Level::TRACE,
            console_group_spans: false,
        }
    )
}

#[test]
fn test_set_console_config_no_reporting() {
    let config = WasmLayerConfig::new().disable();

    assert!(!config.enabled);
}

#[test]
fn test_set_config_log_level_warn() {
    let config = WasmLayerConfig::new().with_max_level(tracing::Level::WARN);

    assert_eq!(config.max_level, tracing::Level::WARN);
}
