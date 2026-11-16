//! The JSON Schema features every Jazz runtime evaluates alike.
//!
//! Native runtimes validate JSON columns with the `jsonschema` crate; the
//! browser runtime with the JS validator in
//! `packages/jazz-tools/src/runtime/json-schema-validator.ts`, which refuses
//! the features it cannot evaluate as the crate does. A schema using one of
//! them would be enforced on a server and refused in a browser, so
//! deployments refuse it everywhere with
//! [`check_supported`]: the `$dynamicRef`/`$dynamicAnchor` keywords
//! (2020-12), `contentMediaType` (drafts 6 and 7), the internationalized
//! formats in the drafts that assert formats, and the regex constructs
//! [`regex`] lists.
//!
//! This walks the schema as the browser validator normalizes it: the
//! dialect comes from the root `$schema`, keywords a dialect does not define
//! are skipped with their subschemas, and so are the siblings of `$ref` in
//! drafts 4, 6 and 7.

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
mod dialect;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
mod regex;

use serde_json::Value;

/// A feature of a declared JSON Schema that Jazz does not support yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedJsonSchema {
    /// JSON Pointer to the schema object using the feature; empty for the
    /// root.
    pub location: String,
    pub feature: UnsupportedFeature,
}

/// What [`UnsupportedJsonSchema`] refuses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnsupportedFeature {
    /// A keyword, such as `$dynamicRef`.
    Keyword(String),
    /// An asserted `format`, such as `idn-hostname`.
    Format(String),
    /// A `pattern` or `patternProperties` key using a regex construct.
    Pattern { pattern: String, construct: String },
}

impl std::fmt::Display for UnsupportedJsonSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.feature {
            UnsupportedFeature::Keyword(keyword) => write!(
                f,
                "the `{keyword}` keyword is not supported in Jazz JSON Schemas yet"
            )?,
            UnsupportedFeature::Format(format) => write!(
                f,
                "the \"{format}\" format is not supported in Jazz JSON Schemas yet"
            )?,
            UnsupportedFeature::Pattern { pattern, construct } => write!(
                f,
                "the regular expression \"{pattern}\" is not supported in Jazz JSON Schemas \
                 yet: {construct}"
            )?,
        }
        if self.location.is_empty() {
            Ok(())
        } else {
            write!(f, " (at {})", self.location)
        }
    }
}

impl std::error::Error for UnsupportedJsonSchema {}

/// Refuse a declared schema that uses a feature not every Jazz runtime
/// evaluates. Meant for schemas [`super::json_schema::check_schema`]
/// accepts; a schema with an unknown `$schema` dialect is left to it.
///
/// The browser build checks nothing here: its host validator already
/// refuses these features when [`super::json_schema::check_schema`] compiles
/// the schema, and leaving the walk out keeps the WASM binary as it was.
pub fn check_supported(schema: &Value) -> Result<(), UnsupportedJsonSchema> {
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    return dialect::check_supported(schema);
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    {
        let _ = schema;
        Ok(())
    }
}
