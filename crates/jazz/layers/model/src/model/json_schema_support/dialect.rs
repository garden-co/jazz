//! The browser validator's view of a schema: which keywords it walks into,
//! per dialect, and which it refuses.

use serde_json::Value;

use super::{UnsupportedFeature, UnsupportedJsonSchema, regex};

pub(super) fn check_supported(schema: &Value) -> Result<(), UnsupportedJsonSchema> {
    match dialect_of(schema) {
        Some(dialect) => walk(schema, dialect, ""),
        None => Ok(()),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Dialect {
    Draft4,
    Draft6,
    Draft7,
    Draft2019,
    Draft2020,
}

fn dialect_of(schema: &Value) -> Option<Dialect> {
    let Some(declared) = schema.get("$schema").and_then(Value::as_str) else {
        return Some(Dialect::Draft2020);
    };
    let uri = declared.trim_end_matches('#');
    let uri = uri
        .strip_prefix("https://")
        .or_else(|| uri.strip_prefix("http://"))
        .unwrap_or(uri);
    match uri {
        "json-schema.org/draft/2020-12/schema" => Some(Dialect::Draft2020),
        "json-schema.org/draft/2019-09/schema" => Some(Dialect::Draft2019),
        "json-schema.org/draft-07/schema" => Some(Dialect::Draft7),
        "json-schema.org/draft-06/schema" => Some(Dialect::Draft6),
        "json-schema.org/draft-04/schema" => Some(Dialect::Draft4),
        _ => None,
    }
}

impl Dialect {
    fn legacy(self) -> bool {
        matches!(self, Self::Draft4 | Self::Draft6 | Self::Draft7)
    }

    /// Keywords holding one subschema.
    fn single(self, keyword: &str) -> bool {
        match keyword {
            "additionalProperties" | "items" | "not" => true,
            "additionalItems" => self != Self::Draft2020,
            "contains" | "propertyNames" => self != Self::Draft4,
            "if" | "then" | "else" => !matches!(self, Self::Draft4 | Self::Draft6),
            "unevaluatedItems" | "unevaluatedProperties" => !self.legacy(),
            _ => false,
        }
    }

    /// Keywords holding an array of subschemas.
    fn array(self, keyword: &str) -> bool {
        match keyword {
            "allOf" | "anyOf" | "oneOf" => true,
            "items" => self != Self::Draft2020,
            "prefixItems" => self == Self::Draft2020,
            _ => false,
        }
    }

    /// Keywords holding a map of subschemas.
    fn map(self, keyword: &str) -> bool {
        match keyword {
            "definitions" | "dependencies" | "patternProperties" | "properties" => true,
            "$defs" | "dependentSchemas" => !self.legacy(),
            _ => false,
        }
    }

    /// Keywords holding plain values.
    fn plain(self, keyword: &str) -> bool {
        match keyword {
            "$ref" | "enum" | "exclusiveMaximum" | "exclusiveMinimum" | "maxItems"
            | "maxLength" | "maxProperties" | "maximum" | "minItems" | "minLength"
            | "minProperties" | "minimum" | "multipleOf" | "pattern" | "required" | "type"
            | "uniqueItems" => true,
            "id" => self == Self::Draft4,
            "$id" | "const" => self != Self::Draft4,
            // `format` only annotates from 2019-09 on.
            "format" => self.legacy(),
            "$anchor" | "dependentRequired" | "maxContains" | "minContains" => !self.legacy(),
            "$recursiveAnchor" | "$recursiveRef" => self == Self::Draft2019,
            _ => false,
        }
    }

    fn defines(self, keyword: &str) -> bool {
        self.single(keyword) || self.array(keyword) || self.map(keyword) || self.plain(keyword)
    }

    /// Keywords the dialect defines that the browser cannot evaluate.
    fn unsupported_keyword(self, keyword: &str) -> bool {
        match self {
            Self::Draft6 | Self::Draft7 => keyword == "contentMediaType",
            Self::Draft2020 => matches!(keyword, "$dynamicAnchor" | "$dynamicRef"),
            Self::Draft4 | Self::Draft2019 => false,
        }
    }

    /// `format`s the native runtime asserts in this dialect.
    fn asserts_format(self, format: &str) -> bool {
        let draft4 = matches!(
            format,
            "date"
                | "date-time"
                | "email"
                | "hostname"
                | "idn-email"
                | "ipv4"
                | "ipv6"
                | "regex"
                | "time"
                | "uri"
        );
        let draft6 = draft4 || matches!(format, "json-pointer" | "uri-reference" | "uri-template");
        let draft7 = draft6
            || matches!(
                format,
                "idn-hostname" | "iri" | "iri-reference" | "relative-json-pointer"
            );
        match self {
            Self::Draft4 => draft4,
            Self::Draft6 => draft6,
            Self::Draft7 => draft7,
            Self::Draft2019 | Self::Draft2020 => false,
        }
    }
}

/// Every keyword the browser's evaluator acts on, whatever the draft.
const VALIDATOR_KEYWORDS: &[&str] = &[
    "$anchor",
    "$id",
    "$recursiveAnchor",
    "$recursiveRef",
    "$ref",
    "additionalItems",
    "additionalProperties",
    "allOf",
    "anyOf",
    "const",
    "contains",
    "dependencies",
    "dependentRequired",
    "dependentSchemas",
    "else",
    "enum",
    "exclusiveMaximum",
    "exclusiveMinimum",
    "format",
    "id",
    "if",
    "items",
    "maxContains",
    "maxItems",
    "maxLength",
    "maxProperties",
    "maximum",
    "minContains",
    "minItems",
    "minLength",
    "minProperties",
    "minimum",
    "multipleOf",
    "not",
    "oneOf",
    "pattern",
    "patternProperties",
    "prefixItems",
    "properties",
    "propertyNames",
    "required",
    "then",
    "type",
    "unevaluatedItems",
    "unevaluatedProperties",
    "uniqueItems",
];

/// Asserted `format`s the browser has no check for (they need IDNA tables).
const UNSUPPORTED_FORMATS: &[&str] = &["idn-email", "idn-hostname", "iri", "iri-reference"];

fn walk(schema: &Value, dialect: Dialect, location: &str) -> Result<(), UnsupportedJsonSchema> {
    let Value::Object(entries) = schema else {
        return Ok(());
    };
    let unsupported = |feature| {
        Err(UnsupportedJsonSchema {
            location: location.to_owned(),
            feature,
        })
    };
    // Drafts 4-7 ignore every keyword next to `$ref`.
    let ref_only = dialect.legacy() && entries.get("$ref").is_some_and(Value::is_string);
    for (keyword, value) in entries {
        if dialect.unsupported_keyword(keyword) {
            return unsupported(UnsupportedFeature::Keyword(keyword.clone()));
        }
        if VALIDATOR_KEYWORDS.contains(&keyword.as_str())
            && (!dialect.defines(keyword) || (ref_only && keyword != "$ref"))
        {
            continue;
        }
        let patterns: Vec<&str> = match (keyword.as_str(), value) {
            ("pattern", Value::String(pattern)) => vec![pattern],
            ("patternProperties", Value::Object(properties)) => {
                properties.keys().map(String::as_str).collect()
            }
            _ => Vec::new(),
        };
        for pattern in patterns {
            if let Some(construct) = regex::browser_refusal(pattern) {
                return unsupported(UnsupportedFeature::Pattern {
                    pattern: pattern.to_owned(),
                    construct,
                });
            }
        }
        if let ("format", Value::String(format)) = (keyword.as_str(), value) {
            if dialect.asserts_format(format) && UNSUPPORTED_FORMATS.contains(&format.as_str()) {
                return unsupported(UnsupportedFeature::Format(format.clone()));
            }
        }
        let child = |key: &str| format!("{location}/{}", key.replace('~', "~0").replace('/', "~1"));
        match value {
            Value::Array(items) if dialect.array(keyword) => {
                for (index, item) in items.iter().enumerate() {
                    walk(item, dialect, &format!("{}/{index}", child(keyword)))?;
                }
            }
            _ if dialect.single(keyword) => walk(value, dialect, &child(keyword))?,
            Value::Object(map) if dialect.map(keyword) => {
                let location = child(keyword);
                for (key, entry) in map {
                    // `dependencies` values may be property-name arrays.
                    let child = format!("{location}/{}", key.replace('~', "~0").replace('/', "~1"));
                    walk(entry, dialect, &child)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}
