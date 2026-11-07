//! JSON Schema validation for JSON columns.
//!
//! Native targets compile declared schemas with the `jsonschema` crate. The
//! browser build (`wasm32-unknown-unknown`) leaves that crate and its
//! regex/IDNA/bignum dependencies out of the binary: its JS host installs a
//! validator with [`install_host_validator`] instead, and until it does every
//! schema check fails with [`JsonSchemaError::Unavailable`].
//!
//! Compiled validators are cached per schema on the current thread, so a
//! column's schema is compiled once rather than on every write.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

/// Why a declared schema or a value was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonSchemaError {
    /// The declared schema itself is invalid.
    InvalidSchema(String),
    /// The value does not match the declared schema.
    Mismatch(String),
    /// No validator is available on this target, or the host's failed.
    Unavailable(String),
}

/// A validator the host of the browser build provides.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub trait HostJsonSchemaValidator {
    /// Compile a declared schema given as JSON text, or say why it is invalid.
    fn compile(&self, schema_json: &str) -> Result<Box<dyn HostCompiledJsonSchema>, String>;
}

/// A schema compiled by a [`HostJsonSchemaValidator`].
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub trait HostCompiledJsonSchema {
    /// Check a value given as JSON text: `Ok(None)` if it matches,
    /// `Ok(Some(reason))` if it does not, `Err` if the check itself failed.
    fn validate(&self, instance_json: &str) -> Result<Option<String>, String>;
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
thread_local! {
    static HOST_VALIDATOR: RefCell<Option<Box<dyn HostJsonSchemaValidator>>> =
        const { RefCell::new(None) };
}

/// Install the validator JSON columns are checked with on this thread.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub fn install_host_validator(validator: Box<dyn HostJsonSchemaValidator>) {
    HOST_VALIDATOR.with(|host| *host.borrow_mut() = Some(validator));
    COMPILED.with(|compiled| compiled.borrow_mut().clear());
}

enum Compiled {
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    Native(jsonschema::Validator),
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    Host(Box<dyn HostCompiledJsonSchema>),
}

/// Bound on cached validators; schemas are per column, so this is only hit by
/// processes that admit very many schemas, and they then recompile.
const MAX_COMPILED: usize = 256;

thread_local! {
    static COMPILED: RefCell<HashMap<String, Rc<Compiled>>> = RefCell::new(HashMap::new());
}

fn compiled(schema: &serde_json::Value) -> Result<Rc<Compiled>, JsonSchemaError> {
    let key = schema.to_string();
    if let Some(compiled) = COMPILED.with(|compiled| compiled.borrow().get(&key).cloned()) {
        return Ok(compiled);
    }
    let compiled = Rc::new(compile(schema, &key)?);
    COMPILED.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.len() >= MAX_COMPILED {
            cache.clear();
        }
        cache.insert(key, compiled.clone());
    });
    Ok(compiled)
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn compile(schema: &serde_json::Value, _schema_json: &str) -> Result<Compiled, JsonSchemaError> {
    jsonschema::options()
        .with_keyword("uniqueItems", unique_items::factory)
        .build(schema)
        .map(Compiled::Native)
        .map_err(|error| JsonSchemaError::InvalidSchema(error.to_string()))
}

/// `uniqueItems` by JSON Schema equality, with the same verdict on every run.
///
/// The `jsonschema` crate checks arrays of more than 15 items with a randomly
/// seeded hash set that hashes `0` and `-0.0` apart, so whether such an array
/// passes depended on the seed. Equal numbers are equal whatever their
/// spelling, so `[0, -0.0]` is never unique. Items are bucketed by a hash
/// that equal values share (numbers by their `f64` value with `-0.0` folded
/// into `0.0`, objects independent of key order) and compared within a
/// bucket by the crate's own equality.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
mod unique_items {
    use std::collections::HashMap;
    use std::hash::{DefaultHasher, Hash, Hasher};

    use jsonschema::paths::Location;
    use jsonschema::{Keyword, ValidationError};
    use serde_json::{Map, Value};

    struct UniqueItems;

    pub(super) fn factory<'a>(
        _parent: &'a Map<String, Value>,
        value: &'a Value,
        _location: Location,
    ) -> Result<Box<dyn Keyword>, ValidationError<'a>> {
        // The meta-schema has already required a boolean.
        Ok(match value {
            Value::Bool(true) => Box::new(UniqueItems),
            _ => Box::new(AnyItems),
        })
    }

    struct AnyItems;

    impl Keyword for AnyItems {
        fn validate<'i>(&self, _instance: &'i Value) -> Result<(), ValidationError<'i>> {
            Ok(())
        }

        fn is_valid(&self, _instance: &Value) -> bool {
            true
        }
    }

    impl Keyword for UniqueItems {
        fn validate<'i>(&self, instance: &'i Value) -> Result<(), ValidationError<'i>> {
            if self.is_valid(instance) {
                Ok(())
            } else {
                Err(ValidationError::custom(format!(
                    "{instance} has non-unique elements"
                )))
            }
        }

        fn is_valid(&self, instance: &Value) -> bool {
            match instance {
                Value::Array(items) => unique(items),
                _ => true,
            }
        }
    }

    pub(super) fn unique(items: &[Value]) -> bool {
        let mut buckets: HashMap<u64, Vec<&Value>> = HashMap::with_capacity(items.len());
        for item in items {
            let bucket = buckets.entry(value_hash(item)).or_default();
            if bucket
                .iter()
                .any(|seen| jsonschema::ext::cmp::equal(seen, item))
            {
                return false;
            }
            bucket.push(item);
        }
        true
    }

    /// A hash that values JSON Schema calls equal share. `DefaultHasher::new`
    /// has fixed keys, so the result does not vary between runs.
    fn value_hash(value: &Value) -> u64 {
        let mut hasher = DefaultHasher::new();
        hash_into(value, &mut hasher);
        hasher.finish()
    }

    fn hash_into(value: &Value, hasher: &mut DefaultHasher) {
        match value {
            Value::Null => 0u8.hash(hasher),
            Value::Bool(value) => (1u8, value).hash(hasher),
            Value::Number(number) => {
                // Equal numbers have the same `f64` value; `+ 0.0` turns
                // `-0.0` into `0.0`.
                let value = number.as_f64().unwrap_or(f64::NAN) + 0.0;
                (2u8, value.to_bits()).hash(hasher);
            }
            Value::String(value) => (3u8, value).hash(hasher),
            Value::Array(items) => {
                (4u8, items.len()).hash(hasher);
                for item in items {
                    hash_into(item, hasher);
                }
            }
            Value::Object(entries) => {
                // Key order does not matter to equality, so entries combine
                // by a commutative sum.
                let sum = entries.iter().fold(0u64, |sum, (key, value)| {
                    let mut entry = DefaultHasher::new();
                    key.hash(&mut entry);
                    hash_into(value, &mut entry);
                    sum.wrapping_add(entry.finish())
                });
                (5u8, entries.len(), sum).hash(hasher);
            }
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn compile(_schema: &serde_json::Value, schema_json: &str) -> Result<Compiled, JsonSchemaError> {
    HOST_VALIDATOR.with(|host| match host.borrow().as_ref() {
        Some(host) => host
            .compile(schema_json)
            .map(Compiled::Host)
            .map_err(JsonSchemaError::InvalidSchema),
        None => Err(JsonSchemaError::Unavailable(
            "no JSON Schema validator is installed in this WASM module".to_owned(),
        )),
    })
}

/// Check that a declared schema compiles.
pub fn check_schema(schema: &serde_json::Value) -> Result<(), JsonSchemaError> {
    compiled(schema).map(|_| ())
}

/// Check a value against a declared schema.
pub fn validate(
    schema: &serde_json::Value,
    instance: &serde_json::Value,
) -> Result<(), JsonSchemaError> {
    CompiledSchema::new(schema)?.validate(instance)
}

/// A declared schema compiled once, for a holder that checks many values
/// against it without looking its schema up again.
#[derive(Clone)]
pub struct CompiledSchema(Rc<Compiled>);

impl std::fmt::Debug for CompiledSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CompiledSchema")
    }
}

impl CompiledSchema {
    /// Compile a declared schema, or say why it is invalid.
    pub fn new(schema: &serde_json::Value) -> Result<Self, JsonSchemaError> {
        compiled(schema).map(Self)
    }

    /// Check a value against the schema.
    pub fn validate(&self, instance: &serde_json::Value) -> Result<(), JsonSchemaError> {
        match &*self.0 {
            #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
            Compiled::Native(validator) => validator
                .validate(instance)
                .map_err(|error| JsonSchemaError::Mismatch(error.to_string())),
            // The host gets the value re-serialized rather than its source text, so
            // it sees the numbers `serde_json` parsed, exactly as the native
            // validator does.
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Compiled::Host(validator) => match validator.validate(&instance.to_string()) {
                Ok(None) => Ok(()),
                Ok(Some(reason)) => Err(JsonSchemaError::Mismatch(reason)),
                Err(error) => Err(JsonSchemaError::Unavailable(format!(
                    "the JSON Schema validator failed: {error}"
                ))),
            },
        }
    }
}
