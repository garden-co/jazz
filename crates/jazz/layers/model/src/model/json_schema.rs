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
    jsonschema::validator_for(schema)
        .map(Compiled::Native)
        .map_err(|error| JsonSchemaError::InvalidSchema(error.to_string()))
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
    match &*compiled(schema)? {
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
