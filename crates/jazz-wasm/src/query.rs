//! QueryBuilder wrapper for WASM.
//!
//! Provides a JavaScript-friendly interface to the Jazz QueryBuilder.

use wasm_bindgen::prelude::*;

use jazz_tools::query_manager::query::{Query, QueryBuilder as JazzQueryBuilder};
use jazz_tools::query_manager::{parse_query_json, parse_query_value};

use crate::types::Value;

/// WASM-exposed QueryBuilder with camelCase methods.
#[wasm_bindgen]
pub struct WasmQueryBuilder {
    inner: JazzQueryBuilder,
}

#[wasm_bindgen]
impl WasmQueryBuilder {
    /// Create a new QueryBuilder for a table.
    #[wasm_bindgen(constructor)]
    pub fn new(table: &str) -> Self {
        Self {
            inner: JazzQueryBuilder::new(table),
        }
    }

    /// Set the branch to query.
    #[wasm_bindgen(js_name = branch)]
    pub fn branch(mut self, branch: &str) -> Self {
        self.inner = self.inner.branch(branch);
        self
    }

    /// Set multiple branches to query.
    #[wasm_bindgen(js_name = branches)]
    pub fn branches(mut self, branches: Vec<String>) -> Self {
        let branch_refs: Vec<&str> = branches.iter().map(|s| s.as_str()).collect();
        self.inner = self.inner.branches(&branch_refs);
        self
    }

    /// Add an equals filter.
    #[wasm_bindgen(js_name = filterEq)]
    pub fn filter_eq(mut self, column: &str, value: JsValue) -> Result<WasmQueryBuilder, JsError> {
        let value: Value = serde_wasm_bindgen::from_value(value)?;
        self.inner = self.inner.filter_eq(column, value);
        Ok(self)
    }

    /// Add a not-equals filter.
    #[wasm_bindgen(js_name = filterNe)]
    pub fn filter_ne(mut self, column: &str, value: JsValue) -> Result<WasmQueryBuilder, JsError> {
        let value: Value = serde_wasm_bindgen::from_value(value)?;
        self.inner = self.inner.filter_ne(column, value);
        Ok(self)
    }

    /// Add a less-than filter.
    #[wasm_bindgen(js_name = filterLt)]
    pub fn filter_lt(mut self, column: &str, value: JsValue) -> Result<WasmQueryBuilder, JsError> {
        let value: Value = serde_wasm_bindgen::from_value(value)?;
        self.inner = self.inner.filter_lt(column, value);
        Ok(self)
    }

    /// Add a less-than-or-equal filter.
    #[wasm_bindgen(js_name = filterLe)]
    pub fn filter_le(mut self, column: &str, value: JsValue) -> Result<WasmQueryBuilder, JsError> {
        let value: Value = serde_wasm_bindgen::from_value(value)?;
        self.inner = self.inner.filter_le(column, value);
        Ok(self)
    }

    /// Add a greater-than filter.
    #[wasm_bindgen(js_name = filterGt)]
    pub fn filter_gt(mut self, column: &str, value: JsValue) -> Result<WasmQueryBuilder, JsError> {
        let value: Value = serde_wasm_bindgen::from_value(value)?;
        self.inner = self.inner.filter_gt(column, value);
        Ok(self)
    }

    /// Add a greater-than-or-equal filter.
    #[wasm_bindgen(js_name = filterGe)]
    pub fn filter_ge(mut self, column: &str, value: JsValue) -> Result<WasmQueryBuilder, JsError> {
        let value: Value = serde_wasm_bindgen::from_value(value)?;
        self.inner = self.inner.filter_ge(column, value);
        Ok(self)
    }

    /// Start a new OR branch.
    #[wasm_bindgen(js_name = or)]
    pub fn or(mut self) -> Self {
        self.inner = self.inner.or();
        self
    }

    /// Add ascending order by.
    #[wasm_bindgen(js_name = orderBy)]
    pub fn order_by(mut self, column: &str) -> Self {
        self.inner = self.inner.order_by(column);
        self
    }

    /// Add descending order by.
    #[wasm_bindgen(js_name = orderByDesc)]
    pub fn order_by_desc(mut self, column: &str) -> Self {
        self.inner = self.inner.order_by_desc(column);
        self
    }

    /// Set a limit.
    #[wasm_bindgen(js_name = limit)]
    pub fn limit(mut self, n: usize) -> Self {
        self.inner = self.inner.limit(n);
        self
    }

    /// Set an offset.
    #[wasm_bindgen(js_name = offset)]
    pub fn offset(mut self, n: usize) -> Self {
        self.inner = self.inner.offset(n);
        self
    }

    /// Include soft-deleted rows.
    #[wasm_bindgen(js_name = includeDeleted)]
    pub fn include_deleted(mut self) -> Self {
        self.inner = self.inner.include_deleted();
        self
    }

    /// Select specific columns.
    #[wasm_bindgen(js_name = select)]
    pub fn select(mut self, columns: Vec<String>) -> Self {
        let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        self.inner = self.inner.select(&col_refs);
        self
    }

    /// Set a table alias.
    #[wasm_bindgen(js_name = alias)]
    pub fn alias(mut self, alias: &str) -> Self {
        self.inner = self.inner.alias(alias);
        self
    }

    /// Join another table.
    #[wasm_bindgen(js_name = join)]
    pub fn join(mut self, table: &str) -> Self {
        self.inner = self.inner.join(table);
        self
    }

    /// Specify join condition.
    #[wasm_bindgen(js_name = on)]
    pub fn on(mut self, left_col: &str, right_col: &str) -> Self {
        self.inner = self.inner.on(left_col, right_col);
        self
    }

    /// Build the query and return as JSON string.
    #[wasm_bindgen(js_name = build)]
    pub fn build(self) -> Result<String, JsError> {
        let query = self
            .inner
            .try_build()
            .map_err(|e| JsError::new(&format!("Query build error: {}", e)))?;
        serde_json::to_string(&query).map_err(|e| JsError::new(&format!("Serialize error: {}", e)))
    }

    /// Build and return as JsValue.
    #[wasm_bindgen(js_name = buildJs)]
    pub fn build_js(self) -> Result<JsValue, JsError> {
        let query = self
            .inner
            .try_build()
            .map_err(|e| JsError::new(&format!("Query build error: {}", e)))?;
        serde_wasm_bindgen::to_value(&query).map_err(|e| JsError::new(&e.to_string()))
    }
}

/// Parse a Query from JSON string.
pub fn parse_query(json: &str) -> Result<Query, String> {
    parse_query_json(json)
}

/// Parse a Query from JsValue.
pub fn parse_query_js(value: JsValue) -> Result<Query, String> {
    let query_json: serde_json::Value =
        serde_wasm_bindgen::from_value(value).map_err(|e| format!("Parse error: {}", e))?;
    parse_query_value(query_json)
}
