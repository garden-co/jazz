use crate::layout::{quote_ident, system_column};
use crate::schema::{FieldDef, TableDef};
use rusqlite::types::Value as SqlValue;

#[derive(Clone, Debug)]
pub struct Query {
    pub(crate) table: String,
    pub(crate) filters: Vec<Filter>,
    pub(crate) include: Option<Include>,
    pub(crate) order: Option<Order>,
    pub(crate) limit: Option<usize>,
}

pub fn query(table: &str) -> Query {
    Query {
        table: table.to_owned(),
        filters: Vec::new(),
        include: None,
        order: None,
        limit: None,
    }
}

impl Query {
    pub fn filter(mut self, filter: Filter) -> Self {
        self.filters.push(filter);
        self
    }

    pub fn include_required(mut self, alias: &str, fk_column: &str) -> Self {
        self.include = Some(Include {
            alias: alias.to_owned(),
            fk_column: fk_column.to_owned(),
            required: true,
        });
        self
    }

    pub fn include_optional(mut self, alias: &str, fk_column: &str) -> Self {
        self.include = Some(Include {
            alias: alias.to_owned(),
            fk_column: fk_column.to_owned(),
            required: false,
        });
        self
    }

    pub fn order_by(mut self, column: &str, direction: SortDirection) -> Self {
        self.order = Some(Order {
            column: column.to_owned(),
            direction,
        });
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Include {
    pub(crate) alias: String,
    pub(crate) fk_column: String,
    pub(crate) required: bool,
}

pub(crate) struct LoweredInclude<'a> {
    pub(crate) alias: String,
    pub(crate) required: bool,
    pub(crate) fk_field: &'a FieldDef,
    pub(crate) table: &'a TableDef,
}

#[derive(Clone, Debug)]
pub struct Filter {
    pub(crate) column: String,
    op: FilterOp,
    pub(crate) value: FilterValue,
}

pub fn eq(column: &str, value: impl Into<FilterValue>) -> Filter {
    Filter {
        column: column.to_owned(),
        op: FilterOp::Eq,
        value: value.into(),
    }
}

pub fn gt(column: &str, value: impl Into<FilterValue>) -> Filter {
    Filter {
        column: column.to_owned(),
        op: FilterOp::Gt,
        value: value.into(),
    }
}

#[derive(Clone, Debug)]
enum FilterOp {
    Eq,
    Gt,
}

#[derive(Clone, Debug)]
pub enum FilterValue {
    Bool(bool),
    Int(i64),
    Text(String),
}

impl FilterValue {
    pub(crate) fn to_sql_value(&self) -> SqlValue {
        match self {
            Self::Bool(value) => SqlValue::Integer(i64::from(*value)),
            Self::Int(value) => SqlValue::Integer(*value),
            Self::Text(value) => SqlValue::Text(value.clone()),
        }
    }
}

impl From<bool> for FilterValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i64> for FilterValue {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<i32> for FilterValue {
    fn from(value: i32) -> Self {
        Self::Int(value.into())
    }
}

impl From<&str> for FilterValue {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Order {
    pub(crate) column: String,
    pub(crate) direction: SortDirection,
}

#[derive(Clone, Copy, Debug)]
pub enum SortDirection {
    Asc,
    Desc,
}

pub use SortDirection::Desc;

pub(crate) fn filter_sql(alias: &str, filter: &Filter) -> String {
    let op = match filter.op {
        FilterOp::Eq => "=",
        FilterOp::Gt => ">",
    };
    format!("{} {op} ?", aliased_column(alias, &filter.column))
}

pub(crate) fn filter_scope_parts(filter: &Filter) -> (&str, &str, String) {
    let op = match filter.op {
        FilterOp::Eq => "=",
        FilterOp::Gt => ">",
    };
    let value = match &filter.value {
        FilterValue::Bool(value) => value.to_string(),
        FilterValue::Int(value) => value.to_string(),
        FilterValue::Text(value) => value.clone(),
    };
    (&filter.column, op, value)
}

fn aliased_column(alias: &str, column: &str) -> String {
    let col = system_column(column)
        .map(str::to_owned)
        .unwrap_or_else(|| quote_ident(column));
    format!("{alias}.{col}")
}
