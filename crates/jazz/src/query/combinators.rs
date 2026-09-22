/// Construct a column operand.
///
/// ```rust
/// # use jazz::query::{col, doctest_support, eq, lit, Query};
/// let query = Query::from("issues").filter(eq(col("state"), lit("open")));
///
/// query.validate(&doctest_support::schema())?;
/// # Ok::<(), jazz::query::QueryError>(())
/// ```
pub fn col(name: impl Into<String>) -> Operand {
    Operand::Column(name.into())
}

/// Construct a parameter operand.
///
/// ```rust
/// # use jazz::query::{col, doctest_support, eq, param, Query};
/// let query = Query::from("issues").filter(eq(col("assignee"), param("user")));
///
/// query.validate(&doctest_support::schema())?;
/// # Ok::<(), jazz::query::QueryError>(())
/// ```
pub fn param(name: impl Into<String>) -> Operand {
    Operand::Param(name.into())
}

/// Construct a claim operand.
pub fn claim(name: impl Into<String>) -> Operand {
    Operand::Claim(name.into())
}

/// Construct a literal operand.
///
/// ```rust
/// # use jazz::query::{col, doctest_support, eq, lit, Query};
/// let query = Query::from("issues").filter(eq(col("state"), lit("open")));
///
/// query.validate(&doctest_support::schema())?;
/// # Ok::<(), jazz::query::QueryError>(())
/// ```
pub fn lit(value: impl Into<Value>) -> Operand {
    Operand::Literal(value.into())
}

/// Construct an equality predicate.
///
/// ```rust
/// # use jazz::query::{col, doctest_support, eq, lit, Query};
/// let query = Query::from("issues").filter(eq(col("state"), lit("open")));
///
/// query.validate(&doctest_support::schema())?;
/// # Ok::<(), jazz::query::QueryError>(())
/// ```
pub fn eq(left: Operand, right: Operand) -> Predicate {
    Predicate::Eq(left, right)
}

/// Construct an inequality predicate.
pub fn ne(left: Operand, right: Operand) -> Predicate {
    Predicate::Ne(left, right)
}

/// Construct an all-of predicate.
///
/// ```rust
/// # use jazz::query::{all_of, col, doctest_support, eq, gt, lit, Query};
/// let query = Query::from("issues").filter(all_of([
///     eq(col("state"), lit("open")),
///     gt(col("priority"), lit(1_u64)),
/// ]));
///
/// query.validate(&doctest_support::schema())?;
/// # Ok::<(), jazz::query::QueryError>(())
/// ```
pub fn all_of(predicates: impl IntoIterator<Item = Predicate>) -> Predicate {
    Predicate::All(predicates.into_iter().collect())
}

/// Construct an any-of predicate.
///
/// ```rust
/// # use jazz::query::{any_of, col, doctest_support, eq, lit, Query};
/// let query = Query::from("issues").filter(any_of([
///     eq(col("state"), lit("open")),
///     eq(col("state"), lit("triage")),
/// ]));
///
/// query.validate(&doctest_support::schema())?;
/// # Ok::<(), jazz::query::QueryError>(())
/// ```
pub fn any_of(predicates: impl IntoIterator<Item = Predicate>) -> Predicate {
    Predicate::Any(predicates.into_iter().collect())
}

/// Construct a negated predicate.
///
/// ```rust
/// # use jazz::query::{col, doctest_support, eq, lit, not, Query};
/// let query = Query::from("issues").filter(not(eq(col("state"), lit("closed"))));
///
/// query.validate(&doctest_support::schema())?;
/// # Ok::<(), jazz::query::QueryError>(())
/// ```
pub fn not(predicate: Predicate) -> Predicate {
    Predicate::Not(Box::new(predicate))
}

/// Construct an `IN` predicate.
///
/// ```rust
/// # use jazz::query::{col, doctest_support, in_list, lit, Query};
/// let query = Query::from("issues")
///     .filter(in_list(col("state"), [lit("open"), lit("triage")]));
///
/// query.validate(&doctest_support::schema())?;
/// # Ok::<(), jazz::query::QueryError>(())
/// ```
pub fn in_list(left: Operand, values: impl IntoIterator<Item = Operand>) -> Predicate {
    Predicate::In(left, values.into_iter().collect())
}

/// Construct a greater-than predicate.
///
/// ```rust
/// # use jazz::query::{col, doctest_support, gt, lit, Query};
/// let query = Query::from("issues").filter(gt(col("priority"), lit(3_u64)));
///
/// query.validate(&doctest_support::schema())?;
/// # Ok::<(), jazz::query::QueryError>(())
/// ```
pub fn gt(left: Operand, right: Operand) -> Predicate {
    Predicate::Gt(left, right)
}

/// Construct a greater-than-or-equal predicate.
pub fn gte(left: Operand, right: Operand) -> Predicate {
    Predicate::Gte(left, right)
}

/// Construct a less-than predicate.
///
/// ```rust
/// # use jazz::query::{col, doctest_support, lit, lt, Query};
/// let query = Query::from("issues").filter(lt(col("priority"), lit(10_u64)));
///
/// query.validate(&doctest_support::schema())?;
/// # Ok::<(), jazz::query::QueryError>(())
/// ```
pub fn lt(left: Operand, right: Operand) -> Predicate {
    Predicate::Lt(left, right)
}

/// Construct a less-than-or-equal predicate.
pub fn lte(left: Operand, right: Operand) -> Predicate {
    Predicate::Lte(left, right)
}

/// Construct a string substring or array membership predicate.
pub fn contains(left: Operand, right: Operand) -> Predicate {
    Predicate::Contains(left, right)
}

/// Construct a nullable-is-null predicate.
pub fn is_null(operand: Operand) -> Predicate {
    Predicate::IsNull(operand)
}
