#[allow(dead_code)]
mod support {
    pub use jazz_testkit::*;
}

#[allow(dead_code)]
mod common;
// Enable the copied suites one at a time as each public-API migration lands.
mod joins;
mod pagination;
mod recursive_queries;
mod subqueries;
mod subscriptions;
