#![cfg(feature = "test")]

#[path = "../support/mod.rs"]
#[allow(dead_code)]
mod support;

mod common;
mod joins;
mod pagination;
mod recursive_queries;
mod subqueries;
mod subscriptions;
