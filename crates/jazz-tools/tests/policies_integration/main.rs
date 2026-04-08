#![cfg(feature = "test")]

#[macro_use]
extern crate jazz_tools;

#[path = "../support/mod.rs"]
mod support;

mod authorship_policies;
mod claims_policies;
mod complex_policies;
mod inherited_policies;
mod recursive_policies;
mod session_cases;
mod simple_policies;
