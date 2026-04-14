#![cfg(feature = "test")]

#[macro_use]
extern crate jazz_tools;

#[path = "../support/mod.rs"]
mod support;

fn explicit_allow_all_policies(
    mut policies: jazz_tools::query_manager::types::TablePolicies,
) -> jazz_tools::query_manager::types::TablePolicies {
    use jazz_tools::query_manager::policy::PolicyExpr;

    if policies.select.using.is_none() {
        policies.select.using = Some(PolicyExpr::True);
    }
    if policies.insert.with_check.is_none() {
        policies.insert.with_check = Some(PolicyExpr::True);
    }
    if policies.update.using.is_none() && policies.update.with_check.is_none() {
        policies.update.using = Some(PolicyExpr::True);
        policies.update.with_check = Some(PolicyExpr::True);
    }
    if policies.delete.using.is_none() {
        policies.delete.using = Some(PolicyExpr::True);
    }

    policies
}

mod authorship_policies;
mod claims_policies;
mod complex_policies;
mod inherited_policies;
mod recursive_policies;
mod session_cases;
mod simple_policies;
