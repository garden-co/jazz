#[expect(
    dead_code,
    reason = "This regression uses only enrolment, insert and query"
)]
#[path = "../src/client_worker.rs"]
mod client_worker;
#[path = "../../../../crates/jazz-testkit/src/permissions.rs"]
mod permissions_support;

include!("../../../todo-server-rs/tests/support/native_user_cases.rs");
