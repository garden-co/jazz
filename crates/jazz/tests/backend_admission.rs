//! An app backend's admission of a client's socket prelude, the one rule
//! `createJazz` and the native endpoint share.
//!
//! Lower-level than a socket test, because the decision is a pure function
//! that three hosts drive through their bindings: the table pins what shipped
//! `createJazz` decided for each shape, and the hosts' socket tests cover
//! that they ask it.

use jazz::tools::backend_admission::{
    Access, Account, Authenticated, BackendClient, Facts, Lookup, Refusal, Step, Who, admit,
    pin_bundle,
};
use serde::Deserialize;
use serde_json::{Value, json};

const AUTHENTICATED_AT_MS: u64 = 1_800_000_000_000;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Table {
    app_id: String,
    backend_secret: String,
    now_ms: u64,
    issuer: String,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Case {
    name: String,
    prelude: String,
    client: BackendClient,
    endpoints: Vec<String>,
    device_accepted: bool,
    user: Option<String>,
    claims: Value,
    account: Option<String>,
    shipped: Value,
    changed: Option<Value>,
}

fn table() -> Table {
    serde_json::from_str(include_str!("../fixtures/backend_admission_v1.json")).unwrap()
}

impl Table {
    fn facts(&self) -> Facts {
        Facts {
            backend_secret: self.backend_secret.clone(),
            app_id: self.app_id.clone(),
            now_ms: self.now_ms,
            endpoints: Vec::new(),
            client: None,
            device_accepted: None,
            user: None,
            account: None,
        }
    }

    /// Drive `case` as a host does: the lookups it ran, and the outcome.
    fn run(&self, case: &Case) -> (Vec<&'static str>, Value) {
        let mut facts = self.facts();
        let mut ran = Vec::new();
        loop {
            assert!(ran.len() <= 4, "{}: admission never settles", case.name);
            match admit(case.prelude.as_bytes(), &facts).unwrap() {
                Step::Lookup(Lookup::BackendClient) => {
                    ran.push("backendClient");
                    facts.client = Some(case.client);
                    facts.endpoints = case.endpoints.clone();
                }
                Step::Lookup(Lookup::DeviceAccount { .. }) => {
                    ran.push("deviceAccount");
                    facts.device_accepted = Some(case.device_accepted);
                }
                Step::Lookup(Lookup::User) => {
                    ran.push("user");
                    facts.user = Some(Authenticated {
                        user_id: case.user.clone(),
                        authenticated_at_ms: AUTHENTICATED_AT_MS,
                    });
                }
                Step::Lookup(Lookup::Account) => {
                    ran.push("account");
                    facts.account = Some(Account {
                        id: case.account.clone(),
                        issuer: self.issuer.clone(),
                        claims: case.claims.clone(),
                    });
                }
                Step::Refused(refusal) => {
                    let refused = match refusal {
                        Refusal::Missing => "missing",
                        Refusal::Unverified | Refusal::UnverifiedDeviceProof { .. } => "unverified",
                    };
                    return (ran, json!({ "refused": refused }));
                }
                Step::Admitted(admitted) => {
                    let who = match admitted.who {
                        Who::BackendClient => "backendClient",
                        Who::DeviceAccount => "deviceAccount",
                        Who::User => "user",
                    };
                    let access = match admitted.access {
                        Access::Session => "session",
                        Access::Backend => "backend",
                    };
                    let forwarded: Value = serde_json::from_str(&admitted.prelude).unwrap();
                    let outcome = json!({
                        "who": who,
                        "access": access,
                        "renewal": admitted.renewal,
                        "forwarded": forwarded,
                    });
                    return (ran, outcome);
                }
            }
        }
    }
}

/// Alice's, her device's and a backend client's preludes, and the crafted
/// ones mallory might send, are each admitted or refused as shipped
/// `createJazz` did, after the same lookups in the same order, forwarding
/// only the allowlisted fields. The table names the exceptions.
#[test]
fn admits_what_shipped_create_jazz_admitted() {
    let table = table();
    let mut wrong = Vec::new();
    for case in &table.cases {
        let mut shipped = case.shipped.clone();
        let lookups = shipped.as_object_mut().unwrap().remove("lookups").unwrap();
        let expected = case.changed.clone().unwrap_or(shipped);
        let (ran, outcome) = table.run(case);
        if outcome != expected || json!(ran) != lookups {
            wrong.push(format!(
                "{}: {outcome} after {ran:?}, not {expected} after {lookups}",
                case.name
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// A host that has answered nothing, or skipped the account lookup, is asked
/// for what is missing; alice is never admitted on fewer facts.
#[test]
fn admits_no_one_before_every_lookup_is_answered() {
    let table = table();
    let prelude = br#"{"auth":{"backend_session":{"user_id":"alice"}}}"#;
    let mut facts = table.facts();
    facts.account = Some(Account {
        id: Some("account".to_owned()),
        issuer: table.issuer.clone(),
        claims: json!({}),
    });
    assert_eq!(
        admit(prelude, &facts).unwrap(),
        Step::Lookup(Lookup::BackendClient)
    );
    facts.client = Some(BackendClient::None);
    assert_eq!(admit(prelude, &facts).unwrap(), Step::Lookup(Lookup::User));
    facts.account = None;
    facts.user = Some(Authenticated {
        user_id: Some("alice".to_owned()),
        authenticated_at_ms: AUTHENTICATED_AT_MS,
    });
    assert_eq!(
        admit(prelude, &facts).unwrap(),
        Step::Lookup(Lookup::Account)
    );
}

/// Mallory's deeply nested or very wide prelude is skipped or refused
/// without a tree of it being built, and a bad byte is no panic.
#[test]
fn a_hostile_prelude_is_skipped_or_refused() {
    let table = table();
    let mut facts = table.facts();
    facts.client = Some(BackendClient::None);
    let deep = format!("{}1{}", r#"{"auth":"#.repeat(5_000), "}".repeat(5_000));
    let ignored = format!(
        r#"{{"x":{}{},"auth":{{}}}}"#,
        "[".repeat(100_000),
        "]".repeat(100_000)
    );
    for prelude in [deep.as_bytes(), ignored.as_bytes(), b"{\"a\":\"\xff\"}"] {
        assert_eq!(
            admit(prelude, &facts).unwrap(),
            Step::Refused(Refusal::Missing)
        );
    }
}

/// The backend's bundle replaces whatever mallory stated, none is stated
/// before one is published, and only a JSON object can be pinned.
#[test]
fn pins_the_backends_own_bundle() {
    let pinned = |bundle| {
        let bytes = pin_bundle(br#"{"peer_identity":"p","bundle":"theirs"}"#, bundle).unwrap();
        serde_json::from_slice::<Value>(&bytes).unwrap()
    };
    assert_eq!(
        pinned(Some("ours")),
        json!({ "peer_identity": "p", "bundle": "ours" })
    );
    assert_eq!(pinned(None), json!({ "peer_identity": "p" }));
    for prelude in [&b"{not json"[..], b"[]", b"null"] {
        assert_eq!(pin_bundle(prelude, Some("ours")), None);
    }
}
