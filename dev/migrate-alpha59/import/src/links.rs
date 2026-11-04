//! Account continuity for apps moving to `createJazz`.
//!
//! `createJazz` names every user `(jazz:app:<appId>, userId)`. An alpha.59
//! app authenticated users by their own JWTs, as `(iss, sub)`. So that a
//! returning user's `authenticate` answering `{ userId: sub }` opens their
//! old account, the import links `(jazz:app:<appId>, sub)` to the account of
//! each admitted external identity `(iss, sub)`, through the account
//! registry's own link commands.
//!
//! Local-first, anonymous, system and other built-in issuers are left
//! alone, as are revoked identities. Two issuers that share a subject on
//! different accounts would both claim one `jazz:app` identity: the import
//! refuses that and reports it, rather than pick one.

use std::collections::BTreeMap;
use std::sync::Arc;

use jazz_server::ServerState;
use jazz_server::server::migrate_import::{migrate_import_account, migrate_import_login};
use serde_json::{Value as Json, json};

use crate::err;

pub fn app_issuer(app: &str) -> String {
    format!("jazz:app:{}", app.to_lowercase())
}

fn eligible(issuer: &str) -> bool {
    !issuer.starts_with("urn:jazz:") && !issuer.starts_with("jazz:app:")
}

/// One admitted identity, as the new server answers its login.
#[derive(Clone, Debug)]
pub struct Admitted {
    pub issuer: String,
    pub subject: String,
    pub account: String,
    pub manage_identities: bool,
}

/// A planned link: `subject` joins `account` as `(jazz:app:<app>, subject)`,
/// approved by `approver` (issuer, subject), which manages that account.
#[derive(Clone, Debug, PartialEq)]
pub struct Link {
    pub subject: String,
    pub account: String,
    pub approver: (String, String),
}

/// Plan the links for every eligible admitted identity. Fails, naming only
/// issuers, when one subject belongs to different accounts or an account has
/// no identity that may approve links.
pub fn plan(admitted: &[Admitted]) -> Result<Vec<Link>, String> {
    let mut by_subject: BTreeMap<&str, Vec<&Admitted>> = BTreeMap::new();
    for identity in admitted
        .iter()
        .filter(|identity| eligible(&identity.issuer))
    {
        by_subject
            .entry(&identity.subject)
            .or_default()
            .push(identity);
    }
    let mut conflicts = Vec::new();
    let mut unmanaged = 0;
    let mut links = Vec::new();
    for (subject, identities) in by_subject {
        let account = &identities[0].account;
        if identities
            .iter()
            .any(|identity| &identity.account != account)
        {
            let mut issuers: Vec<&str> = identities.iter().map(|i| i.issuer.as_str()).collect();
            issuers.sort();
            conflicts.push(issuers.join(" + "));
            continue;
        }
        let approver = admitted
            .iter()
            .filter(|identity| &identity.account == account && identity.manage_identities)
            .min_by_key(|identity| (!eligible(&identity.issuer), &identity.issuer));
        let Some(approver) = approver else {
            unmanaged += 1;
            continue;
        };
        links.push(Link {
            subject: subject.to_owned(),
            account: account.clone(),
            approver: (approver.issuer.clone(), approver.subject.clone()),
        });
    }
    if !conflicts.is_empty() || unmanaged > 0 {
        conflicts.sort();
        conflicts.dedup();
        return Err(format!(
            "cannot link backend identities: {} subjects are shared by issuers on different \
             accounts ({}), {unmanaged} accounts have no identity that may approve links; \
             pass --no-backend-identity-links to import without them",
            conflicts.len(),
            conflicts.join(", ")
        ));
    }
    Ok(links)
}

/// Every admitted identity among `principals` (JSON), as the server answers.
pub async fn admitted(state: &ServerState, principals: &[String]) -> Result<Vec<Admitted>, String> {
    let mut out = Vec::new();
    for principal_json in principals {
        let principal: Json = serde_json::from_str(principal_json).map_err(err)?;
        let login = migrate_import_login(state, principal_json).await?;
        let Some(assignment) = login.get("Ok") else {
            continue;
        };
        out.push(Admitted {
            issuer: principal["issuer"].as_str().unwrap_or_default().to_owned(),
            subject: principal["subject"].as_str().unwrap_or_default().to_owned(),
            account: assignment["account"].to_string(),
            manage_identities: assignment["manage_identities"].as_bool() == Some(true),
        });
    }
    Ok(out)
}

/// Link every planned identity. One already on its account is left as is;
/// one on another account is an error.
pub async fn apply(state: &Arc<ServerState>, app: &str, links: &[Link]) -> Result<u64, String> {
    let issuer = app_issuer(app);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(err)?
        .as_secs();
    let mut linked = 0;
    for link in links {
        let candidate = json!({ "issuer": issuer, "subject": link.subject });
        let existing = migrate_import_login(state, &candidate.to_string()).await?;
        if let Some(assignment) = existing.get("Ok") {
            if assignment["account"].to_string() != link.account {
                return Err(format!(
                    "a {issuer} identity is already on another account than the one it would link to"
                ));
            }
            continue;
        }
        let nonce = uuid::Uuid::new_v4();
        let approver = json!({ "issuer": link.approver.0, "subject": link.approver.1 });
        for command in [
            json!({ "RequestLink": {
                "approver": approver, "candidate": candidate, "nonce": nonce,
                "now": now, "expires_at": now + 600,
            }}),
            json!({ "AcceptLink": { "candidate": candidate, "nonce": nonce, "now": now } }),
        ] {
            migrate_import_account(state, &command.to_string())
                .await
                .map_err(|error| format!("linking a backend identity: {error}"))?;
        }
        linked += 1;
    }
    Ok(linked)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admitted(issuer: &str, subject: &str, account: &str, manage: bool) -> Admitted {
        Admitted {
            issuer: issuer.into(),
            subject: subject.into(),
            account: account.into(),
            manage_identities: manage,
        }
    }

    #[test]
    fn links_external_identities_and_skips_built_in_issuers() {
        let links = plan(&[
            admitted("https://a", "u1", "A", true),
            admitted("https://a", "u2", "B", false),
            admitted("urn:jazz:local-first", "x", "B", true),
            admitted("jazz:app:1", "u3", "C", true),
        ])
        .unwrap();
        assert_eq!(links.len(), 2);
        assert_eq!(links[0].approver, ("https://a".into(), "u1".into()));
        // u2 cannot approve; the local-first manager on its account does.
        assert_eq!(
            links[1].approver,
            ("urn:jazz:local-first".into(), "x".into())
        );
    }

    #[test]
    fn same_subject_on_one_account_links_once() {
        let links = plan(&[
            admitted("https://a", "u1", "A", true),
            admitted("https://b", "u1", "A", false),
        ])
        .unwrap();
        assert_eq!(links.len(), 1);
    }

    #[test]
    fn refuses_a_subject_shared_across_accounts() {
        let error = plan(&[
            admitted("https://a", "u1", "A", true),
            admitted("https://b", "u1", "B", true),
        ])
        .unwrap_err();
        assert!(error.contains("https://a + https://b"), "{error}");
        assert!(
            !error.contains("u1"),
            "subjects must not be reported: {error}"
        );
    }

    #[test]
    fn refuses_an_account_nobody_may_manage() {
        let error = plan(&[admitted("https://a", "u1", "A", false)]).unwrap_err();
        assert!(error.contains("1 accounts have no identity"), "{error}");
    }
}
