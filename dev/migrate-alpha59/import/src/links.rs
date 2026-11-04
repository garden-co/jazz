//! Account continuity for apps moving to `createJazz`.
//!
//! `createJazz` names every user `(jazz:backend, userId)`. An alpha.59
//! app authenticated users by their own JWTs, as `(iss, sub)`. So that a
//! returning user's `authenticate` answering `{ userId: sub }` opens their
//! old account, the import links `(jazz:backend, sub)` to the account of
//! each admitted external identity `(iss, sub)`, through the account
//! registry's own link commands.
//!
//! The registry grants every linked identity the right to manage the
//! account's identities, so only an identity that holds that right itself is
//! linked; anything else is refused rather than escalated. (alpha.59 grants
//! it to every identity it registers or links, so this never refuses a
//! store alpha.59 built.)
//!
//! Local-first, anonymous, system and other built-in issuers are left
//! alone. A revoked identity gets no link, but still counts as holding its
//! subject: two issuers that share a subject on different accounts, revoked
//! or not, would both claim one `jazz:backend` identity, and the import refuses
//! that rather than pick one.

use std::collections::BTreeMap;
use std::sync::Arc;

use jazz::account_registry::{AccountCommand, AccountRegistry, AccountState, Principal};
use jazz_server::ServerState;
use jazz_server::server::migrate_import::{migrate_import_account, migrate_import_login};
use migrate_alpha59_dump as dump;
use serde_json::json;

use crate::err;

/// The issuer `createJazz` names every user by. A core admits only its own
/// app's backend, so it names no app.
pub const BACKEND_ISSUER: &str = "jazz:backend";

fn eligible(issuer: &str) -> bool {
    !issuer.starts_with("urn:jazz:") && !issuer.starts_with("jazz:")
}

/// One identity with an account, as the account journal leaves it.
#[derive(Clone, Debug)]
pub struct Identity {
    pub issuer: String,
    pub subject: String,
    /// The account id, as JSON.
    pub account: String,
    pub active: bool,
    pub manage_identities: bool,
}

/// Every identity among `principals` (JSON) that the journal assigned an
/// account, admitted or revoked. The journal is replayed through the
/// registry's own decisions, in memory.
pub fn identities(
    accounts: &[dump::Account],
    principals: &[String],
) -> Result<Vec<Identity>, String> {
    let mut registry = AccountRegistry::default();
    for account in accounts {
        let command: AccountCommand = serde_json::from_str(&account.command_json)
            .map_err(|_| format!("account command {} does not decode", account.revision))?;
        registry
            .apply(&command)
            .map_err(|error| format!("account command {}: {error}", account.revision))?;
    }
    let mut out = Vec::new();
    for principal_json in principals {
        let principal: Principal = serde_json::from_str(principal_json)
            .map_err(|_| "a principal in the dump does not decode".to_owned())?;
        let Some(assignment) = registry.assignment(&principal).map_err(err)? else {
            continue;
        };
        out.push(Identity {
            issuer: principal.issuer,
            subject: principal.subject,
            account: serde_json::to_string(&assignment.account).map_err(err)?,
            active: assignment.active,
            manage_identities: assignment.manage_identities,
        });
    }
    Ok(out)
}

/// A planned link: `subject` joins `account` as `(jazz:backend, subject)`,
/// approved by `approver` (issuer, subject), which manages that account.
#[derive(Clone, Debug, PartialEq)]
pub struct Link {
    pub subject: String,
    pub account: String,
    pub approver: (String, String),
}

/// Plan a link for every subject an admitted eligible identity holds.
/// Fails, naming only issuers and counts, when a subject is held on
/// different accounts, when an admitted identity may not manage identities,
/// or when an account has no admitted identity that may approve links.
pub fn plan(identities: &[Identity]) -> Result<Vec<Link>, String> {
    let mut by_subject: BTreeMap<&str, Vec<&Identity>> = BTreeMap::new();
    for identity in identities
        .iter()
        .filter(|identity| eligible(&identity.issuer))
    {
        by_subject
            .entry(&identity.subject)
            .or_default()
            .push(identity);
    }
    let mut conflicts = Vec::new();
    let mut unprivileged = 0;
    let mut unmanaged = 0;
    let mut links = Vec::new();
    for (subject, holders) in by_subject {
        let account = &holders[0].account;
        if holders.iter().any(|identity| &identity.account != account) {
            let mut issuers: Vec<&str> = holders.iter().map(|i| i.issuer.as_str()).collect();
            issuers.sort();
            conflicts.push(issuers.join(" + "));
            continue;
        }
        let admitted: Vec<&&Identity> = holders.iter().filter(|identity| identity.active).collect();
        if admitted.is_empty() {
            continue;
        }
        if admitted.iter().any(|identity| !identity.manage_identities) {
            unprivileged += 1;
            continue;
        }
        let approver = identities
            .iter()
            .filter(|identity| {
                &identity.account == account && identity.active && identity.manage_identities
            })
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
    if !conflicts.is_empty() || unprivileged > 0 || unmanaged > 0 {
        conflicts.sort();
        conflicts.dedup();
        return Err(format!(
            "cannot link backend identities: {} subjects are held by issuers on different \
             accounts ({}), {unprivileged} subjects belong to identities that may not manage \
             identities (a link would grant that), {unmanaged} accounts have no identity that \
             may approve links; pass --no-backend-identity-links to import without them",
            conflicts.len(),
            conflicts.join(", ")
        ));
    }
    Ok(links)
}

/// Link every planned identity. One already on its account is left as is;
/// one on another account is an error.
pub async fn apply(state: &Arc<ServerState>, links: &[Link]) -> Result<u64, String> {
    let issuer = BACKEND_ISSUER;
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

    fn identity(
        issuer: &str,
        subject: &str,
        account: &str,
        active: bool,
        manage: bool,
    ) -> Identity {
        Identity {
            issuer: issuer.into(),
            subject: subject.into(),
            account: account.into(),
            active,
            manage_identities: manage,
        }
    }

    #[test]
    fn links_external_identities_and_skips_built_in_issuers() {
        let links = plan(&[
            identity("https://a", "u1", "A", true, true),
            identity("https://a", "u2", "B", true, true),
            identity("urn:jazz:local-first", "x", "B", true, true),
            identity("jazz:backend", "u3", "C", true, true),
            identity("jazz:app:1", "u4", "C", true, true),
        ])
        .unwrap();
        assert_eq!(links.len(), 2);
        assert_eq!(links[0].approver, ("https://a".into(), "u1".into()));
        // An external manager approves before a built-in one.
        assert_eq!(links[1].approver, ("https://a".into(), "u2".into()));
    }

    #[test]
    fn same_subject_on_one_account_links_once() {
        let links = plan(&[
            identity("https://a", "u1", "A", true, true),
            identity("https://b", "u1", "A", true, true),
        ])
        .unwrap();
        assert_eq!(links.len(), 1);
    }

    #[test]
    fn refuses_a_subject_shared_across_accounts() {
        let error = plan(&[
            identity("https://a", "u1", "A", true, true),
            identity("https://b", "u1", "B", true, true),
        ])
        .unwrap_err();
        assert!(error.contains("https://a + https://b"), "{error}");
        assert!(
            !error.contains("u1"),
            "subjects must not be reported: {error}"
        );
    }

    #[test]
    fn a_revoked_identity_still_holds_its_subject() {
        let error = plan(&[
            identity("https://a", "u1", "B", true, true),
            identity("https://b", "u1", "A", false, true),
        ])
        .unwrap_err();
        assert!(error.contains("https://a + https://b"), "{error}");
    }

    #[test]
    fn a_revoked_identity_alone_gets_no_link() {
        let links = plan(&[
            identity("https://a", "u1", "A", true, true),
            identity("https://a", "u2", "A", false, true),
        ])
        .unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].subject, "u1");
    }

    #[test]
    fn refuses_to_grant_management_a_source_lacks() {
        let error = plan(&[
            identity("https://a", "u1", "A", true, true),
            identity("https://a", "u2", "A", true, false),
        ])
        .unwrap_err();
        assert!(
            error.contains("1 subjects belong to identities that may not manage"),
            "{error}"
        );
    }
}
