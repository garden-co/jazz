//! An app backend's admission of a client's socket prelude: who the backend
//! vouches for to its core, and the prelude the core then gets.
//!
//! Every backend host decides through [`admit`]: `createJazz` on Node
//! (jazz-napi), in a Cloudflare Worker (jazz-wasm's backend build) and the
//! native endpoint (jazz-app-support). Each host keeps its I/O: it answers
//! the [`Lookup`] `admit` names and asks again with the answer in [`Facts`].
//! Every call decides from the prelude afresh, so a host that skips or
//! reorders a lookup is asked for it and cannot admit a socket early.
//!
//! The rules:
//! - A client's credentials are those of its `auth` object. A prelude
//!   without one (flat) states them at the top level. Beside an `auth`
//!   object, top-level copies are never read.
//! - A backend client trusted as the backend states a stand-in secret and,
//!   to act for a user, their session.
//! - A device account states its own proof alone, for this app or for one
//!   of the endpoints the host serves, and the app lets it in.
//! - Anyone else states the user the host's `authenticate` names, who has
//!   an account.
//! - What reaches the core is built here: `peer_identity`, `requested_link`,
//!   an `auth` of the backend secret and at most one identity, and the
//!   audience the backend checked a proof against. Nothing else is copied.
//!
//! JSON that is only passed on (claims, a trusted client's session, a
//! prelude being pinned) keeps its text: parsing it into a [`serde_json::Value`]
//! would move some numbers, and policies compare claims exactly.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;

use base64::Engine as _;
use serde::de::{self, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::value::{RawValue, to_raw_value};

use super::identity::{self, DEVICE_ACCOUNT_ISSUER};

/// The prelude field by which a client asks to renew its session in place.
/// It is between the client and its backend: a core never gets it.
const RENEWAL: &str = "jazz_renewal";

/// The only members [`Stated`] keeps. All others are skipped unparsed, so a
/// hostile prelude costs no more memory than its bytes.
const READ: [&str; 11] = [
    "auth",
    "jwt_token",
    "backend_secret",
    "admin_secret",
    "backend_session",
    "user_id",
    "peer_identity",
    "requested_link",
    "bootstrap_catalogue",
    "local_first_audience",
    RENEWAL,
];

/// What the host knows so far.
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Facts {
    pub backend_secret: String,
    pub app_id: String,
    /// The host's clock, in Unix milliseconds.
    pub now_ms: u64,
    /// The endpoint URLs a device account's proof may name besides the app.
    /// None: this host takes proofs for the app only.
    #[serde(default)]
    pub endpoints: Vec<String>,
    /// [`Lookup::BackendClient`]'s answer.
    #[serde(default)]
    pub client: Option<BackendClient>,
    /// [`Lookup::DeviceAccount`]'s answer.
    #[serde(default)]
    pub device_accepted: Option<bool>,
    /// [`Lookup::User`]'s answer.
    #[serde(default)]
    pub user: Option<Authenticated>,
    /// [`Lookup::Account`]'s answer.
    #[serde(default)]
    pub account: Option<Account>,
}

/// What the request's backend-client credential is trusted as.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum BackendClient {
    /// It carries none.
    None,
    /// It carries one that doesn't verify.
    Refused,
    /// It is trusted with the backend's authority.
    Trusted,
    /// It acts as one user, whom `authenticate` names.
    Scoped,
}

/// Who the host's `authenticate` named.
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Authenticated {
    pub user_id: Option<String>,
    /// When the host started asking, in Unix milliseconds: read before
    /// `authenticate` ran. The core compares it with the user's revocation
    /// mark, so a revoke that raced the sign-in refuses the session.
    pub authenticated_at_ms: u64,
}

/// The named user's account.
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Account {
    /// `None`: they have none. Admission never makes one.
    pub id: Option<String>,
    pub issuer: String,
    pub claims: Box<RawValue>,
}

/// What [`admit`] answers.
#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "step", rename_all = "camelCase")]
pub enum Step {
    /// The host is to answer this and ask again.
    Lookup(Lookup),
    Refused(Refusal),
    Admitted(Admitted),
}

/// What only the host can answer.
#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "lookup", rename_all = "camelCase")]
pub enum Lookup {
    /// Resolve the request's backend-client credential.
    BackendClient,
    /// Whether the app lets this verified device account in. `endpoint` is
    /// the audience its proof named, when that is not the app.
    DeviceAccount { endpoint: Option<String> },
    /// Read the clock, then run `authenticate`.
    User,
    /// The named user is the one stated: look their account up.
    Account,
}

/// Why a socket isn't admitted.
#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "refusal", rename_all = "camelCase")]
pub enum Refusal {
    /// No credential that names anyone: `session missing` to the client.
    Missing,
    /// A credential was sent and not accepted: `session invalid`.
    Unverified,
    /// Also `session invalid`: a device account's proof that doesn't hold
    /// for this app or endpoint. The host may log the audience it named.
    UnverifiedDeviceProof { token: String },
}

/// The access the backend opens the core's socket with.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Access {
    /// It relays a client that proves who it is.
    Session,
    /// It vouches.
    Backend,
}

/// Who an admitted socket is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Who {
    BackendClient,
    DeviceAccount,
    User,
}

/// An admitted socket.
#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Admitted {
    /// The prelude for the core. Its bundle is stamped when the host opens
    /// the socket ([`pin_bundle`]).
    pub prelude: String,
    pub who: Who,
    pub access: Access,
    /// The user's client renews this session in place.
    pub renewal: bool,
}

/// A host's facts contradict the prelude or aren't JSON: a fault of the
/// backend, never the client's refusal.
#[derive(Debug, thiserror::Error)]
#[error("cannot admit the socket: {0}")]
pub struct AdmitError(#[from] serde_json::Error);

/// Decide `prelude` as far as `facts` allow.
pub fn admit(prelude: &[u8], facts: &Facts) -> Result<Step, AdmitError> {
    let text = text_of(prelude);
    let Ok(top @ Stated::Object(_)) = serde_json::from_str::<Stated>(&text) else {
        return Ok(Step::Refused(Refusal::Missing));
    };
    let nested = top
        .get("auth")
        .filter(|auth| matches!(auth, Stated::Object(_)));
    let client = Client {
        text: &text,
        auth: nested.unwrap_or(&top),
        flat: nested.is_none(),
        top: &top,
    };
    match facts.client {
        None => Ok(Step::Lookup(Lookup::BackendClient)),
        Some(BackendClient::Refused) => Ok(Step::Refused(Refusal::Unverified)),
        Some(BackendClient::Trusted) => client.backend(facts),
        Some(kind) if client.states("jwt_token") => client.device_account(kind, facts),
        Some(kind) => client.user(kind, facts),
    }
}

/// [`admit`], with the facts and the step as JSON: what bindings export.
pub fn admit_json(prelude: &[u8], facts: &str) -> Result<String, AdmitError> {
    let step = admit(prelude, &serde_json::from_str(facts)?)?;
    Ok(serde_json::to_string(&step)?)
}

/// `prelude`, pinned to the `bundle` the backend published, or unpinned
/// while there is none. Whatever it stated is replaced. `None` when it is
/// not a JSON object: such a socket must not open.
pub fn pin_bundle(prelude: &[u8], bundle: Option<&str>) -> Option<Vec<u8>> {
    let mut fields: BTreeMap<String, Box<RawValue>> =
        serde_json::from_str(&text_of(prelude)).ok()?;
    fields.remove("bundle");
    if let Some(bundle) = bundle {
        fields.insert("bundle".to_owned(), to_raw_value(bundle).ok()?);
    }
    serde_json::to_vec(&fields).ok()
}

/// A prelude's text, as `TextDecoder` reads it.
fn text_of(bytes: &[u8]) -> Cow<'_, str> {
    String::from_utf8_lossy(bytes.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(bytes))
}

/// A client's prelude, read.
struct Client<'a> {
    text: &'a str,
    top: &'a Stated,
    /// Where its credentials are.
    auth: &'a Stated,
    flat: bool,
}

impl Client<'_> {
    fn states(&self, key: &str) -> bool {
        self.auth
            .get(key)
            .is_some_and(|value| *value != Stated::Null)
    }

    fn backend(&self, facts: &Facts) -> Result<Step, AdmitError> {
        // Never an admin secret or a token of its own.
        if self.states("admin_secret") || self.states("jwt_token") || !self.states("backend_secret")
        {
            return Ok(Step::Refused(Refusal::Missing));
        }
        let mut auth = ForwardedAuth::default();
        if self.states("backend_session") {
            // Forwarded as the client stated it: it has the backend's
            // authority.
            let Some(session) = self.stated_session() else {
                return Ok(Step::Refused(Refusal::Unverified));
            };
            auth.backend_session = Some(session);
        }
        self.admitted(
            facts,
            auth,
            None,
            Who::BackendClient,
            Access::Backend,
            false,
        )
    }

    /// The text of the `backend_session` the client stated.
    fn stated_session(&self) -> Option<Box<RawValue>> {
        let top: BTreeMap<String, &RawValue> = serde_json::from_str(self.text).ok()?;
        let auth: BTreeMap<String, &RawValue> = if self.flat {
            top
        } else {
            serde_json::from_str(top.get("auth")?.get()).ok()?
        };
        auth.get("backend_session")
            .map(|session| (*session).to_owned())
    }

    fn device_account(&self, client: BackendClient, facts: &Facts) -> Result<Step, AdmitError> {
        let unverified = Ok(Step::Refused(Refusal::Unverified));
        let Some(Stated::Text(token)) = self.auth.get("jwt_token") else {
            return unverified;
        };
        let alone = !["backend_secret", "admin_secret", "backend_session"]
            .into_iter()
            .any(|key| self.states(key));
        // The audience is the backend's word. Beside an `auth` object a
        // stated one is a top-level copy, never read.
        let states_audience = self.flat && self.states("local_first_audience");
        // A scoped backend client is its user, never a device account.
        if !alone || states_audience || client != BackendClient::None || !names_device_issuer(token)
        {
            return unverified;
        }
        let well_formed = has_object_parts(token);
        let holds_for = |audience: &str| {
            well_formed
                && identity::verify_jazz_self_signed_proof_at(token, audience, facts.now_ms / 1000)
                    .is_ok_and(|proof| proof.issuer == DEVICE_ACCOUNT_ISSUER)
        };
        let endpoint = if holds_for(&facts.app_id) {
            None
        } else {
            let named = facts
                .endpoints
                .iter()
                .find(|endpoint| !endpoint.is_empty() && holds_for(endpoint));
            let Some(endpoint) = named else {
                return Ok(Step::Refused(Refusal::UnverifiedDeviceProof {
                    token: token.clone(),
                }));
            };
            Some(endpoint.clone())
        };
        match facts.device_accepted {
            None => Ok(Step::Lookup(Lookup::DeviceAccount { endpoint })),
            Some(false) => unverified,
            Some(true) => {
                // The core knows the app's id; any other audience takes the
                // backend's word, and so its access.
                let access = if endpoint.is_some() {
                    Access::Backend
                } else {
                    Access::Session
                };
                let auth = ForwardedAuth {
                    jwt_token: Some(token),
                    ..ForwardedAuth::default()
                };
                self.admitted(facts, auth, endpoint, Who::DeviceAccount, access, false)
            }
        }
    }

    fn user(&self, client: BackendClient, facts: &Facts) -> Result<Step, AdmitError> {
        let missing = Ok(Step::Refused(Refusal::Missing));
        let Some(session @ (Stated::Object(_) | Stated::List)) = self.auth.get("backend_session")
        else {
            return missing;
        };
        if self.states("backend_secret") || self.states("admin_secret") {
            return missing;
        }
        let Some(authenticated) = &facts.user else {
            return Ok(Step::Lookup(Lookup::User));
        };
        let (Some(user), Some(Stated::Text(stated))) =
            (&authenticated.user_id, session.get("user_id"))
        else {
            return missing;
        };
        if user != stated {
            return missing;
        }
        let Some(account) = &facts.account else {
            return Ok(Step::Lookup(Lookup::Account));
        };
        let Some(id) = &account.id else {
            return missing;
        };
        // Built from the host's answers: never the client's claims.
        let session = to_raw_value(&VouchedSession {
            account_id: id,
            issuer: &account.issuer,
            user_id: user,
            claims: &account.claims,
            auth_mode: "external",
            authenticated_at: authenticated.authenticated_at_ms,
        })?;
        let auth = ForwardedAuth {
            backend_session: Some(session),
            ..ForwardedAuth::default()
        };
        // A scoped backend client's session ends with its token instead.
        let renewal = client == BackendClient::None
            && matches!(self.top.get(RENEWAL), Some(Stated::Number(asked)) if *asked == 1.0);
        self.admitted(facts, auth, None, Who::User, Access::Backend, renewal)
    }

    fn admitted<'a>(
        &'a self,
        facts: &'a Facts,
        mut auth: ForwardedAuth<'a>,
        audience: Option<String>,
        who: Who,
        access: Access,
        renewal: bool,
    ) -> Result<Step, AdmitError> {
        // What no core takes is refused here: dropping it would admit the
        // socket in its place.
        let unverified = Ok(Step::Refused(Refusal::Unverified));
        let (Ok(peer_identity), Ok(requested_link)) = (
            self.text_field("peer_identity"),
            self.text_field("requested_link"),
        ) else {
            return unverified;
        };
        // A core refuses the retired bootstrap.
        if !matches!(
            self.top.get("bootstrap_catalogue"),
            None | Some(Stated::Bool(false))
        ) {
            return unverified;
        }
        // A flat prelude's audience is never forwarded, but one no core
        // takes (not a string, or not a header value) refuses the socket.
        if self.flat {
            match self.top.get("local_first_audience") {
                None | Some(Stated::Null) => {}
                Some(Stated::Text(stated)) if is_header_value(stated) => {}
                Some(_) => return unverified,
            }
        }
        auth.backend_secret = &facts.backend_secret;
        let prelude = serde_json::to_string(&Forwarded {
            peer_identity,
            requested_link,
            auth,
            local_first_audience: audience.as_deref(),
        })?;
        Ok(Step::Admitted(Admitted {
            prelude,
            who,
            access,
            renewal,
        }))
    }

    /// A top-level field that is a string when stated.
    fn text_field(&self, key: &str) -> Result<Option<&str>, NotText> {
        match self.top.get(key) {
            None => Ok(None),
            Some(Stated::Text(value)) => Ok(Some(value)),
            Some(_) => Err(NotText),
        }
    }
}

struct NotText;

/// Everything a core gets of a prelude, bar the bundle: there is no other
/// field to copy a client's into.
#[derive(Serialize)]
struct Forwarded<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    peer_identity: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    requested_link: Option<&'a str>,
    auth: ForwardedAuth<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    local_first_audience: Option<&'a str>,
}

#[derive(Default, Serialize)]
struct ForwardedAuth<'a> {
    backend_secret: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    jwt_token: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    backend_session: Option<Box<RawValue>>,
}

/// The session a backend vouches for.
#[derive(Serialize)]
struct VouchedSession<'a> {
    account_id: &'a str,
    issuer: &'a str,
    user_id: &'a str,
    claims: &'a RawValue,
    #[serde(rename = "authMode")]
    auth_mode: &'static str,
    #[serde(rename = "authenticatedAt")]
    authenticated_at: u64,
}

/// Whether a core can read `value` as the header it relays an audience in.
fn is_header_value(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte == b'\t' || (byte >= 0x20 && byte != 0x7f))
}

/// Whether `token` names the device-account issuer, unverified: only such a
/// token is worth verifying, and its failure worth logging.
fn names_device_issuer(token: &str) -> bool {
    #[derive(Deserialize)]
    struct Issuer {
        iss: Option<String>,
    }
    token
        .split('.')
        .nth(1)
        .and_then(|part| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(part)
                .ok()
        })
        .and_then(|bytes| serde_json::from_slice::<Issuer>(&bytes).ok())
        .and_then(|claims| claims.iss)
        .is_some_and(|issuer| issuer == DEVICE_ACCOUNT_ISSUER)
}

/// Whether a proof's header and claims are JSON objects in UTF-8. The
/// verifier alone also takes a header that is a list, or isn't UTF-8 where
/// it skips it; a backend never admitted such a proof.
fn has_object_parts(token: &str) -> bool {
    token.split('.').take(2).all(|part| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(part)
            .ok()
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .and_then(|text| serde_json::from_str::<Stated>(&text).ok())
            .is_some_and(|json| matches!(json, Stated::Object(_)))
    })
}

/// A JSON value reduced to what admission reads: scalars, and of an object
/// only the [`READ`] members. A repeated member's last value counts.
#[derive(Debug, PartialEq)]
enum Stated {
    Null,
    Bool(bool),
    Number(f64),
    Text(String),
    List,
    Object(Vec<(&'static str, Stated)>),
}

impl Stated {
    fn get(&self, key: &str) -> Option<&Stated> {
        let Stated::Object(members) = self else {
            return None;
        };
        members
            .iter()
            .find(|(name, _)| *name == key)
            .map(|(_, value)| value)
    }
}

impl<'de> Deserialize<'de> for Stated {
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(StatedVisitor)
    }
}

struct StatedVisitor;

impl<'de> Visitor<'de> for StatedVisitor {
    type Value = Stated;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JSON")
    }

    fn visit_unit<E>(self) -> Result<Stated, E> {
        Ok(Stated::Null)
    }

    fn visit_bool<E>(self, value: bool) -> Result<Stated, E> {
        Ok(Stated::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Stated, E> {
        Ok(Stated::Number(value as f64))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Stated, E> {
        Ok(Stated::Number(value as f64))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Stated, E> {
        Ok(Stated::Number(value))
    }

    fn visit_str<E>(self, value: &str) -> Result<Stated, E> {
        Ok(Stated::Text(value.to_owned()))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut items: A) -> Result<Stated, A::Error> {
        while items.next_element::<IgnoredAny>()?.is_some() {}
        Ok(Stated::List)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut entries: A) -> Result<Stated, A::Error> {
        let mut members: Vec<(&'static str, Stated)> = Vec::new();
        while let Some(name) = entries.next_key::<String>()? {
            let Some(key) = READ.into_iter().find(|key| *key == name) else {
                entries.next_value::<IgnoredAny>()?;
                continue;
            };
            let value = entries.next_value()?;
            match members.iter_mut().find(|(known, _)| *known == key) {
                Some(member) => member.1 = value,
                None => members.push((key, value)),
            }
        }
        Ok(Stated::Object(members))
    }
}
