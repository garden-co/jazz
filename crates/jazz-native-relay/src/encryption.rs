//! End-to-end encryption on a native relay, behind the `e2ee-client` feature
//! (`E2EE_PROTOCOL.md` §1.4, §4; core hooks note H9, H10).
//!
//! The relay's own durable database holds no keys: it opens with keyless
//! hooks built from the app's declarations, so it stores and relays what the
//! devices write and never opens an encrypted cell. A UI peer that is a
//! device attaches with the runtime ([`EncryptedDevice`]): its in-memory
//! database opens with the runtime's hooks over a private store the app
//! supplies, and the relay runs the runtime's loop between its drives.
//! A UI peer attached without one is keyless like the relay.

use std::rc::Rc;
use std::sync::Arc;

use jazz::tools::public_schema::E2eeDeclarations;
use jazz_e2ee_client::host::bridge::JsStore;
use jazz_e2ee_client::host::runtime as host;

use crate::JazzNativeRelayStatus as Status;
use crate::account_session::AccountSessionOwner;

/// The app's encryption declarations, which a relay opens its store with
/// ([`crate::NativeRelay::spawn_encrypted`]).
pub type Declarations = E2eeDeclarations;

/// Immutable declarations admitted with the account and the delivered schema.
#[derive(Clone)]
pub(super) struct ForegroundEncryption {
    pub declarations: Declarations,
    bindings: Arc<[u8]>,
}

impl ForegroundEncryption {
    pub fn admit(
        bindings: &str,
        schema: &serde_json::Value,
        owner: &AccountSessionOwner,
    ) -> Result<Self, Status> {
        use jazz::tools::deployment::artifact::{bound_declarations, schema_hash};
        let invalid = |_| Status::LifecycleFailure;
        let bound: serde_json::Value = serde_json::from_str(bindings).map_err(invalid)?;
        let current = bound["schema"]["current"]
            .as_str()
            .ok_or(Status::LifecycleFailure)?;
        let shipped = bound["schema"]["schemas"]
            .as_array()
            .and_then(|entries| {
                entries
                    .iter()
                    .find(|entry| entry["hash"].as_str() == Some(current))
            })
            .ok_or(Status::LifecycleFailure)?;
        let stored_schema = |value| {
            let bytes = serde_json::to_vec(value).map_err(|_| Status::LifecycleFailure)?;
            schema_hash(&bytes).map_err(|_| Status::LifecycleFailure)
        };
        let schema = stored_schema(schema)?;
        if schema != stored_schema(&shipped["schema"])? || schema.to_string() != current {
            return Err(Status::LifecycleFailure);
        }
        let app_id = jazz::tools::AppId::from_string(&owner.app_id)
            .unwrap_or_else(|_| jazz::tools::AppId::from_name(&owner.app_id));
        let declarations = bound_declarations(bindings.as_bytes(), app_id, &owner.environment)
            .map_err(|_| Status::LifecycleFailure)?
            .ok_or(Status::LifecycleFailure)?;
        Ok(Self {
            declarations,
            bindings: Arc::from(bindings.as_bytes()),
        })
    }

    pub async fn build(
        self,
        account: String,
        device_account_secret: Option<zeroize::Zeroizing<String>>,
        store: Rc<JsStore>,
    ) -> Result<Built, String> {
        // Read straight into a zeroizing `RecoverySecret`; the hexadecimal
        // text is wiped when it drops here.
        let device_account_secret = device_account_secret
            .as_ref()
            .map(|secret| account_secret(secret.as_str()))
            .transpose()?;
        let built = host::build_native(
            host::NativeOptions {
                app_file: self.bindings.to_vec(),
                from_bindings: true,
                device_account_secret,
                app_id: self.declarations.app_id.uuid().to_string(),
                environment: self.declarations.environment.clone(),
            },
            account,
            Rc::clone(&store),
            Some(&self.declarations),
        )
        .await?;
        Ok(Built { built, store })
    }
}

/// A device UI peer: what its runtime is built from. The store is made on
/// the relay's owner thread, where its requests are answered on the spot
/// (see [`JsStore::served_by`]).
pub struct EncryptedDevice {
    /// The app's `jazz.app.json`.
    pub app_file: Vec<u8>,
    /// The deployment's environment (part of every cell's domain).
    pub environment: String,
    /// The app, as the declarations name it.
    pub app_id: String,
    /// The device account's secret as 64 hexadecimal characters, for an
    /// account the device holds itself; `None` for one the app signs in.
    pub device_account_secret: Option<String>,
    /// Makes the device's private store, on the owner thread.
    pub store: Box<dyn FnOnce() -> Rc<JsStore> + Send>,
}

/// A device's runtime, built and waiting for its database.
pub(crate) struct Built {
    pub built: host::NativeBuilt,
    pub store: Rc<JsStore>,
}

pub(crate) fn keyless(declarations: Declarations) -> Rc<dyn jazz::e2ee::E2eeHooks> {
    jazz_e2ee_client::KeylessHooks::hooks(declarations)
}

/// The device account's secret from its 64 hexadecimal characters, as the
/// [`RecoverySecret`](jazz_e2ee_client::host::RecoverySecret) the runtime
/// takes (which zeroizes its bytes when dropped).
fn account_secret(hex: &str) -> Result<jazz_e2ee_client::host::RecoverySecret, String> {
    if hex.len() != 64 || !hex.is_ascii() {
        return Err("a recovery secret is 64 hexadecimal characters".to_owned());
    }
    let mut bytes = [0u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * index..2 * index + 2], 16)
            .map_err(|_| "a recovery secret is hexadecimal".to_owned())?;
    }
    let secret = jazz_e2ee_client::host::RecoverySecret::from_bytes(bytes);
    bytes.fill(0);
    Ok(secret)
}

/// The runtime for `account`, built over the device's store.
pub(crate) fn build(
    device: EncryptedDevice,
    account: String,
    declarations: &Declarations,
) -> Result<Built, String> {
    let device_account_secret = device
        .device_account_secret
        .as_deref()
        .map(account_secret)
        .transpose()?;
    let store = (device.store)();
    let built = jazz::db::block_on(host::build_native(
        host::NativeOptions {
            app_file: device.app_file,
            from_bindings: false,
            device_account_secret,
            app_id: device.app_id,
            environment: device.environment,
        },
        account,
        Rc::clone(&store),
        Some(declarations),
    ))?;
    Ok(Built { built, store })
}
