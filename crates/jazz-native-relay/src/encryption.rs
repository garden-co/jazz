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

use jazz::tools::public_schema::E2eeDeclarations;
use jazz_e2ee_client::host::bridge::JsStore;
use jazz_e2ee_client::host::runtime as host;

/// The app's encryption declarations, which a relay opens its store with
/// ([`crate::NativeRelay::spawn_encrypted`]).
pub type Declarations = E2eeDeclarations;

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
