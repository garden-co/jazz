//! End-to-end encryption on a native relay, behind the `e2ee-client` feature
//! (`E2EE_PROTOCOL.md` §1.4, §4; core hooks note H9, H10).
//!
//! The relay's own durable database holds no keys: it opens with keyless
//! hooks built from the app's declarations, so it stores and relays what the
//! devices write and never opens an encrypted cell. A UI peer that is a
//! device opens a leased foreground with the runtime: its in-memory
//! database opens with the runtime's hooks over a private store the app
//! supplies, and the relay runs the runtime's loop between its drives.
//! A UI peer attached without one is keyless like the relay.

use std::rc::Rc;

use jazz::tools::public_schema::E2eeDeclarations;
use jazz_e2ee_client::host::bridge::JsStore;
use jazz_e2ee_client::host::runtime as host;

use crate::JazzNativeRelayStatus as Status;
use crate::account_session::AccountSessionOwner;

/// The app's encryption declarations used to open the relay store.
pub type Declarations = E2eeDeclarations;

/// Immutable declarations admitted with the account and the delivered schema.
#[derive(Clone)]
pub(super) struct ForegroundEncryption {
    pub declarations: Declarations,
}

impl ForegroundEncryption {
    pub fn admit(
        bindings: &str,
        schema: &serde_json::Value,
        owner: &AccountSessionOwner,
    ) -> Result<Self, Status> {
        let (current, declarations) =
            host::bound_declarations(bindings.as_bytes(), &owner.app_id, &owner.environment, true)
                .map_err(|_| Status::LifecycleFailure)?;
        let bytes = serde_json::to_vec(schema).map_err(|_| Status::LifecycleFailure)?;
        let delivered = jazz::tools::deployment::artifact::schema_hash(&bytes)
            .map_err(|_| Status::LifecycleFailure)?;
        if delivered != current {
            return Err(Status::LifecycleFailure);
        }
        let declarations = declarations.ok_or(Status::LifecycleFailure)?;
        Ok(Self { declarations })
    }

    pub async fn build(
        self,
        account: String,
        device_account_secret: Option<jazz::identity::DeviceAccountSecret>,
        store: Rc<JsStore>,
    ) -> Result<Built, String> {
        let built = host::build_native(
            self.declarations,
            account,
            store.clone(),
            device_account_secret,
        )
        .await?;
        Ok(Built { built, store })
    }
}

/// A device's runtime, built and waiting for its database.
pub(crate) struct Built {
    pub built: host::NativeBuilt,
    pub store: Rc<JsStore>,
}

pub(crate) fn keyless(declarations: Declarations) -> Rc<dyn jazz::e2ee::E2eeHooks> {
    jazz_e2ee_client::KeylessHooks::hooks(declarations)
}
