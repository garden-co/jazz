//! An app's definition: its schema history from `jazz.app.json` and, on a
//! backend, its permissions from `jazz.permissions.json`.

use std::path::Path;

use jazz::db::Migrations;
use jazz::tools::deployment::artifact::{self, LoadedApp};
use jazz::tools::{AppId, Schema};

use crate::error::{Error, Result};

/// What a client opens an app with.
#[derive(Debug, Clone)]
pub struct AppFiles {
    app: Vec<u8>,
    permissions: Option<Vec<u8>>,
}

impl AppFiles {
    /// The app's `jazz.app.json`, and its `jazz.permissions.json` if it has
    /// one (a backend needs it).
    pub fn new(app: impl Into<Vec<u8>>, permissions: Option<Vec<u8>>) -> Self {
        Self {
            app: app.into(),
            permissions,
        }
    }

    /// Reads `jazz.app.json` and, if present, `jazz.permissions.json` from
    /// `dir`.
    pub fn read(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        let read = |file: &str| std::fs::read(dir.join(file));
        let app = read(artifact::APP_FILE).map_err(|error| {
            Error::App(format!(
                "cannot read {}: {error}",
                dir.join(artifact::APP_FILE).display()
            ))
        })?;
        let permissions = match read(artifact::PERMISSIONS_FILE) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                return Err(Error::App(format!(
                    "cannot read {}: {error}",
                    dir.join(artifact::PERMISSIONS_FILE).display()
                )));
            }
        };
        Ok(Self { app, permissions })
    }

    /// Replaces the permissions, as a backend embedding them states them.
    pub fn with_permissions(mut self, permissions: impl Into<Vec<u8>>) -> Self {
        self.permissions = Some(permissions.into());
        self
    }

    /// The bytes of `jazz.app.json`.
    #[cfg_attr(not(feature = "e2ee-client"), allow(dead_code))]
    pub(crate) fn app_bytes(&self) -> &[u8] {
        &self.app
    }

    pub(crate) fn has_permissions(&self) -> bool {
        self.permissions.is_some()
    }

    /// Checks the files, as a client (schema history only) or as a backend
    /// (also the permissions, and the bundle it publishes).
    ///
    /// With `encryption` (the app and its environment), an app that declares
    /// encryption loads, with its declarations, for the client to open with
    /// hooks; without it such an app is refused.
    pub(crate) fn load(&self, encryption: Option<(AppId, &str)>) -> Result<LoadedFiles> {
        let refused = |error: artifact::ArtifactError| Error::App(error.to_string());
        match &self.permissions {
            Some(permissions) => {
                let loaded = artifact::load(&self.app, permissions).map_err(refused)?;
                let encrypted = loaded
                    .current_schema()
                    .keys()
                    .any(|name| artifact::system_tables::is_system_table(name.as_str()));
                let declarations = match encryption {
                    Some((app_id, environment)) => {
                        artifact::load_client_encrypted(&self.app, app_id, environment)
                            .map_err(refused)?
                            .2
                    }
                    None => {
                        refuse_encrypted(encrypted)?;
                        None
                    }
                };
                Ok(LoadedFiles {
                    schema: loaded.current_schema().clone(),
                    migrations: loaded.client_migrations(),
                    backend: Some(loaded),
                    declarations,
                })
            }
            None => match encryption {
                Some((app_id, environment)) => {
                    let (schema, migrations, declarations) =
                        artifact::load_client_encrypted(&self.app, app_id, environment)
                            .map_err(refused)?;
                    Ok(LoadedFiles {
                        schema,
                        migrations,
                        backend: None,
                        declarations,
                    })
                }
                None => {
                    let (schema, migrations) = artifact::load_client_current(&self.app)
                        .map_err(|error| Error::App(error.to_string()))?;
                    Ok(LoadedFiles {
                        schema,
                        migrations,
                        backend: None,
                        declarations: None,
                    })
                }
            },
        }
    }
}

/// A client without encryption hooks refuses an app whose files declare
/// encryption (`declared`).
fn refuse_encrypted(declared: bool) -> Result<()> {
    if declared {
        return Err(Error::App(
            jazz::binding_host::ENCRYPTED_APP_REFUSAL.to_owned(),
        ));
    }
    Ok(())
}

pub(crate) struct LoadedFiles {
    pub schema: Schema,
    pub migrations: Migrations,
    /// With permissions: what a backend publishes.
    pub backend: Option<LoadedApp>,
    /// The app's encryption declarations, when it declares encryption and
    /// the client has hooks to open it with.
    pub declarations: Option<jazz::tools::public_schema::E2eeDeclarations>,
}
