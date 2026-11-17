//! The service's machine credential: an OIDC token the app's TypeScript main
//! backend trusts with `trustWorkload` (see `main.rs`).

use std::path::PathBuf;

use crate::jazz_app::{Error, TokenProvider, TokenReason};

/// Where the token comes from. It is looked up again for every token the
/// client asks for, so a short-lived token that a file or a command renews
/// keeps working.
#[derive(Clone, Debug)]
pub enum Credential {
    /// `JAZZ_TOKEN`: the token itself.
    Token(String),
    /// `JAZZ_TOKEN_FILE`: a file holding it.
    File(PathBuf),
    /// `JAZZ_TOKEN_COMMAND`: a shell command printing it.
    Command(String),
}

impl Credential {
    /// From `JAZZ_TOKEN`, `JAZZ_TOKEN_FILE` or `JAZZ_TOKEN_COMMAND`.
    pub fn from_env() -> Result<Self, String> {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
        if let Some(token) = var("JAZZ_TOKEN") {
            Ok(Self::Token(token.trim().to_owned()))
        } else if let Some(path) = var("JAZZ_TOKEN_FILE") {
            Ok(Self::File(path.into()))
        } else if let Some(command) = var("JAZZ_TOKEN_COMMAND") {
            Ok(Self::Command(command))
        } else {
            Err(
                "set JAZZ_TOKEN, JAZZ_TOKEN_FILE or JAZZ_TOKEN_COMMAND to the machine credential"
                    .into(),
            )
        }
    }

    /// The current token.
    pub async fn token(&self) -> Result<String, Error> {
        let token = match self {
            Self::Token(token) => token.clone(),
            Self::File(path) => tokio::fs::read_to_string(path).await.map_err(|error| {
                Error::Config(format!(
                    "reading JAZZ_TOKEN_FILE {}: {error}",
                    path.display()
                ))
            })?,
            Self::Command(command) => {
                let output = tokio::process::Command::new("sh")
                    .arg("-c")
                    .arg(command)
                    .stderr(std::process::Stdio::inherit())
                    .output()
                    .await
                    .map_err(|error| {
                        Error::Config(format!("running JAZZ_TOKEN_COMMAND: {error}"))
                    })?;
                if !output.status.success() {
                    return Err(Error::Config(format!(
                        "JAZZ_TOKEN_COMMAND failed: {}",
                        output.status
                    )));
                }
                String::from_utf8_lossy(&output.stdout).into_owned()
            }
        };
        let token = token.trim().to_owned();
        if token.is_empty() {
            return Err(Error::Config("the machine credential is empty".into()));
        }
        Ok(token)
    }

    /// The token provider for `Config::credential`.
    pub fn provider(self) -> impl TokenProvider + 'static {
        move |_reason: TokenReason| {
            let credential = self.clone();
            async move { credential.token().await.map(Some) }
        }
    }
}
