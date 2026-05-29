pub const ADMIN_SYSTEM_USER: &str = "@system/admin";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeAuth {
    Client(User),
    TrustedPeer { session: TrustedSession },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct User(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TrustedSession {
    Admin,
    AsUser(User),
    AttributingToUser(User),
}

impl RuntimeAuth {
    pub fn client(user: &str) -> Self {
        Self::Client(User(user.to_owned()))
    }

    pub fn trusted_admin() -> Self {
        Self::TrustedPeer {
            session: TrustedSession::Admin,
        }
    }

    pub fn trusted_as_user(user: &str) -> Self {
        Self::TrustedPeer {
            session: TrustedSession::AsUser(User(user.to_owned())),
        }
    }

    pub fn trusted_attributing_to_user(user: &str) -> Self {
        Self::TrustedPeer {
            session: TrustedSession::AttributingToUser(User(user.to_owned())),
        }
    }

    pub fn is_trusted(&self) -> bool {
        matches!(self, Self::TrustedPeer { .. })
    }

    pub fn policy_user(&self) -> &str {
        match self {
            Self::Client(User(user))
            | Self::TrustedPeer {
                session:
                    TrustedSession::AsUser(User(user)) | TrustedSession::AttributingToUser(User(user)),
            } => user,
            Self::TrustedPeer {
                session: TrustedSession::Admin,
            } => ADMIN_SYSTEM_USER,
        }
    }

    pub fn attribution_user(&self) -> &str {
        match self {
            Self::Client(User(user))
            | Self::TrustedPeer {
                session:
                    TrustedSession::AsUser(User(user)) | TrustedSession::AttributingToUser(User(user)),
            } => user,
            Self::TrustedPeer {
                session: TrustedSession::Admin,
            } => ADMIN_SYSTEM_USER,
        }
    }

    pub fn bypasses_policy(&self) -> bool {
        matches!(
            self,
            Self::TrustedPeer {
                session: TrustedSession::Admin | TrustedSession::AttributingToUser(_)
            }
        )
    }
}
