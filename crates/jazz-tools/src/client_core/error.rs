#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientError {
    message: String,
    binding_message: Option<String>,
}

impl ClientError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            binding_message: None,
        }
    }

    pub(crate) fn from_runtime(error: &(impl std::fmt::Debug + std::fmt::Display)) -> Self {
        Self {
            message: error.to_string(),
            binding_message: Some(format!("{error:?}")),
        }
    }

    pub(crate) fn binding_message(&self) -> &str {
        self.binding_message.as_deref().unwrap_or(&self.message)
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ClientError {}
