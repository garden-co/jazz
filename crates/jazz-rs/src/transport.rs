//! HTTP/SSE transport for server communication.

#![allow(dead_code)]

use groove::sync_manager::{ClientId, SyncPayload};
use jazz_transport::{
    ConnectionId, CreateObjectRequest, CreateObjectResponse, SubscribeRequest, SubscribeResponse,
    SyncPayloadRequest, UnsubscribeRequest,
};
use reqwest::Client;

use crate::{JazzError, Result};

/// Connection to a Jazz server.
pub struct ServerConnection {
    client: Client,
    base_url: String,
    connection_id: Option<ConnectionId>,
}

impl ServerConnection {
    /// Connect to a Jazz server.
    pub async fn connect(base_url: &str) -> Result<Self> {
        let client = Client::new();

        // Test connection with health check
        let health_url = format!("{}/health", base_url);
        client
            .get(&health_url)
            .send()
            .await?
            .error_for_status()
            .map_err(|e| JazzError::Connection(e.to_string()))?;

        Ok(Self {
            client,
            base_url: base_url.to_string(),
            connection_id: None,
        })
    }

    /// Subscribe to a query on the server.
    pub async fn subscribe(&self, request: SubscribeRequest) -> Result<SubscribeResponse> {
        let url = format!("{}/sync/subscribe", self.base_url);
        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await?
            .error_for_status()
            .map_err(|e| JazzError::Sync(e.to_string()))?;

        let result: SubscribeResponse = response.json().await?;
        Ok(result)
    }

    /// Unsubscribe from a query.
    pub async fn unsubscribe(&self, request: UnsubscribeRequest) -> Result<()> {
        let url = format!("{}/sync/unsubscribe", self.base_url);
        self.client
            .post(&url)
            .json(&request)
            .send()
            .await?
            .error_for_status()
            .map_err(|e| JazzError::Sync(e.to_string()))?;

        Ok(())
    }

    /// Create an object on the server.
    pub async fn create_object(
        &self,
        request: CreateObjectRequest,
    ) -> Result<CreateObjectResponse> {
        let url = format!("{}/sync/object", self.base_url);
        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await?
            .error_for_status()
            .map_err(|e| JazzError::Sync(e.to_string()))?;

        let result: CreateObjectResponse = response.json().await?;
        Ok(result)
    }

    /// Push a sync payload to the server.
    pub async fn push_sync(&self, payload: SyncPayload, client_id: ClientId) -> Result<()> {
        let url = format!("{}/sync", self.base_url);
        let request = SyncPayloadRequest { payload, client_id };

        self.client
            .post(&url)
            .json(&request)
            .send()
            .await?
            .error_for_status()
            .map_err(|e| JazzError::Sync(e.to_string()))?;

        Ok(())
    }

    /// Get the connection ID (once connected via SSE).
    pub fn connection_id(&self) -> Option<ConnectionId> {
        self.connection_id
    }

    /// Get the base URL for this connection.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }
}
