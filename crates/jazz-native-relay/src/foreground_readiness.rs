//! Readiness receipts for open, lease-owned foregrounds. Waiting for the
//! relay's lifecycle lock could make a sibling store's open or close spend
//! this foreground's readiness deadline.

use super::{
    ForegroundDbCommandRequest as Request, ForegroundDbCommandResponse as Response,
    ForegroundReady, JazzNativeRelayStatus as Status, NativeRelayClient, foreground_engine_status,
    relay::NATIVE_RELAY_FOREGROUND_PENDING_MAX,
};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// The upper half of the JS-safe integer range belongs to host readiness
/// receipts. Owner-local operation handles grow from 1.
const FIRST_RECEIPT: u64 = 1 << 52;

#[derive(Default)]
pub(super) struct ForegroundReadiness {
    foregrounds: BTreeMap<u64, OpenForeground>,
    next_receipt: u64,
}

struct OpenForeground {
    runtime_token: u64,
    client: NativeRelayClient,
    pending: BTreeMap<u64, ForegroundReady>,
}

pub(super) enum ReadinessCommand {
    Start(Result<Duration, serde_json::Error>),
    Poll(u64),
    Cancel(u64),
}

impl ReadinessCommand {
    pub(super) fn from_foreground(command: &Request) -> Option<Self> {
        match command {
            Request::EncryptionCall { op, args_json } if op == "ready" => Some(Self::Start(
                serde_json::from_str::<serde_json::Value>(args_json).map(|args| {
                    Duration::from_millis(args["timeoutMs"].as_u64().unwrap_or(60_000))
                }),
            )),
            Request::Poll { operation } if *operation >= FIRST_RECEIPT => {
                Some(Self::Poll(*operation))
            }
            Request::Cancel { operation } if *operation >= FIRST_RECEIPT => {
                Some(Self::Cancel(*operation))
            }
            _ => None,
        }
    }
}

impl ForegroundReadiness {
    pub(super) fn register(
        &mut self,
        foreground: u64,
        runtime_token: u64,
        client: NativeRelayClient,
    ) {
        self.foregrounds.insert(
            foreground,
            OpenForeground {
                runtime_token,
                client,
                pending: BTreeMap::new(),
            },
        );
    }

    pub(super) fn revoke(&mut self, foreground: u64) {
        if let Some(opened) = self.foregrounds.remove(&foreground) {
            for ready in opened.pending.values() {
                ready.cancel();
            }
        }
    }

    pub(super) fn execute(
        &mut self,
        foreground: u64,
        runtime_token: u64,
        command: ReadinessCommand,
        started: Instant,
    ) -> Result<Response, Status> {
        let opened = self
            .foregrounds
            .get_mut(&foreground)
            .filter(|opened| opened.runtime_token == runtime_token)
            .ok_or(Status::InvalidHandle)?;
        match command {
            ReadinessCommand::Start(Ok(timeout)) => {
                Self::start(opened, &mut self.next_receipt, started, timeout)
            }
            ReadinessCommand::Start(Err(error)) => Ok(Response::OperationError {
                reason: error.to_string(),
            }),
            ReadinessCommand::Poll(operation) => {
                let Some(ready) = opened.pending.get(&operation) else {
                    return Ok(Response::OperationError {
                        reason: "unknown foreground operation".to_owned(),
                    });
                };
                match ready.poll() {
                    Some(result) => {
                        opened.pending.remove(&operation);
                        Ok(result)
                    }
                    None => Ok(Response::Pending { operation }),
                }
            }
            ReadinessCommand::Cancel(operation) => {
                let ready = opened.pending.remove(&operation);
                if let Some(ready) = &ready {
                    ready.cancel();
                }
                Ok(Response::Cancelled {
                    cancelled: ready.is_some(),
                })
            }
        }
    }

    fn start(
        opened: &mut OpenForeground,
        next_receipt: &mut u64,
        started: Instant,
        timeout: Duration,
    ) -> Result<Response, Status> {
        if opened.pending.len() >= NATIVE_RELAY_FOREGROUND_PENDING_MAX {
            return Ok(Response::OperationError {
                reason: "foreground operation capacity reached; poll or cancel one first"
                    .to_owned(),
            });
        }
        let ready = ForegroundReady::new(started, timeout);
        if let Some(result) = ready.poll() {
            return Ok(result);
        }
        if *next_receipt >= FIRST_RECEIPT {
            return Err(Status::LifecycleFailure);
        }
        let operation = FIRST_RECEIPT + *next_receipt;
        opened
            .client
            .enqueue_foreground_ready(ready.clone())
            .map_err(foreground_engine_status)?;
        *next_receipt += 1;
        opened.pending.insert(operation, ready);
        Ok(Response::Pending { operation })
    }
}
