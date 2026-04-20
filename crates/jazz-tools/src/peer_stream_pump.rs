//! PeerStreamPump — framing-only bytes pump over a StreamAdapter.
//!
//! Extracted from `transport_manager::run_connected`. Owns no handshake,
//! no auth, no reconnect — just: drain outbox → send, recv → inbox, exit
//! on shutdown control.
//!
//! Note: `TransportManager` continues to own its own select-loop in
//! `run_connected` because the server path decodes length-prefixed
//! `ServerEvent` frames. `PeerStreamPump` is the byte-level alternative
//! used where a frame = one whole message (the worker bridge).

use crate::transport_manager::StreamAdapter;
use futures::channel::mpsc;

pub enum PumpControl {
    Shutdown,
}

pub enum PumpExit {
    NetworkError(String),
    Shutdown,
}

pub struct PeerStreamPump<S: StreamAdapter> {
    pub outbox_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    pub inbox_tx: mpsc::UnboundedSender<Vec<u8>>,
    pub control_rx: mpsc::UnboundedReceiver<PumpControl>,
    pub stream: S,
}

impl<S: StreamAdapter + 'static> PeerStreamPump<S> {
    pub async fn run(self) -> PumpExit {
        #[cfg(feature = "runtime-tokio")]
        {
            self.run_tokio().await
        }
        #[cfg(not(feature = "runtime-tokio"))]
        {
            self.run_wasm().await
        }
    }

    #[cfg(feature = "runtime-tokio")]
    async fn run_tokio(mut self) -> PumpExit {
        use futures::StreamExt as _;
        loop {
            tokio::select! {
                out = self.outbox_rx.next() => {
                    let Some(bytes) = out else { return PumpExit::Shutdown; };
                    if self.stream.send(bytes).await.is_err() {
                        return PumpExit::NetworkError("send failed".into());
                    }
                }
                incoming = self.stream.recv() => {
                    match incoming {
                        Ok(Some(bytes)) => {
                            let _ = self.inbox_tx.unbounded_send(bytes);
                        }
                        Ok(None) => return PumpExit::NetworkError("stream closed".into()),
                        Err(e) => return PumpExit::NetworkError(format!("{e}")),
                    }
                }
                ctrl = self.control_rx.next() => {
                    match ctrl {
                        None | Some(PumpControl::Shutdown) => {
                            self.stream.close().await;
                            return PumpExit::Shutdown;
                        }
                    }
                }
            }
        }
    }

    #[cfg(not(feature = "runtime-tokio"))]
    async fn run_wasm(mut self) -> PumpExit {
        use futures::{FutureExt as _, StreamExt as _};
        loop {
            futures::select! {
                out = self.outbox_rx.next().fuse() => {
                    let Some(bytes) = out else { return PumpExit::Shutdown; };
                    if self.stream.send(bytes).await.is_err() {
                        return PumpExit::NetworkError("send failed".into());
                    }
                }
                incoming = self.stream.recv().fuse() => {
                    match incoming {
                        Ok(Some(bytes)) => {
                            let _ = self.inbox_tx.unbounded_send(bytes);
                        }
                        Ok(None) => return PumpExit::NetworkError("stream closed".into()),
                        Err(e) => return PumpExit::NetworkError(format!("{e}")),
                    }
                }
                ctrl = self.control_rx.next().fuse() => {
                    match ctrl {
                        None | Some(PumpControl::Shutdown) => {
                            self.stream.close().await;
                            return PumpExit::Shutdown;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stream that blocks on recv until data arrives via a channel,
    /// so the pump does not immediately see EOF when the inbound queue is empty.
    struct LoopbackStream {
        sent: Vec<Vec<u8>>,
        inbound_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    }
    impl StreamAdapter for LoopbackStream {
        type Error = &'static str;
        async fn connect(_: &str) -> Result<Self, Self::Error> {
            let (_tx, rx) = mpsc::unbounded();
            Ok(Self {
                sent: Vec::new(),
                inbound_rx: rx,
            })
        }
        async fn send(&mut self, data: Vec<u8>) -> Result<(), Self::Error> {
            self.sent.push(data);
            Ok(())
        }
        async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
            use futures::StreamExt as _;
            Ok(self.inbound_rx.next().await)
        }
        async fn close(&mut self) {}
    }

    #[tokio::test]
    #[cfg(feature = "runtime-tokio")]
    async fn pump_drains_outbox_and_shuts_down() {
        let (outbox_tx, outbox_rx) = mpsc::unbounded();
        let (inbox_tx, mut inbox_rx) = mpsc::unbounded::<Vec<u8>>();
        let (control_tx, control_rx) = mpsc::unbounded();
        let (_inbound_tx, inbound_rx) = mpsc::unbounded::<Vec<u8>>();
        outbox_tx.unbounded_send(b"x".to_vec()).unwrap();
        let pump = PeerStreamPump {
            outbox_rx,
            inbox_tx,
            control_rx,
            stream: LoopbackStream {
                sent: Vec::new(),
                inbound_rx,
            },
        };
        let handle = tokio::spawn(pump.run());
        tokio::task::yield_now().await;
        control_tx.unbounded_send(PumpControl::Shutdown).unwrap();
        let exit = handle.await.unwrap();
        assert!(matches!(exit, PumpExit::Shutdown));
        // No inbound data should have been forwarded to inbox.
        assert!(inbox_rx.try_recv().is_err());
    }
}
