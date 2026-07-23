//! Blocking-WebSocket `WireTransport` bridging a Jazz client to the loopback
//! server. Reads are non-blocking so it fits the synchronous `tick()` model.

use std::collections::VecDeque;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use jazz::wire::{TransportError, WireTransport};
use tungstenite::Message;
use tungstenite::stream::MaybeTlsStream;

pub(crate) struct WsClientTransport {
    pub(crate) socket: tungstenite::WebSocket<MaybeTlsStream<TcpStream>>,
    pub(crate) inbox: VecDeque<Vec<u8>>,
    pub(crate) sent: Arc<AtomicU64>,
    pub(crate) recv: Arc<AtomicU64>,
    /// Set once the server closes the connection or a read errors, so a blocked
    /// write bails with an error instead of spinning forever.
    pub(crate) closed: bool,
}

/// Max wall-clock a single frame's flush may spend under backpressure before we
/// treat the connection as dead. Very generous, since late batches ingest slowly
/// (the server's history consolidation is super-linear in rows already stored).
const FLUSH_DEADLINE: Duration = Duration::from_secs(600);

fn is_would_block(error: &tungstenite::Error) -> bool {
    matches!(error, tungstenite::Error::Io(e) if e.kind() == std::io::ErrorKind::WouldBlock)
}

impl WsClientTransport {
    /// Drain every message currently available on the (non-blocking) socket into
    /// the inbox. Never blocks. Also relieves the server's send buffer, which is
    /// what lets a blocked write make progress. Flags the connection closed on a
    /// non-would-block error.
    fn pump_reads(&mut self) {
        loop {
            match self.socket.read() {
                Ok(Message::Binary(bytes)) => {
                    if let Ok(frames) = postcard::from_bytes::<Vec<Vec<u8>>>(&bytes) {
                        self.recv.fetch_add(frames.len() as u64, Ordering::Relaxed);
                        self.inbox.extend(frames);
                    }
                }
                Ok(Message::Ping(payload)) => {
                    let _ = self.socket.write(Message::Pong(payload));
                }
                Ok(_) => {}
                Err(e) if is_would_block(&e) => break,
                Err(_) => {
                    self.closed = true;
                    break;
                }
            }
        }
    }
}

impl WireTransport for WsClientTransport {
    // Non-blocking send that never drops: on write-buffer/would-block pressure it
    // drains inbound frames (relieving the server so it keeps reading us) and
    // retries until the frame is fully flushed. A single-threaded blocking send
    // would deadlock once both directions' buffers fill mid-burst. Bails with an
    // error if the connection closes or a flush stalls past FLUSH_DEADLINE.
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        if self.closed {
            return Err(TransportError::Failed("ws connection closed".to_owned()));
        }
        let batch = postcard::to_allocvec(&vec![frame])
            .map_err(|e| TransportError::Failed(format!("encode frame batch: {e}")))?;
        let deadline = Instant::now();
        let mut pending = Some(Message::Binary(batch.into()));
        while let Some(message) = pending.take() {
            match self.socket.write(message) {
                Ok(()) => {}
                Err(tungstenite::Error::WriteBufferFull(returned)) => {
                    self.pump_reads();
                    pending = Some(*returned);
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) if is_would_block(&e) => {} // queued; fall through to flush
                Err(e) => return Err(TransportError::Failed(format!("ws write: {e}"))),
            }
            if self.closed || deadline.elapsed() > FLUSH_DEADLINE {
                return Err(TransportError::Failed("ws write stalled/closed".to_owned()));
            }
        }
        loop {
            match self.socket.flush() {
                Ok(()) => break,
                Err(tungstenite::Error::WriteBufferFull(_)) => self.pump_reads(),
                Err(e) if is_would_block(&e) => {
                    self.pump_reads();
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => return Err(TransportError::Failed(format!("ws flush: {e}"))),
            }
            if self.closed || deadline.elapsed() > FLUSH_DEADLINE {
                return Err(TransportError::Failed("ws flush stalled/closed".to_owned()));
            }
        }
        self.sent.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        if let Some(frame) = self.inbox.pop_front() {
            return Some(frame);
        }
        self.pump_reads();
        self.inbox.pop_front()
    }
}
