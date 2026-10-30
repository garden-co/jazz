//! Explicit network gates for integration tests using the real native transport.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak};

use jazz::tools::native_transport_connector::{
    NativeTransportConnector, NativeTransportFuture, NativeTransportRequest,
};
use jazz::wire::{TransportError, WireTransport};

/// Controls delivery on one test client's server connections without disconnecting.
/// Blocked frames retain their original order. New connections inherit the gates.
#[derive(Clone, Default)]
pub struct TransportControl(Arc<Mutex<Gates>>);

#[derive(Default)]
struct Gates {
    inbound: bool,
    outbound: bool,
    links: Vec<Weak<Mutex<Link>>>,
}

struct Link {
    transport: Box<dyn WireTransport + Send>,
    outbound: VecDeque<Vec<u8>>,
    wake: Arc<dyn Fn() + Send + Sync>,
}

impl TransportControl {
    /// Stop delivery in both directions. Frames already sent cannot be recalled.
    pub fn block(&self) {
        let mut gates = self.0.lock().unwrap();
        gates.inbound = true;
        gates.outbound = true;
    }

    /// Resume both directions and wake the client to consume queued responses.
    pub fn unblock(&self) -> Result<(), TransportError> {
        let mut gates = self.0.lock().unwrap();
        gates.links.retain(|link| link.strong_count() > 0);
        let mut wakes = Vec::new();
        // Keep sends blocked until the old queue has been flushed, so a
        // concurrent new send cannot overtake buffered frames.
        for link in gates.links.iter().filter_map(Weak::upgrade) {
            let mut link = link.lock().unwrap();
            while let Some(frame) = link.outbound.pop_front() {
                link.transport.send_frame(frame)?;
            }
            wakes.push(link.wake.clone());
        }
        gates.inbound = false;
        gates.outbound = false;
        drop(gates);
        for wake in wakes {
            wake();
        }
        Ok(())
    }

    /// Stop only server-to-client delivery, allowing writes to reach the server.
    pub fn block_inbound(&self) {
        self.0.lock().unwrap().inbound = true;
    }
}

pub(crate) struct ControlledConnector {
    pub control: TransportControl,
    pub inner: Arc<dyn NativeTransportConnector>,
}
impl NativeTransportConnector for ControlledConnector {
    fn connect(&self, request: NativeTransportRequest) -> NativeTransportFuture {
        let control = self.control.clone();
        let connector = self.inner.clone();
        let wake = request.wake.clone();
        Box::pin(async move {
            let mut connected = connector.connect(request).await?;
            let link = Arc::new(Mutex::new(Link {
                transport: connected.transport,
                outbound: VecDeque::new(),
                wake,
            }));
            control.0.lock().unwrap().links.push(Arc::downgrade(&link));
            connected.transport = Box::new(ControlledTransport { control, link });
            Ok(connected)
        })
    }
}

struct ControlledTransport {
    control: TransportControl,
    link: Arc<Mutex<Link>>,
}

impl WireTransport for ControlledTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        let gates = self.control.0.lock().unwrap();
        let mut link = self.link.lock().unwrap();
        if gates.outbound {
            link.outbound.push_back(frame);
            Ok(())
        } else {
            link.transport.send_frame(frame)
        }
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        let gates = self.control.0.lock().unwrap();
        if gates.inbound {
            return None;
        }
        self.link.lock().unwrap().transport.try_recv_frame()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct Frames {
        sent: Vec<Vec<u8>>,
        received: VecDeque<Vec<u8>>,
    }

    struct RecordingTransport(Arc<Mutex<Frames>>);
    impl WireTransport for RecordingTransport {
        fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
            self.0.lock().unwrap().sent.push(frame);
            Ok(())
        }
        fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
            self.0.lock().unwrap().received.pop_front()
        }
    }

    /// Alice's blocked link retains both directions and releases outgoing frames
    /// in order before her next send. This tests the transport fixture itself,
    /// so recording bytes directly is necessary to verify delivery order.
    #[test]
    fn unblock_releases_queued_frames_in_order_and_wakes_client() {
        let control = TransportControl::default();
        let frames = Arc::new(Mutex::new(Frames::default()));
        frames.lock().unwrap().received.push_back(vec![9]);
        let wakes = Arc::new(AtomicUsize::new(0));
        let wake_count = wakes.clone();
        let link = Arc::new(Mutex::new(Link {
            transport: Box::new(RecordingTransport(frames.clone())),
            outbound: VecDeque::new(),
            wake: Arc::new(move || {
                wake_count.fetch_add(1, Ordering::SeqCst);
            }),
        }));
        control.0.lock().unwrap().links.push(Arc::downgrade(&link));
        let mut transport = ControlledTransport {
            control: control.clone(),
            link,
        };

        control.block();
        transport.send_frame(vec![1]).unwrap();
        transport.send_frame(vec![2]).unwrap();
        assert!(frames.lock().unwrap().sent.is_empty());
        assert_eq!(transport.try_recv_frame(), None);

        control.unblock().unwrap();
        transport.send_frame(vec![3]).unwrap();
        assert_eq!(frames.lock().unwrap().sent, vec![vec![1], vec![2], vec![3]]);
        assert_eq!(transport.try_recv_frame(), Some(vec![9]));
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
    }
}
