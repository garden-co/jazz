//! WASM WebSocket stream adapter via web-sys::WebSocket.

#[cfg(target_arch = "wasm32")]
mod inner {
    use std::cell::RefCell;
    use std::rc::Rc;

    use jazz_tools::transport_manager::StreamAdapter;
    use wasm_bindgen::prelude::*;
    use web_sys::{BinaryType, WebSocket};

    type OpenSender = futures::channel::oneshot::Sender<Result<(), String>>;

    pub struct WasmWsStream {
        ws: WebSocket,
        rx: futures::channel::mpsc::UnboundedReceiver<Result<Vec<u8>, String>>,
        // Closures are stored so they outlive the WebSocket. Drop clears the
        // JS-side handlers BEFORE these are freed, so a late-fired event
        // never lands on a dropped closure.
        _on_open: Closure<dyn FnMut()>,
        _on_message: Closure<dyn FnMut(web_sys::MessageEvent)>,
        _on_close: Closure<dyn FnMut(web_sys::CloseEvent)>,
        _on_error: Closure<dyn FnMut(web_sys::Event)>,
    }

    impl Drop for WasmWsStream {
        fn drop(&mut self) {
            // Detach handlers BEFORE the closures get dropped. If we don't
            // do this, a queued event that fires after the struct dies will
            // hit a freed Closure → "closure invoked recursively or after
            // being dropped".
            self.ws.set_onopen(None);
            self.ws.set_onmessage(None);
            self.ws.set_onclose(None);
            self.ws.set_onerror(None);
            let _ = self.ws.close();
        }
    }

    impl StreamAdapter for WasmWsStream {
        type Error = String;

        async fn connect(url: &str) -> Result<Self, String> {
            use futures::channel::{mpsc, oneshot};

            let (open_tx, open_rx) = oneshot::channel::<Result<(), String>>();
            let (msg_tx, msg_rx) = mpsc::unbounded::<Result<Vec<u8>, String>>();

            let ws = WebSocket::new(url).map_err(|e| format!("{e:?}"))?;
            ws.set_binary_type(BinaryType::Arraybuffer);

            // Shared one-shot slot so onopen / onerror / onclose can all
            // resolve the open future, but only the first one wins.
            let open_slot: Rc<RefCell<Option<OpenSender>>> = Rc::new(RefCell::new(Some(open_tx)));

            // onopen — resolves connect() with Ok(())
            let open_slot_open = open_slot.clone();
            let on_open = Closure::wrap(Box::new(move || {
                if let Some(tx) = open_slot_open.borrow_mut().take() {
                    let _ = tx.send(Ok(()));
                }
            }) as Box<dyn FnMut()>);
            ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));

            // onmessage — push binary frames into the recv channel
            let msg_tx_msg = msg_tx.clone();
            let on_message = Closure::wrap(Box::new(move |e: web_sys::MessageEvent| {
                let buf: js_sys::ArrayBuffer = match e.data().dyn_into() {
                    Ok(b) => b,
                    Err(_) => return,
                };
                let data = js_sys::Uint8Array::new(&buf).to_vec();
                let _ = msg_tx_msg.unbounded_send(Ok(data));
            }) as Box<dyn FnMut(web_sys::MessageEvent)>);
            ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));

            // onclose — signals EOF on the recv channel; if it fires before
            // open, also rejects connect().
            let msg_tx_close = msg_tx.clone();
            let open_slot_close = open_slot.clone();
            let on_close = Closure::wrap(Box::new(move |e: web_sys::CloseEvent| {
                if let Some(tx) = open_slot_close.borrow_mut().take() {
                    let _ = tx.send(Err(format!(
                        "closed before open (code={}, reason={:?})",
                        e.code(),
                        e.reason()
                    )));
                }
                let _ = msg_tx_close.unbounded_send(Err("closed".into()));
            }) as Box<dyn FnMut(web_sys::CloseEvent)>);
            ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));

            // onerror — primary fail-fast path for DNS/TLS/offline.
            let open_slot_err = open_slot;
            let on_error = Closure::wrap(Box::new(move |_e: web_sys::Event| {
                if let Some(tx) = open_slot_err.borrow_mut().take() {
                    let _ = tx.send(Err("websocket error".into()));
                }
                // Don't push to msg_tx here; the browser will follow with
                // an onclose event that handles the read-side EOF.
            }) as Box<dyn FnMut(web_sys::Event)>);
            ws.set_onerror(Some(on_error.as_ref().unchecked_ref()));

            // Wait for open / error / close
            let outcome = open_rx
                .await
                .map_err(|_| "open channel dropped".to_string())?;

            // If we never opened, the WebSocket is dead — clear handlers
            // before bubbling the error so we don't leak closures.
            if let Err(e) = outcome {
                ws.set_onopen(None);
                ws.set_onmessage(None);
                ws.set_onclose(None);
                ws.set_onerror(None);
                let _ = ws.close();
                drop(on_open);
                drop(on_message);
                drop(on_close);
                drop(on_error);
                return Err(e);
            }

            Ok(Self {
                ws,
                rx: msg_rx,
                _on_open: on_open,
                _on_message: on_message,
                _on_close: on_close,
                _on_error: on_error,
            })
        }

        async fn send(&mut self, data: &[u8]) -> Result<(), String> {
            self.ws
                .send_with_u8_array(data)
                .map_err(|e| format!("{e:?}"))
        }

        async fn recv(&mut self) -> Result<Option<Vec<u8>>, String> {
            use futures::StreamExt as _;
            match self.rx.next().await {
                Some(Ok(data)) => Ok(Some(data)),
                Some(Err(_)) | None => Ok(None),
            }
        }

        async fn close(&mut self) {
            // Detach handlers first to avoid late events on dropped closures.
            self.ws.set_onopen(None);
            self.ws.set_onmessage(None);
            self.ws.set_onclose(None);
            self.ws.set_onerror(None);
            let _ = self.ws.close();
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub use inner::WasmWsStream;
