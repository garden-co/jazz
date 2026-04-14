//! WASM WebSocket stream adapter via web-sys::WebSocket.

#[cfg(target_arch = "wasm32")]
mod inner {
    use jazz_tools::transport_manager::StreamAdapter;
    use wasm_bindgen::prelude::*;
    use web_sys::{BinaryType, WebSocket};

    pub struct WasmWsStream {
        ws: WebSocket,
        rx: futures::channel::mpsc::UnboundedReceiver<Result<Vec<u8>, String>>,
        _on_message: Closure<dyn FnMut(web_sys::MessageEvent)>,
        _on_close: Closure<dyn FnMut(web_sys::CloseEvent)>,
    }

    impl StreamAdapter for WasmWsStream {
        type Error = String;

        async fn connect(url: &str) -> Result<Self, String> {
            use futures::channel::{mpsc, oneshot};

            let (open_tx, open_rx) = oneshot::channel::<Result<(), String>>();
            let (msg_tx, msg_rx) = mpsc::unbounded::<Result<Vec<u8>, String>>();

            let ws = WebSocket::new(url).map_err(|e| format!("{e:?}"))?;
            ws.set_binary_type(BinaryType::Arraybuffer);

            // onopen — resolves the connect() future
            let open_tx_cell = std::cell::Cell::new(Some(open_tx));
            let on_open = Closure::once(move || {
                if let Some(tx) = open_tx_cell.take() {
                    let _ = tx.send(Ok(()));
                }
            });
            ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));
            on_open.forget();

            // onmessage — pushes binary frames into the recv channel
            let msg_tx_msg = msg_tx.clone();
            let on_message = Closure::wrap(Box::new(move |e: web_sys::MessageEvent| {
                let buf: js_sys::ArrayBuffer = e.data().dyn_into().unwrap();
                let data = js_sys::Uint8Array::new(&buf).to_vec();
                let _ = msg_tx_msg.unbounded_send(Ok(data));
            }) as Box<dyn FnMut(web_sys::MessageEvent)>);
            ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));

            // onclose — signals EOF on the recv channel
            let msg_tx_close = msg_tx;
            let on_close = Closure::wrap(Box::new(move |_e: web_sys::CloseEvent| {
                let _ = msg_tx_close.unbounded_send(Err("closed".into()));
            }) as Box<dyn FnMut(web_sys::CloseEvent)>);
            ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));

            // Wait for the open event
            open_rx
                .await
                .map_err(|_| "open channel dropped".to_string())??;

            Ok(Self {
                ws,
                rx: msg_rx,
                _on_message: on_message,
                _on_close: on_close,
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
            let _ = self.ws.close();
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub use inner::WasmWsStream;
