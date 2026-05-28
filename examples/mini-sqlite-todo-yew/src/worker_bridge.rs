#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]

use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[cfg(target_arch = "wasm32")]
use std::cell::{Cell, RefCell};
#[cfg(target_arch = "wasm32")]
use std::marker::PhantomData;
#[cfg(target_arch = "wasm32")]
use std::rc::Rc;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::{closure::Closure, JsCast, JsValue};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local;
#[cfg(target_arch = "wasm32")]
use web_sys::{DedicatedWorkerGlobalScope, MessageEvent, Worker};

#[derive(Serialize, Deserialize)]
enum MainToWorker<I> {
    Input(I),
    Inputs(Vec<I>),
}

#[derive(Serialize, Deserialize)]
enum WorkerToMain<O> {
    Ready,
    Output(O),
    Outputs(Vec<O>),
    DecodeError { message: String },
}

#[cfg(test)]
pub(crate) fn encode_main_input<I>(input: &I) -> Result<String, String>
where
    I: Serialize + ?Sized,
{
    serde_json::to_string(&MainToWorker::Input(input)).map_err(|error| error.to_string())
}

pub(crate) fn encode_main_inputs<I>(inputs: Vec<I>) -> Result<String, String>
where
    I: Serialize,
{
    serde_json::to_string(&MainToWorker::Inputs(inputs)).map_err(|error| error.to_string())
}

#[cfg(test)]
pub(crate) fn decode_main_input<I>(encoded: &str) -> Result<I, String>
where
    I: DeserializeOwned,
{
    let mut inputs = decode_main_inputs(encoded)?;
    if inputs.len() == 1 {
        Ok(inputs.remove(0))
    } else {
        Err(format!("expected one worker input, got {}", inputs.len()))
    }
}

pub(crate) fn decode_main_inputs<I>(encoded: &str) -> Result<Vec<I>, String>
where
    I: DeserializeOwned,
{
    match serde_json::from_str(encoded).map_err(|error| error.to_string())? {
        MainToWorker::Input(input) => Ok(vec![input]),
        MainToWorker::Inputs(inputs) => Ok(inputs),
    }
}

#[cfg(target_arch = "wasm32")]
pub struct WorkerClient<I, O> {
    worker: Worker,
    ready: Rc<Cell<bool>>,
    pending: Rc<RefCell<Vec<I>>>,
    outbox: Rc<RefCell<Vec<I>>>,
    flush_scheduled: Rc<Cell<bool>>,
    _onmessage: Closure<dyn FnMut(MessageEvent)>,
    _output: PhantomData<fn() -> O>,
}

#[cfg(target_arch = "wasm32")]
impl<I, O> WorkerClient<I, O>
where
    I: Serialize + 'static,
    O: DeserializeOwned + 'static,
{
    pub fn spawn(
        loader_url: &str,
        on_output: impl Fn(O) + 'static,
    ) -> Result<WorkerClient<I, O>, String> {
        let worker = Worker::new(loader_url).map_err(|error| format!("spawn worker: {error:?}"))?;
        let ready = Rc::new(Cell::new(false));
        let pending = Rc::new(RefCell::new(Vec::<I>::new()));
        let outbox = Rc::new(RefCell::new(Vec::<I>::new()));
        let flush_scheduled = Rc::new(Cell::new(false));
        let on_output = Rc::new(on_output);

        let onmessage = Closure::wrap(Box::new({
            let worker = worker.clone();
            let ready = ready.clone();
            let pending = pending.clone();
            let outbox = outbox.clone();
            let flush_scheduled = flush_scheduled.clone();
            let on_output = on_output.clone();
            move |event: MessageEvent| {
                let Some(encoded) = event.data().as_string() else {
                    log_bridge_error("worker sent a non-string message");
                    return;
                };

                match decode_worker_frame::<O>(&encoded) {
                    Ok(WorkerToMain::Ready) => {
                        ready.set(true);
                        let pending_inputs = pending.replace(Vec::new());
                        if !pending_inputs.is_empty() {
                            outbox.borrow_mut().extend(pending_inputs);
                            schedule_main_flush(
                                worker.clone(),
                                outbox.clone(),
                                flush_scheduled.clone(),
                            );
                        }
                    }
                    Ok(WorkerToMain::Output(output)) => {
                        on_output(output);
                    }
                    Ok(WorkerToMain::Outputs(outputs)) => {
                        for output in outputs {
                            on_output(output);
                        }
                    }
                    Ok(WorkerToMain::DecodeError { message }) => {
                        log_bridge_error(&message);
                    }
                    Err(error) => {
                        log_bridge_error(&error);
                    }
                }
            }
        }) as Box<dyn FnMut(MessageEvent)>);

        worker.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));

        Ok(Self {
            worker,
            ready,
            pending,
            outbox,
            flush_scheduled,
            _onmessage: onmessage,
            _output: PhantomData,
        })
    }

    pub fn send(&self, input: I) -> Result<(), String> {
        if self.ready.get() {
            self.outbox.borrow_mut().push(input);
            schedule_main_flush(
                self.worker.clone(),
                self.outbox.clone(),
                self.flush_scheduled.clone(),
            );
            Ok(())
        } else {
            self.pending.borrow_mut().push(input);
            Ok(())
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub struct WorkerResponder<O> {
    scope: DedicatedWorkerGlobalScope,
    outbox: Rc<RefCell<Vec<O>>>,
    flush_scheduled: Rc<Cell<bool>>,
    _output: PhantomData<fn() -> O>,
}

#[cfg(target_arch = "wasm32")]
impl<O> Clone for WorkerResponder<O> {
    fn clone(&self) -> Self {
        Self {
            scope: self.scope.clone(),
            outbox: self.outbox.clone(),
            flush_scheduled: self.flush_scheduled.clone(),
            _output: PhantomData,
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl<O> WorkerResponder<O>
where
    O: Serialize + 'static,
{
    pub fn send(&self, output: O) {
        self.outbox.borrow_mut().push(output);
        schedule_worker_flush(
            self.scope.clone(),
            self.outbox.clone(),
            self.flush_scheduled.clone(),
        );
    }

    fn decode_error(&self, message: String) {
        if let Err(error) =
            post_worker_frame(&self.scope, &WorkerToMain::<O>::DecodeError { message })
        {
            log_bridge_error(&error);
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub fn register_worker<I, O>(mut handler: impl FnMut(I, WorkerResponder<O>) + 'static)
where
    I: DeserializeOwned + 'static,
    O: Serialize + 'static,
{
    let scope: DedicatedWorkerGlobalScope = js_sys::global().unchecked_into();
    let responder = WorkerResponder {
        scope: scope.clone(),
        outbox: Rc::new(RefCell::new(Vec::new())),
        flush_scheduled: Rc::new(Cell::new(false)),
        _output: PhantomData,
    };
    let onmessage = Closure::wrap(Box::new({
        let responder = responder.clone();
        move |event: MessageEvent| {
            let Some(encoded) = event.data().as_string() else {
                responder.decode_error("main thread sent a non-string message".to_owned());
                return;
            };

            match decode_main_inputs(&encoded) {
                Ok(inputs) => {
                    for input in inputs {
                        handler(input, responder.clone());
                    }
                }
                Err(message) => responder.decode_error(message),
            }
        }
    }) as Box<dyn FnMut(MessageEvent)>);

    scope.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
    onmessage.forget();

    if let Err(error) = post_worker_frame(&scope, &WorkerToMain::<O>::Ready) {
        log_bridge_error(&error);
    }
}

#[cfg(target_arch = "wasm32")]
fn post_main_inputs<I>(worker: &Worker, inputs: Vec<I>) -> Result<(), String>
where
    I: Serialize,
{
    let encoded = encode_main_inputs(inputs)?;
    worker
        .post_message(&JsValue::from_str(&encoded))
        .map_err(|error| format!("post message to worker: {error:?}"))
}

#[cfg(target_arch = "wasm32")]
fn schedule_main_flush<I>(
    worker: Worker,
    outbox: Rc<RefCell<Vec<I>>>,
    flush_scheduled: Rc<Cell<bool>>,
) where
    I: Serialize + 'static,
{
    if flush_scheduled.replace(true) {
        return;
    }

    spawn_local(async move {
        flush_scheduled.set(false);
        let inputs = outbox.replace(Vec::new());
        if inputs.is_empty() {
            return;
        }
        if let Err(error) = post_main_inputs(&worker, inputs) {
            log_bridge_error(&error);
        }
    });
}

#[cfg(target_arch = "wasm32")]
fn post_worker_frame<O>(
    scope: &DedicatedWorkerGlobalScope,
    frame: &WorkerToMain<O>,
) -> Result<(), String>
where
    O: Serialize,
{
    let encoded = encode_worker_frame(frame)?;
    scope
        .post_message(&JsValue::from_str(&encoded))
        .map_err(|error| format!("post message to main thread: {error:?}"))
}

#[cfg(target_arch = "wasm32")]
fn post_worker_outputs<O>(scope: &DedicatedWorkerGlobalScope, outputs: Vec<O>) -> Result<(), String>
where
    O: Serialize,
{
    let encoded = encode_worker_frame(&WorkerToMain::Outputs(outputs))?;
    scope
        .post_message(&JsValue::from_str(&encoded))
        .map_err(|error| format!("post message to main thread: {error:?}"))
}

#[cfg(target_arch = "wasm32")]
fn schedule_worker_flush<O>(
    scope: DedicatedWorkerGlobalScope,
    outbox: Rc<RefCell<Vec<O>>>,
    flush_scheduled: Rc<Cell<bool>>,
) where
    O: Serialize + 'static,
{
    if flush_scheduled.replace(true) {
        return;
    }

    spawn_local(async move {
        flush_scheduled.set(false);
        let outputs = outbox.replace(Vec::new());
        if outputs.is_empty() {
            return;
        }
        if let Err(error) = post_worker_outputs(&scope, outputs) {
            log_bridge_error(&error);
        }
    });
}

#[cfg(target_arch = "wasm32")]
fn encode_worker_frame<O>(frame: &WorkerToMain<O>) -> Result<String, String>
where
    O: Serialize,
{
    serde_json::to_string(frame).map_err(|error| error.to_string())
}

#[cfg(target_arch = "wasm32")]
fn decode_worker_frame<O>(encoded: &str) -> Result<WorkerToMain<O>, String>
where
    O: DeserializeOwned,
{
    serde_json::from_str(encoded).map_err(|error| error.to_string())
}

#[cfg(target_arch = "wasm32")]
fn log_bridge_error(message: &str) {
    web_sys::console::error_1(&JsValue::from_str(message));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct TestMessage {
        id: u64,
        title: String,
    }

    #[test]
    fn bridge_messages_round_trip_through_serde_json() {
        let encoded = super::encode_main_input(&TestMessage {
            id: 7,
            title: "serde bridge".to_owned(),
        })
        .unwrap();

        let decoded: TestMessage = super::decode_main_input(&encoded).unwrap();

        assert_eq!(
            decoded,
            TestMessage {
                id: 7,
                title: "serde bridge".to_owned()
            }
        );
    }

    #[test]
    fn batched_bridge_messages_round_trip_through_serde_json() {
        let encoded = super::encode_main_inputs(vec![
            TestMessage {
                id: 1,
                title: "first".to_owned(),
            },
            TestMessage {
                id: 2,
                title: "second".to_owned(),
            },
        ])
        .unwrap();

        let decoded: Vec<TestMessage> = super::decode_main_inputs(&encoded).unwrap();

        assert_eq!(
            decoded,
            vec![
                TestMessage {
                    id: 1,
                    title: "first".to_owned()
                },
                TestMessage {
                    id: 2,
                    title: "second".to_owned()
                }
            ]
        );
    }
}
