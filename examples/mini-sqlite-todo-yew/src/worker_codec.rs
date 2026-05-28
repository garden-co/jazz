use gloo_worker::Codec;
use serde::{Deserialize, Serialize};
use wasm_bindgen::JsValue;

pub struct JsonCodec;

impl Codec for JsonCodec {
    fn encode<I>(input: I) -> JsValue
    where
        I: Serialize,
    {
        JsValue::from_str(&serde_json::to_string(&input).expect("can't serialize worker message"))
    }

    fn decode<O>(input: JsValue) -> O
    where
        O: for<'de> Deserialize<'de>,
    {
        let encoded = input.as_string().expect("worker message is not a string");
        serde_json::from_str(&encoded).expect("can't deserialize worker message")
    }
}
