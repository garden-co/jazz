use crate::core::{CoID, SessionID};
use serde_json::Value as JsonValue;

#[derive(Clone, Debug)]
pub struct NonceGenerator {
    co_id: CoID,
    session_id: SessionID,
}

impl NonceGenerator {
    pub fn new(co_id: CoID, session_id: SessionID) -> Self {
        Self { co_id, session_id }
    }

    pub fn get_nonce(&self, tx_index: u32) -> [u8; 24] {
        let nonce_material = self.generate_nonce_material(tx_index);
        self.generate_json_nonce(&nonce_material)
    }

    /// Generate the nonce material (as JSON) for a given transaction index.
    /// This ensures each transaction gets a unique nonce based on session and index.
    fn generate_nonce_material(&self, tx_index: u32) -> JsonValue {
        // Avoid unnecessary allocations by constructing the map directly
        let mut map = serde_json::Map::with_capacity(2);
        map.insert("in".into(), JsonValue::String(self.co_id.0.clone()));
        // Directly construct the TransactionID as a JSON object to avoid serialization overhead
        let mut tx_map = serde_json::Map::with_capacity(2);
        tx_map.insert(
            "sessionID".into(),
            JsonValue::String(self.session_id.0.clone()),
        );
        tx_map.insert("txIndex".into(), JsonValue::from(tx_index));
        map.insert("tx".into(), JsonValue::Object(tx_map));
        JsonValue::Object(map)
    }

    /// Generate a 24-byte nonce from arbitrary bytes using blake3.
    fn generate_nonce(&self, material: &[u8]) -> [u8; 24] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(material);
        let mut output = [0u8; 24];
        let mut output_reader = hasher.finalize_xof();
        output_reader.fill(&mut output);
        output
    }

    /// Generate a 24-byte nonce from a JSON value by serializing it and hashing.
    fn generate_json_nonce(&self, material: &JsonValue) -> [u8; 24] {
        let stable_json = serde_json::to_string(&material).unwrap();
        self.generate_nonce(stable_json.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nonce_generator() {
        let nonce_generator = NonceGenerator::new(
            CoID(String::from("test_co_id")),
            SessionID(String::from("test_session_id")),
        );
        let nonce = nonce_generator.get_nonce(0);
        assert_eq!(nonce.len(), 24);
    }

    #[test]
    fn test_nonce_generator_different_transactions() {
        let nonce_generator = NonceGenerator::new(
            CoID(String::from("test_co_id")),
            SessionID(String::from("test_session_id")),
        );
        let nonce = nonce_generator.get_nonce(0);
        let nonce2 = nonce_generator.get_nonce(1);
        assert_ne!(nonce.to_vec(), nonce2.to_vec());
        assert_eq!(nonce.len(), 24);
        assert_eq!(nonce2.len(), 24);
    }

    #[test]
    fn test_nonce_generator_different_sessions() {
        let nonce_generator = NonceGenerator::new(
            CoID(String::from("test_co_id")),
            SessionID(String::from("test_session_id")),
        );
        let nonce_generator2 = NonceGenerator::new(
            CoID(String::from("test_co_id")),
            SessionID(String::from("test_session_id_2")),
        );
        let nonce = nonce_generator.get_nonce(0);
        let nonce2 = nonce_generator2.get_nonce(0);
        assert_ne!(nonce.to_vec(), nonce2.to_vec());
        assert_eq!(nonce.len(), 24);
        assert_eq!(nonce2.len(), 24);
    }
}
