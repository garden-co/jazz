use std::collections::BTreeMap;

use crate::object::Object;

/// Generate a new UUIDv7 as u128.
pub fn generate_object_id() -> u128 {
    uuid::Uuid::now_v7().as_u128()
}

/// A local node managing multiple objects.
#[derive(Debug, Default)]
pub struct LocalNode {
    objects: BTreeMap<u128, Object>,
}

impl LocalNode {
    pub fn new() -> Self {
        LocalNode {
            objects: BTreeMap::new(),
        }
    }

    /// Create a new object with the given prefix. Returns the object ID.
    pub fn create_object(&mut self, prefix: impl Into<String>) -> u128 {
        let id = generate_object_id();
        let object = Object::new(id, prefix);
        self.objects.insert(id, object);
        id
    }

    /// Get an object by ID.
    pub fn get_object(&self, id: u128) -> Option<&Object> {
        self.objects.get(&id)
    }

    /// Get an object by ID mutably.
    pub fn get_object_mut(&mut self, id: u128) -> Option<&mut Object> {
        self.objects.get_mut(&id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_node_create_and_get_objects() {
        let mut node = LocalNode::new();

        let id1 = node.create_object("chat");
        let id2 = node.create_object("message");

        assert!(node.get_object(id1).is_some());
        assert!(node.get_object(id2).is_some());
        assert!(node.get_object(999).is_none());

        assert_eq!(node.get_object(id1).unwrap().prefix, "chat");
        assert_eq!(node.get_object(id2).unwrap().prefix, "message");
    }

    #[test]
    fn uuidv7_is_unique_and_ordered() {
        let id1 = generate_object_id();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let id2 = generate_object_id();

        assert_ne!(id1, id2);
        // UUIDv7 should be roughly time-ordered
        assert!(id2 > id1);
    }

    #[test]
    fn local_node_uses_uuidv7() {
        let mut node = LocalNode::new();

        let id1 = node.create_object("test1");
        std::thread::sleep(std::time::Duration::from_millis(1));
        let id2 = node.create_object("test2");

        // IDs should be valid UUIDv7 (time-ordered)
        assert!(id2 > id1);

        // Should be large numbers (not sequential 1, 2, 3...)
        assert!(id1 > 1000);
    }
}
