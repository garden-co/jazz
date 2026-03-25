use crate::object::ObjectId;
use std::collections::HashMap;

pub type SubscriptionId = u64;

pub struct SubscriptionManager {
    next_id: u64,
    global_subscribers: HashMap<SubscriptionId, Box<dyn FnMut(ObjectId)>>,
}

impl SubscriptionManager {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            global_subscribers: HashMap::new(),
        }
    }

    pub fn subscribe_all(&mut self, callback: impl FnMut(ObjectId) + 'static) -> SubscriptionId {
        let id = self.next_id;
        self.next_id += 1;
        self.global_subscribers.insert(id, Box::new(callback));
        id
    }

    pub fn unsubscribe_all(&mut self, id: SubscriptionId) {
        self.global_subscribers.remove(&id);
    }

    pub fn notify_change(&mut self, doc_id: ObjectId) {
        for callback in self.global_subscribers.values_mut() {
            callback(doc_id);
        }
    }
}
