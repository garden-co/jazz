use crate::core::ids::{CoID, SessionID};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct KnownState<'a> {
    id: CoID,
    header: bool,
    sessions: HashMap<&'a SessionID, u64>,
}

impl<'a> KnownState<'a> {
    pub fn empty(id: CoID) -> Self {
        Self {
            id,
            header: false,
            sessions: HashMap::new(),
        }
    }

    pub fn combine(&mut self, source: &mut Self) {
        for (session_id, count) in source.sessions.iter() {
            if count > self.sessions.get(session_id).unwrap_or(&0u64) {
                self.sessions.insert(*session_id, *count);
            }
        }

        if source.header && !self.header {
            self.header = true;
        }
    }

    pub fn set_session_counter(&mut self, session_id: &'a SessionID, count: u64) {
        self.sessions.insert(session_id, count);
    }

    pub fn update_session_counter(&mut self, session_id: &'a SessionID, count: u64) {
        self.sessions.insert(
            session_id,
            std::cmp::max(*self.sessions.get(session_id).unwrap_or(&0u64), count),
        );
    }
}
