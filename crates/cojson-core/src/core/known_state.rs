use crate::core::ids::{CoID, SessionID};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnownState {
    id: CoID,
    header: bool,
    sessions: KnownStateSessions,
}

// TODO: Avoid cloning the session_id
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnownStateSessions(HashMap<SessionID, u64>);

impl KnownStateSessions {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /**
     * Set the session counter for a sessionId in the known state.
     */
    pub fn set_session_counter(&mut self, session_id: &SessionID, count: u64) {
        self.0.insert(session_id.clone(), count);
    }

    /**
     * Update the session counter for a sessionId in the known state.
     *
     * The function assigns the value to the target only when the value in the knownState is less than the provided value.
     */
    pub fn update_session_counter(&mut self, session_id: &SessionID, count: u64) {
        self.0.insert(
            session_id.clone(),
            std::cmp::max(*self.0.get(session_id).unwrap_or(&0u64), count),
        );
    }

    /**
     * Mutate the target sessions counter by combining the entries from the source.
     *
     * The function assigns the sessions to the target only when the value in the source is greater.
     */
    pub fn combine(&mut self, source: &Self) {
        for (session_id, count) in source.0.iter() {
            if count > self.0.get(session_id).unwrap_or(&0u64) {
                self.0.insert(session_id.clone(), *count);
            }
        }
    }

    /**
     * Checks if all the local sessions have the same counters as in target.
     */
    pub fn are_current_sessions_in_sync_with(&self, target: &Self) -> bool {
        for (session_id, count) in self.0.iter() {
            if count != target.0.get(session_id).unwrap_or(&0u64) {
                return false;
            }
        }
        true
    }

    /**
     * Checks if all the local sessions have the same counters as in target.
     */
    pub fn is_known_state_subset_of(&self, target: &Self) -> bool {
        for (session_id, count) in self.0.iter() {
            if count > target.0.get(session_id).unwrap_or(&0u64) {
                return false;
            }
        }
        true
    }

    /**
     * Returns the record with the sessions that need to be sent to the target
     */
    pub fn get_sessions_to_send(&self, target: &Self) -> Self {
        let mut to_send = KnownStateSessions::new();
        for (session_id, count) in self.0.iter() {
            if count > target.0.get(session_id).unwrap_or(&0u64) {
                to_send.0.insert(session_id.clone(), *count);
            }
        }
        to_send
    }
}

/**
 * A known state is a record of the sessions that have been seen by the local peer.
 */
impl KnownState {
    pub fn new(id: CoID, header: bool, sessions: KnownStateSessions) -> Self {
        Self {
            id,
            header,
            sessions,
        }
    }

    // emptyKnownState
    pub fn empty(id: CoID) -> Self {
        Self::new(id, false, KnownStateSessions(HashMap::new()))
    }

    pub fn combine(&mut self, source: &mut Self) {
        self.sessions.combine(&source.sessions);

        if source.header && !self.header {
            self.header = true;
        }
    }

    pub fn set_session_counter(&mut self, session_id: &SessionID, count: u64) {
        self.sessions.0.insert(session_id.clone(), count);
    }

    pub fn update_session_counter(&mut self, session_id: &SessionID, count: u64) {
        self.sessions.0.insert(
            session_id.clone(),
            std::cmp::max(*self.sessions.0.get(session_id).unwrap_or(&0u64), count),
        );
    }
}


mod tests {
    use super::*;

    fn s(id: &str) -> SessionID {
        SessionID::new(id)
    }

    fn ks(map: &[(&str, u64)]) -> KnownStateSessions {
        let mut sessions = KnownStateSessions::new();
        for (id, count) in map {
            sessions.set_session_counter(&s(id), *count);
        }
        sessions
    }

    #[test]
    fn empty_known_state() {
        let id = CoID("test-id".to_string());
        let state = KnownState::empty(id.clone());

        assert_eq!(state.id, id);
        assert!(!state.header);
        assert!(state.sessions.0.is_empty());
    }

    #[test]
    fn known_state_from_keeps_fields() {
        let id = CoID("test-id".to_string());
        let mut original = KnownState::new(id.clone(), true, ks(&[("session-1", 5)]));

        let copy = original.clone();

        assert_eq!(copy.id, id);
        assert!(copy.header);
        assert_eq!(copy.sessions, original.sessions);

        // mutate original to ensure copy is independent (Rust clone semantics)
        original.set_session_counter(&s("session-1"), 10);
        assert_ne!(copy.sessions, original.sessions);
    }

    #[test]
    fn combine_known_states_merges_sessions_and_header() {
        let id = CoID("test-id".to_string());
        let mut target = KnownState::new(id.clone(), false, ks(&[("session-1", 3)]));
        let mut source = KnownState::new(id.clone(), false, ks(&[("session-2", 7)]));

        target.combine(&mut source);

        assert_eq!(
            target.sessions,
            ks(&[("session-1", 3), ("session-2", 7)])
        );
        assert!(!target.header);
    }

    #[test]
    fn combine_known_states_updates_higher_counters() {
        let id = CoID("test-id".to_string());
        let mut target = KnownState::new(id.clone(), false, ks(&[("session-1", 3)]));
        let mut source = KnownState::new(id.clone(), false, ks(&[("session-1", 7)]));

        target.combine(&mut source);

        assert_eq!(target.sessions, ks(&[("session-1", 7)]));
    }

    #[test]
    fn combine_known_states_respects_lower_counters() {
        let id = CoID("test-id".to_string());
        let mut target = KnownState::new(id.clone(), false, ks(&[("session-1", 10)]));
        let mut source = KnownState::new(id.clone(), false, ks(&[("session-1", 5)]));

        target.combine(&mut source);

        assert_eq!(target.sessions, ks(&[("session-1", 10)]));
    }

    #[test]
    fn combine_known_states_sets_header_when_source_true() {
        let id = CoID("test-id".to_string());
        let mut target = KnownState::new(id.clone(), false, ks(&[]));
        let mut source = KnownState::new(id.clone(), true, ks(&[]));

        target.combine(&mut source);

        assert!(target.header);
    }

    #[test]
    fn combine_known_states_keeps_header_when_both_true() {
        let id = CoID("test-id".to_string());
        let mut target = KnownState::new(id.clone(), true, ks(&[]));
        let mut source = KnownState::new(id.clone(), true, ks(&[]));

        target.combine(&mut source);

        assert!(target.header);
    }

    #[test]
    fn combine_known_states_keeps_header_when_source_false() {
        let id = CoID("test-id".to_string());
        let mut target = KnownState::new(id.clone(), true, ks(&[]));
        let mut source = KnownState::new(id.clone(), false, ks(&[]));

        target.combine(&mut source);

        assert!(target.header);
    }

    #[test]
    fn combine_known_states_returns_target() {
        let id = CoID("test-id".to_string());
        let mut target = KnownState::new(id.clone(), false, ks(&[]));
        let mut source = KnownState::new(id.clone(), false, ks(&[]));

        let ptr_before: *const KnownState = &target;
        let _ = target.combine(&mut source);
        let ptr_after: *const KnownState = &target;

        assert_eq!(ptr_before, ptr_after);
    }

    #[test]
    fn combine_known_state_sessions_adds_and_updates() {
        let mut target = ks(&[("session-1", 3)]);
        let source = ks(&[("session-2", 7)]);

        target.combine(&source);

        assert_eq!(target, ks(&[("session-1", 3), ("session-2", 7)]));
    }

    #[test]
    fn combine_known_state_sessions_updates_when_higher() {
        let mut target = ks(&[("session-1", 3)]);
        let source = ks(&[("session-1", 7)]);

        target.combine(&source);

        assert_eq!(target, ks(&[("session-1", 7)]));
    }

    #[test]
    fn combine_known_state_sessions_does_not_update_when_lower() {
        let mut target = ks(&[("session-1", 10)]);
        let source = ks(&[("session-1", 5)]);

        target.combine(&source);

        assert_eq!(target, ks(&[("session-1", 10)]));
    }

    #[test]
    fn combine_known_state_sessions_handles_empty_source() {
        let mut target = ks(&[("session-1", 3)]);
        let source = ks(&[]);

        target.combine(&source);

        assert_eq!(target, ks(&[("session-1", 3)]));
    }

    #[test]
    fn combine_known_state_sessions_handles_empty_target() {
        let mut target = ks(&[]);
        let source = ks(&[("session-1", 5)]);

        target.combine(&source);

        assert_eq!(target, ks(&[("session-1", 5)]));
    }

    #[test]
    fn combine_known_state_sessions_handles_multiple() {
        let mut target = ks(&[("session-1", 5), ("session-2", 10)]);
        let source = ks(&[("session-1", 3), ("session-2", 15), ("session-3", 8)]);

        target.combine(&source);

        assert_eq!(
            target,
            ks(&[("session-1", 5), ("session-2", 15), ("session-3", 8)])
        );
    }

    #[test]
    fn set_session_counter_sets_and_overwrites() {
        let mut sessions = ks(&[("session-1", 3)]);
        sessions.set_session_counter(&s("session-1"), 10);

        assert_eq!(sessions, ks(&[("session-1", 10)]));
    }

    #[test]
    fn set_session_counter_allows_zero_and_lower_values() {
        let mut sessions = ks(&[("session-1", 5)]);
        sessions.set_session_counter(&s("session-1"), 0);
        assert_eq!(sessions, ks(&[("session-1", 0)]));

        sessions.set_session_counter(&s("session-1"), 3);
        assert_eq!(sessions, ks(&[("session-1", 3)]));
    }

    #[test]
    fn set_session_counter_mutates_input() {
        let mut sessions = ks(&[]);
        sessions.set_session_counter(&s("session-1"), 5);
        assert_eq!(sessions, ks(&[("session-1", 5)]));
    }

    #[test]
    fn update_session_counter_only_when_higher() {
        let mut sessions = ks(&[("session-1", 3)]);
        sessions.update_session_counter(&s("session-1"), 10);
        assert_eq!(sessions, ks(&[("session-1", 10)]));

        sessions.update_session_counter(&s("session-1"), 5);
        assert_eq!(sessions, ks(&[("session-1", 10)]));
    }

    #[test]
    fn update_session_counter_handles_equal_and_zero_values() {
        let mut sessions = ks(&[("session-1", 5)]);
        sessions.update_session_counter(&s("session-1"), 5);
        assert_eq!(sessions, ks(&[("session-1", 5)]));

        sessions.update_session_counter(&s("session-1"), 0);
        assert_eq!(sessions, ks(&[("session-1", 5)]));

        let mut empty = ks(&[]);
        empty.update_session_counter(&s("session-1"), 0);
        assert_eq!(empty, ks(&[("session-1", 0)]));
    }

    #[test]
    fn update_session_counter_mutates_input() {
        let mut sessions = ks(&[]);
        sessions.update_session_counter(&s("session-1"), 5);
        assert_eq!(sessions, ks(&[("session-1", 5)]));
    }

    #[test]
    fn clone_known_state_produces_independent_copy() {
        let original = KnownState::new(
            CoID("test-id".to_string()),
            true,
            ks(&[("session-1", 5)]),
        );

        let mut cloned = original.clone();

        assert_eq!(cloned, original);
        assert!(!std::ptr::eq(&cloned, &original));
        assert!(!std::ptr::eq(&cloned.sessions, &original.sessions));

        cloned.header = false;
        cloned.sessions.set_session_counter(&s("session-2"), 10);
        assert_ne!(cloned.header, original.header);
        assert_ne!(cloned.sessions, original.sessions);
    }

    #[test]
    fn are_current_sessions_in_sync_with_behaves_like_ts() {
        let sessions_a = ks(&[("s1", 5), ("s2", 10)]);
        let sessions_b = ks(&[("s1", 5), ("s2", 10)]);
        assert!(sessions_a.are_current_sessions_in_sync_with(&sessions_b));

        let sessions_c = ks(&[("s1", 5)]);
        let sessions_d = ks(&[("s1", 3)]);
        assert!(!sessions_c.are_current_sessions_in_sync_with(&sessions_d));

        let sessions_e = ks(&[]);
        let sessions_f = ks(&[("s1", 5)]);
        assert!(sessions_e.are_current_sessions_in_sync_with(&sessions_f));

        let sessions_g = ks(&[("s1", 5)]);
        let sessions_h = ks(&[("s1", 5), ("s2", 10)]);
        assert!(sessions_g.are_current_sessions_in_sync_with(&sessions_h));

        let sessions_i = ks(&[("s1", 5)]);
        let sessions_j = ks(&[("s1", 10)]);
        assert!(!sessions_i.are_current_sessions_in_sync_with(&sessions_j));

        let empty_a = ks(&[]);
        let empty_b = ks(&[]);
        assert!(empty_a.are_current_sessions_in_sync_with(&empty_b));
    }

    #[test]
    fn is_known_state_subset_of_behaves_like_ts() {
        let sessions_a = ks(&[("s1", 5), ("s2", 10)]);
        let sessions_b = ks(&[("s1", 5), ("s2", 10)]);
        assert!(sessions_a.is_known_state_subset_of(&sessions_b));

        let sessions_c = ks(&[("s1", 5)]);
        let sessions_d = ks(&[("s1", 3)]);
        assert!(!sessions_c.is_known_state_subset_of(&sessions_d));

        let sessions_e = ks(&[]);
        let sessions_f = ks(&[("s1", 5)]);
        assert!(sessions_e.is_known_state_subset_of(&sessions_f));

        let sessions_g = ks(&[("s1", 5)]);
        let sessions_h = ks(&[("s1", 5), ("s2", 10)]);
        assert!(sessions_g.is_known_state_subset_of(&sessions_h));
    }

    #[test]
    fn get_sessions_to_send_returns_only_higher_counters() {
        let current = ks(&[("s1", 5), ("s2", 10)]);
        let target = ks(&[("s1", 3), ("s2", 12)]);

        let to_send = current.get_sessions_to_send(&target);
        assert_eq!(to_send, ks(&[("s1", 5)]));
    }
}
