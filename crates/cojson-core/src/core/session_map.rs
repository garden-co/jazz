struct SessionMap {
    sessions: HashMap<SessionID, SessionLog>,
    known_state: KnownState,
    known_state_with_streaming: KnownState,
    immutable_known_state: KnownState,
    immutable_known_state_with_streaming: KnownState,
    streaming_known_state: KnownState,
}

impl SessionMap {
    fn new(known_state: KnownState, known_state_with_streaming: KnownState) -> Self {
        Self { sessions: HashMap::new(), known_state, known_state_with_streaming, immutable_known_state, immutable_known_state_with_streaming, streaming_known_state }
    }
}
