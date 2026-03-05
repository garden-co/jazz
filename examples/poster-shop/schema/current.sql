CREATE TABLE counter_events (
    actor_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE POLICY counter_events_select_policy ON counter_events FOR SELECT USING (TRUE);
CREATE POLICY counter_events_insert_policy ON counter_events FOR INSERT WITH CHECK (TRUE);
