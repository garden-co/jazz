CREATE TABLE beats (
    jam UUID REFERENCES jams NOT NULL,
    instrument UUID REFERENCES instruments NOT NULL,
    beat_index INTEGER NOT NULL,
    placed_by TEXT NOT NULL
);
CREATE POLICY beats_select_policy ON beats FOR SELECT USING (TRUE);
CREATE POLICY beats_insert_policy ON beats FOR INSERT WITH CHECK (TRUE);
CREATE POLICY beats_delete_policy ON beats FOR DELETE USING (TRUE);

CREATE TABLE instruments (
    name TEXT NOT NULL,
    sound BYTEA NOT NULL,
    display_order INTEGER NOT NULL
);
CREATE POLICY instruments_select_policy ON instruments FOR SELECT USING (TRUE);
CREATE POLICY instruments_insert_policy ON instruments FOR INSERT WITH CHECK (TRUE);

CREATE TABLE jams (
    created_at REAL NOT NULL,
    transport_start REAL
);
CREATE POLICY jams_select_policy ON jams FOR SELECT USING (TRUE);
CREATE POLICY jams_insert_policy ON jams FOR INSERT WITH CHECK (TRUE);
CREATE POLICY jams_update_policy ON jams FOR UPDATE USING (TRUE) WITH CHECK (TRUE);

CREATE TABLE participants (
    jam UUID REFERENCES jams NOT NULL,
    user_id TEXT NOT NULL,
    display_name TEXT NOT NULL
);
CREATE POLICY participants_select_policy ON participants FOR SELECT USING (TRUE);
CREATE POLICY participants_insert_policy ON participants FOR INSERT WITH CHECK (TRUE);
CREATE POLICY participants_delete_policy ON participants FOR DELETE USING (user_id = @session.user_id);