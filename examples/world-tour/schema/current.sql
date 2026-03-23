CREATE TABLE files (
    name TEXT NOT NULL,
    mimeType TEXT NOT NULL,
    partIds UUID[] REFERENCES file_parts NOT NULL,
    partSizes INTEGER[] NOT NULL
);
CREATE POLICY files_select_policy ON files FOR SELECT USING (TRUE);
CREATE POLICY files_insert_policy ON files FOR INSERT WITH CHECK (EXISTS (SELECT FROM members WHERE userId = @session.user_id));
CREATE POLICY files_update_policy ON files FOR UPDATE USING (EXISTS (SELECT FROM members WHERE userId = @session.user_id)) WITH CHECK (EXISTS (SELECT FROM members WHERE userId = @session.user_id));
CREATE POLICY files_delete_policy ON files FOR DELETE USING (EXISTS (SELECT FROM members WHERE userId = @session.user_id));

CREATE TABLE file_parts (
    data BYTEA NOT NULL
);
CREATE POLICY file_parts_select_policy ON file_parts FOR SELECT USING (TRUE);
CREATE POLICY file_parts_insert_policy ON file_parts FOR INSERT WITH CHECK (EXISTS (SELECT FROM members WHERE userId = @session.user_id));
CREATE POLICY file_parts_update_policy ON file_parts FOR UPDATE USING (EXISTS (SELECT FROM members WHERE userId = @session.user_id)) WITH CHECK (EXISTS (SELECT FROM members WHERE userId = @session.user_id));
CREATE POLICY file_parts_delete_policy ON file_parts FOR DELETE USING (EXISTS (SELECT FROM members WHERE userId = @session.user_id));

CREATE TABLE bands (
    name TEXT NOT NULL,
    logoFileId UUID REFERENCES files
);
CREATE POLICY bands_select_policy ON bands FOR SELECT USING (TRUE);
CREATE POLICY bands_insert_policy ON bands FOR INSERT WITH CHECK (EXISTS (SELECT FROM members WHERE userId = @session.user_id));
CREATE POLICY bands_update_policy ON bands FOR UPDATE USING (EXISTS (SELECT FROM members WHERE userId = @session.user_id)) WITH CHECK (EXISTS (SELECT FROM members WHERE userId = @session.user_id));
CREATE POLICY bands_delete_policy ON bands FOR DELETE USING (EXISTS (SELECT FROM members WHERE userId = @session.user_id));

CREATE TABLE venues (
    name TEXT NOT NULL,
    city TEXT NOT NULL,
    country TEXT NOT NULL,
    lat REAL NOT NULL,
    lng REAL NOT NULL,
    capacity INTEGER
);
CREATE POLICY venues_select_policy ON venues FOR SELECT USING (TRUE);
CREATE POLICY venues_insert_policy ON venues FOR INSERT WITH CHECK (EXISTS (SELECT FROM members WHERE userId = @session.user_id));
CREATE POLICY venues_update_policy ON venues FOR UPDATE USING (EXISTS (SELECT FROM members WHERE userId = @session.user_id)) WITH CHECK (EXISTS (SELECT FROM members WHERE userId = @session.user_id));
CREATE POLICY venues_delete_policy ON venues FOR DELETE USING (EXISTS (SELECT FROM members WHERE userId = @session.user_id));

CREATE TABLE members (
    bandId UUID REFERENCES bands NOT NULL,
    userId TEXT NOT NULL
);
CREATE POLICY members_select_policy ON members FOR SELECT USING (userId = @session.user_id);
CREATE POLICY members_insert_policy ON members FOR INSERT WITH CHECK (TRUE);

CREATE TABLE stops (
    bandId UUID REFERENCES bands NOT NULL,
    venueId UUID REFERENCES venues NOT NULL,
    date TIMESTAMP NOT NULL,
    status ENUM('cancelled','confirmed','tentative') NOT NULL,
    publicDescription TEXT NOT NULL,
    privateNotes TEXT
);
CREATE POLICY stops_select_policy ON stops FOR SELECT USING ((status = 'confirmed') OR (EXISTS (SELECT FROM members WHERE userId = @session.user_id)));
CREATE POLICY stops_insert_policy ON stops FOR INSERT WITH CHECK (EXISTS (SELECT FROM members WHERE userId = @session.user_id));
CREATE POLICY stops_update_policy ON stops FOR UPDATE USING (EXISTS (SELECT FROM members WHERE userId = @session.user_id)) WITH CHECK (EXISTS (SELECT FROM members WHERE userId = @session.user_id));
CREATE POLICY stops_delete_policy ON stops FOR DELETE USING (EXISTS (SELECT FROM members WHERE userId = @session.user_id));
