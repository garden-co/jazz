CREATE TABLE profiles (
    userId TEXT NOT NULL,
    name TEXT NOT NULL,
    avatar TEXT
);
CREATE POLICY profiles_select_policy ON profiles FOR SELECT USING (TRUE);
CREATE POLICY profiles_insert_policy ON profiles FOR INSERT WITH CHECK (userId = @session.user_id);
CREATE POLICY profiles_update_policy ON profiles FOR UPDATE USING (userId = @session.user_id) WITH CHECK (userId = @session.user_id);

CREATE TABLE chats (
    isPublic BOOLEAN NOT NULL,
    createdBy TEXT NOT NULL,
    joinCode TEXT
);
CREATE POLICY chats_select_policy ON chats FOR SELECT USING ((isPublic = TRUE) OR (EXISTS (SELECT FROM chatMembers WHERE (chat = @session.__jazz_outer_row.id) AND (userId = @session.user_id))) OR (joinCode = @session.claims.join_code));
CREATE POLICY chats_insert_policy ON chats FOR INSERT WITH CHECK (createdBy = @session.user_id);

CREATE TABLE chatMembers (
    chat UUID REFERENCES chats NOT NULL,
    userId TEXT NOT NULL,
    joinCode TEXT
);
CREATE POLICY chatMembers_select_policy ON chatMembers FOR SELECT USING (userId = @session.user_id);
CREATE POLICY chatMembers_insert_policy ON chatMembers FOR INSERT WITH CHECK (userId = @session.user_id);

CREATE TABLE messages (
    chat UUID REFERENCES chats NOT NULL,
    text TEXT NOT NULL,
    sender UUID REFERENCES profiles NOT NULL,
    senderId TEXT NOT NULL,
    createdAt TIMESTAMP NOT NULL
);
CREATE POLICY messages_select_policy ON messages FOR SELECT USING ((INHERITS SELECT VIA chat) OR (EXISTS (SELECT FROM chatMembers WHERE (chat = @session.__jazz_outer_row.chat) AND (userId = @session.user_id))));
CREATE POLICY messages_insert_policy ON messages FOR INSERT WITH CHECK (EXISTS (SELECT FROM chatMembers WHERE (chat = @session.__jazz_outer_row.chat) AND (userId = @session.user_id)));
CREATE POLICY messages_delete_policy ON messages FOR DELETE USING (senderId = @session.user_id);

CREATE TABLE reactions (
    message UUID REFERENCES messages NOT NULL,
    userId TEXT NOT NULL,
    emoji TEXT NOT NULL
);
CREATE POLICY reactions_select_policy ON reactions FOR SELECT USING (INHERITS SELECT VIA message);
CREATE POLICY reactions_insert_policy ON reactions FOR INSERT WITH CHECK (userId = @session.user_id);
CREATE POLICY reactions_delete_policy ON reactions FOR DELETE USING (userId = @session.user_id);

CREATE TABLE canvases (
    chat UUID REFERENCES chats NOT NULL,
    createdAt TIMESTAMP NOT NULL
);
CREATE POLICY canvases_select_policy ON canvases FOR SELECT USING ((INHERITS SELECT VIA chat) OR (EXISTS (SELECT FROM chatMembers WHERE (chat = @session.__jazz_outer_row.chat) AND (userId = @session.user_id))));
CREATE POLICY canvases_insert_policy ON canvases FOR INSERT WITH CHECK (EXISTS (SELECT FROM chatMembers WHERE (chat = @session.__jazz_outer_row.chat) AND (userId = @session.user_id)));

CREATE TABLE strokes (
    canvas UUID REFERENCES canvases NOT NULL,
    ownerId TEXT NOT NULL,
    color TEXT NOT NULL,
    width INTEGER NOT NULL,
    pointsJson TEXT NOT NULL,
    createdAt TIMESTAMP NOT NULL
);
CREATE POLICY strokes_select_policy ON strokes FOR SELECT USING (INHERITS SELECT VIA canvas);
CREATE POLICY strokes_insert_policy ON strokes FOR INSERT WITH CHECK (INHERITS SELECT VIA canvas);
CREATE POLICY strokes_delete_policy ON strokes FOR DELETE USING (ownerId = @session.user_id);

CREATE TABLE attachments (
    message UUID REFERENCES messages NOT NULL,
    type TEXT NOT NULL,
    name TEXT NOT NULL,
    data TEXT NOT NULL,
    mimeType TEXT NOT NULL,
    size INTEGER NOT NULL
);
CREATE POLICY attachments_select_policy ON attachments FOR SELECT USING (INHERITS SELECT VIA message);
CREATE POLICY attachments_insert_policy ON attachments FOR INSERT WITH CHECK (INHERITS SELECT VIA message);
