CREATE TABLE messages (
    author_id TEXT NOT NULL,
    author_name TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    text TEXT NOT NULL,
    sent_at TIMESTAMP NOT NULL
);
CREATE POLICY messages_select_policy ON messages FOR SELECT USING ((chat_id = 'announcements') OR ((chat_id = 'chat-01') AND (@session.claims.role IN ('admin', 'member'))));
CREATE POLICY messages_insert_policy ON messages FOR INSERT WITH CHECK (((chat_id = 'announcements') AND (@session.claims.role = 'admin')) OR ((chat_id = 'chat-01') AND ((author_id = @session.user_id) OR (@session.claims.role = 'admin'))));
CREATE POLICY messages_update_policy ON messages FOR UPDATE USING (((chat_id = 'announcements') AND (@session.claims.role = 'admin')) OR ((chat_id = 'chat-01') AND ((author_id = @session.user_id) OR (@session.claims.role = 'admin')))) WITH CHECK ((chat_id = 'announcements') OR (chat_id = 'chat-01'));
CREATE POLICY messages_delete_policy ON messages FOR DELETE USING (((chat_id = 'announcements') AND (@session.claims.role = 'admin')) OR ((chat_id = 'chat-01') AND ((author_id = @session.user_id) OR (@session.claims.role = 'admin'))));
