CREATE TABLE owned_items (
    title TEXT NOT NULL,
    ownerId TEXT NOT NULL
);
CREATE POLICY owned_items_select_policy ON owned_items FOR SELECT USING (ownerId = @session.user_id);