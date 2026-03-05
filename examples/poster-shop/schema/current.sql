CREATE TABLE users (
    user_id TEXT NOT NULL,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE POLICY users_select_policy ON users FOR SELECT USING (TRUE);
CREATE POLICY users_insert_policy ON users FOR INSERT WITH CHECK (TRUE);

CREATE TABLE canvases (
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE POLICY canvases_select_policy ON canvases FOR SELECT USING (TRUE);
CREATE POLICY canvases_insert_policy ON canvases FOR INSERT WITH CHECK (TRUE);

CREATE TABLE strokes (
    canvas_id UUID REFERENCES canvases NOT NULL,
    user_id TEXT NOT NULL,
    points JSON NOT NULL,
    created_at TEXT NOT NULL
);
CREATE POLICY strokes_select_policy ON strokes FOR SELECT USING (TRUE);
CREATE POLICY strokes_insert_policy ON strokes FOR INSERT WITH CHECK (TRUE);
