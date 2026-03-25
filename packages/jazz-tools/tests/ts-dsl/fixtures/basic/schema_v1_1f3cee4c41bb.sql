CREATE TABLE projects (
    name TEXT NOT NULL
);

CREATE TABLE table_with_defaults (
    integer INTEGER NOT NULL,
    float REAL NOT NULL,
    bytes BYTEA NOT NULL,
    enum ENUM('a','b','c') NOT NULL,
    json JSON('{"$schema":"http://json-schema.org/draft-07/schema#","properties":{"age":{"type":"number"},"name":{"type":"string"}},"required":["name"],"type":"object"}') NOT NULL,
    timestampDate TIMESTAMP NOT NULL,
    timestampNumber TIMESTAMP NOT NULL,
    string TEXT NOT NULL,
    array TEXT[] NOT NULL,
    boolean BOOLEAN NOT NULL,
    nullable TEXT,
    refId UUID REFERENCES todos
);

CREATE TABLE todos (
    title TEXT NOT NULL,
    done BOOLEAN NOT NULL,
    tags TEXT[] NOT NULL,
    projectId UUID REFERENCES projects NOT NULL,
    ownerId UUID REFERENCES users,
    assigneesIds UUID[] REFERENCES users NOT NULL
);
CREATE POLICY todos_select_policy ON todos FOR SELECT USING (TRUE);
CREATE POLICY todos_insert_policy ON todos FOR INSERT WITH CHECK (TRUE);
CREATE POLICY todos_update_policy ON todos FOR UPDATE USING (done = FALSE) WITH CHECK (TRUE);
CREATE POLICY todos_delete_policy ON todos FOR DELETE USING (done = FALSE);

CREATE TABLE users (
    name TEXT NOT NULL,
    friendsIds UUID[] REFERENCES users NOT NULL
);