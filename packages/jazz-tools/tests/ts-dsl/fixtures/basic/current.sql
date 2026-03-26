CREATE TABLE users (
    name TEXT NOT NULL,
    friendsIds UUID[] REFERENCES users DEFAULT ARRAY[] NOT NULL
);

CREATE TABLE projects (
    name TEXT NOT NULL
);

CREATE TABLE todos (
    title TEXT NOT NULL,
    done BOOLEAN DEFAULT FALSE NOT NULL,
    tags TEXT[] DEFAULT ARRAY[] NOT NULL,
    projectId UUID REFERENCES projects NOT NULL,
    ownerId UUID REFERENCES users,
    assigneesIds UUID[] REFERENCES users DEFAULT ARRAY[] NOT NULL
);
CREATE POLICY todos_select_policy ON todos FOR SELECT USING (TRUE);
CREATE POLICY todos_insert_policy ON todos FOR INSERT WITH CHECK (TRUE);
CREATE POLICY todos_update_policy ON todos FOR UPDATE USING (done = FALSE) WITH CHECK (TRUE);
CREATE POLICY todos_delete_policy ON todos FOR DELETE USING (done = FALSE);

CREATE TABLE table_with_defaults (
    integer INTEGER DEFAULT 1 NOT NULL,
    float REAL DEFAULT 1 NOT NULL,
    bytes BYTEA DEFAULT '\\x0001ff' NOT NULL,
    enum ENUM('a','b','c') DEFAULT 'a' NOT NULL,
    json JSON('{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","properties":{"name":{"type":"string"},"age":{"type":"number"}},"required":["name"]}') DEFAULT '{"name":"default name"}' NOT NULL,
    timestampDate TIMESTAMP DEFAULT 1767225600000 NOT NULL,
    timestampNumber TIMESTAMP DEFAULT 0 NOT NULL,
    string TEXT DEFAULT 'default value' NOT NULL,
    array TEXT[] DEFAULT ARRAY['a', 'b', 'c'] NOT NULL,
    boolean BOOLEAN DEFAULT TRUE NOT NULL,
    nullable TEXT DEFAULT NULL,
    refId UUID REFERENCES todos DEFAULT '00000000-0000-0000-0000-000000000000'
);
