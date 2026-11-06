-- Independently written storage contract for the two observation v1 sidecars.
CREATE TABLE _jazz_observation_pending_v1 (
  tx INTEGER NOT NULL CHECK(typeof(tx)='integer'),
  op INTEGER NOT NULL CHECK(typeof(op)='integer' AND op >= 0),
  schema BLOB NOT NULL CHECK(typeof(schema)='blob' AND length(schema)=16),
  scope BLOB NOT NULL CHECK(typeof(scope)='blob' AND length(scope)=32),
  PRIMARY KEY(tx,op)
) WITHOUT ROWID;
CREATE TRIGGER _jazz_observation_pending_forget_v1
  AFTER DELETE ON _jazz_pending BEGIN
  DELETE FROM _jazz_observation_pending_v1 WHERE tx=OLD.tx;
END;
CREATE TABLE _jazz_observation_evidence_v1 (
  admission INTEGER PRIMARY KEY AUTOINCREMENT CHECK(typeof(admission)='integer' AND admission > 0),
  schema BLOB NOT NULL CHECK(typeof(schema)='blob' AND length(schema)=16),
  tbl TEXT NOT NULL CHECK(typeof(tbl)='text'),
  id BLOB NOT NULL CHECK(typeof(id)='blob' AND length(id)=16),
  state INTEGER NOT NULL CHECK(typeof(state)='integer' AND state > 0 AND state <= 4503599627370495),
  scope BLOB NOT NULL CHECK(typeof(scope)='blob' AND length(scope)=32),
  proof INTEGER NOT NULL CHECK(typeof(proof)='integer' AND proof IN (0,1)),
  state_node INTEGER NOT NULL CHECK(typeof(state_node)='integer' AND state_node > 0),
  state_tx INTEGER NOT NULL CHECK(typeof(state_tx)='integer'),
  content_node INTEGER NOT NULL CHECK(typeof(content_node)='integer' AND content_node > 0),
  content_tx INTEGER NOT NULL CHECK(typeof(content_tx)='integer'),
  content INTEGER NOT NULL CHECK(typeof(content)='integer' AND content > 0 AND content <= state),
  UNIQUE(schema,tbl,id,state,scope,proof)
);
CREATE INDEX _jazz_observation_state_v1 ON _jazz_observation_evidence_v1(state);
