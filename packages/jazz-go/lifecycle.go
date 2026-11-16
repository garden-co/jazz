package jazz

import (
	"context"
	"encoding/json"
)

// Disconnect leaves the server: local reads and writes go on, and sync
// resumes on Reconnect.
func (db *DB) Disconnect(ctx context.Context) error {
	return db.call(ctx, "disconnect", map[string]any{}, nil)
}

// Reconnect resumes sync after Disconnect.
func (db *DB) Reconnect(ctx context.Context) error {
	return db.call(ctx, "reconnect", map[string]any{}, nil)
}

// DeleteClientStorage closes the app and deletes its local store, so the
// next Open starts empty and syncs again. Writes the server hasn't
// accepted yet are lost. It is refused (ErrRefused) while another open
// app holds the same DataDir.
func (db *DB) DeleteClientStorage(ctx context.Context) error {
	if err := db.call(ctx, "deleteClientStorage", map[string]any{}, nil); err != nil {
		return err
	}
	return db.Close()
}

// MutationError is a write the server refused that nothing waited on
// (no WaitForSync, no global tier), as TS's onMutationError reports it.
type MutationError struct {
	// Transaction is the refused commit's id.
	Transaction string `json:"transaction"`
	Message     string `json:"message"`
	// Denied: a permission refused it.
	Denied bool `json:"denied"`
	// Serializable: a serializable transaction's commit.
	Serializable bool `json:"serializable"`
}

// OnMutationError is the writes the server refuses that nothing waited
// on, those from before the first call first, until ctx is done.
func (db *DB) OnMutationError(ctx context.Context) (<-chan MutationError, error) {
	return watch(ctx, db, "mutationErrors", func(e event) (MutationError, bool) {
		var m MutationError
		if e.kind == "error" || json.Unmarshal([]byte(e.json), &m) != nil {
			return m, true
		}
		return m, false
	})
}
