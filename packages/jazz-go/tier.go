package jazz

import "context"

// ReadTier is what a read waits for before it returns.
type ReadTier string

const (
	// ReadLocal reads this device's store as it is.
	ReadLocal ReadTier = "local"
	// ReadLocalFirst reads the local store, then follows the server.
	ReadLocalFirst ReadTier = "localFirst"
	// ReadRemote waits for the server's result.
	ReadRemote ReadTier = "remote"
)

// WriteTier is how durable a write must become before it returns.
type WriteTier string

const (
	// WriteLocal returns once the write is in this device's store.
	WriteLocal WriteTier = "local"
	// WriteGlobal returns once the sync server has the write.
	WriteGlobal WriteTier = "global"
)

// Tier makes the query read at tier.
func (q *Query[R]) Tier(tier ReadTier) *Query[R] {
	c := q.clone()
	c.tier = tier
	return c
}

// WriteOption configures a write.
type WriteOption func(map[string]any)

// WaitFor makes a write return only once it has reached tier. (Inside a
// transaction, give the tier to the commit instead: CommitAt.)
func WaitFor(tier WriteTier) WriteOption {
	return func(args map[string]any) { args["tier"] = string(tier) }
}

func withOptions(args map[string]any, opts []WriteOption) map[string]any {
	for _, opt := range opts {
		opt(args)
	}
	return args
}

// WaitForSync returns once every write made so far has reached tier.
func (db *DB) WaitForSync(ctx context.Context, tier WriteTier) error {
	return db.call(ctx, "waitForSync", map[string]any{"tier": string(tier)}, nil)
}
