package jazz

import (
	"context"
	"encoding/json"
	"fmt"
)

// Querier is what reads and writes go through: a *DB, or a *Tx inside a
// transaction. Generated table methods take one.
type Querier interface {
	call(ctx context.Context, method string, args map[string]any, out any) error
	// computed is the app's Computed, for generated rows' hooks.
	computed() any
}

// Tx is a transaction: pass it where a Querier goes. It belongs to the
// goroutine running the function given to DB.Tx.
type Tx struct {
	db   *DB
	id   string
	tier WriteTier
	done bool
}

// TxOption configures DB.Tx.
type TxOption func(*txOptions)

type txOptions struct {
	serializable bool
	tier         WriteTier
}

// Serializable makes the transaction serializable, as TS's
// db.serializableTransaction: its reads are checked at commit, which fails
// with ErrConflict if another write changed them meanwhile, and is not
// retried.
func Serializable() TxOption { return func(o *txOptions) { o.serializable = true } }

// Mergeable makes the transaction mergeable, which a Tx is by default (as
// TS's db.transaction): it only says so at the call.
func Mergeable() TxOption { return func(o *txOptions) { o.serializable = false } }

// CommitAt makes the commit return once the transaction has reached tier.
func CommitAt(tier WriteTier) TxOption { return func(o *txOptions) { o.tier = tier } }

// Tx runs fn in a transaction. It commits when fn returns nil, and rolls
// back when fn returns an error (which Tx returns), panics (Tx rolls
// back and panics again) or doesn't return (runtime.Goexit, t.Fatal).
// When ctx ends before the commit, Tx rolls back and returns ctx.Err(),
// also while it begins.
//
// A Tx is mergeable, as TS's db.transaction: its writes merge with
// concurrent ones by Jazz's normal rules, and its commit never fails with
// ErrConflict. Pass Serializable() to have its reads checked at commit
// instead.
//
// When ctx ends during the commit, Tx returns ctx.Err() at once, but the
// commit still runs to its end: the transaction may have committed. Check
// before retrying a ctx.Err() from Tx.
func (db *DB) Tx(ctx context.Context, fn func(tx *Tx) error, opts ...TxOption) (err error) {
	var o txOptions
	for _, opt := range opts {
		opt(&o)
	}
	var begun struct {
		Tx string `json:"tx"`
	}
	begin := map[string]any{"serializable": o.serializable}
	// A begin that ctx's end returned from is rolled back once it answers.
	rollBack := func(text string, err error) {
		var late struct {
			Tx string `json:"tx"`
		}
		if err == nil && json.Unmarshal([]byte(text), &late) == nil && late.Tx != "" {
			_ = db.call(context.Background(), "txRollback", map[string]any{"tx": late.Tx}, nil)
		}
	}
	if err := db.callOr(ctx, "txBegin", begin, &begun, rollBack); err != nil {
		return err
	}
	if begun.Tx == "" {
		return &Error{Kind: KindNotSupported, Message: "transactions are not supported by this jazz-ffi"}
	}
	tx := &Tx{db: db, id: begun.Tx, tier: o.tier}
	// rollback ends the transaction, at once if ctx has ended (the
	// rollback then finishes behind).
	rollback := func() error {
		if ctx.Err() != nil {
			go tx.end(context.Background(), "txRollback")
			return nil
		}
		return tx.end(context.WithoutCancel(ctx), "txRollback")
	}
	// A panic, or runtime.Goexit (t.Fatal), in fn rolls back too.
	returned := false
	defer func() {
		if !returned {
			tx.end(context.WithoutCancel(ctx), "txRollback")
		}
	}()
	err = fn(tx)
	returned = true
	if err != nil {
		if rbErr := rollback(); rbErr != nil {
			return fmt.Errorf("%w (and rolling back: %v)", err, rbErr)
		}
		return err
	}
	// A ctx cancelled while fn ran would stop the commit before it reached
	// Jazz and leave the transaction open: roll it back instead.
	if err := ctx.Err(); err != nil {
		rollback()
		return err
	}
	// The commit is sent even if ctx is cancelled from here on, so the
	// transaction never stays open; a cancelled ctx returns ctx.Err()
	// without waiting for it to end.
	done := make(chan error, 1)
	go func() { done <- tx.end(context.WithoutCancel(ctx), "txCommit") }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (tx *Tx) end(ctx context.Context, method string) error {
	if tx.done {
		return nil
	}
	tx.done = true
	args := map[string]any{"tx": tx.id}
	if method == "txCommit" && tx.tier != "" {
		args["tier"] = string(tx.tier)
	}
	return tx.db.call(ctx, method, args, nil)
}

func (tx *Tx) call(ctx context.Context, method string, args map[string]any, out any) error {
	if tx.done {
		return &Error{Kind: KindClosed, Message: "the transaction has ended"}
	}
	withTx := make(map[string]any, len(args)+1)
	for k, v := range args {
		withTx[k] = v
	}
	withTx["tx"] = tx.id
	return tx.db.call(ctx, method, withTx, out)
}

// Update changes the fields patch sets on the row of table with id.
func (tx *Tx) Update(ctx context.Context, table, id string, patch any) error {
	return update(ctx, tx, table, id, patch, nil)
}

// Delete deletes the row of table with id.
func (tx *Tx) Delete(ctx context.Context, table, id string) error {
	return remove(ctx, tx, table, id, nil)
}

func update(ctx context.Context, q Querier, table, id string, patch any, opts []WriteOption) error {
	patch, err := encode(q, patch)
	if err != nil {
		return err
	}
	return q.call(ctx, "update", withOptions(map[string]any{"table": table, "id": id, "patch": patch}, opts), nil)
}

func remove(ctx context.Context, q Querier, table, id string, opts []WriteOption) error {
	return q.call(ctx, "delete", withOptions(map[string]any{"table": table, "id": id}, opts), nil)
}

// Update changes the fields patch sets on the row of table with id
// through q (a *DB or a *Tx).
func Update(ctx context.Context, q Querier, table, id string, patch any, opts ...WriteOption) error {
	return update(ctx, q, table, id, patch, opts)
}

// Delete deletes the row of table with id through q (a *DB or a *Tx).
func Delete(ctx context.Context, q Querier, table, id string, opts ...WriteOption) error {
	return remove(ctx, q, table, id, opts)
}

func (tx *Tx) computed() any { return tx.db.app }
