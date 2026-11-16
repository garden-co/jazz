package jazz

import (
	"context"
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
	mergeable    bool
	tier         WriteTier
}

// Serializable says the transaction is serializable: its reads are checked
// at commit, which fails with ErrConflict if they changed meanwhile, and
// is not retried. A Tx without Mergeable is serializable, so this only
// documents it at the call.
func Serializable() TxOption {
	return func(o *txOptions) { o.serializable, o.mergeable = true, false }
}

// Mergeable makes the transaction mergeable, as TS's default transaction:
// its writes merge with concurrent ones, and its commit never fails with
// ErrConflict. A libjazz_ffi without mergeable transactions refuses it
// with ErrNotSupported.
func Mergeable() TxOption {
	return func(o *txOptions) { o.mergeable, o.serializable = true, false }
}

// CommitAt makes the commit return once the transaction has reached tier.
func CommitAt(tier WriteTier) TxOption { return func(o *txOptions) { o.tier = tier } }

// Tx runs fn in a transaction. It commits when fn returns nil, and rolls
// back when fn returns an error (which Tx returns) or panics (Tx rolls
// back and panics again).
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
	begin := map[string]any{}
	if o.serializable {
		begin["serializable"] = true
	}
	if o.mergeable {
		begin["serializable"] = false
	}
	if err := db.call(ctx, "txBegin", begin, &begun); err != nil {
		return err
	}
	if begun.Tx == "" {
		return &Error{Kind: KindNotSupported, Message: "transactions are not supported by this jazz-ffi"}
	}
	tx := &Tx{db: db, id: begun.Tx, tier: o.tier}
	defer func() {
		if p := recover(); p != nil {
			tx.end(context.WithoutCancel(ctx), "txRollback")
			panic(p)
		}
	}()
	if err := fn(tx); err != nil {
		if rbErr := tx.end(context.WithoutCancel(ctx), "txRollback"); rbErr != nil {
			return fmt.Errorf("%w (and rolling back: %v)", err, rbErr)
		}
		return err
	}
	// A ctx cancelled while fn ran would stop the commit before it reached
	// Jazz and leave the transaction open: roll it back instead.
	if err := ctx.Err(); err != nil {
		tx.end(context.WithoutCancel(ctx), "txRollback")
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
