package jazz

import (
	"context"
	"fmt"
)

// Querier is what reads and writes go through: a *DB, or a *Tx inside a
// transaction. Generated table methods take one.
type Querier interface {
	call(ctx context.Context, method string, args map[string]any, out any) error
}

// Tx is a transaction: pass it where a Querier goes. It belongs to the
// goroutine running the function given to DB.Tx.
type Tx struct {
	db   *DB
	id   string
	done bool
}

// TxOption configures DB.Tx.
type TxOption func(*txOptions)

type txOptions struct{ serializable bool }

// Serializable makes the transaction serializable: its reads are checked
// at commit, which fails with ErrConflict if they changed meanwhile. It is
// not retried.
func Serializable() TxOption { return func(o *txOptions) { o.serializable = true } }

// Tx runs fn in a transaction. It commits when fn returns nil, and rolls
// back when fn returns an error (which Tx returns) or panics (Tx rolls
// back and panics again).
func (db *DB) Tx(ctx context.Context, fn func(tx *Tx) error, opts ...TxOption) (err error) {
	var o txOptions
	for _, opt := range opts {
		opt(&o)
	}
	var begun struct {
		Tx string `json:"tx"`
	}
	if err := db.call(ctx, "txBegin", map[string]any{"serializable": o.serializable}, &begun); err != nil {
		return err
	}
	if begun.Tx == "" {
		return &Error{Kind: KindNotSupported, Message: "transactions are not supported by this jazz-ffi"}
	}
	tx := &Tx{db: db, id: begun.Tx}
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
	// Once sent, the commit runs to its end even if ctx is cancelled
	// meanwhile (the call returns ctx.Err() without waiting for it).
	return tx.end(ctx, "txCommit")
}

func (tx *Tx) end(ctx context.Context, method string) error {
	if tx.done {
		return nil
	}
	tx.done = true
	return tx.db.call(ctx, method, map[string]any{"tx": tx.id}, nil)
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
	return update(ctx, tx, table, id, patch)
}

// Delete deletes the row of table with id.
func (tx *Tx) Delete(ctx context.Context, table, id string) error {
	return remove(ctx, tx, table, id)
}

func update(ctx context.Context, q Querier, table, id string, patch any) error {
	return q.call(ctx, "update", map[string]any{"table": table, "id": id, "patch": patch}, nil)
}

func remove(ctx context.Context, q Querier, table, id string) error {
	return q.call(ctx, "delete", map[string]any{"table": table, "id": id}, nil)
}

// Update changes the fields patch sets on the row of table with id
// through q (a *DB or a *Tx).
func Update(ctx context.Context, q Querier, table, id string, patch any) error {
	return update(ctx, q, table, id, patch)
}

// Delete deletes the row of table with id through q (a *DB or a *Tx).
func Delete(ctx context.Context, q Querier, table, id string) error {
	return remove(ctx, q, table, id)
}
