// Package todoclient holds the Go snippets of the docs, on the docs todo
// app (examples/docs/todo-client-localfirst-ts/schema.ts, re-exported by
// schema.ts here). Each #region is one complete function, compiled and run
// by this package's tests; docs/lib/languages/SNIPPETS.md says what each
// does, after the TypeScript reference.
package todoclient

import (
	"context"
	"errors"
	"fmt"
	"time"

	jazz "github.com/garden-co/jazz/packages/jazz-go/v2"

	"github.com/garden-co/jazz/examples/docs/todo-client-go/jazzapp"
	orders "github.com/garden-co/jazz/examples/docs/todo-client-go/recipes/required-references/jazzapp"
)

// #region context-setup-go-client
func openAsUser(ctx context.Context, appID, serverURL, dataDir string, token jazz.TokenProvider) (*jazz.DB, error) {
	// Sign in through your main backend's /jazz with a token from your auth
	// provider; rows stay in dataDir and sync through it.
	return jazzapp.Open(ctx, jazz.Config{
		AppID:     appID,
		ServerURL: serverURL,
		Storage:   jazz.StoragePersistent,
		DataDir:   dataDir,
		Auth:      jazz.Token(token),
	})
}

// #endregion context-setup-go-client

// #region reading-oneshot-go
func ReadOpenTodos(ctx context.Context, db *jazz.DB) ([]jazzapp.Todo, error) {
	return jazzapp.Todos.Query().Where(jazzapp.Todos.Done.Eq(false)).All(ctx, db)
}

// #endregion reading-oneshot-go

// #region reading-one-go
func ReadTodo(ctx context.Context, db *jazz.DB, id string) (*jazzapp.Todo, error) {
	// nil if no row matches, or you can't read it.
	return jazzapp.Todos.Query().Where(jazzapp.Todos.ID.Eq(id)).One(ctx, db)
}

// #endregion reading-one-go

// #region reading-subscriptions-go
func WatchOpenTodos(ctx context.Context, db *jazz.DB, onChange func([]jazzapp.Todo)) error {
	// The Live keeps the whole result; cancelling ctx unsubscribes.
	live, err := jazzapp.Todos.Query().Where(jazzapp.Todos.Done.Eq(false)).Live(ctx, db)
	if err != nil {
		return err
	}
	for {
		select {
		case <-live.Changes():
			onChange(live.Rows())
		case <-live.Done():
			return live.Err()
		}
	}
}

// #endregion reading-subscriptions-go

// #region where-subscription-go
func SubscribeOpenTodos(ctx context.Context, db *jazz.DB) (*jazz.Live[jazzapp.Todo], error) {
	return jazzapp.Todos.Query().Where(jazzapp.Todos.Done.Eq(false)).Live(ctx, db)
}

// #endregion where-subscription-go

// #region reading-filters-go
func ReadOpenDocsTodos(ctx context.Context, db *jazz.DB) ([]jazzapp.Todo, error) {
	// Conditions in one Where must all hold.
	return jazzapp.Todos.Query().
		Where(jazzapp.Todos.Done.Eq(false), jazzapp.Todos.Title.Contains("docs")).
		All(ctx, db)
}

// #endregion reading-filters-go

// #region reading-sorting-go
func ReadOpenTodosByTitle(ctx context.Context, db *jazz.DB) ([]jazzapp.Todo, error) {
	return jazzapp.Todos.Query().
		Where(jazzapp.Todos.Done.Eq(false)).
		OrderBy(jazzapp.Todos.Title.Asc()).
		All(ctx, db)
}

// #endregion reading-sorting-go

// #region reading-pagination-go
func ReadTodoPage(ctx context.Context, db *jazz.DB, page, pageSize int) ([]jazzapp.Todo, error) {
	// Pages count from 1.
	offset := max(0, (page-1)*pageSize)
	return jazzapp.Todos.Query().
		Where(jazzapp.Todos.Done.Eq(false)).
		OrderBy(jazzapp.Todos.Title.Asc(), jazzapp.Todos.ID.Asc()).
		Limit(pageSize).
		Offset(offset).
		All(ctx, db)
}

// #endregion reading-pagination-go

// #region reading-durability-tier-go
func ReadOpenTodosFromServer(ctx context.Context, db *jazz.DB) ([]jazzapp.Todo, error) {
	// ReadRemote waits for the server's result; the default reads locally.
	return jazzapp.Todos.Query().
		Where(jazzapp.Todos.Done.Eq(false)).
		Tier(jazz.ReadRemote).
		All(ctx, db)
}

// #endregion reading-durability-tier-go

// #region reading-includes-go
func ReadOpenTodosWithProjects(ctx context.Context, db *jazz.DB) ([]jazzapp.Todo, error) {
	// Fills each todo's Project, and its Parent with the parent's Project.
	return jazzapp.Todos.Query().
		Where(jazzapp.Todos.Done.Eq(false)).
		Include(jazzapp.Todos.Project, jazzapp.Todos.Parent.Include(jazzapp.Todos.Project)).
		All(ctx, db)
}

// #endregion reading-includes-go

// #region reading-reverse-relation-go
func ReadProjectsWithOpenTodos(ctx context.Context, db *jazz.DB) ([]jazzapp.Project, error) {
	return jazzapp.Projects.Query().
		Include(jazzapp.Projects.Todos.With(jazzapp.Todos.Query().Where(jazzapp.Todos.Done.Eq(false)))).
		All(ctx, db)
}

// #endregion reading-reverse-relation-go

// #region reading-select-go
// Your structs, whose json tags name the selected columns (and relation).
type ProjectName struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type TodoTitle struct {
	ID      string       `json:"id"`
	Title   string       `json:"title"`
	Project *ProjectName `json:"project"`
}

func ReadOpenTodoTitles(ctx context.Context, db *jazz.DB) ([]TodoTitle, error) {
	return jazz.Select[TodoTitle](
		jazzapp.Todos.Query().
			Where(jazzapp.Todos.Done.Eq(false)).
			Include(jazzapp.Todos.Project.Select(jazzapp.Projects.Name)),
		jazzapp.Todos.Title,
	).All(ctx, db)
}

// #endregion reading-select-go

// #region reading-require-includes-go
func ReadOrdersWithTheirCustomer(ctx context.Context, db *jazz.DB) ([]orders.Order, error) {
	// Only orders whose customer resolves (one you can read).
	return orders.Orders.Query().
		Include(orders.Orders.Customer).
		RequireIncludes().
		All(ctx, db)
}

// #endregion reading-require-includes-go

// #region reading-recursive-go
func TodoLineageQuery() *jazz.Query[jazzapp.Todo] {
	// From each open todo, its chain of parents, up to 10 deep.
	return jazz.Gather(jazzapp.Todos.Query().Where(jazzapp.Todos.Done.Eq(false)), jazzapp.Todos.Parent, 10)
}

// #endregion reading-recursive-go

func whereOperators(ctx context.Context, db *jazz.DB, searchTerm, todoIDA, todoIDB string) error {
	// #region where-eq-ne-go
	incompleteTodos, err := jazzapp.Todos.Query().Where(jazzapp.Todos.Done.Eq(false)).All(ctx, db)
	nonDraftTodos, err := jazzapp.Todos.Query().Where(jazzapp.Todos.Title.Ne("Draft")).All(ctx, db)
	selectedTodos, err := jazzapp.Todos.Query().Where(jazzapp.Todos.ID.In(todoIDA, todoIDB)).All(ctx, db)
	// #endregion where-eq-ne-go

	// #region where-numeric-go
	createdAt := jazz.NewColumn[jazzapp.Todo, jazz.Timestamp]("$createdAt") // a magic column
	oneWeekAgo := jazz.TimestampOf(time.Now().Add(-7 * 24 * time.Hour))
	recentTodos, err := jazzapp.Todos.Query().Where(createdAt.Gt(oneWeekAgo)).All(ctx, db)
	highPriority, err := jazzapp.Todos.Query().Where(jazzapp.Todos.Priority.Gte(3)).All(ctx, db)
	lowPriority, err := jazzapp.Todos.Query().Where(jazzapp.Todos.Priority.Lt(10)).All(ctx, db)
	// #endregion where-numeric-go

	// #region where-contains-go
	// Substring match (case-sensitive).
	matches, err := jazzapp.Todos.Query().Where(jazzapp.Todos.Title.Contains(searchTerm)).All(ctx, db)
	// #endregion where-contains-go

	// #region where-null-go
	// Rows where the optional reference is not set, and where it is.
	unlinkedTodos, err := jazzapp.Todos.Query().Where(jazzapp.Todos.ParentID.IsNull()).All(ctx, db)
	linkedTodos, err := jazzapp.Todos.Query().Where(jazzapp.Todos.ParentID.IsNotNull()).All(ctx, db)
	// #endregion where-null-go

	// #region where-and-go
	// Done and assigned to a project: conditions in one Where all hold.
	doneWithProject, err := jazzapp.Todos.Query().
		Where(jazzapp.Todos.Done.Eq(true), jazzapp.Todos.ProjectID.IsNotNull()).
		All(ctx, db)
	// #endregion where-and-go

	// #region where-order-limit-go
	recentIncomplete, err := jazzapp.Todos.Query().
		Where(jazzapp.Todos.Done.Eq(false)).
		OrderBy(jazz.NewColumn[jazzapp.Todo, jazz.Timestamp]("$createdAt").Asc()).
		Limit(50).
		All(ctx, db)
	// #endregion where-order-limit-go

	_ = []any{incompleteTodos, nonDraftTodos, selectedTodos, recentTodos, highPriority, lowPriority,
		matches, unlinkedTodos, linkedTodos, doneWithProject, recentIncomplete}
	return err
}

// #region writing-crud-go
func WriteTodoCrud(ctx context.Context, db *jazz.DB, todoID, ownerID, projectID string) error {
	// Nullable columns are pointers: nil leaves them null.
	if _, err := jazzapp.Todos.Insert(ctx, db, jazzapp.NewTodo{
		Title:     "Write docs",
		Done:      false,
		OwnerID:   &ownerID,
		ProjectID: &projectID,
	}); err != nil {
		return err
	}
	// A patch changes only the fields it sets.
	if err := jazzapp.Todos.Update(ctx, db, todoID, jazzapp.TodoPatch{Done: jazz.Set(true)}); err != nil {
		return err
	}
	return jazzapp.Todos.Delete(ctx, db, todoID)
}

// #endregion writing-crud-go

// #region writing-nullable-update-go
func ClearTodoOwner(ctx context.Context, db *jazz.DB, todoID string) error {
	// A nullable column's patch field holds a pointer: Set(nil) clears it.
	// Description isn't set, so it stays unchanged.
	return jazzapp.Todos.Update(ctx, db, todoID, jazzapp.TodoPatch{
		OwnerID: jazz.Set[*string](nil),
	})
}

// #endregion writing-nullable-update-go

// #region writing-upsert-go
func UpsertTodo(ctx context.Context, db *jazz.DB, importedTodoID string) (jazzapp.Todo, error) {
	return jazzapp.Todos.Upsert(ctx, db, importedTodoID, jazzapp.NewTodo{
		Title: "Imported task",
		Done:  false,
	}, jazz.WaitFor(jazz.WriteGlobal))
}

// #endregion writing-upsert-go

// #region writing-restore-go
func RestoreDeletedTodo(ctx context.Context, db *jazz.DB, todoID, ownerID, projectID string) (jazzapp.Todo, error) {
	if err := jazzapp.Todos.Delete(ctx, db, todoID); err != nil {
		return jazzapp.Todo{}, err
	}
	deleted, err := jazz.IncludeDeleted(jazzapp.Todos.Query().Where(jazzapp.Todos.ID.Eq(todoID))).One(ctx, db)
	if err != nil {
		return jazzapp.Todo{}, err
	}
	if deleted == nil {
		return jazzapp.Todo{}, errors.New("deleted todo not found")
	}
	// Restores it with these values.
	return jazz.Restore[jazzapp.Todo](ctx, db, "todos", todoID, jazzapp.NewTodo{
		Title:     "Restored task",
		Done:      false,
		OwnerID:   &ownerID,
		ProjectID: &projectID,
	})
}

// #endregion writing-restore-go

// #region writing-durability-tier-go
func WriteTodoDurably(ctx context.Context, db *jazz.DB, ownerID, projectID string) error {
	// Each write returns once the sync server has it.
	durable := jazz.WaitFor(jazz.WriteGlobal)
	todo, err := jazzapp.Todos.Insert(ctx, db, jazzapp.NewTodo{
		Title:     "Write docs with durability tier",
		Done:      false,
		OwnerID:   &ownerID,
		ProjectID: &projectID,
	}, durable)
	if err != nil {
		return err
	}
	if err := jazzapp.Todos.Update(ctx, db, todo.ID, jazzapp.TodoPatch{Done: jazz.Set(true)}, durable); err != nil {
		return err
	}
	return jazzapp.Todos.Delete(ctx, db, todo.ID, durable)
}

// #endregion writing-durability-tier-go

// #region writing-mutation-errors-go
func InsertTodoAndWait(ctx context.Context, db *jazz.DB, ownerID, projectID string) (string, error) {
	todo, err := jazzapp.Todos.Insert(ctx, db, jazzapp.NewTodo{
		Title:     "Ship review fixes",
		Done:      false,
		OwnerID:   &ownerID,
		ProjectID: &projectID,
	}, jazz.WaitFor(jazz.WriteGlobal))
	var rejected *jazz.Error
	if errors.As(err, &rejected) {
		// The server refused it: its kind (e.g. permission) and reason.
		return "", fmt.Errorf("rejected (%s): %s", rejected.Kind, rejected.Message)
	}
	return todo.ID, err
}

// #endregion writing-mutation-errors-go

// #region writing-transaction-go
func GroupTodoWrites(ctx context.Context, db *jazz.DB, existingTodoID, ownerID, projectID string) (string, error) {
	var createdID string
	// Commits when the function returns nil; an error or a panic rolls every
	// write back. CommitAt waits until the server has the transaction.
	err := db.Tx(ctx, func(tx *jazz.Tx) error {
		created, err := jazzapp.Todos.Insert(ctx, tx, jazzapp.NewTodo{
			Title:     "Write transaction docs",
			Done:      false,
			OwnerID:   &ownerID,
			ProjectID: &projectID,
		})
		if err != nil {
			return err
		}
		if err := jazzapp.Todos.Update(ctx, tx, existingTodoID, jazzapp.TodoPatch{Done: jazz.Set(true)}); err != nil {
			return err
		}
		staged, err := jazzapp.Todos.Query().Where(jazzapp.Todos.ID.Eq(created.ID)).One(ctx, tx)
		if err != nil {
			return err
		}
		if staged == nil {
			return errors.New("staged todo not found")
		}
		createdID = staged.ID
		return nil
	}, jazz.CommitAt(jazz.WriteGlobal))
	return createdID, err
}

// #endregion writing-transaction-go

// #region writing-transaction-errors-go
func CompleteTodoInTransaction(ctx context.Context, db *jazz.DB, todoID string) error {
	err := db.Tx(ctx, func(tx *jazz.Tx) error {
		return jazzapp.Todos.Update(ctx, tx, todoID, jazzapp.TodoPatch{Done: jazz.Set(true)})
	}, jazz.CommitAt(jazz.WriteGlobal))
	var rejected *jazz.Error
	if errors.As(err, &rejected) {
		return fmt.Errorf("rejected (%s): %s", rejected.Kind, rejected.Message)
	}
	return err
}

// #endregion writing-transaction-errors-go

// #region writing-serializable-transaction-go
func FinishTodoSerializably(ctx context.Context, db *jazz.DB, todoID string) (string, error) {
	var finishedID string
	// Serializable: if the todo changed after it was read here, the commit
	// fails with jazz.ErrConflict.
	err := db.Tx(ctx, func(tx *jazz.Tx) error {
		todo, err := jazzapp.Todos.Query().Where(jazzapp.Todos.ID.Eq(todoID)).One(ctx, tx)
		if err != nil {
			return err
		}
		if todo == nil {
			return errors.New("todo not found")
		}
		finishedID = todo.ID
		return jazzapp.Todos.Update(ctx, tx, todo.ID, jazzapp.TodoPatch{Done: jazz.Set(true)})
	}, jazz.Serializable())
	return finishedID, err
}

// #endregion writing-serializable-transaction-go

// #region dry-run-permissions-go
// Advice, not enforcement: jazz.Allowed, jazz.Denied, or jazz.Unknown
// (e.g. offline).
func CanReadTodo(ctx context.Context, db *jazz.DB, todoID string) (jazz.Advice, error) {
	return jazzapp.Todos.CanRead(ctx, db, todoID)
}

func TodosYouMayDelete(ctx context.Context, db *jazz.DB) ([]jazzapp.Todo, error) {
	todos, err := jazzapp.Todos.Query().OrderBy(jazzapp.Todos.Title.Asc()).All(ctx, db)
	if err != nil {
		return nil, err
	}
	var allowed []jazzapp.Todo
	for _, todo := range todos {
		advice, err := jazzapp.Todos.CanDelete(ctx, db, todo.ID)
		if err != nil {
			return nil, err
		}
		if advice == jazz.Allowed {
			allowed = append(allowed, todo)
		}
	}
	return allowed, nil
}

func TodosYouMayEdit(ctx context.Context, db *jazz.DB) ([]jazzapp.Todo, error) {
	todos, err := jazzapp.Todos.Query().OrderBy(jazzapp.Todos.Title.Asc()).All(ctx, db)
	if err != nil {
		return nil, err
	}
	var allowed []jazzapp.Todo
	for _, todo := range todos {
		advice, err := jazzapp.Todos.CanUpdate(ctx, db, todo.ID, jazzapp.TodoPatch{Title: jazz.Set(todo.Title)})
		if err != nil {
			return nil, err
		}
		if advice == jazz.Allowed {
			allowed = append(allowed, todo)
		}
	}
	return allowed, nil
}

func CanCreateTodo(ctx context.Context, db *jazz.DB, title string) (jazz.Advice, error) {
	return jazzapp.Todos.CanInsert(ctx, db, jazzapp.NewTodo{Title: title, Done: false})
}

// #endregion dry-run-permissions-go
