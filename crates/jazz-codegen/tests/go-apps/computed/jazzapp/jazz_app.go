// Code generated from jazz.app.json by `jazz-tools generate`. DO NOT EDIT.
// Bindings format 1. Regenerate it after changing jazz.app.json.

// Package jazzapp holds the typed bindings of this app's jazz.app.json.
package jazzapp

import (
	"context"
	"encoding/json"

	jazz "github.com/garden-co/jazz/packages/jazz-go/v2"
)

// Source is the hash of the jazz.app.json these bindings were generated from.
const Source = "sha256:2f20856ebe925b7f40369c1f3cf504a1845a623d5a8eb503ab202c9f19d2f569"

// App is the jazz.app.json these bindings were generated from.
const App = `{
  "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
  "tables": {
    "tasks": {
      "columns": {
        "title": "string",
        "status": { "enum": ["todo", "doing", "done"], "default": "todo" },
        "score": { "type": "int", "default": 0 },
        "dueAt": { "type": "timestamp", "optional": true, "transform": "isoDate" }
      },
      "computed": {
        "priority": { "type": { "enum": ["low", "high"] }, "from": ["score", "status"] },
        "label": { "type": "string?", "from": ["title", "dueAt"] }
      }
    },
    "people": {
      "columns": { "name": "string", "email": { "type": "string", "transform": "lowercase" } }
    }
  },
  "transforms": {
    "isoDate": { "stored": "timestamp", "view": "string" },
    "lowercase": { "stored": "string", "view": "string" }
  }
}
`

// Open opens this app: locally, or as a client of cfg.ServerURL.
// computed is your Computed: what this app's computed fields and transforms run.
func Open(ctx context.Context, cfg jazz.Config, computed Computed) (*jazz.DB, error) {
	if computed == nil {
		return nil, jazz.ErrNoComputed
	}
	cfg.App = App
	cfg.Computed = computed
	return jazz.Open(ctx, cfg)
}

// Computed is what this app computes, which you implement and pass to Open: its
// computed fields, and its transforms' conversions.
type Computed interface {
	// LowercaseView reads a value stored through transform `lowercase`.
	LowercaseView(stored string) (string, error)
	// LowercaseStored is what transform `lowercase` stores for view.
	LowercaseStored(view string) (string, error)
	// IsoDateView reads a value stored through transform `isoDate`.
	IsoDateView(stored jazz.Timestamp) (string, error)
	// IsoDateStored is what transform `isoDate` stores for view.
	IsoDateStored(view string) (jazz.Timestamp, error)
	// TaskPriority computes `tasks.priority` from `score`, `status`.
	TaskPriority(score int32, status TaskStatus) TaskPriority
	// TaskLabel computes `tasks.label` from `title`, `dueAt`.
	TaskLabel(title string, dueAt *string) *string
}

// TaskPriority is a value of computed field `tasks.priority`.
type TaskPriority string

// The values of TaskPriority.
const (
	TaskPriorityLow  TaskPriority = "low"
	TaskPriorityHigh TaskPriority = "high"
)

// TaskStatus is a value of column `tasks.status`.
type TaskStatus string

// The values of TaskStatus.
const (
	TaskStatusTodo  TaskStatus = "todo"
	TaskStatusDoing TaskStatus = "doing"
	TaskStatusDone  TaskStatus = "done"
)

// Person is a row of `people`.
type Person struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"-"`
}

// JazzDecode reads the transformed columns' views and fills the computed fields.
func (r *Person) JazzDecode(app any, fields map[string]json.RawMessage) error {
	c, ok := app.(Computed)
	if !ok {
		return jazz.ErrNoComputed
	}
	if raw, ok := fields["email"]; ok {
		var stored string
		if err := json.Unmarshal(raw, &stored); err != nil {
			return err
		}
		view, err := c.LowercaseView(stored)
		if err != nil {
			return err
		}
		r.Email = view
	}
	return nil
}

// NewPerson is a row to insert into `people`: nil fields are null, or take their default.
type NewPerson struct {
	Name  string `json:"name"`
	Email string `json:"-"`
}

// JazzEncode is the row to insert, its transformed columns stored.
func (r NewPerson) JazzEncode(app any) (any, error) {
	c, ok := app.(Computed)
	if !ok {
		return nil, jazz.ErrNoComputed
	}
	type plain NewPerson
	out, err := jazz.Fields(plain(r))
	if err != nil {
		return nil, err
	}
	{
		stored, err := c.LowercaseStored(r.Email)
		if err != nil {
			return nil, err
		}
		out["email"] = stored
	}
	return out, nil
}

// PersonPatch changes the fields of a row of `people` that are set (jazz.Set).
type PersonPatch struct {
	Name  jazz.Patch[string]
	Email jazz.Patch[string]
}

// JazzEncode is the patch, its transformed columns stored.
func (p PersonPatch) JazzEncode(app any) (any, error) {
	c, ok := app.(Computed)
	if !ok {
		return nil, jazz.ErrNoComputed
	}
	entries := []jazz.PatchEntry{
		p.Name.Entry("name"),
	}
	if p.Email.IsSet() {
		stored, err := c.LowercaseStored(p.Email.Value())
		if err != nil {
			return nil, err
		}
		entries = append(entries, jazz.Set(stored).Entry("email"))
	}
	return jazz.PatchJSON(entries...)
}

// peopleTable is the type of People: the columns of `people`, to build conditions and orders from, and its relations, to include.
type peopleTable struct {
	ID    jazz.Column[Person, string]
	Name  jazz.Column[Person, string]
	Email jazz.Column[Person, string]
}

// People is the table `people`.
var People = peopleTable{
	ID:    jazz.NewColumn[Person, string]("id"),
	Name:  jazz.NewColumn[Person, string]("name"),
	Email: jazz.NewColumn[Person, string]("email"),
}

// Query starts a query of `people`.
func (peopleTable) Query() *jazz.Query[Person] {
	return jazz.NewQuery[Person]("people")
}

// Insert inserts a row into `people` and returns it, with its id.
func (peopleTable) Insert(ctx context.Context, db jazz.Querier, row NewPerson, opts ...jazz.WriteOption) (Person, error) {
	return jazz.Insert[NewPerson, Person](ctx, db, "people", row, opts...)
}

// Update changes the fields patch sets of the row of `people` with id.
func (peopleTable) Update(ctx context.Context, db jazz.Querier, id string, patch PersonPatch, opts ...jazz.WriteOption) error {
	return jazz.Update(ctx, db, "people", id, patch, opts...)
}

// Upsert writes row as the row of `people` with id: inserted, or updated if it exists.
func (peopleTable) Upsert(ctx context.Context, db jazz.Querier, id string, row NewPerson, opts ...jazz.WriteOption) (Person, error) {
	return jazz.Upsert[NewPerson, Person](ctx, db, "people", id, row, opts...)
}

// Restore brings back the deleted row of `people` with id.
func (peopleTable) Restore(ctx context.Context, db jazz.Querier, id string) (Person, error) {
	return jazz.Restore[Person](ctx, db, "people", id, nil)
}

// History starts a query of `people`'s history: every accepted version of its rows.
func (peopleTable) History() *jazz.Query[jazz.HistoryRow[Person]] {
	return jazz.History(jazz.NewQuery[Person]("people"))
}

// CanRead advises whether the app's permissions allow reading the row with id in `people`.
func (peopleTable) CanRead(ctx context.Context, db jazz.Querier, id string) (jazz.Advice, error) {
	return jazz.CanRead(ctx, db, "people", id)
}

// CanInsert advises whether the app's permissions allow inserting row in `people`.
func (peopleTable) CanInsert(ctx context.Context, db jazz.Querier, row NewPerson) (jazz.Advice, error) {
	return jazz.CanInsert(ctx, db, "people", row)
}

// CanUpdate advises whether the app's permissions allow applying patch to the row with id in `people`.
func (peopleTable) CanUpdate(ctx context.Context, db jazz.Querier, id string, patch PersonPatch) (jazz.Advice, error) {
	return jazz.CanUpdate(ctx, db, "people", id, patch)
}

// CanDelete advises whether the app's permissions allow deleting the row with id in `people`.
func (peopleTable) CanDelete(ctx context.Context, db jazz.Querier, id string) (jazz.Advice, error) {
	return jazz.CanDelete(ctx, db, "people", id)
}

// Delete deletes the row of `people` with id.
func (peopleTable) Delete(ctx context.Context, db jazz.Querier, id string, opts ...jazz.WriteOption) error {
	return jazz.Delete(ctx, db, "people", id, opts...)
}

// Task is a row of `tasks`.
type Task struct {
	ID       string       `json:"id"`
	Title    string       `json:"title"`
	Status   TaskStatus   `json:"status"`
	Score    int32        `json:"score"`
	DueAt    *string      `json:"-"`
	Priority TaskPriority `json:"-"`
	Label    *string      `json:"-"`
}

// JazzDecode reads the transformed columns' views and fills the computed fields.
func (r *Task) JazzDecode(app any, fields map[string]json.RawMessage) error {
	c, ok := app.(Computed)
	if !ok {
		return jazz.ErrNoComputed
	}
	if raw, ok := fields["dueAt"]; ok {
		var stored *jazz.Timestamp
		if err := json.Unmarshal(raw, &stored); err != nil {
			return err
		}
		r.DueAt = nil
		if stored != nil {
			view, err := c.IsoDateView(*stored)
			if err != nil {
				return err
			}
			r.DueAt = &view
		}
	}
	r.Priority = c.TaskPriority(r.Score, r.Status)
	r.Label = c.TaskLabel(r.Title, r.DueAt)
	return nil
}

// NewTask is a row to insert into `tasks`: nil fields are null, or take their default.
type NewTask struct {
	Title  string      `json:"title"`
	Status *TaskStatus `json:"status,omitempty"`
	Score  *int32      `json:"score,omitempty"`
	DueAt  *string     `json:"-"`
}

// JazzEncode is the row to insert, its transformed columns stored.
func (r NewTask) JazzEncode(app any) (any, error) {
	c, ok := app.(Computed)
	if !ok {
		return nil, jazz.ErrNoComputed
	}
	type plain NewTask
	out, err := jazz.Fields(plain(r))
	if err != nil {
		return nil, err
	}
	if r.DueAt != nil {
		stored, err := c.IsoDateStored(*r.DueAt)
		if err != nil {
			return nil, err
		}
		out["dueAt"] = stored
	}
	return out, nil
}

// TaskPatch changes the fields of a row of `tasks` that are set (jazz.Set).
type TaskPatch struct {
	Title  jazz.Patch[string]
	Status jazz.Patch[TaskStatus]
	Score  jazz.Patch[int32]
	DueAt  jazz.Patch[*string]
}

// JazzEncode is the patch, its transformed columns stored.
func (p TaskPatch) JazzEncode(app any) (any, error) {
	c, ok := app.(Computed)
	if !ok {
		return nil, jazz.ErrNoComputed
	}
	entries := []jazz.PatchEntry{
		p.Title.Entry("title"),
		p.Status.Entry("status"),
		p.Score.Entry("score"),
	}
	if p.DueAt.IsSet() {
		var stored *jazz.Timestamp
		if view := p.DueAt.Value(); view != nil {
			v, err := c.IsoDateStored(*view)
			if err != nil {
				return nil, err
			}
			stored = &v
		}
		entries = append(entries, jazz.Set(stored).Entry("dueAt"))
	}
	return jazz.PatchJSON(entries...)
}

// tasksTable is the type of Tasks: the columns of `tasks`, to build conditions and orders from, and its relations, to include.
type tasksTable struct {
	ID     jazz.Column[Task, string]
	Title  jazz.Column[Task, string]
	Status jazz.Column[Task, TaskStatus]
	Score  jazz.Column[Task, int32]
	DueAt  jazz.Column[Task, jazz.Timestamp]
}

// Tasks is the table `tasks`.
var Tasks = tasksTable{
	ID:     jazz.NewColumn[Task, string]("id"),
	Title:  jazz.NewColumn[Task, string]("title"),
	Status: jazz.NewColumn[Task, TaskStatus]("status"),
	Score:  jazz.NewColumn[Task, int32]("score"),
	DueAt:  jazz.NewColumn[Task, jazz.Timestamp]("dueAt"),
}

// Query starts a query of `tasks`.
func (tasksTable) Query() *jazz.Query[Task] {
	return jazz.NewQuery[Task]("tasks")
}

// Insert inserts a row into `tasks` and returns it, with its id.
func (tasksTable) Insert(ctx context.Context, db jazz.Querier, row NewTask, opts ...jazz.WriteOption) (Task, error) {
	return jazz.Insert[NewTask, Task](ctx, db, "tasks", row, opts...)
}

// Update changes the fields patch sets of the row of `tasks` with id.
func (tasksTable) Update(ctx context.Context, db jazz.Querier, id string, patch TaskPatch, opts ...jazz.WriteOption) error {
	return jazz.Update(ctx, db, "tasks", id, patch, opts...)
}

// Upsert writes row as the row of `tasks` with id: inserted, or updated if it exists.
func (tasksTable) Upsert(ctx context.Context, db jazz.Querier, id string, row NewTask, opts ...jazz.WriteOption) (Task, error) {
	return jazz.Upsert[NewTask, Task](ctx, db, "tasks", id, row, opts...)
}

// Restore brings back the deleted row of `tasks` with id.
func (tasksTable) Restore(ctx context.Context, db jazz.Querier, id string) (Task, error) {
	return jazz.Restore[Task](ctx, db, "tasks", id, nil)
}

// History starts a query of `tasks`'s history: every accepted version of its rows.
func (tasksTable) History() *jazz.Query[jazz.HistoryRow[Task]] {
	return jazz.History(jazz.NewQuery[Task]("tasks"))
}

// CanRead advises whether the app's permissions allow reading the row with id in `tasks`.
func (tasksTable) CanRead(ctx context.Context, db jazz.Querier, id string) (jazz.Advice, error) {
	return jazz.CanRead(ctx, db, "tasks", id)
}

// CanInsert advises whether the app's permissions allow inserting row in `tasks`.
func (tasksTable) CanInsert(ctx context.Context, db jazz.Querier, row NewTask) (jazz.Advice, error) {
	return jazz.CanInsert(ctx, db, "tasks", row)
}

// CanUpdate advises whether the app's permissions allow applying patch to the row with id in `tasks`.
func (tasksTable) CanUpdate(ctx context.Context, db jazz.Querier, id string, patch TaskPatch) (jazz.Advice, error) {
	return jazz.CanUpdate(ctx, db, "tasks", id, patch)
}

// CanDelete advises whether the app's permissions allow deleting the row with id in `tasks`.
func (tasksTable) CanDelete(ctx context.Context, db jazz.Querier, id string) (jazz.Advice, error) {
	return jazz.CanDelete(ctx, db, "tasks", id)
}

// Delete deletes the row of `tasks` with id.
func (tasksTable) Delete(ctx context.Context, db jazz.Querier, id string, opts ...jazz.WriteOption) error {
	return jazz.Delete(ctx, db, "tasks", id, opts...)
}
