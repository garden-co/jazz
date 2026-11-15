// Code generated from jazz.app.json by `jazz-tools generate`. DO NOT EDIT.
// Bindings format 1. Regenerate it after changing jazz.app.json.

// Package jazzapp holds the typed bindings of this app's jazz.app.json.
package jazzapp

import (
	"context"
	"encoding/json"

	jazz "github.com/garden-co/jazz/packages/jazz-go"
)

// Source is the hash of the jazz.app.json these bindings were generated from.
const Source = "sha256:c3a78d6f1010cd68b627799c7ba80cace6ea90b54493f2d9dd0a1c024ccc46a1"

// App is the jazz.app.json these bindings were generated from.
const App = `{
  "$schema": "https://jazz.tools/schemas/jazz.app.schema.json",
  "format": 1,
  "tables": {
    "projects": {
      "columns": { "name": "string", "budget": "bigint?", "logo": "bytes?" },
      "relations": { "tasksViaProject": { "reverse": "tasks", "relation": "project" } }
    },
    "tasks": {
      "columns": {
        "title": { "type": "string", "merge": "text" },
        "projectId": "uuid",
        "status": { "enum": ["todo", "doing", "done"], "default": "todo" },
        "score": { "type": "int", "default": 0, "merge": "counter" },
        "tags": { "array": "string" },
        "dueAt": { "type": "timestamp", "optional": true, "transform": "isoDate" },
        "meta": {
          "json": {
            "type": "object",
            "properties": {
              "color": { "type": "string" },
              "size": { "enum": ["s", "m", "l"] },
              "links": { "type": "array", "items": { "type": "string" } }
            },
            "required": ["color"],
            "additionalProperties": false
          },
          "optional": true
        },
        "extra": { "json": true, "optional": true },
        "review": {
          "enum": {
            "pending": {},
            "approved": { "by": "uuid", "note": "string?" },
            "rejected": { "reason": "string" }
          },
          "optional": true
        },
        "assigneeIds": { "array": "uuid" }
      },
      "relations": {
        "project": { "rel": "projects", "column": "projectId" },
        "assignees": { "rel": "people", "column": "assigneeIds" }
      },
      "indexes": ["projectId", ["projectId", "$createdAt"]],
      "computed": {
        "priority": { "type": { "enum": ["low", "high"] }, "from": ["score", "status"] },
        "label": { "type": "string?", "from": ["title", "dueAt"] }
      }
    },
    "people": {
      "columns": { "name": "string", "email": { "type": "string", "transform": "lowercase" } },
      "relations": { "tasksViaAssignees": { "reverse": "tasks", "relation": "assignees" } },
      "history": "latest"
    }
  },
  "transforms": {
    "isoDate": { "stored": "timestamp", "view": "string" },
    "lowercase": { "stored": "string", "view": "string" }
  }
}
`

// Open opens this app: a client, or a backend when cfg.Permissions is set.
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

// TaskReview is a value of column `tasks.review`: one of TaskReviewPending, TaskReviewApproved, TaskReviewRejected.
type TaskReview interface{ isTaskReview() }

// TaskReviewPending is the case `pending` of TaskReview.
type TaskReviewPending struct{}

func (TaskReviewPending) isTaskReview() {}

// MarshalJSON writes the case as Jazz carries it: {case, fields}.
func (v TaskReviewPending) MarshalJSON() ([]byte, error) {
	type fields TaskReviewPending
	return jazz.MarshalCase("pending", fields(v))
}

// TaskReviewApproved is the case `approved` of TaskReview.
type TaskReviewApproved struct {
	By   string  `json:"by"`
	Note *string `json:"note"`
}

func (TaskReviewApproved) isTaskReview() {}

// MarshalJSON writes the case as Jazz carries it: {case, fields}.
func (v TaskReviewApproved) MarshalJSON() ([]byte, error) {
	type fields TaskReviewApproved
	return jazz.MarshalCase("approved", fields(v))
}

// TaskReviewRejected is the case `rejected` of TaskReview.
type TaskReviewRejected struct {
	Reason string `json:"reason"`
}

func (TaskReviewRejected) isTaskReview() {}

// MarshalJSON writes the case as Jazz carries it: {case, fields}.
func (v TaskReviewRejected) MarshalJSON() ([]byte, error) {
	type fields TaskReviewRejected
	return jazz.MarshalCase("rejected", fields(v))
}

func decodeTaskReview(raw json.RawMessage) (TaskReview, error) {
	case_, fields, err := jazz.DecodeCase(raw)
	if err != nil || case_ == "" {
		return nil, err
	}
	switch case_ {
	case "pending":
		var v TaskReviewPending
		err := json.Unmarshal(fields, &v)
		return v, err
	case "approved":
		var v TaskReviewApproved
		err := json.Unmarshal(fields, &v)
		return v, err
	case "rejected":
		var v TaskReviewRejected
		err := json.Unmarshal(fields, &v)
		return v, err
	}
	return nil, jazz.UnknownCase("column `tasks.review`", case_)
}

// Person is a row of `people`.
type Person struct {
	ID                string `json:"id"`
	Name              string `json:"name"`
	Email             string `json:"-"`
	TasksViaAssignees []Task `json:"tasksViaAssignees,omitempty"`
}

// JazzDecode reads what plain JSON can't: payload enums, the transformed columns' views and
// the computed fields.
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
	ID                jazz.Column[Person, string]
	Name              jazz.Column[Person, string]
	Email             jazz.Column[Person, string]
	TasksViaAssignees jazz.Relation[Person, Task]
}

// People is the table `people`.
var People = peopleTable{
	ID:                jazz.NewColumn[Person, string]("id"),
	Name:              jazz.NewColumn[Person, string]("name"),
	Email:             jazz.NewColumn[Person, string]("email"),
	TasksViaAssignees: jazz.NewRelation[Person, Task]("tasksViaAssignees"),
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

// History starts a query of `people`'s history: every accepted version of its rows.
func (peopleTable) History() *jazz.Query[jazz.Version[Person]] {
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

// Project is a row of `projects`.
type Project struct {
	ID              string       `json:"id"`
	Name            string       `json:"name"`
	Budget          *jazz.BigInt `json:"budget"`
	Logo            *[]byte      `json:"logo"`
	TasksViaProject []Task       `json:"tasksViaProject,omitempty"`
}

// NewProject is a row to insert into `projects`: nil fields are null, or take their default.
type NewProject struct {
	Name   string       `json:"name"`
	Budget *jazz.BigInt `json:"budget,omitempty"`
	Logo   *[]byte      `json:"logo,omitempty"`
}

// ProjectPatch changes the fields of a row of `projects` that are set (jazz.Set).
type ProjectPatch struct {
	Name   jazz.Patch[string]
	Budget jazz.Patch[*jazz.BigInt]
	Logo   jazz.Patch[*[]byte]
}

// MarshalJSON writes the fields that are set.
func (p ProjectPatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.Name.Entry("name"),
		p.Budget.Entry("budget"),
		p.Logo.Entry("logo"),
	)
}

// projectsTable is the type of Projects: the columns of `projects`, to build conditions and orders from, and its relations, to include.
type projectsTable struct {
	ID              jazz.Column[Project, string]
	Name            jazz.Column[Project, string]
	Budget          jazz.Column[Project, jazz.BigInt]
	Logo            jazz.Column[Project, []byte]
	TasksViaProject jazz.Relation[Project, Task]
}

// Projects is the table `projects`.
var Projects = projectsTable{
	ID:              jazz.NewColumn[Project, string]("id"),
	Name:            jazz.NewColumn[Project, string]("name"),
	Budget:          jazz.NewColumn[Project, jazz.BigInt]("budget"),
	Logo:            jazz.NewColumn[Project, []byte]("logo"),
	TasksViaProject: jazz.NewRelation[Project, Task]("tasksViaProject"),
}

// Query starts a query of `projects`.
func (projectsTable) Query() *jazz.Query[Project] {
	return jazz.NewQuery[Project]("projects")
}

// Insert inserts a row into `projects` and returns it, with its id.
func (projectsTable) Insert(ctx context.Context, db jazz.Querier, row NewProject, opts ...jazz.WriteOption) (Project, error) {
	return jazz.Insert[NewProject, Project](ctx, db, "projects", row, opts...)
}

// Update changes the fields patch sets of the row of `projects` with id.
func (projectsTable) Update(ctx context.Context, db jazz.Querier, id string, patch ProjectPatch, opts ...jazz.WriteOption) error {
	return jazz.Update(ctx, db, "projects", id, patch, opts...)
}

// History starts a query of `projects`'s history: every accepted version of its rows.
func (projectsTable) History() *jazz.Query[jazz.Version[Project]] {
	return jazz.History(jazz.NewQuery[Project]("projects"))
}

// CanRead advises whether the app's permissions allow reading the row with id in `projects`.
func (projectsTable) CanRead(ctx context.Context, db jazz.Querier, id string) (jazz.Advice, error) {
	return jazz.CanRead(ctx, db, "projects", id)
}

// CanInsert advises whether the app's permissions allow inserting row in `projects`.
func (projectsTable) CanInsert(ctx context.Context, db jazz.Querier, row NewProject) (jazz.Advice, error) {
	return jazz.CanInsert(ctx, db, "projects", row)
}

// CanUpdate advises whether the app's permissions allow applying patch to the row with id in `projects`.
func (projectsTable) CanUpdate(ctx context.Context, db jazz.Querier, id string, patch ProjectPatch) (jazz.Advice, error) {
	return jazz.CanUpdate(ctx, db, "projects", id, patch)
}

// CanDelete advises whether the app's permissions allow deleting the row with id in `projects`.
func (projectsTable) CanDelete(ctx context.Context, db jazz.Querier, id string) (jazz.Advice, error) {
	return jazz.CanDelete(ctx, db, "projects", id)
}

// Delete deletes the row of `projects` with id.
func (projectsTable) Delete(ctx context.Context, db jazz.Querier, id string, opts ...jazz.WriteOption) error {
	return jazz.Delete(ctx, db, "projects", id, opts...)
}

// Task is a row of `tasks`.
type Task struct {
	ID          string           `json:"id"`
	Title       string           `json:"title"`
	ProjectID   string           `json:"projectId"`
	Status      TaskStatus       `json:"status"`
	Score       int32            `json:"score"`
	Tags        []string         `json:"tags"`
	DueAt       *string          `json:"-"`
	Meta        *json.RawMessage `json:"meta"`
	Extra       *json.RawMessage `json:"extra"`
	Review      TaskReview       `json:"-"`
	AssigneeIDs []string         `json:"assigneeIds"`
	Priority    TaskPriority     `json:"-"`
	Label       *string          `json:"-"`
	Assignees   []Person         `json:"assignees,omitempty"`
	Project     *Project         `json:"project,omitempty"`
}

// JazzDecode reads what plain JSON can't: payload enums, the transformed columns' views and
// the computed fields.
func (r *Task) JazzDecode(app any, fields map[string]json.RawMessage) error {
	c, ok := app.(Computed)
	if !ok {
		return jazz.ErrNoComputed
	}
	if raw, ok := fields["review"]; ok {
		v, err := decodeTaskReview(raw)
		if err != nil {
			return err
		}
		r.Review = v
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
	Title       string           `json:"title"`
	ProjectID   string           `json:"projectId"`
	Status      *TaskStatus      `json:"status,omitempty"`
	Score       *int32           `json:"score,omitempty"`
	Tags        []string         `json:"tags"`
	DueAt       *string          `json:"-"`
	Meta        *json.RawMessage `json:"meta,omitempty"`
	Extra       *json.RawMessage `json:"extra,omitempty"`
	Review      TaskReview       `json:"review,omitempty"`
	AssigneeIDs []string         `json:"assigneeIds"`
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
	Title       jazz.Patch[string]
	ProjectID   jazz.Patch[string]
	Status      jazz.Patch[TaskStatus]
	Score       jazz.Patch[int32]
	Tags        jazz.Patch[[]string]
	DueAt       jazz.Patch[*string]
	Meta        jazz.Patch[*json.RawMessage]
	Extra       jazz.Patch[*json.RawMessage]
	Review      jazz.Patch[TaskReview]
	AssigneeIDs jazz.Patch[[]string]
}

// JazzEncode is the patch, its transformed columns stored.
func (p TaskPatch) JazzEncode(app any) (any, error) {
	c, ok := app.(Computed)
	if !ok {
		return nil, jazz.ErrNoComputed
	}
	entries := []jazz.PatchEntry{
		p.Title.Entry("title"),
		p.ProjectID.Entry("projectId"),
		p.Status.Entry("status"),
		p.Score.Entry("score"),
		p.Tags.Entry("tags"),
		p.Meta.Entry("meta"),
		p.Extra.Entry("extra"),
		p.Review.Entry("review"),
		p.AssigneeIDs.Entry("assigneeIds"),
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
	ID          jazz.Column[Task, string]
	Title       jazz.Column[Task, string]
	ProjectID   jazz.Column[Task, string]
	Status      jazz.Column[Task, TaskStatus]
	Score       jazz.Column[Task, int32]
	Tags        jazz.Column[Task, []string]
	DueAt       jazz.Column[Task, jazz.Timestamp]
	Meta        jazz.Column[Task, json.RawMessage]
	Extra       jazz.Column[Task, json.RawMessage]
	Review      jazz.Column[Task, TaskReview]
	AssigneeIDs jazz.Column[Task, []string]
	Assignees   jazz.Relation[Task, Person]
	Project     jazz.Relation[Task, Project]
}

// Tasks is the table `tasks`.
var Tasks = tasksTable{
	ID:          jazz.NewColumn[Task, string]("id"),
	Title:       jazz.NewColumn[Task, string]("title"),
	ProjectID:   jazz.NewColumn[Task, string]("projectId"),
	Status:      jazz.NewColumn[Task, TaskStatus]("status"),
	Score:       jazz.NewColumn[Task, int32]("score"),
	Tags:        jazz.NewColumn[Task, []string]("tags"),
	DueAt:       jazz.NewColumn[Task, jazz.Timestamp]("dueAt"),
	Meta:        jazz.NewColumn[Task, json.RawMessage]("meta"),
	Extra:       jazz.NewColumn[Task, json.RawMessage]("extra"),
	Review:      jazz.NewColumn[Task, TaskReview]("review"),
	AssigneeIDs: jazz.NewColumn[Task, []string]("assigneeIds"),
	Assignees:   jazz.NewRelation[Task, Person]("assignees"),
	Project:     jazz.NewRelation[Task, Project]("project"),
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

// EditTitle edits `tasks.title`, a mergeable text: positions are UTF-8 byte offsets.
func (tasksTable) EditTitle(ctx context.Context, db jazz.Querier, id string, edits ...jazz.TextEdit) error {
	return jazz.EditText(ctx, db, "tasks", id, "title", edits...)
}

// History starts a query of `tasks`'s history: every accepted version of its rows.
func (tasksTable) History() *jazz.Query[jazz.Version[Task]] {
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
