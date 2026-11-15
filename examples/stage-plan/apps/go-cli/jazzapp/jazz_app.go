// Code generated from jazz.app.json by `jazz-tools generate`. DO NOT EDIT.
// Bindings format 1. Regenerate it after changing jazz.app.json.

// Package jazzapp holds the typed bindings of this app's jazz.app.json.
package jazzapp

import (
	"context"

	jazz "github.com/garden-co/jazz/packages/jazz-go/v2"
)

// Source is the hash of the jazz.app.json these bindings were generated from.
const Source = "sha256:1adfb9c805c18f17655799f7003dd9ac6f75950914995d9c25b0fbcaad7a545f"

// App is the jazz.app.json these bindings were generated from.
const App = `{
  "$schema": "https://jazz.tools/schemas/jazz.app.schema.json",
  "format": 1,
  "tables": {
    "activity": {
      "columns": {
        "showId": "uuid",
        "taskId": "uuid",
        "actorId": "uuid",
        "kind": { "enum": ["created", "moved", "assigned", "renamed", "commented", "deleted"] },
        "detail": "string?"
      },
      "relations": {
        "actor": { "rel": "crew", "column": "actorId" },
        "show": { "rel": "shows", "column": "showId" },
        "task": { "rel": "tasks", "column": "taskId" }
      }
    },
    "checklistItems": {
      "columns": { "title": "string", "done": "boolean", "ownerAccount": "uuid" }
    },
    "comments": {
      "columns": { "taskId": "uuid", "authorId": "uuid", "body": "string" },
      "relations": {
        "author": { "rel": "crew", "column": "authorId" },
        "task": { "rel": "tasks", "column": "taskId" }
      }
    },
    "crew": {
      "columns": { "account": "uuid", "name": "string" },
      "relations": {
        "assignedTasks": { "reverse": "tasks", "relation": "assignee" },
        "memberships": { "reverse": "showCrew", "relation": "crew" }
      }
    },
    "showCrew": {
      "columns": {
        "showId": "uuid",
        "crewId": "uuid",
        "account": "uuid",
        "role": { "enum": ["chief", "crew"] },
        "inviteCode": "string?"
      },
      "relations": {
        "crew": { "rel": "crew", "column": "crewId" },
        "show": { "rel": "shows", "column": "showId" }
      }
    },
    "showInvites": {
      "columns": { "showId": "uuid", "code": "string" },
      "relations": { "show": { "rel": "shows", "column": "showId" } }
    },
    "shows": {
      "columns": {
        "name": "string",
        "venue": "string",
        "date": "string",
        "doors": "string",
        "chiefAccount": "uuid"
      },
      "relations": {
        "activity": { "reverse": "activity", "relation": "show" },
        "crew": { "reverse": "showCrew", "relation": "show" },
        "invites": { "reverse": "showInvites", "relation": "show" },
        "tasks": { "reverse": "tasks", "relation": "show" }
      }
    },
    "tasks": {
      "columns": {
        "showId": "uuid",
        "title": "string",
        "status": { "enum": ["todo", "doing", "done", "blocked"] },
        "assigneeId": "uuid?",
        "notes": { "type": "string", "optional": true, "merge": "text" },
        "rank": "float"
      },
      "relations": {
        "activity": { "reverse": "activity", "relation": "task" },
        "assignee": { "rel": "crew", "column": "assigneeId" },
        "comments": { "reverse": "comments", "relation": "task" },
        "show": { "rel": "shows", "column": "showId" }
      }
    }
  }
}
`

// Open opens this app: a client, or a backend when cfg.Permissions is set.
func Open(ctx context.Context, cfg jazz.Config) (*jazz.DB, error) {
	cfg.App = App
	return jazz.Open(ctx, cfg)
}

// ActivityKind is a value of column `activity.kind`.
type ActivityKind string

// The values of ActivityKind.
const (
	ActivityKindCreated   ActivityKind = "created"
	ActivityKindMoved     ActivityKind = "moved"
	ActivityKindAssigned  ActivityKind = "assigned"
	ActivityKindRenamed   ActivityKind = "renamed"
	ActivityKindCommented ActivityKind = "commented"
	ActivityKindDeleted   ActivityKind = "deleted"
)

// ShowCrewRole is a value of column `showCrew.role`.
type ShowCrewRole string

// The values of ShowCrewRole.
const (
	ShowCrewRoleChief ShowCrewRole = "chief"
	ShowCrewRoleCrew  ShowCrewRole = "crew"
)

// TaskStatus is a value of column `tasks.status`.
type TaskStatus string

// The values of TaskStatus.
const (
	TaskStatusTodo    TaskStatus = "todo"
	TaskStatusDoing   TaskStatus = "doing"
	TaskStatusDone    TaskStatus = "done"
	TaskStatusBlocked TaskStatus = "blocked"
)

// Activity is a row of `activity`.
type Activity struct {
	ID      string       `json:"id"`
	ShowID  string       `json:"showId"`
	TaskID  string       `json:"taskId"`
	ActorID string       `json:"actorId"`
	Kind    ActivityKind `json:"kind"`
	Detail  *string      `json:"detail"`
	Actor   *Crew        `json:"actor,omitempty"`
	Show    *Show        `json:"show,omitempty"`
	Task    *Task        `json:"task,omitempty"`
}

// NewActivity is a row to insert into `activity`: nil fields are null, or take their default.
type NewActivity struct {
	ShowID  string       `json:"showId"`
	TaskID  string       `json:"taskId"`
	ActorID string       `json:"actorId"`
	Kind    ActivityKind `json:"kind"`
	Detail  *string      `json:"detail,omitempty"`
}

// ActivityPatch changes the fields of a row of `activity` that are set (jazz.Set).
type ActivityPatch struct {
	ShowID  jazz.Patch[string]
	TaskID  jazz.Patch[string]
	ActorID jazz.Patch[string]
	Kind    jazz.Patch[ActivityKind]
	Detail  jazz.Patch[*string]
}

// MarshalJSON writes the fields that are set.
func (p ActivityPatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.ShowID.Entry("showId"),
		p.TaskID.Entry("taskId"),
		p.ActorID.Entry("actorId"),
		p.Kind.Entry("kind"),
		p.Detail.Entry("detail"),
	)
}

// activityTable is the type of ActivityTable: the columns of `activity`, to build conditions and orders from, and its relations, to include.
type activityTable struct {
	ID      jazz.Column[Activity, string]
	ShowID  jazz.Column[Activity, string]
	TaskID  jazz.Column[Activity, string]
	ActorID jazz.Column[Activity, string]
	Kind    jazz.Column[Activity, ActivityKind]
	Detail  jazz.Column[Activity, string]
	Actor   jazz.Relation[Activity, Crew]
	Show    jazz.Relation[Activity, Show]
	Task    jazz.Relation[Activity, Task]
}

// ActivityTable is the table `activity`.
var ActivityTable = activityTable{
	ID:      jazz.NewColumn[Activity, string]("id"),
	ShowID:  jazz.NewColumn[Activity, string]("showId"),
	TaskID:  jazz.NewColumn[Activity, string]("taskId"),
	ActorID: jazz.NewColumn[Activity, string]("actorId"),
	Kind:    jazz.NewColumn[Activity, ActivityKind]("kind"),
	Detail:  jazz.NewColumn[Activity, string]("detail"),
	Actor:   jazz.NewRelation[Activity, Crew]("actor"),
	Show:    jazz.NewRelation[Activity, Show]("show"),
	Task:    jazz.NewRelation[Activity, Task]("task"),
}

// Query starts a query of `activity`.
func (activityTable) Query() *jazz.Query[Activity] {
	return jazz.NewQuery[Activity]("activity")
}

// Insert inserts a row into `activity` and returns it, with its id.
func (activityTable) Insert(ctx context.Context, db jazz.Querier, row NewActivity, opts ...jazz.WriteOption) (Activity, error) {
	return jazz.Insert[NewActivity, Activity](ctx, db, "activity", row, opts...)
}

// Update changes the fields patch sets of the row of `activity` with id.
func (activityTable) Update(ctx context.Context, db jazz.Querier, id string, patch ActivityPatch, opts ...jazz.WriteOption) error {
	return jazz.Update(ctx, db, "activity", id, patch, opts...)
}

// Upsert writes row as the row of `activity` with id: inserted, or updated if it exists.
func (activityTable) Upsert(ctx context.Context, db jazz.Querier, id string, row NewActivity, opts ...jazz.WriteOption) (Activity, error) {
	return jazz.Upsert[NewActivity, Activity](ctx, db, "activity", id, row, opts...)
}

// Restore brings back the deleted row of `activity` with id.
func (activityTable) Restore(ctx context.Context, db jazz.Querier, id string) (Activity, error) {
	return jazz.Restore[Activity](ctx, db, "activity", id, nil)
}

// History starts a query of `activity`'s history: every accepted version of its rows.
func (activityTable) History() *jazz.Query[jazz.Version[Activity]] {
	return jazz.History(jazz.NewQuery[Activity]("activity"))
}

// CanRead advises whether the app's permissions allow reading the row with id in `activity`.
func (activityTable) CanRead(ctx context.Context, db jazz.Querier, id string) (jazz.Advice, error) {
	return jazz.CanRead(ctx, db, "activity", id)
}

// CanInsert advises whether the app's permissions allow inserting row in `activity`.
func (activityTable) CanInsert(ctx context.Context, db jazz.Querier, row NewActivity) (jazz.Advice, error) {
	return jazz.CanInsert(ctx, db, "activity", row)
}

// CanUpdate advises whether the app's permissions allow applying patch to the row with id in `activity`.
func (activityTable) CanUpdate(ctx context.Context, db jazz.Querier, id string, patch ActivityPatch) (jazz.Advice, error) {
	return jazz.CanUpdate(ctx, db, "activity", id, patch)
}

// CanDelete advises whether the app's permissions allow deleting the row with id in `activity`.
func (activityTable) CanDelete(ctx context.Context, db jazz.Querier, id string) (jazz.Advice, error) {
	return jazz.CanDelete(ctx, db, "activity", id)
}

// Delete deletes the row of `activity` with id.
func (activityTable) Delete(ctx context.Context, db jazz.Querier, id string, opts ...jazz.WriteOption) error {
	return jazz.Delete(ctx, db, "activity", id, opts...)
}

// ChecklistItem is a row of `checklistItems`.
type ChecklistItem struct {
	ID           string `json:"id"`
	Title        string `json:"title"`
	Done         bool   `json:"done"`
	OwnerAccount string `json:"ownerAccount"`
}

// NewChecklistItem is a row to insert into `checklistItems`: nil fields are null, or take their default.
type NewChecklistItem struct {
	Title        string `json:"title"`
	Done         bool   `json:"done"`
	OwnerAccount string `json:"ownerAccount"`
}

// ChecklistItemPatch changes the fields of a row of `checklistItems` that are set (jazz.Set).
type ChecklistItemPatch struct {
	Title        jazz.Patch[string]
	Done         jazz.Patch[bool]
	OwnerAccount jazz.Patch[string]
}

// MarshalJSON writes the fields that are set.
func (p ChecklistItemPatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.Title.Entry("title"),
		p.Done.Entry("done"),
		p.OwnerAccount.Entry("ownerAccount"),
	)
}

// checklistItemsTable is the type of ChecklistItems: the columns of `checklistItems`, to build conditions and orders from, and its relations, to include.
type checklistItemsTable struct {
	ID           jazz.Column[ChecklistItem, string]
	Title        jazz.Column[ChecklistItem, string]
	Done         jazz.Column[ChecklistItem, bool]
	OwnerAccount jazz.Column[ChecklistItem, string]
}

// ChecklistItems is the table `checklistItems`.
var ChecklistItems = checklistItemsTable{
	ID:           jazz.NewColumn[ChecklistItem, string]("id"),
	Title:        jazz.NewColumn[ChecklistItem, string]("title"),
	Done:         jazz.NewColumn[ChecklistItem, bool]("done"),
	OwnerAccount: jazz.NewColumn[ChecklistItem, string]("ownerAccount"),
}

// Query starts a query of `checklistItems`.
func (checklistItemsTable) Query() *jazz.Query[ChecklistItem] {
	return jazz.NewQuery[ChecklistItem]("checklistItems")
}

// Insert inserts a row into `checklistItems` and returns it, with its id.
func (checklistItemsTable) Insert(ctx context.Context, db jazz.Querier, row NewChecklistItem, opts ...jazz.WriteOption) (ChecklistItem, error) {
	return jazz.Insert[NewChecklistItem, ChecklistItem](ctx, db, "checklistItems", row, opts...)
}

// Update changes the fields patch sets of the row of `checklistItems` with id.
func (checklistItemsTable) Update(ctx context.Context, db jazz.Querier, id string, patch ChecklistItemPatch, opts ...jazz.WriteOption) error {
	return jazz.Update(ctx, db, "checklistItems", id, patch, opts...)
}

// Upsert writes row as the row of `checklistItems` with id: inserted, or updated if it exists.
func (checklistItemsTable) Upsert(ctx context.Context, db jazz.Querier, id string, row NewChecklistItem, opts ...jazz.WriteOption) (ChecklistItem, error) {
	return jazz.Upsert[NewChecklistItem, ChecklistItem](ctx, db, "checklistItems", id, row, opts...)
}

// Restore brings back the deleted row of `checklistItems` with id.
func (checklistItemsTable) Restore(ctx context.Context, db jazz.Querier, id string) (ChecklistItem, error) {
	return jazz.Restore[ChecklistItem](ctx, db, "checklistItems", id, nil)
}

// History starts a query of `checklistItems`'s history: every accepted version of its rows.
func (checklistItemsTable) History() *jazz.Query[jazz.Version[ChecklistItem]] {
	return jazz.History(jazz.NewQuery[ChecklistItem]("checklistItems"))
}

// CanRead advises whether the app's permissions allow reading the row with id in `checklistItems`.
func (checklistItemsTable) CanRead(ctx context.Context, db jazz.Querier, id string) (jazz.Advice, error) {
	return jazz.CanRead(ctx, db, "checklistItems", id)
}

// CanInsert advises whether the app's permissions allow inserting row in `checklistItems`.
func (checklistItemsTable) CanInsert(ctx context.Context, db jazz.Querier, row NewChecklistItem) (jazz.Advice, error) {
	return jazz.CanInsert(ctx, db, "checklistItems", row)
}

// CanUpdate advises whether the app's permissions allow applying patch to the row with id in `checklistItems`.
func (checklistItemsTable) CanUpdate(ctx context.Context, db jazz.Querier, id string, patch ChecklistItemPatch) (jazz.Advice, error) {
	return jazz.CanUpdate(ctx, db, "checklistItems", id, patch)
}

// CanDelete advises whether the app's permissions allow deleting the row with id in `checklistItems`.
func (checklistItemsTable) CanDelete(ctx context.Context, db jazz.Querier, id string) (jazz.Advice, error) {
	return jazz.CanDelete(ctx, db, "checklistItems", id)
}

// Delete deletes the row of `checklistItems` with id.
func (checklistItemsTable) Delete(ctx context.Context, db jazz.Querier, id string, opts ...jazz.WriteOption) error {
	return jazz.Delete(ctx, db, "checklistItems", id, opts...)
}

// Comment is a row of `comments`.
type Comment struct {
	ID       string `json:"id"`
	TaskID   string `json:"taskId"`
	AuthorID string `json:"authorId"`
	Body     string `json:"body"`
	Author   *Crew  `json:"author,omitempty"`
	Task     *Task  `json:"task,omitempty"`
}

// NewComment is a row to insert into `comments`: nil fields are null, or take their default.
type NewComment struct {
	TaskID   string `json:"taskId"`
	AuthorID string `json:"authorId"`
	Body     string `json:"body"`
}

// CommentPatch changes the fields of a row of `comments` that are set (jazz.Set).
type CommentPatch struct {
	TaskID   jazz.Patch[string]
	AuthorID jazz.Patch[string]
	Body     jazz.Patch[string]
}

// MarshalJSON writes the fields that are set.
func (p CommentPatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.TaskID.Entry("taskId"),
		p.AuthorID.Entry("authorId"),
		p.Body.Entry("body"),
	)
}

// commentsTable is the type of Comments: the columns of `comments`, to build conditions and orders from, and its relations, to include.
type commentsTable struct {
	ID       jazz.Column[Comment, string]
	TaskID   jazz.Column[Comment, string]
	AuthorID jazz.Column[Comment, string]
	Body     jazz.Column[Comment, string]
	Author   jazz.Relation[Comment, Crew]
	Task     jazz.Relation[Comment, Task]
}

// Comments is the table `comments`.
var Comments = commentsTable{
	ID:       jazz.NewColumn[Comment, string]("id"),
	TaskID:   jazz.NewColumn[Comment, string]("taskId"),
	AuthorID: jazz.NewColumn[Comment, string]("authorId"),
	Body:     jazz.NewColumn[Comment, string]("body"),
	Author:   jazz.NewRelation[Comment, Crew]("author"),
	Task:     jazz.NewRelation[Comment, Task]("task"),
}

// Query starts a query of `comments`.
func (commentsTable) Query() *jazz.Query[Comment] {
	return jazz.NewQuery[Comment]("comments")
}

// Insert inserts a row into `comments` and returns it, with its id.
func (commentsTable) Insert(ctx context.Context, db jazz.Querier, row NewComment, opts ...jazz.WriteOption) (Comment, error) {
	return jazz.Insert[NewComment, Comment](ctx, db, "comments", row, opts...)
}

// Update changes the fields patch sets of the row of `comments` with id.
func (commentsTable) Update(ctx context.Context, db jazz.Querier, id string, patch CommentPatch, opts ...jazz.WriteOption) error {
	return jazz.Update(ctx, db, "comments", id, patch, opts...)
}

// Upsert writes row as the row of `comments` with id: inserted, or updated if it exists.
func (commentsTable) Upsert(ctx context.Context, db jazz.Querier, id string, row NewComment, opts ...jazz.WriteOption) (Comment, error) {
	return jazz.Upsert[NewComment, Comment](ctx, db, "comments", id, row, opts...)
}

// Restore brings back the deleted row of `comments` with id.
func (commentsTable) Restore(ctx context.Context, db jazz.Querier, id string) (Comment, error) {
	return jazz.Restore[Comment](ctx, db, "comments", id, nil)
}

// History starts a query of `comments`'s history: every accepted version of its rows.
func (commentsTable) History() *jazz.Query[jazz.Version[Comment]] {
	return jazz.History(jazz.NewQuery[Comment]("comments"))
}

// CanRead advises whether the app's permissions allow reading the row with id in `comments`.
func (commentsTable) CanRead(ctx context.Context, db jazz.Querier, id string) (jazz.Advice, error) {
	return jazz.CanRead(ctx, db, "comments", id)
}

// CanInsert advises whether the app's permissions allow inserting row in `comments`.
func (commentsTable) CanInsert(ctx context.Context, db jazz.Querier, row NewComment) (jazz.Advice, error) {
	return jazz.CanInsert(ctx, db, "comments", row)
}

// CanUpdate advises whether the app's permissions allow applying patch to the row with id in `comments`.
func (commentsTable) CanUpdate(ctx context.Context, db jazz.Querier, id string, patch CommentPatch) (jazz.Advice, error) {
	return jazz.CanUpdate(ctx, db, "comments", id, patch)
}

// CanDelete advises whether the app's permissions allow deleting the row with id in `comments`.
func (commentsTable) CanDelete(ctx context.Context, db jazz.Querier, id string) (jazz.Advice, error) {
	return jazz.CanDelete(ctx, db, "comments", id)
}

// Delete deletes the row of `comments` with id.
func (commentsTable) Delete(ctx context.Context, db jazz.Querier, id string, opts ...jazz.WriteOption) error {
	return jazz.Delete(ctx, db, "comments", id, opts...)
}

// Crew is a row of `crew`.
type Crew struct {
	ID            string     `json:"id"`
	Account       string     `json:"account"`
	Name          string     `json:"name"`
	AssignedTasks []Task     `json:"assignedTasks,omitempty"`
	Memberships   []ShowCrew `json:"memberships,omitempty"`
}

// NewCrew is a row to insert into `crew`: nil fields are null, or take their default.
type NewCrew struct {
	Account string `json:"account"`
	Name    string `json:"name"`
}

// CrewPatch changes the fields of a row of `crew` that are set (jazz.Set).
type CrewPatch struct {
	Account jazz.Patch[string]
	Name    jazz.Patch[string]
}

// MarshalJSON writes the fields that are set.
func (p CrewPatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.Account.Entry("account"),
		p.Name.Entry("name"),
	)
}

// crewTable is the type of CrewTable: the columns of `crew`, to build conditions and orders from, and its relations, to include.
type crewTable struct {
	ID            jazz.Column[Crew, string]
	Account       jazz.Column[Crew, string]
	Name          jazz.Column[Crew, string]
	AssignedTasks jazz.Relation[Crew, Task]
	Memberships   jazz.Relation[Crew, ShowCrew]
}

// CrewTable is the table `crew`.
var CrewTable = crewTable{
	ID:            jazz.NewColumn[Crew, string]("id"),
	Account:       jazz.NewColumn[Crew, string]("account"),
	Name:          jazz.NewColumn[Crew, string]("name"),
	AssignedTasks: jazz.NewRelation[Crew, Task]("assignedTasks"),
	Memberships:   jazz.NewRelation[Crew, ShowCrew]("memberships"),
}

// Query starts a query of `crew`.
func (crewTable) Query() *jazz.Query[Crew] {
	return jazz.NewQuery[Crew]("crew")
}

// Insert inserts a row into `crew` and returns it, with its id.
func (crewTable) Insert(ctx context.Context, db jazz.Querier, row NewCrew, opts ...jazz.WriteOption) (Crew, error) {
	return jazz.Insert[NewCrew, Crew](ctx, db, "crew", row, opts...)
}

// Update changes the fields patch sets of the row of `crew` with id.
func (crewTable) Update(ctx context.Context, db jazz.Querier, id string, patch CrewPatch, opts ...jazz.WriteOption) error {
	return jazz.Update(ctx, db, "crew", id, patch, opts...)
}

// Upsert writes row as the row of `crew` with id: inserted, or updated if it exists.
func (crewTable) Upsert(ctx context.Context, db jazz.Querier, id string, row NewCrew, opts ...jazz.WriteOption) (Crew, error) {
	return jazz.Upsert[NewCrew, Crew](ctx, db, "crew", id, row, opts...)
}

// Restore brings back the deleted row of `crew` with id.
func (crewTable) Restore(ctx context.Context, db jazz.Querier, id string) (Crew, error) {
	return jazz.Restore[Crew](ctx, db, "crew", id, nil)
}

// History starts a query of `crew`'s history: every accepted version of its rows.
func (crewTable) History() *jazz.Query[jazz.Version[Crew]] {
	return jazz.History(jazz.NewQuery[Crew]("crew"))
}

// CanRead advises whether the app's permissions allow reading the row with id in `crew`.
func (crewTable) CanRead(ctx context.Context, db jazz.Querier, id string) (jazz.Advice, error) {
	return jazz.CanRead(ctx, db, "crew", id)
}

// CanInsert advises whether the app's permissions allow inserting row in `crew`.
func (crewTable) CanInsert(ctx context.Context, db jazz.Querier, row NewCrew) (jazz.Advice, error) {
	return jazz.CanInsert(ctx, db, "crew", row)
}

// CanUpdate advises whether the app's permissions allow applying patch to the row with id in `crew`.
func (crewTable) CanUpdate(ctx context.Context, db jazz.Querier, id string, patch CrewPatch) (jazz.Advice, error) {
	return jazz.CanUpdate(ctx, db, "crew", id, patch)
}

// CanDelete advises whether the app's permissions allow deleting the row with id in `crew`.
func (crewTable) CanDelete(ctx context.Context, db jazz.Querier, id string) (jazz.Advice, error) {
	return jazz.CanDelete(ctx, db, "crew", id)
}

// Delete deletes the row of `crew` with id.
func (crewTable) Delete(ctx context.Context, db jazz.Querier, id string, opts ...jazz.WriteOption) error {
	return jazz.Delete(ctx, db, "crew", id, opts...)
}

// ShowCrew is a row of `showCrew`.
type ShowCrew struct {
	ID         string       `json:"id"`
	ShowID     string       `json:"showId"`
	CrewID     string       `json:"crewId"`
	Account    string       `json:"account"`
	Role       ShowCrewRole `json:"role"`
	InviteCode *string      `json:"inviteCode"`
	Crew       *Crew        `json:"crew,omitempty"`
	Show       *Show        `json:"show,omitempty"`
}

// NewShowCrew is a row to insert into `showCrew`: nil fields are null, or take their default.
type NewShowCrew struct {
	ShowID     string       `json:"showId"`
	CrewID     string       `json:"crewId"`
	Account    string       `json:"account"`
	Role       ShowCrewRole `json:"role"`
	InviteCode *string      `json:"inviteCode,omitempty"`
}

// ShowCrewPatch changes the fields of a row of `showCrew` that are set (jazz.Set).
type ShowCrewPatch struct {
	ShowID     jazz.Patch[string]
	CrewID     jazz.Patch[string]
	Account    jazz.Patch[string]
	Role       jazz.Patch[ShowCrewRole]
	InviteCode jazz.Patch[*string]
}

// MarshalJSON writes the fields that are set.
func (p ShowCrewPatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.ShowID.Entry("showId"),
		p.CrewID.Entry("crewId"),
		p.Account.Entry("account"),
		p.Role.Entry("role"),
		p.InviteCode.Entry("inviteCode"),
	)
}

// showCrewTable is the type of ShowCrewTable: the columns of `showCrew`, to build conditions and orders from, and its relations, to include.
type showCrewTable struct {
	ID         jazz.Column[ShowCrew, string]
	ShowID     jazz.Column[ShowCrew, string]
	CrewID     jazz.Column[ShowCrew, string]
	Account    jazz.Column[ShowCrew, string]
	Role       jazz.Column[ShowCrew, ShowCrewRole]
	InviteCode jazz.Column[ShowCrew, string]
	Crew       jazz.Relation[ShowCrew, Crew]
	Show       jazz.Relation[ShowCrew, Show]
}

// ShowCrewTable is the table `showCrew`.
var ShowCrewTable = showCrewTable{
	ID:         jazz.NewColumn[ShowCrew, string]("id"),
	ShowID:     jazz.NewColumn[ShowCrew, string]("showId"),
	CrewID:     jazz.NewColumn[ShowCrew, string]("crewId"),
	Account:    jazz.NewColumn[ShowCrew, string]("account"),
	Role:       jazz.NewColumn[ShowCrew, ShowCrewRole]("role"),
	InviteCode: jazz.NewColumn[ShowCrew, string]("inviteCode"),
	Crew:       jazz.NewRelation[ShowCrew, Crew]("crew"),
	Show:       jazz.NewRelation[ShowCrew, Show]("show"),
}

// Query starts a query of `showCrew`.
func (showCrewTable) Query() *jazz.Query[ShowCrew] {
	return jazz.NewQuery[ShowCrew]("showCrew")
}

// Insert inserts a row into `showCrew` and returns it, with its id.
func (showCrewTable) Insert(ctx context.Context, db jazz.Querier, row NewShowCrew, opts ...jazz.WriteOption) (ShowCrew, error) {
	return jazz.Insert[NewShowCrew, ShowCrew](ctx, db, "showCrew", row, opts...)
}

// Update changes the fields patch sets of the row of `showCrew` with id.
func (showCrewTable) Update(ctx context.Context, db jazz.Querier, id string, patch ShowCrewPatch, opts ...jazz.WriteOption) error {
	return jazz.Update(ctx, db, "showCrew", id, patch, opts...)
}

// Upsert writes row as the row of `showCrew` with id: inserted, or updated if it exists.
func (showCrewTable) Upsert(ctx context.Context, db jazz.Querier, id string, row NewShowCrew, opts ...jazz.WriteOption) (ShowCrew, error) {
	return jazz.Upsert[NewShowCrew, ShowCrew](ctx, db, "showCrew", id, row, opts...)
}

// Restore brings back the deleted row of `showCrew` with id.
func (showCrewTable) Restore(ctx context.Context, db jazz.Querier, id string) (ShowCrew, error) {
	return jazz.Restore[ShowCrew](ctx, db, "showCrew", id, nil)
}

// History starts a query of `showCrew`'s history: every accepted version of its rows.
func (showCrewTable) History() *jazz.Query[jazz.Version[ShowCrew]] {
	return jazz.History(jazz.NewQuery[ShowCrew]("showCrew"))
}

// CanRead advises whether the app's permissions allow reading the row with id in `showCrew`.
func (showCrewTable) CanRead(ctx context.Context, db jazz.Querier, id string) (jazz.Advice, error) {
	return jazz.CanRead(ctx, db, "showCrew", id)
}

// CanInsert advises whether the app's permissions allow inserting row in `showCrew`.
func (showCrewTable) CanInsert(ctx context.Context, db jazz.Querier, row NewShowCrew) (jazz.Advice, error) {
	return jazz.CanInsert(ctx, db, "showCrew", row)
}

// CanUpdate advises whether the app's permissions allow applying patch to the row with id in `showCrew`.
func (showCrewTable) CanUpdate(ctx context.Context, db jazz.Querier, id string, patch ShowCrewPatch) (jazz.Advice, error) {
	return jazz.CanUpdate(ctx, db, "showCrew", id, patch)
}

// CanDelete advises whether the app's permissions allow deleting the row with id in `showCrew`.
func (showCrewTable) CanDelete(ctx context.Context, db jazz.Querier, id string) (jazz.Advice, error) {
	return jazz.CanDelete(ctx, db, "showCrew", id)
}

// Delete deletes the row of `showCrew` with id.
func (showCrewTable) Delete(ctx context.Context, db jazz.Querier, id string, opts ...jazz.WriteOption) error {
	return jazz.Delete(ctx, db, "showCrew", id, opts...)
}

// ShowInvite is a row of `showInvites`.
type ShowInvite struct {
	ID     string `json:"id"`
	ShowID string `json:"showId"`
	Code   string `json:"code"`
	Show   *Show  `json:"show,omitempty"`
}

// NewShowInvite is a row to insert into `showInvites`: nil fields are null, or take their default.
type NewShowInvite struct {
	ShowID string `json:"showId"`
	Code   string `json:"code"`
}

// ShowInvitePatch changes the fields of a row of `showInvites` that are set (jazz.Set).
type ShowInvitePatch struct {
	ShowID jazz.Patch[string]
	Code   jazz.Patch[string]
}

// MarshalJSON writes the fields that are set.
func (p ShowInvitePatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.ShowID.Entry("showId"),
		p.Code.Entry("code"),
	)
}

// showInvitesTable is the type of ShowInvites: the columns of `showInvites`, to build conditions and orders from, and its relations, to include.
type showInvitesTable struct {
	ID     jazz.Column[ShowInvite, string]
	ShowID jazz.Column[ShowInvite, string]
	Code   jazz.Column[ShowInvite, string]
	Show   jazz.Relation[ShowInvite, Show]
}

// ShowInvites is the table `showInvites`.
var ShowInvites = showInvitesTable{
	ID:     jazz.NewColumn[ShowInvite, string]("id"),
	ShowID: jazz.NewColumn[ShowInvite, string]("showId"),
	Code:   jazz.NewColumn[ShowInvite, string]("code"),
	Show:   jazz.NewRelation[ShowInvite, Show]("show"),
}

// Query starts a query of `showInvites`.
func (showInvitesTable) Query() *jazz.Query[ShowInvite] {
	return jazz.NewQuery[ShowInvite]("showInvites")
}

// Insert inserts a row into `showInvites` and returns it, with its id.
func (showInvitesTable) Insert(ctx context.Context, db jazz.Querier, row NewShowInvite, opts ...jazz.WriteOption) (ShowInvite, error) {
	return jazz.Insert[NewShowInvite, ShowInvite](ctx, db, "showInvites", row, opts...)
}

// Update changes the fields patch sets of the row of `showInvites` with id.
func (showInvitesTable) Update(ctx context.Context, db jazz.Querier, id string, patch ShowInvitePatch, opts ...jazz.WriteOption) error {
	return jazz.Update(ctx, db, "showInvites", id, patch, opts...)
}

// Upsert writes row as the row of `showInvites` with id: inserted, or updated if it exists.
func (showInvitesTable) Upsert(ctx context.Context, db jazz.Querier, id string, row NewShowInvite, opts ...jazz.WriteOption) (ShowInvite, error) {
	return jazz.Upsert[NewShowInvite, ShowInvite](ctx, db, "showInvites", id, row, opts...)
}

// Restore brings back the deleted row of `showInvites` with id.
func (showInvitesTable) Restore(ctx context.Context, db jazz.Querier, id string) (ShowInvite, error) {
	return jazz.Restore[ShowInvite](ctx, db, "showInvites", id, nil)
}

// History starts a query of `showInvites`'s history: every accepted version of its rows.
func (showInvitesTable) History() *jazz.Query[jazz.Version[ShowInvite]] {
	return jazz.History(jazz.NewQuery[ShowInvite]("showInvites"))
}

// CanRead advises whether the app's permissions allow reading the row with id in `showInvites`.
func (showInvitesTable) CanRead(ctx context.Context, db jazz.Querier, id string) (jazz.Advice, error) {
	return jazz.CanRead(ctx, db, "showInvites", id)
}

// CanInsert advises whether the app's permissions allow inserting row in `showInvites`.
func (showInvitesTable) CanInsert(ctx context.Context, db jazz.Querier, row NewShowInvite) (jazz.Advice, error) {
	return jazz.CanInsert(ctx, db, "showInvites", row)
}

// CanUpdate advises whether the app's permissions allow applying patch to the row with id in `showInvites`.
func (showInvitesTable) CanUpdate(ctx context.Context, db jazz.Querier, id string, patch ShowInvitePatch) (jazz.Advice, error) {
	return jazz.CanUpdate(ctx, db, "showInvites", id, patch)
}

// CanDelete advises whether the app's permissions allow deleting the row with id in `showInvites`.
func (showInvitesTable) CanDelete(ctx context.Context, db jazz.Querier, id string) (jazz.Advice, error) {
	return jazz.CanDelete(ctx, db, "showInvites", id)
}

// Delete deletes the row of `showInvites` with id.
func (showInvitesTable) Delete(ctx context.Context, db jazz.Querier, id string, opts ...jazz.WriteOption) error {
	return jazz.Delete(ctx, db, "showInvites", id, opts...)
}

// Show is a row of `shows`.
type Show struct {
	ID           string       `json:"id"`
	Name         string       `json:"name"`
	Venue        string       `json:"venue"`
	Date         string       `json:"date"`
	Doors        string       `json:"doors"`
	ChiefAccount string       `json:"chiefAccount"`
	Activity     []Activity   `json:"activity,omitempty"`
	Crew         []ShowCrew   `json:"crew,omitempty"`
	Invites      []ShowInvite `json:"invites,omitempty"`
	Tasks        []Task       `json:"tasks,omitempty"`
}

// NewShow is a row to insert into `shows`: nil fields are null, or take their default.
type NewShow struct {
	Name         string `json:"name"`
	Venue        string `json:"venue"`
	Date         string `json:"date"`
	Doors        string `json:"doors"`
	ChiefAccount string `json:"chiefAccount"`
}

// ShowPatch changes the fields of a row of `shows` that are set (jazz.Set).
type ShowPatch struct {
	Name         jazz.Patch[string]
	Venue        jazz.Patch[string]
	Date         jazz.Patch[string]
	Doors        jazz.Patch[string]
	ChiefAccount jazz.Patch[string]
}

// MarshalJSON writes the fields that are set.
func (p ShowPatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.Name.Entry("name"),
		p.Venue.Entry("venue"),
		p.Date.Entry("date"),
		p.Doors.Entry("doors"),
		p.ChiefAccount.Entry("chiefAccount"),
	)
}

// showsTable is the type of Shows: the columns of `shows`, to build conditions and orders from, and its relations, to include.
type showsTable struct {
	ID           jazz.Column[Show, string]
	Name         jazz.Column[Show, string]
	Venue        jazz.Column[Show, string]
	Date         jazz.Column[Show, string]
	Doors        jazz.Column[Show, string]
	ChiefAccount jazz.Column[Show, string]
	Activity     jazz.Relation[Show, Activity]
	Crew         jazz.Relation[Show, ShowCrew]
	Invites      jazz.Relation[Show, ShowInvite]
	Tasks        jazz.Relation[Show, Task]
}

// Shows is the table `shows`.
var Shows = showsTable{
	ID:           jazz.NewColumn[Show, string]("id"),
	Name:         jazz.NewColumn[Show, string]("name"),
	Venue:        jazz.NewColumn[Show, string]("venue"),
	Date:         jazz.NewColumn[Show, string]("date"),
	Doors:        jazz.NewColumn[Show, string]("doors"),
	ChiefAccount: jazz.NewColumn[Show, string]("chiefAccount"),
	Activity:     jazz.NewRelation[Show, Activity]("activity"),
	Crew:         jazz.NewRelation[Show, ShowCrew]("crew"),
	Invites:      jazz.NewRelation[Show, ShowInvite]("invites"),
	Tasks:        jazz.NewRelation[Show, Task]("tasks"),
}

// Query starts a query of `shows`.
func (showsTable) Query() *jazz.Query[Show] {
	return jazz.NewQuery[Show]("shows")
}

// Insert inserts a row into `shows` and returns it, with its id.
func (showsTable) Insert(ctx context.Context, db jazz.Querier, row NewShow, opts ...jazz.WriteOption) (Show, error) {
	return jazz.Insert[NewShow, Show](ctx, db, "shows", row, opts...)
}

// Update changes the fields patch sets of the row of `shows` with id.
func (showsTable) Update(ctx context.Context, db jazz.Querier, id string, patch ShowPatch, opts ...jazz.WriteOption) error {
	return jazz.Update(ctx, db, "shows", id, patch, opts...)
}

// Upsert writes row as the row of `shows` with id: inserted, or updated if it exists.
func (showsTable) Upsert(ctx context.Context, db jazz.Querier, id string, row NewShow, opts ...jazz.WriteOption) (Show, error) {
	return jazz.Upsert[NewShow, Show](ctx, db, "shows", id, row, opts...)
}

// Restore brings back the deleted row of `shows` with id.
func (showsTable) Restore(ctx context.Context, db jazz.Querier, id string) (Show, error) {
	return jazz.Restore[Show](ctx, db, "shows", id, nil)
}

// History starts a query of `shows`'s history: every accepted version of its rows.
func (showsTable) History() *jazz.Query[jazz.Version[Show]] {
	return jazz.History(jazz.NewQuery[Show]("shows"))
}

// CanRead advises whether the app's permissions allow reading the row with id in `shows`.
func (showsTable) CanRead(ctx context.Context, db jazz.Querier, id string) (jazz.Advice, error) {
	return jazz.CanRead(ctx, db, "shows", id)
}

// CanInsert advises whether the app's permissions allow inserting row in `shows`.
func (showsTable) CanInsert(ctx context.Context, db jazz.Querier, row NewShow) (jazz.Advice, error) {
	return jazz.CanInsert(ctx, db, "shows", row)
}

// CanUpdate advises whether the app's permissions allow applying patch to the row with id in `shows`.
func (showsTable) CanUpdate(ctx context.Context, db jazz.Querier, id string, patch ShowPatch) (jazz.Advice, error) {
	return jazz.CanUpdate(ctx, db, "shows", id, patch)
}

// CanDelete advises whether the app's permissions allow deleting the row with id in `shows`.
func (showsTable) CanDelete(ctx context.Context, db jazz.Querier, id string) (jazz.Advice, error) {
	return jazz.CanDelete(ctx, db, "shows", id)
}

// Delete deletes the row of `shows` with id.
func (showsTable) Delete(ctx context.Context, db jazz.Querier, id string, opts ...jazz.WriteOption) error {
	return jazz.Delete(ctx, db, "shows", id, opts...)
}

// Task is a row of `tasks`.
type Task struct {
	ID         string     `json:"id"`
	ShowID     string     `json:"showId"`
	Title      string     `json:"title"`
	Status     TaskStatus `json:"status"`
	AssigneeID *string    `json:"assigneeId"`
	Notes      *string    `json:"notes"`
	Rank       float64    `json:"rank"`
	Activity   []Activity `json:"activity,omitempty"`
	Assignee   *Crew      `json:"assignee,omitempty"`
	Comments   []Comment  `json:"comments,omitempty"`
	Show       *Show      `json:"show,omitempty"`
}

// NewTask is a row to insert into `tasks`: nil fields are null, or take their default.
type NewTask struct {
	ShowID     string     `json:"showId"`
	Title      string     `json:"title"`
	Status     TaskStatus `json:"status"`
	AssigneeID *string    `json:"assigneeId,omitempty"`
	Notes      *string    `json:"notes,omitempty"`
	Rank       float64    `json:"rank"`
}

// TaskPatch changes the fields of a row of `tasks` that are set (jazz.Set).
type TaskPatch struct {
	ShowID     jazz.Patch[string]
	Title      jazz.Patch[string]
	Status     jazz.Patch[TaskStatus]
	AssigneeID jazz.Patch[*string]
	Notes      jazz.Patch[*string]
	Rank       jazz.Patch[float64]
}

// MarshalJSON writes the fields that are set.
func (p TaskPatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.ShowID.Entry("showId"),
		p.Title.Entry("title"),
		p.Status.Entry("status"),
		p.AssigneeID.Entry("assigneeId"),
		p.Notes.Entry("notes"),
		p.Rank.Entry("rank"),
	)
}

// tasksTable is the type of Tasks: the columns of `tasks`, to build conditions and orders from, and its relations, to include.
type tasksTable struct {
	ID         jazz.Column[Task, string]
	ShowID     jazz.Column[Task, string]
	Title      jazz.Column[Task, string]
	Status     jazz.Column[Task, TaskStatus]
	AssigneeID jazz.Column[Task, string]
	Notes      jazz.Column[Task, string]
	Rank       jazz.Column[Task, float64]
	Activity   jazz.Relation[Task, Activity]
	Assignee   jazz.Relation[Task, Crew]
	Comments   jazz.Relation[Task, Comment]
	Show       jazz.Relation[Task, Show]
}

// Tasks is the table `tasks`.
var Tasks = tasksTable{
	ID:         jazz.NewColumn[Task, string]("id"),
	ShowID:     jazz.NewColumn[Task, string]("showId"),
	Title:      jazz.NewColumn[Task, string]("title"),
	Status:     jazz.NewColumn[Task, TaskStatus]("status"),
	AssigneeID: jazz.NewColumn[Task, string]("assigneeId"),
	Notes:      jazz.NewColumn[Task, string]("notes"),
	Rank:       jazz.NewColumn[Task, float64]("rank"),
	Activity:   jazz.NewRelation[Task, Activity]("activity"),
	Assignee:   jazz.NewRelation[Task, Crew]("assignee"),
	Comments:   jazz.NewRelation[Task, Comment]("comments"),
	Show:       jazz.NewRelation[Task, Show]("show"),
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

// EditNotes edits `tasks.notes`, a mergeable text: positions are UTF-8 byte offsets, and
// the edits merge with others made meanwhile.
func (tasksTable) EditNotes(ctx context.Context, db jazz.Querier, id string, edits ...jazz.TextEdit) error {
	return jazz.EditText(ctx, db, "tasks", id, "notes", edits...)
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
