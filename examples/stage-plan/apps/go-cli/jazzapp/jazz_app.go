// Code generated from jazz.app.json by `jazz-tools generate`. DO NOT EDIT.
// Bindings format 1. Regenerate it after changing jazz.app.json.

// Package jazzapp holds the typed bindings of this app's jazz.app.json.
package jazzapp

import (
	"context"

	jazz "github.com/garden-co/jazz/packages/jazz-go"
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

// activityTable is the type of ActivityTable: the columns of `activity`, to build conditions and orders from.
type activityTable struct {
	ID      jazz.Column[Activity, string]
	ShowID  jazz.Column[Activity, string]
	TaskID  jazz.Column[Activity, string]
	ActorID jazz.Column[Activity, string]
	Kind    jazz.Column[Activity, ActivityKind]
	Detail  jazz.Column[Activity, string]
}

// ActivityTable is the table `activity`.
var ActivityTable = activityTable{
	ID:      jazz.NewColumn[Activity, string]("id"),
	ShowID:  jazz.NewColumn[Activity, string]("showId"),
	TaskID:  jazz.NewColumn[Activity, string]("taskId"),
	ActorID: jazz.NewColumn[Activity, string]("actorId"),
	Kind:    jazz.NewColumn[Activity, ActivityKind]("kind"),
	Detail:  jazz.NewColumn[Activity, string]("detail"),
}

// Query starts a query of `activity`.
func (activityTable) Query() *jazz.Query[Activity] {
	return jazz.NewQuery[Activity]("activity")
}

// Insert inserts a row into `activity` and returns it, with its id.
func (activityTable) Insert(ctx context.Context, db *jazz.DB, row NewActivity) (Activity, error) {
	return jazz.Insert[NewActivity, Activity](ctx, db, "activity", row)
}

// Update changes the fields patch sets of the row of `activity` with id.
func (activityTable) Update(ctx context.Context, db *jazz.DB, id string, patch ActivityPatch) error {
	return db.Update(ctx, "activity", id, patch)
}

// Delete deletes the row of `activity` with id.
func (activityTable) Delete(ctx context.Context, db *jazz.DB, id string) error {
	return db.Delete(ctx, "activity", id)
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

// checklistItemsTable is the type of ChecklistItems: the columns of `checklistItems`, to build conditions and orders from.
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
func (checklistItemsTable) Insert(ctx context.Context, db *jazz.DB, row NewChecklistItem) (ChecklistItem, error) {
	return jazz.Insert[NewChecklistItem, ChecklistItem](ctx, db, "checklistItems", row)
}

// Update changes the fields patch sets of the row of `checklistItems` with id.
func (checklistItemsTable) Update(ctx context.Context, db *jazz.DB, id string, patch ChecklistItemPatch) error {
	return db.Update(ctx, "checklistItems", id, patch)
}

// Delete deletes the row of `checklistItems` with id.
func (checklistItemsTable) Delete(ctx context.Context, db *jazz.DB, id string) error {
	return db.Delete(ctx, "checklistItems", id)
}

// Comment is a row of `comments`.
type Comment struct {
	ID       string `json:"id"`
	TaskID   string `json:"taskId"`
	AuthorID string `json:"authorId"`
	Body     string `json:"body"`
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

// commentsTable is the type of Comments: the columns of `comments`, to build conditions and orders from.
type commentsTable struct {
	ID       jazz.Column[Comment, string]
	TaskID   jazz.Column[Comment, string]
	AuthorID jazz.Column[Comment, string]
	Body     jazz.Column[Comment, string]
}

// Comments is the table `comments`.
var Comments = commentsTable{
	ID:       jazz.NewColumn[Comment, string]("id"),
	TaskID:   jazz.NewColumn[Comment, string]("taskId"),
	AuthorID: jazz.NewColumn[Comment, string]("authorId"),
	Body:     jazz.NewColumn[Comment, string]("body"),
}

// Query starts a query of `comments`.
func (commentsTable) Query() *jazz.Query[Comment] {
	return jazz.NewQuery[Comment]("comments")
}

// Insert inserts a row into `comments` and returns it, with its id.
func (commentsTable) Insert(ctx context.Context, db *jazz.DB, row NewComment) (Comment, error) {
	return jazz.Insert[NewComment, Comment](ctx, db, "comments", row)
}

// Update changes the fields patch sets of the row of `comments` with id.
func (commentsTable) Update(ctx context.Context, db *jazz.DB, id string, patch CommentPatch) error {
	return db.Update(ctx, "comments", id, patch)
}

// Delete deletes the row of `comments` with id.
func (commentsTable) Delete(ctx context.Context, db *jazz.DB, id string) error {
	return db.Delete(ctx, "comments", id)
}

// Crew is a row of `crew`.
type Crew struct {
	ID      string `json:"id"`
	Account string `json:"account"`
	Name    string `json:"name"`
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

// crewTable is the type of CrewTable: the columns of `crew`, to build conditions and orders from.
type crewTable struct {
	ID      jazz.Column[Crew, string]
	Account jazz.Column[Crew, string]
	Name    jazz.Column[Crew, string]
}

// CrewTable is the table `crew`.
var CrewTable = crewTable{
	ID:      jazz.NewColumn[Crew, string]("id"),
	Account: jazz.NewColumn[Crew, string]("account"),
	Name:    jazz.NewColumn[Crew, string]("name"),
}

// Query starts a query of `crew`.
func (crewTable) Query() *jazz.Query[Crew] {
	return jazz.NewQuery[Crew]("crew")
}

// Insert inserts a row into `crew` and returns it, with its id.
func (crewTable) Insert(ctx context.Context, db *jazz.DB, row NewCrew) (Crew, error) {
	return jazz.Insert[NewCrew, Crew](ctx, db, "crew", row)
}

// Update changes the fields patch sets of the row of `crew` with id.
func (crewTable) Update(ctx context.Context, db *jazz.DB, id string, patch CrewPatch) error {
	return db.Update(ctx, "crew", id, patch)
}

// Delete deletes the row of `crew` with id.
func (crewTable) Delete(ctx context.Context, db *jazz.DB, id string) error {
	return db.Delete(ctx, "crew", id)
}

// ShowCrew is a row of `showCrew`.
type ShowCrew struct {
	ID         string       `json:"id"`
	ShowID     string       `json:"showId"`
	CrewID     string       `json:"crewId"`
	Account    string       `json:"account"`
	Role       ShowCrewRole `json:"role"`
	InviteCode *string      `json:"inviteCode"`
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

// showCrewTable is the type of ShowCrewTable: the columns of `showCrew`, to build conditions and orders from.
type showCrewTable struct {
	ID         jazz.Column[ShowCrew, string]
	ShowID     jazz.Column[ShowCrew, string]
	CrewID     jazz.Column[ShowCrew, string]
	Account    jazz.Column[ShowCrew, string]
	Role       jazz.Column[ShowCrew, ShowCrewRole]
	InviteCode jazz.Column[ShowCrew, string]
}

// ShowCrewTable is the table `showCrew`.
var ShowCrewTable = showCrewTable{
	ID:         jazz.NewColumn[ShowCrew, string]("id"),
	ShowID:     jazz.NewColumn[ShowCrew, string]("showId"),
	CrewID:     jazz.NewColumn[ShowCrew, string]("crewId"),
	Account:    jazz.NewColumn[ShowCrew, string]("account"),
	Role:       jazz.NewColumn[ShowCrew, ShowCrewRole]("role"),
	InviteCode: jazz.NewColumn[ShowCrew, string]("inviteCode"),
}

// Query starts a query of `showCrew`.
func (showCrewTable) Query() *jazz.Query[ShowCrew] {
	return jazz.NewQuery[ShowCrew]("showCrew")
}

// Insert inserts a row into `showCrew` and returns it, with its id.
func (showCrewTable) Insert(ctx context.Context, db *jazz.DB, row NewShowCrew) (ShowCrew, error) {
	return jazz.Insert[NewShowCrew, ShowCrew](ctx, db, "showCrew", row)
}

// Update changes the fields patch sets of the row of `showCrew` with id.
func (showCrewTable) Update(ctx context.Context, db *jazz.DB, id string, patch ShowCrewPatch) error {
	return db.Update(ctx, "showCrew", id, patch)
}

// Delete deletes the row of `showCrew` with id.
func (showCrewTable) Delete(ctx context.Context, db *jazz.DB, id string) error {
	return db.Delete(ctx, "showCrew", id)
}

// ShowInvite is a row of `showInvites`.
type ShowInvite struct {
	ID     string `json:"id"`
	ShowID string `json:"showId"`
	Code   string `json:"code"`
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

// showInvitesTable is the type of ShowInvites: the columns of `showInvites`, to build conditions and orders from.
type showInvitesTable struct {
	ID     jazz.Column[ShowInvite, string]
	ShowID jazz.Column[ShowInvite, string]
	Code   jazz.Column[ShowInvite, string]
}

// ShowInvites is the table `showInvites`.
var ShowInvites = showInvitesTable{
	ID:     jazz.NewColumn[ShowInvite, string]("id"),
	ShowID: jazz.NewColumn[ShowInvite, string]("showId"),
	Code:   jazz.NewColumn[ShowInvite, string]("code"),
}

// Query starts a query of `showInvites`.
func (showInvitesTable) Query() *jazz.Query[ShowInvite] {
	return jazz.NewQuery[ShowInvite]("showInvites")
}

// Insert inserts a row into `showInvites` and returns it, with its id.
func (showInvitesTable) Insert(ctx context.Context, db *jazz.DB, row NewShowInvite) (ShowInvite, error) {
	return jazz.Insert[NewShowInvite, ShowInvite](ctx, db, "showInvites", row)
}

// Update changes the fields patch sets of the row of `showInvites` with id.
func (showInvitesTable) Update(ctx context.Context, db *jazz.DB, id string, patch ShowInvitePatch) error {
	return db.Update(ctx, "showInvites", id, patch)
}

// Delete deletes the row of `showInvites` with id.
func (showInvitesTable) Delete(ctx context.Context, db *jazz.DB, id string) error {
	return db.Delete(ctx, "showInvites", id)
}

// Show is a row of `shows`.
type Show struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Venue        string `json:"venue"`
	Date         string `json:"date"`
	Doors        string `json:"doors"`
	ChiefAccount string `json:"chiefAccount"`
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

// showsTable is the type of Shows: the columns of `shows`, to build conditions and orders from.
type showsTable struct {
	ID           jazz.Column[Show, string]
	Name         jazz.Column[Show, string]
	Venue        jazz.Column[Show, string]
	Date         jazz.Column[Show, string]
	Doors        jazz.Column[Show, string]
	ChiefAccount jazz.Column[Show, string]
}

// Shows is the table `shows`.
var Shows = showsTable{
	ID:           jazz.NewColumn[Show, string]("id"),
	Name:         jazz.NewColumn[Show, string]("name"),
	Venue:        jazz.NewColumn[Show, string]("venue"),
	Date:         jazz.NewColumn[Show, string]("date"),
	Doors:        jazz.NewColumn[Show, string]("doors"),
	ChiefAccount: jazz.NewColumn[Show, string]("chiefAccount"),
}

// Query starts a query of `shows`.
func (showsTable) Query() *jazz.Query[Show] {
	return jazz.NewQuery[Show]("shows")
}

// Insert inserts a row into `shows` and returns it, with its id.
func (showsTable) Insert(ctx context.Context, db *jazz.DB, row NewShow) (Show, error) {
	return jazz.Insert[NewShow, Show](ctx, db, "shows", row)
}

// Update changes the fields patch sets of the row of `shows` with id.
func (showsTable) Update(ctx context.Context, db *jazz.DB, id string, patch ShowPatch) error {
	return db.Update(ctx, "shows", id, patch)
}

// Delete deletes the row of `shows` with id.
func (showsTable) Delete(ctx context.Context, db *jazz.DB, id string) error {
	return db.Delete(ctx, "shows", id)
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

// tasksTable is the type of Tasks: the columns of `tasks`, to build conditions and orders from.
type tasksTable struct {
	ID         jazz.Column[Task, string]
	ShowID     jazz.Column[Task, string]
	Title      jazz.Column[Task, string]
	Status     jazz.Column[Task, TaskStatus]
	AssigneeID jazz.Column[Task, string]
	Notes      jazz.Column[Task, string]
	Rank       jazz.Column[Task, float64]
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
}

// Query starts a query of `tasks`.
func (tasksTable) Query() *jazz.Query[Task] {
	return jazz.NewQuery[Task]("tasks")
}

// Insert inserts a row into `tasks` and returns it, with its id.
func (tasksTable) Insert(ctx context.Context, db *jazz.DB, row NewTask) (Task, error) {
	return jazz.Insert[NewTask, Task](ctx, db, "tasks", row)
}

// Update changes the fields patch sets of the row of `tasks` with id.
func (tasksTable) Update(ctx context.Context, db *jazz.DB, id string, patch TaskPatch) error {
	return db.Update(ctx, "tasks", id, patch)
}

// Delete deletes the row of `tasks` with id.
func (tasksTable) Delete(ctx context.Context, db *jazz.DB, id string) error {
	return db.Delete(ctx, "tasks", id)
}
