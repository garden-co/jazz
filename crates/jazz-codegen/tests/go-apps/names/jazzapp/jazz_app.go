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
const Source = "sha256:6574692702ca7280a94a001619d8d285067f14dfe04da33522579186a303c14d"

// App is the jazz.app.json these bindings were generated from.
const App = `{
  "format": 1,
  "tables": {
    "strings": {
      "columns": { "source": "string", "app": "string", "open": "boolean", "string": "string" }
    },
    "errors": {
      "columns": {
        "error": "string?",
        "json": { "json": true, "optional": true },
        "context": "int",
        "level": { "enum": ["1", "2", "in-progress"] }
      }
    },
    "contexts": {
      "columns": {
        "at": "timestamp",
        "big": "bigint",
        "x": "float",
        "tags": { "array": "string" },
        "blob": "bytes?"
      }
    },
    "activity": { "columns": { "kind": "string" } }
  }
}
`

// Open opens this app: a client, or a backend when cfg.Permissions is set.
func Open(ctx context.Context, cfg jazz.Config) (*jazz.DB, error) {
	cfg.App = App
	return jazz.Open(ctx, cfg)
}

// ErrorLevel is a value of column `errors.level`.
type ErrorLevel string

// The values of ErrorLevel.
const (
	ErrorLevel1          ErrorLevel = "1"
	ErrorLevel2          ErrorLevel = "2"
	ErrorLevelInProgress ErrorLevel = "in-progress"
)

// Activity is a row of `activity`.
type Activity struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
}

// NewActivity is a row to insert into `activity`: nil fields are null, or take their default.
type NewActivity struct {
	Kind string `json:"kind"`
}

// ActivityPatch changes the fields of a row of `activity` that are set (jazz.Set).
type ActivityPatch struct {
	Kind jazz.Patch[string]
}

// MarshalJSON writes the fields that are set.
func (p ActivityPatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.Kind.Entry("kind"),
	)
}

// activityTable is the type of ActivityTable: the columns of `activity`, to build conditions and orders from, and its relations, to include.
type activityTable struct {
	ID   jazz.Column[Activity, string]
	Kind jazz.Column[Activity, string]
}

// ActivityTable is the table `activity`.
var ActivityTable = activityTable{
	ID:   jazz.NewColumn[Activity, string]("id"),
	Kind: jazz.NewColumn[Activity, string]("kind"),
}

// Query starts a query of `activity`.
func (activityTable) Query() *jazz.Query[Activity] {
	return jazz.NewQuery[Activity]("activity")
}

// Insert inserts a row into `activity` and returns it, with its id.
func (activityTable) Insert(ctx context.Context, db jazz.Querier, row NewActivity) (Activity, error) {
	return jazz.Insert[NewActivity, Activity](ctx, db, "activity", row)
}

// Update changes the fields patch sets of the row of `activity` with id.
func (activityTable) Update(ctx context.Context, db jazz.Querier, id string, patch ActivityPatch) error {
	return jazz.Update(ctx, db, "activity", id, patch)
}

// Delete deletes the row of `activity` with id.
func (activityTable) Delete(ctx context.Context, db jazz.Querier, id string) error {
	return jazz.Delete(ctx, db, "activity", id)
}

// Context is a row of `contexts`.
type Context struct {
	ID   string         `json:"id"`
	At   jazz.Timestamp `json:"at"`
	Big  jazz.BigInt    `json:"big"`
	X    float64        `json:"x"`
	Tags []string       `json:"tags"`
	Blob *[]byte        `json:"blob"`
}

// NewContext is a row to insert into `contexts`: nil fields are null, or take their default.
type NewContext struct {
	At   jazz.Timestamp `json:"at"`
	Big  jazz.BigInt    `json:"big"`
	X    float64        `json:"x"`
	Tags []string       `json:"tags"`
	Blob *[]byte        `json:"blob,omitempty"`
}

// ContextPatch changes the fields of a row of `contexts` that are set (jazz.Set).
type ContextPatch struct {
	At   jazz.Patch[jazz.Timestamp]
	Big  jazz.Patch[jazz.BigInt]
	X    jazz.Patch[float64]
	Tags jazz.Patch[[]string]
	Blob jazz.Patch[*[]byte]
}

// MarshalJSON writes the fields that are set.
func (p ContextPatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.At.Entry("at"),
		p.Big.Entry("big"),
		p.X.Entry("x"),
		p.Tags.Entry("tags"),
		p.Blob.Entry("blob"),
	)
}

// contextsTable is the type of Contexts: the columns of `contexts`, to build conditions and orders from, and its relations, to include.
type contextsTable struct {
	ID   jazz.Column[Context, string]
	At   jazz.Column[Context, jazz.Timestamp]
	Big  jazz.Column[Context, jazz.BigInt]
	X    jazz.Column[Context, float64]
	Tags jazz.Column[Context, []string]
	Blob jazz.Column[Context, []byte]
}

// Contexts is the table `contexts`.
var Contexts = contextsTable{
	ID:   jazz.NewColumn[Context, string]("id"),
	At:   jazz.NewColumn[Context, jazz.Timestamp]("at"),
	Big:  jazz.NewColumn[Context, jazz.BigInt]("big"),
	X:    jazz.NewColumn[Context, float64]("x"),
	Tags: jazz.NewColumn[Context, []string]("tags"),
	Blob: jazz.NewColumn[Context, []byte]("blob"),
}

// Query starts a query of `contexts`.
func (contextsTable) Query() *jazz.Query[Context] {
	return jazz.NewQuery[Context]("contexts")
}

// Insert inserts a row into `contexts` and returns it, with its id.
func (contextsTable) Insert(ctx context.Context, db jazz.Querier, row NewContext) (Context, error) {
	return jazz.Insert[NewContext, Context](ctx, db, "contexts", row)
}

// Update changes the fields patch sets of the row of `contexts` with id.
func (contextsTable) Update(ctx context.Context, db jazz.Querier, id string, patch ContextPatch) error {
	return jazz.Update(ctx, db, "contexts", id, patch)
}

// Delete deletes the row of `contexts` with id.
func (contextsTable) Delete(ctx context.Context, db jazz.Querier, id string) error {
	return jazz.Delete(ctx, db, "contexts", id)
}

// Error is a row of `errors`.
type Error struct {
	ID      string           `json:"id"`
	Error   *string          `json:"error"`
	JSON    *json.RawMessage `json:"json"`
	Context int32            `json:"context"`
	Level   ErrorLevel       `json:"level"`
}

// NewError is a row to insert into `errors`: nil fields are null, or take their default.
type NewError struct {
	Error   *string          `json:"error,omitempty"`
	JSON    *json.RawMessage `json:"json,omitempty"`
	Context int32            `json:"context"`
	Level   ErrorLevel       `json:"level"`
}

// ErrorPatch changes the fields of a row of `errors` that are set (jazz.Set).
type ErrorPatch struct {
	Error   jazz.Patch[*string]
	JSON    jazz.Patch[*json.RawMessage]
	Context jazz.Patch[int32]
	Level   jazz.Patch[ErrorLevel]
}

// MarshalJSON writes the fields that are set.
func (p ErrorPatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.Error.Entry("error"),
		p.JSON.Entry("json"),
		p.Context.Entry("context"),
		p.Level.Entry("level"),
	)
}

// errorsTable is the type of Errors: the columns of `errors`, to build conditions and orders from, and its relations, to include.
type errorsTable struct {
	ID      jazz.Column[Error, string]
	Error   jazz.Column[Error, string]
	JSON    jazz.Column[Error, json.RawMessage]
	Context jazz.Column[Error, int32]
	Level   jazz.Column[Error, ErrorLevel]
}

// Errors is the table `errors`.
var Errors = errorsTable{
	ID:      jazz.NewColumn[Error, string]("id"),
	Error:   jazz.NewColumn[Error, string]("error"),
	JSON:    jazz.NewColumn[Error, json.RawMessage]("json"),
	Context: jazz.NewColumn[Error, int32]("context"),
	Level:   jazz.NewColumn[Error, ErrorLevel]("level"),
}

// Query starts a query of `errors`.
func (errorsTable) Query() *jazz.Query[Error] {
	return jazz.NewQuery[Error]("errors")
}

// Insert inserts a row into `errors` and returns it, with its id.
func (errorsTable) Insert(ctx context.Context, db jazz.Querier, row NewError) (Error, error) {
	return jazz.Insert[NewError, Error](ctx, db, "errors", row)
}

// Update changes the fields patch sets of the row of `errors` with id.
func (errorsTable) Update(ctx context.Context, db jazz.Querier, id string, patch ErrorPatch) error {
	return jazz.Update(ctx, db, "errors", id, patch)
}

// Delete deletes the row of `errors` with id.
func (errorsTable) Delete(ctx context.Context, db jazz.Querier, id string) error {
	return jazz.Delete(ctx, db, "errors", id)
}

// String is a row of `strings`.
type String struct {
	ID     string `json:"id"`
	Source string `json:"source"`
	App    string `json:"app"`
	Open   bool   `json:"open"`
	String string `json:"string"`
}

// NewString is a row to insert into `strings`: nil fields are null, or take their default.
type NewString struct {
	Source string `json:"source"`
	App    string `json:"app"`
	Open   bool   `json:"open"`
	String string `json:"string"`
}

// StringPatch changes the fields of a row of `strings` that are set (jazz.Set).
type StringPatch struct {
	Source jazz.Patch[string]
	App    jazz.Patch[string]
	Open   jazz.Patch[bool]
	String jazz.Patch[string]
}

// MarshalJSON writes the fields that are set.
func (p StringPatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.Source.Entry("source"),
		p.App.Entry("app"),
		p.Open.Entry("open"),
		p.String.Entry("string"),
	)
}

// stringsTable is the type of Strings: the columns of `strings`, to build conditions and orders from, and its relations, to include.
type stringsTable struct {
	ID     jazz.Column[String, string]
	Source jazz.Column[String, string]
	App    jazz.Column[String, string]
	Open   jazz.Column[String, bool]
	String jazz.Column[String, string]
}

// Strings is the table `strings`.
var Strings = stringsTable{
	ID:     jazz.NewColumn[String, string]("id"),
	Source: jazz.NewColumn[String, string]("source"),
	App:    jazz.NewColumn[String, string]("app"),
	Open:   jazz.NewColumn[String, bool]("open"),
	String: jazz.NewColumn[String, string]("string"),
}

// Query starts a query of `strings`.
func (stringsTable) Query() *jazz.Query[String] {
	return jazz.NewQuery[String]("strings")
}

// Insert inserts a row into `strings` and returns it, with its id.
func (stringsTable) Insert(ctx context.Context, db jazz.Querier, row NewString) (String, error) {
	return jazz.Insert[NewString, String](ctx, db, "strings", row)
}

// Update changes the fields patch sets of the row of `strings` with id.
func (stringsTable) Update(ctx context.Context, db jazz.Querier, id string, patch StringPatch) error {
	return jazz.Update(ctx, db, "strings", id, patch)
}

// Delete deletes the row of `strings` with id.
func (stringsTable) Delete(ctx context.Context, db jazz.Querier, id string) error {
	return jazz.Delete(ctx, db, "strings", id)
}
