// Code generated from jazz.app.json by `jazz-tools generate`. DO NOT EDIT.
// Bindings format 1. Regenerate it after changing jazz.app.json.

// Package jazzapp holds the typed bindings of this app's jazz.app.json.
package jazzapp

import (
	"context"

	jazz "github.com/garden-co/jazz/packages/jazz-go"
)

// Source is the hash of the jazz.app.json these bindings were generated from.
const Source = "sha256:320aeff40326a9b6076a5196c5e24d44d5eaec47c105abdecae9c0b5d77a49ae"

// App is the jazz.app.json these bindings were generated from.
const App = `{
  "$schema": "https://jazz.tools/schemas/jazz.app.schema.json",
  "format": 1,
  "tables": {
    "notes": {
      "columns": {
        "body": "string",
        "pinned": { "type": "boolean", "default": false },
        "folderId": "uuid?"
      },
      "relations": { "folder": { "rel": "folders", "column": "folderId" } }
    },
    "folders": {
      "columns": { "name": "string" },
      "relations": { "notesViaFolder": { "reverse": "notes", "relation": "folder" } }
    }
  },
  "history": {
    "v1": {
      "tables": {
        "notes": { "columns": { "text": "string" } }
      }
    }
  },
  "migrations": [
    {
      "name": "folders",
      "from": "v1",
      "createTables": ["folders"],
      "migrate": {
        "notes": {
          "body": { "renameFrom": "text" },
          "pinned": { "add": "boolean", "default": false },
          "folderId": { "add": "uuid", "default": null }
        }
      }
    }
  ]
}
`

// Open opens this app: a client, or a backend when cfg.Permissions is set.
func Open(ctx context.Context, cfg jazz.Config) (*jazz.DB, error) {
	cfg.App = App
	return jazz.Open(ctx, cfg)
}

// Folder is a row of `folders`.
type Folder struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	NotesViaFolder []Note `json:"notesViaFolder,omitempty"`
}

// NewFolder is a row to insert into `folders`: nil fields are null, or take their default.
type NewFolder struct {
	Name string `json:"name"`
}

// FolderPatch changes the fields of a row of `folders` that are set (jazz.Set).
type FolderPatch struct {
	Name jazz.Patch[string]
}

// MarshalJSON writes the fields that are set.
func (p FolderPatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.Name.Entry("name"),
	)
}

// foldersTable is the type of Folders: the columns of `folders`, to build conditions and orders from, and its relations, to include.
type foldersTable struct {
	ID             jazz.Column[Folder, string]
	Name           jazz.Column[Folder, string]
	NotesViaFolder jazz.Relation[Folder, Note]
}

// Folders is the table `folders`.
var Folders = foldersTable{
	ID:             jazz.NewColumn[Folder, string]("id"),
	Name:           jazz.NewColumn[Folder, string]("name"),
	NotesViaFolder: jazz.NewRelation[Folder, Note]("notesViaFolder"),
}

// Query starts a query of `folders`.
func (foldersTable) Query() *jazz.Query[Folder] {
	return jazz.NewQuery[Folder]("folders")
}

// Insert inserts a row into `folders` and returns it, with its id.
func (foldersTable) Insert(ctx context.Context, db jazz.Querier, row NewFolder) (Folder, error) {
	return jazz.Insert[NewFolder, Folder](ctx, db, "folders", row)
}

// Update changes the fields patch sets of the row of `folders` with id.
func (foldersTable) Update(ctx context.Context, db jazz.Querier, id string, patch FolderPatch) error {
	return jazz.Update(ctx, db, "folders", id, patch)
}

// Delete deletes the row of `folders` with id.
func (foldersTable) Delete(ctx context.Context, db jazz.Querier, id string) error {
	return jazz.Delete(ctx, db, "folders", id)
}

// Note is a row of `notes`.
type Note struct {
	ID       string  `json:"id"`
	Body     string  `json:"body"`
	Pinned   bool    `json:"pinned"`
	FolderID *string `json:"folderId"`
	Folder   *Folder `json:"folder,omitempty"`
}

// NewNote is a row to insert into `notes`: nil fields are null, or take their default.
type NewNote struct {
	Body     string  `json:"body"`
	Pinned   *bool   `json:"pinned,omitempty"`
	FolderID *string `json:"folderId,omitempty"`
}

// NotePatch changes the fields of a row of `notes` that are set (jazz.Set).
type NotePatch struct {
	Body     jazz.Patch[string]
	Pinned   jazz.Patch[bool]
	FolderID jazz.Patch[*string]
}

// MarshalJSON writes the fields that are set.
func (p NotePatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.Body.Entry("body"),
		p.Pinned.Entry("pinned"),
		p.FolderID.Entry("folderId"),
	)
}

// notesTable is the type of Notes: the columns of `notes`, to build conditions and orders from, and its relations, to include.
type notesTable struct {
	ID       jazz.Column[Note, string]
	Body     jazz.Column[Note, string]
	Pinned   jazz.Column[Note, bool]
	FolderID jazz.Column[Note, string]
	Folder   jazz.Relation[Note, Folder]
}

// Notes is the table `notes`.
var Notes = notesTable{
	ID:       jazz.NewColumn[Note, string]("id"),
	Body:     jazz.NewColumn[Note, string]("body"),
	Pinned:   jazz.NewColumn[Note, bool]("pinned"),
	FolderID: jazz.NewColumn[Note, string]("folderId"),
	Folder:   jazz.NewRelation[Note, Folder]("folder"),
}

// Query starts a query of `notes`.
func (notesTable) Query() *jazz.Query[Note] {
	return jazz.NewQuery[Note]("notes")
}

// Insert inserts a row into `notes` and returns it, with its id.
func (notesTable) Insert(ctx context.Context, db jazz.Querier, row NewNote) (Note, error) {
	return jazz.Insert[NewNote, Note](ctx, db, "notes", row)
}

// Update changes the fields patch sets of the row of `notes` with id.
func (notesTable) Update(ctx context.Context, db jazz.Querier, id string, patch NotePatch) error {
	return jazz.Update(ctx, db, "notes", id, patch)
}

// Delete deletes the row of `notes` with id.
func (notesTable) Delete(ctx context.Context, db jazz.Querier, id string) error {
	return jazz.Delete(ctx, db, "notes", id)
}
