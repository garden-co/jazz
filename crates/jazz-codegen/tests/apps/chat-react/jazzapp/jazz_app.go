// Code generated from jazz.app.json by `jazz-tools generate`. DO NOT EDIT.
// Bindings format 1. Regenerate it after changing jazz.app.json.

// Package jazzapp holds the typed bindings of this app's jazz.app.json.
package jazzapp

import (
	"context"

	jazz "github.com/garden-co/jazz/packages/jazz-go"
)

// Source is the hash of the jazz.app.json these bindings were generated from.
const Source = "sha256:e8cdc8c71ce4a4ec76e542704f1bbcf9be8c2f0f4928d79f93b7d7e36716bd5e"

// App is the jazz.app.json these bindings were generated from.
const App = `{
  "$schema": "https://jazz.tools/schemas/jazz.app.schema.json",
  "format": 1,
  "tables": {
    "canvases": {
      "columns": { "chatId": "uuid" },
      "relations": {
        "chat": { "rel": "chats", "column": "chatId" },
        "strokesViaCanvas": { "reverse": "strokes", "relation": "canvas" }
      }
    },
    "chatMembers": {
      "columns": { "chatId": "uuid", "userId": "uuid", "joinCode": "string?" },
      "relations": { "chat": { "rel": "chats", "column": "chatId" } }
    },
    "chats": {
      "columns": { "name": "string?", "isPublic": "boolean", "joinCode": "string?" },
      "relations": {
        "canvasesViaChat": { "reverse": "canvases", "relation": "chat" },
        "chatMembersViaChat": { "reverse": "chatMembers", "relation": "chat" },
        "messagesViaChat": { "reverse": "messages", "relation": "chat" }
      }
    },
    "messages": {
      "columns": { "chatId": "uuid", "text": "string", "senderId": "uuid" },
      "relations": {
        "chat": { "rel": "chats", "column": "chatId" },
        "reactionsViaMessage": { "reverse": "reactions", "relation": "message" },
        "sender": { "rel": "profiles", "column": "senderId" }
      }
    },
    "profiles": {
      "columns": { "userId": "uuid", "name": "string", "avatar": "string?" },
      "relations": { "messagesViaSender": { "reverse": "messages", "relation": "sender" } }
    },
    "reactions": {
      "columns": { "messageId": "uuid", "userId": "uuid", "emoji": "string" },
      "relations": { "message": { "rel": "messages", "column": "messageId" } }
    },
    "strokes": {
      "columns": { "canvasId": "uuid", "color": "string", "width": "int", "pointsJson": "string" },
      "relations": { "canvas": { "rel": "canvases", "column": "canvasId" } }
    }
  }
}
`

// Open opens this app: a client, or a backend when cfg.Permissions is set.
func Open(ctx context.Context, cfg jazz.Config) (*jazz.DB, error) {
	cfg.App = App
	return jazz.Open(ctx, cfg)
}

// Canvas is a row of `canvases`.
type Canvas struct {
	ID     string `json:"id"`
	ChatID string `json:"chatId"`
}

// NewCanvas is a row to insert into `canvases`: nil fields are null, or take their default.
type NewCanvas struct {
	ChatID string `json:"chatId"`
}

// CanvasPatch changes the fields of a row of `canvases` that are set (jazz.Set).
type CanvasPatch struct {
	ChatID jazz.Patch[string]
}

// MarshalJSON writes the fields that are set.
func (p CanvasPatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.ChatID.Entry("chatId"),
	)
}

// canvasesTable is the type of Canvases: the columns of `canvases`, to build conditions and orders from.
type canvasesTable struct {
	ID     jazz.Column[Canvas, string]
	ChatID jazz.Column[Canvas, string]
}

// Canvases is the table `canvases`.
var Canvases = canvasesTable{
	ID:     jazz.NewColumn[Canvas, string]("id"),
	ChatID: jazz.NewColumn[Canvas, string]("chatId"),
}

// Query starts a query of `canvases`.
func (canvasesTable) Query() *jazz.Query[Canvas] {
	return jazz.NewQuery[Canvas]("canvases")
}

// Insert inserts a row into `canvases` and returns it, with its id.
func (canvasesTable) Insert(ctx context.Context, db *jazz.DB, row NewCanvas) (Canvas, error) {
	return jazz.Insert[NewCanvas, Canvas](ctx, db, "canvases", row)
}

// Update changes the fields patch sets of the row of `canvases` with id.
func (canvasesTable) Update(ctx context.Context, db *jazz.DB, id string, patch CanvasPatch) error {
	return db.Update(ctx, "canvases", id, patch)
}

// Delete deletes the row of `canvases` with id.
func (canvasesTable) Delete(ctx context.Context, db *jazz.DB, id string) error {
	return db.Delete(ctx, "canvases", id)
}

// ChatMember is a row of `chatMembers`.
type ChatMember struct {
	ID       string  `json:"id"`
	ChatID   string  `json:"chatId"`
	UserID   string  `json:"userId"`
	JoinCode *string `json:"joinCode"`
}

// NewChatMember is a row to insert into `chatMembers`: nil fields are null, or take their default.
type NewChatMember struct {
	ChatID   string  `json:"chatId"`
	UserID   string  `json:"userId"`
	JoinCode *string `json:"joinCode,omitempty"`
}

// ChatMemberPatch changes the fields of a row of `chatMembers` that are set (jazz.Set).
type ChatMemberPatch struct {
	ChatID   jazz.Patch[string]
	UserID   jazz.Patch[string]
	JoinCode jazz.Patch[*string]
}

// MarshalJSON writes the fields that are set.
func (p ChatMemberPatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.ChatID.Entry("chatId"),
		p.UserID.Entry("userId"),
		p.JoinCode.Entry("joinCode"),
	)
}

// chatMembersTable is the type of ChatMembers: the columns of `chatMembers`, to build conditions and orders from.
type chatMembersTable struct {
	ID       jazz.Column[ChatMember, string]
	ChatID   jazz.Column[ChatMember, string]
	UserID   jazz.Column[ChatMember, string]
	JoinCode jazz.Column[ChatMember, string]
}

// ChatMembers is the table `chatMembers`.
var ChatMembers = chatMembersTable{
	ID:       jazz.NewColumn[ChatMember, string]("id"),
	ChatID:   jazz.NewColumn[ChatMember, string]("chatId"),
	UserID:   jazz.NewColumn[ChatMember, string]("userId"),
	JoinCode: jazz.NewColumn[ChatMember, string]("joinCode"),
}

// Query starts a query of `chatMembers`.
func (chatMembersTable) Query() *jazz.Query[ChatMember] {
	return jazz.NewQuery[ChatMember]("chatMembers")
}

// Insert inserts a row into `chatMembers` and returns it, with its id.
func (chatMembersTable) Insert(ctx context.Context, db *jazz.DB, row NewChatMember) (ChatMember, error) {
	return jazz.Insert[NewChatMember, ChatMember](ctx, db, "chatMembers", row)
}

// Update changes the fields patch sets of the row of `chatMembers` with id.
func (chatMembersTable) Update(ctx context.Context, db *jazz.DB, id string, patch ChatMemberPatch) error {
	return db.Update(ctx, "chatMembers", id, patch)
}

// Delete deletes the row of `chatMembers` with id.
func (chatMembersTable) Delete(ctx context.Context, db *jazz.DB, id string) error {
	return db.Delete(ctx, "chatMembers", id)
}

// Chat is a row of `chats`.
type Chat struct {
	ID       string  `json:"id"`
	Name     *string `json:"name"`
	IsPublic bool    `json:"isPublic"`
	JoinCode *string `json:"joinCode"`
}

// NewChat is a row to insert into `chats`: nil fields are null, or take their default.
type NewChat struct {
	Name     *string `json:"name,omitempty"`
	IsPublic bool    `json:"isPublic"`
	JoinCode *string `json:"joinCode,omitempty"`
}

// ChatPatch changes the fields of a row of `chats` that are set (jazz.Set).
type ChatPatch struct {
	Name     jazz.Patch[*string]
	IsPublic jazz.Patch[bool]
	JoinCode jazz.Patch[*string]
}

// MarshalJSON writes the fields that are set.
func (p ChatPatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.Name.Entry("name"),
		p.IsPublic.Entry("isPublic"),
		p.JoinCode.Entry("joinCode"),
	)
}

// chatsTable is the type of Chats: the columns of `chats`, to build conditions and orders from.
type chatsTable struct {
	ID       jazz.Column[Chat, string]
	Name     jazz.Column[Chat, string]
	IsPublic jazz.Column[Chat, bool]
	JoinCode jazz.Column[Chat, string]
}

// Chats is the table `chats`.
var Chats = chatsTable{
	ID:       jazz.NewColumn[Chat, string]("id"),
	Name:     jazz.NewColumn[Chat, string]("name"),
	IsPublic: jazz.NewColumn[Chat, bool]("isPublic"),
	JoinCode: jazz.NewColumn[Chat, string]("joinCode"),
}

// Query starts a query of `chats`.
func (chatsTable) Query() *jazz.Query[Chat] {
	return jazz.NewQuery[Chat]("chats")
}

// Insert inserts a row into `chats` and returns it, with its id.
func (chatsTable) Insert(ctx context.Context, db *jazz.DB, row NewChat) (Chat, error) {
	return jazz.Insert[NewChat, Chat](ctx, db, "chats", row)
}

// Update changes the fields patch sets of the row of `chats` with id.
func (chatsTable) Update(ctx context.Context, db *jazz.DB, id string, patch ChatPatch) error {
	return db.Update(ctx, "chats", id, patch)
}

// Delete deletes the row of `chats` with id.
func (chatsTable) Delete(ctx context.Context, db *jazz.DB, id string) error {
	return db.Delete(ctx, "chats", id)
}

// Message is a row of `messages`.
type Message struct {
	ID       string `json:"id"`
	ChatID   string `json:"chatId"`
	Text     string `json:"text"`
	SenderID string `json:"senderId"`
}

// NewMessage is a row to insert into `messages`: nil fields are null, or take their default.
type NewMessage struct {
	ChatID   string `json:"chatId"`
	Text     string `json:"text"`
	SenderID string `json:"senderId"`
}

// MessagePatch changes the fields of a row of `messages` that are set (jazz.Set).
type MessagePatch struct {
	ChatID   jazz.Patch[string]
	Text     jazz.Patch[string]
	SenderID jazz.Patch[string]
}

// MarshalJSON writes the fields that are set.
func (p MessagePatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.ChatID.Entry("chatId"),
		p.Text.Entry("text"),
		p.SenderID.Entry("senderId"),
	)
}

// messagesTable is the type of Messages: the columns of `messages`, to build conditions and orders from.
type messagesTable struct {
	ID       jazz.Column[Message, string]
	ChatID   jazz.Column[Message, string]
	Text     jazz.Column[Message, string]
	SenderID jazz.Column[Message, string]
}

// Messages is the table `messages`.
var Messages = messagesTable{
	ID:       jazz.NewColumn[Message, string]("id"),
	ChatID:   jazz.NewColumn[Message, string]("chatId"),
	Text:     jazz.NewColumn[Message, string]("text"),
	SenderID: jazz.NewColumn[Message, string]("senderId"),
}

// Query starts a query of `messages`.
func (messagesTable) Query() *jazz.Query[Message] {
	return jazz.NewQuery[Message]("messages")
}

// Insert inserts a row into `messages` and returns it, with its id.
func (messagesTable) Insert(ctx context.Context, db *jazz.DB, row NewMessage) (Message, error) {
	return jazz.Insert[NewMessage, Message](ctx, db, "messages", row)
}

// Update changes the fields patch sets of the row of `messages` with id.
func (messagesTable) Update(ctx context.Context, db *jazz.DB, id string, patch MessagePatch) error {
	return db.Update(ctx, "messages", id, patch)
}

// Delete deletes the row of `messages` with id.
func (messagesTable) Delete(ctx context.Context, db *jazz.DB, id string) error {
	return db.Delete(ctx, "messages", id)
}

// Profile is a row of `profiles`.
type Profile struct {
	ID     string  `json:"id"`
	UserID string  `json:"userId"`
	Name   string  `json:"name"`
	Avatar *string `json:"avatar"`
}

// NewProfile is a row to insert into `profiles`: nil fields are null, or take their default.
type NewProfile struct {
	UserID string  `json:"userId"`
	Name   string  `json:"name"`
	Avatar *string `json:"avatar,omitempty"`
}

// ProfilePatch changes the fields of a row of `profiles` that are set (jazz.Set).
type ProfilePatch struct {
	UserID jazz.Patch[string]
	Name   jazz.Patch[string]
	Avatar jazz.Patch[*string]
}

// MarshalJSON writes the fields that are set.
func (p ProfilePatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.UserID.Entry("userId"),
		p.Name.Entry("name"),
		p.Avatar.Entry("avatar"),
	)
}

// profilesTable is the type of Profiles: the columns of `profiles`, to build conditions and orders from.
type profilesTable struct {
	ID     jazz.Column[Profile, string]
	UserID jazz.Column[Profile, string]
	Name   jazz.Column[Profile, string]
	Avatar jazz.Column[Profile, string]
}

// Profiles is the table `profiles`.
var Profiles = profilesTable{
	ID:     jazz.NewColumn[Profile, string]("id"),
	UserID: jazz.NewColumn[Profile, string]("userId"),
	Name:   jazz.NewColumn[Profile, string]("name"),
	Avatar: jazz.NewColumn[Profile, string]("avatar"),
}

// Query starts a query of `profiles`.
func (profilesTable) Query() *jazz.Query[Profile] {
	return jazz.NewQuery[Profile]("profiles")
}

// Insert inserts a row into `profiles` and returns it, with its id.
func (profilesTable) Insert(ctx context.Context, db *jazz.DB, row NewProfile) (Profile, error) {
	return jazz.Insert[NewProfile, Profile](ctx, db, "profiles", row)
}

// Update changes the fields patch sets of the row of `profiles` with id.
func (profilesTable) Update(ctx context.Context, db *jazz.DB, id string, patch ProfilePatch) error {
	return db.Update(ctx, "profiles", id, patch)
}

// Delete deletes the row of `profiles` with id.
func (profilesTable) Delete(ctx context.Context, db *jazz.DB, id string) error {
	return db.Delete(ctx, "profiles", id)
}

// Reaction is a row of `reactions`.
type Reaction struct {
	ID        string `json:"id"`
	MessageID string `json:"messageId"`
	UserID    string `json:"userId"`
	Emoji     string `json:"emoji"`
}

// NewReaction is a row to insert into `reactions`: nil fields are null, or take their default.
type NewReaction struct {
	MessageID string `json:"messageId"`
	UserID    string `json:"userId"`
	Emoji     string `json:"emoji"`
}

// ReactionPatch changes the fields of a row of `reactions` that are set (jazz.Set).
type ReactionPatch struct {
	MessageID jazz.Patch[string]
	UserID    jazz.Patch[string]
	Emoji     jazz.Patch[string]
}

// MarshalJSON writes the fields that are set.
func (p ReactionPatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.MessageID.Entry("messageId"),
		p.UserID.Entry("userId"),
		p.Emoji.Entry("emoji"),
	)
}

// reactionsTable is the type of Reactions: the columns of `reactions`, to build conditions and orders from.
type reactionsTable struct {
	ID        jazz.Column[Reaction, string]
	MessageID jazz.Column[Reaction, string]
	UserID    jazz.Column[Reaction, string]
	Emoji     jazz.Column[Reaction, string]
}

// Reactions is the table `reactions`.
var Reactions = reactionsTable{
	ID:        jazz.NewColumn[Reaction, string]("id"),
	MessageID: jazz.NewColumn[Reaction, string]("messageId"),
	UserID:    jazz.NewColumn[Reaction, string]("userId"),
	Emoji:     jazz.NewColumn[Reaction, string]("emoji"),
}

// Query starts a query of `reactions`.
func (reactionsTable) Query() *jazz.Query[Reaction] {
	return jazz.NewQuery[Reaction]("reactions")
}

// Insert inserts a row into `reactions` and returns it, with its id.
func (reactionsTable) Insert(ctx context.Context, db *jazz.DB, row NewReaction) (Reaction, error) {
	return jazz.Insert[NewReaction, Reaction](ctx, db, "reactions", row)
}

// Update changes the fields patch sets of the row of `reactions` with id.
func (reactionsTable) Update(ctx context.Context, db *jazz.DB, id string, patch ReactionPatch) error {
	return db.Update(ctx, "reactions", id, patch)
}

// Delete deletes the row of `reactions` with id.
func (reactionsTable) Delete(ctx context.Context, db *jazz.DB, id string) error {
	return db.Delete(ctx, "reactions", id)
}

// Stroke is a row of `strokes`.
type Stroke struct {
	ID         string `json:"id"`
	CanvasID   string `json:"canvasId"`
	Color      string `json:"color"`
	Width      int32  `json:"width"`
	PointsJSON string `json:"pointsJson"`
}

// NewStroke is a row to insert into `strokes`: nil fields are null, or take their default.
type NewStroke struct {
	CanvasID   string `json:"canvasId"`
	Color      string `json:"color"`
	Width      int32  `json:"width"`
	PointsJSON string `json:"pointsJson"`
}

// StrokePatch changes the fields of a row of `strokes` that are set (jazz.Set).
type StrokePatch struct {
	CanvasID   jazz.Patch[string]
	Color      jazz.Patch[string]
	Width      jazz.Patch[int32]
	PointsJSON jazz.Patch[string]
}

// MarshalJSON writes the fields that are set.
func (p StrokePatch) MarshalJSON() ([]byte, error) {
	return jazz.MarshalPatch(
		p.CanvasID.Entry("canvasId"),
		p.Color.Entry("color"),
		p.Width.Entry("width"),
		p.PointsJSON.Entry("pointsJson"),
	)
}

// strokesTable is the type of Strokes: the columns of `strokes`, to build conditions and orders from.
type strokesTable struct {
	ID         jazz.Column[Stroke, string]
	CanvasID   jazz.Column[Stroke, string]
	Color      jazz.Column[Stroke, string]
	Width      jazz.Column[Stroke, int32]
	PointsJSON jazz.Column[Stroke, string]
}

// Strokes is the table `strokes`.
var Strokes = strokesTable{
	ID:         jazz.NewColumn[Stroke, string]("id"),
	CanvasID:   jazz.NewColumn[Stroke, string]("canvasId"),
	Color:      jazz.NewColumn[Stroke, string]("color"),
	Width:      jazz.NewColumn[Stroke, int32]("width"),
	PointsJSON: jazz.NewColumn[Stroke, string]("pointsJson"),
}

// Query starts a query of `strokes`.
func (strokesTable) Query() *jazz.Query[Stroke] {
	return jazz.NewQuery[Stroke]("strokes")
}

// Insert inserts a row into `strokes` and returns it, with its id.
func (strokesTable) Insert(ctx context.Context, db *jazz.DB, row NewStroke) (Stroke, error) {
	return jazz.Insert[NewStroke, Stroke](ctx, db, "strokes", row)
}

// Update changes the fields patch sets of the row of `strokes` with id.
func (strokesTable) Update(ctx context.Context, db *jazz.DB, id string, patch StrokePatch) error {
	return db.Update(ctx, "strokes", id, patch)
}

// Delete deletes the row of `strokes` with id.
func (strokesTable) Delete(ctx context.Context, db *jazz.DB, id string) error {
	return db.Delete(ctx, "strokes", id)
}
