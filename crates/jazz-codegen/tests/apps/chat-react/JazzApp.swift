// JazzApp.swift: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Regenerate it after changing jazz.app.json.
// swift-format-ignore-file

import Foundation
import Jazz

/// The app: its `jazz.app.json`, opening it, and a type per table and enum,
/// all inside this namespace so table names never hide Swift's own types
/// (`JazzApp.Task`, not `Task`).
public enum JazzApp {
    /// `sha256:…` of the `jazz.app.json` these bindings were generated from.
    public static let source = "sha256:392eedb6f00875a3d0fe9ac829b498e8abc79fdd65aefeed69fedff649ed54d2"

    /// The app's `jazz.app.json`: its schema, every earlier version and the
    /// migrations between them.
    public static let app = #"""
{
  "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
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
"""#

    /// Opens the app as an end-user client (`.client(…)`, signed in through
    /// the app's Jazz endpoint) or a local one (`.local()`).
    public static func open(_ config: Jazz.Config) async throws -> Jazz.Client {
        try await Jazz.Client.open(app: app, config: config)
    }

    /// A row of `canvases`.
    public struct Canvas: Jazz.Row {
        public static let table = "canvases"
        public let id: Jazz.ID
        public var chatId: Jazz.ID
        /// Relation `chat`: the `Chat` row, when the query includes it (and it exists).
        public var chat: Chat? {
            get { _chat?.value }
            set { _chat = newValue.map { Jazz.Included($0) } }
        }
        private var _chat: Jazz.Included<Chat>? = nil
        /// Relation `strokesViaCanvas`: the `Stroke` rows, when the query includes them.
        public var strokesViaCanvas: [Stroke]? = nil

        public init(id: Jazz.ID, chatId: Jazz.ID) {
            self.id = id
            self.chatId = chatId
        }

        /// Equal by id, columns and computed fields: included relations don't count.
        public static func == (a: Self, b: Self) -> Swift.Bool {
            a.id == b.id && a.chatId == b.chatId
        }

        public func hash(into hasher: inout Swift.Hasher) {
            hasher.combine(id)
            hasher.combine(chatId)
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case id
            case chatId = "chatId"
            case _chat = "chat"
            case strokesViaCanvas = "strokesViaCanvas"
        }
    }

    /// A new row of `canvases`: `nil` leaves a column out, which stores its default (or `null`).
    public struct NewCanvas: Jazz.Insert {
        public typealias Row = Canvas
        public var chatId: Jazz.ID

        public init(chatId: Jazz.ID) {
            self.chatId = chatId
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case chatId = "chatId"
        }
    }

    /// A change to a row of `canvases`: only the columns set to `.set(…)` change; `.set(nil)` stores `null`.
    public struct CanvasPatch: Jazz.Patch {
        public typealias Row = Canvas
        public var chatId: Jazz.Change<Jazz.ID>

        public init(chatId: Jazz.Change<Jazz.ID> = .unchanged) {
            self.chatId = chatId
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case chatId = "chatId"
        }

        public func encode(to encoder: any Swift.Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encodeChange(self.chatId, forKey: .chatId)
        }
    }

    /// Queries of `canvases` and its columns.
    public enum canvases {
        /// Every row of `canvases`; narrow it with `where`, `orderBy`, `limit` and `offset`.
        public static func query() -> Jazz.Query<Canvas> { Jazz.Query() }

        /// The row's id.
        public static let id = Jazz.Column<Canvas, Jazz.ID>("id")
        /// Column `chatId`.
        public static let chatId = Jazz.Column<Canvas, Jazz.ID>("chatId")
        /// Relation `chat`, for `include` (narrow it with `where`, `orderBy`, `limit`, `include`).
        public static let chat = Jazz.Relation<Canvas, Chat>("chat")
        /// Relation `strokesViaCanvas`, for `include` (narrow it with `where`, `orderBy`, `limit`, `include`).
        public static let strokesViaCanvas = Jazz.Relation<Canvas, Stroke>("strokesViaCanvas")
    }

    /// A row of `chatMembers`.
    public struct ChatMember: Jazz.Row {
        public static let table = "chatMembers"
        public let id: Jazz.ID
        public var chatId: Jazz.ID
        public var userId: Jazz.ID
        public var joinCode: Swift.String?
        /// Relation `chat`: the `Chat` row, when the query includes it (and it exists).
        public var chat: Chat? {
            get { _chat?.value }
            set { _chat = newValue.map { Jazz.Included($0) } }
        }
        private var _chat: Jazz.Included<Chat>? = nil

        public init(id: Jazz.ID, chatId: Jazz.ID, userId: Jazz.ID, joinCode: Swift.String?) {
            self.id = id
            self.chatId = chatId
            self.userId = userId
            self.joinCode = joinCode
        }

        /// Equal by id, columns and computed fields: included relations don't count.
        public static func == (a: Self, b: Self) -> Swift.Bool {
            a.id == b.id && a.chatId == b.chatId && a.userId == b.userId && a.joinCode == b.joinCode
        }

        public func hash(into hasher: inout Swift.Hasher) {
            hasher.combine(id)
            hasher.combine(chatId)
            hasher.combine(userId)
            hasher.combine(joinCode)
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case id
            case chatId = "chatId"
            case userId = "userId"
            case joinCode = "joinCode"
            case _chat = "chat"
        }
    }

    /// A new row of `chatMembers`: `nil` leaves a column out, which stores its default (or `null`).
    public struct NewChatMember: Jazz.Insert {
        public typealias Row = ChatMember
        public var chatId: Jazz.ID
        public var userId: Jazz.ID
        public var joinCode: Swift.String?

        public init(chatId: Jazz.ID, userId: Jazz.ID, joinCode: Swift.String? = nil) {
            self.chatId = chatId
            self.userId = userId
            self.joinCode = joinCode
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case chatId = "chatId"
            case userId = "userId"
            case joinCode = "joinCode"
        }
    }

    /// A change to a row of `chatMembers`: only the columns set to `.set(…)` change; `.set(nil)` stores `null`.
    public struct ChatMemberPatch: Jazz.Patch {
        public typealias Row = ChatMember
        public var chatId: Jazz.Change<Jazz.ID>
        public var userId: Jazz.Change<Jazz.ID>
        public var joinCode: Jazz.Change<Swift.String?>

        public init(chatId: Jazz.Change<Jazz.ID> = .unchanged, userId: Jazz.Change<Jazz.ID> = .unchanged, joinCode: Jazz.Change<Swift.String?> = .unchanged) {
            self.chatId = chatId
            self.userId = userId
            self.joinCode = joinCode
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case chatId = "chatId"
            case userId = "userId"
            case joinCode = "joinCode"
        }

        public func encode(to encoder: any Swift.Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encodeChange(self.chatId, forKey: .chatId)
            try container.encodeChange(self.userId, forKey: .userId)
            try container.encodeChange(self.joinCode, forKey: .joinCode)
        }
    }

    /// Queries of `chatMembers` and its columns.
    public enum chatMembers {
        /// Every row of `chatMembers`; narrow it with `where`, `orderBy`, `limit` and `offset`.
        public static func query() -> Jazz.Query<ChatMember> { Jazz.Query() }

        /// The row's id.
        public static let id = Jazz.Column<ChatMember, Jazz.ID>("id")
        /// Column `chatId`.
        public static let chatId = Jazz.Column<ChatMember, Jazz.ID>("chatId")
        /// Column `userId`.
        public static let userId = Jazz.Column<ChatMember, Jazz.ID>("userId")
        /// Column `joinCode`.
        public static let joinCode = Jazz.Column<ChatMember, Swift.String?>("joinCode")
        /// Relation `chat`, for `include` (narrow it with `where`, `orderBy`, `limit`, `include`).
        public static let chat = Jazz.Relation<ChatMember, Chat>("chat")
    }

    /// A row of `chats`.
    public struct Chat: Jazz.Row {
        public static let table = "chats"
        public let id: Jazz.ID
        public var name: Swift.String?
        public var isPublic: Swift.Bool
        public var joinCode: Swift.String?
        /// Relation `canvasesViaChat`: the `Canvas` rows, when the query includes them.
        public var canvasesViaChat: [Canvas]? = nil
        /// Relation `chatMembersViaChat`: the `ChatMember` rows, when the query includes them.
        public var chatMembersViaChat: [ChatMember]? = nil
        /// Relation `messagesViaChat`: the `Message` rows, when the query includes them.
        public var messagesViaChat: [Message]? = nil

        public init(id: Jazz.ID, name: Swift.String?, isPublic: Swift.Bool, joinCode: Swift.String?) {
            self.id = id
            self.name = name
            self.isPublic = isPublic
            self.joinCode = joinCode
        }

        /// Equal by id, columns and computed fields: included relations don't count.
        public static func == (a: Self, b: Self) -> Swift.Bool {
            a.id == b.id && a.name == b.name && a.isPublic == b.isPublic && a.joinCode == b.joinCode
        }

        public func hash(into hasher: inout Swift.Hasher) {
            hasher.combine(id)
            hasher.combine(name)
            hasher.combine(isPublic)
            hasher.combine(joinCode)
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case id
            case name = "name"
            case isPublic = "isPublic"
            case joinCode = "joinCode"
            case canvasesViaChat = "canvasesViaChat"
            case chatMembersViaChat = "chatMembersViaChat"
            case messagesViaChat = "messagesViaChat"
        }
    }

    /// A new row of `chats`: `nil` leaves a column out, which stores its default (or `null`).
    public struct NewChat: Jazz.Insert {
        public typealias Row = Chat
        public var name: Swift.String?
        public var isPublic: Swift.Bool
        public var joinCode: Swift.String?

        public init(name: Swift.String? = nil, isPublic: Swift.Bool, joinCode: Swift.String? = nil) {
            self.name = name
            self.isPublic = isPublic
            self.joinCode = joinCode
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case name = "name"
            case isPublic = "isPublic"
            case joinCode = "joinCode"
        }
    }

    /// A change to a row of `chats`: only the columns set to `.set(…)` change; `.set(nil)` stores `null`.
    public struct ChatPatch: Jazz.Patch {
        public typealias Row = Chat
        public var name: Jazz.Change<Swift.String?>
        public var isPublic: Jazz.Change<Swift.Bool>
        public var joinCode: Jazz.Change<Swift.String?>

        public init(name: Jazz.Change<Swift.String?> = .unchanged, isPublic: Jazz.Change<Swift.Bool> = .unchanged, joinCode: Jazz.Change<Swift.String?> = .unchanged) {
            self.name = name
            self.isPublic = isPublic
            self.joinCode = joinCode
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case name = "name"
            case isPublic = "isPublic"
            case joinCode = "joinCode"
        }

        public func encode(to encoder: any Swift.Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encodeChange(self.name, forKey: .name)
            try container.encodeChange(self.isPublic, forKey: .isPublic)
            try container.encodeChange(self.joinCode, forKey: .joinCode)
        }
    }

    /// Queries of `chats` and its columns.
    public enum chats {
        /// Every row of `chats`; narrow it with `where`, `orderBy`, `limit` and `offset`.
        public static func query() -> Jazz.Query<Chat> { Jazz.Query() }

        /// The row's id.
        public static let id = Jazz.Column<Chat, Jazz.ID>("id")
        /// Column `name`.
        public static let name = Jazz.Column<Chat, Swift.String?>("name")
        /// Column `isPublic`.
        public static let isPublic = Jazz.Column<Chat, Swift.Bool>("isPublic")
        /// Column `joinCode`.
        public static let joinCode = Jazz.Column<Chat, Swift.String?>("joinCode")
        /// Relation `canvasesViaChat`, for `include` (narrow it with `where`, `orderBy`, `limit`, `include`).
        public static let canvasesViaChat = Jazz.Relation<Chat, Canvas>("canvasesViaChat")
        /// Relation `chatMembersViaChat`, for `include` (narrow it with `where`, `orderBy`, `limit`, `include`).
        public static let chatMembersViaChat = Jazz.Relation<Chat, ChatMember>("chatMembersViaChat")
        /// Relation `messagesViaChat`, for `include` (narrow it with `where`, `orderBy`, `limit`, `include`).
        public static let messagesViaChat = Jazz.Relation<Chat, Message>("messagesViaChat")
    }

    /// A row of `messages`.
    public struct Message: Jazz.Row {
        public static let table = "messages"
        public let id: Jazz.ID
        public var chatId: Jazz.ID
        public var text: Swift.String
        public var senderId: Jazz.ID
        /// Relation `chat`: the `Chat` row, when the query includes it (and it exists).
        public var chat: Chat? {
            get { _chat?.value }
            set { _chat = newValue.map { Jazz.Included($0) } }
        }
        private var _chat: Jazz.Included<Chat>? = nil
        /// Relation `reactionsViaMessage`: the `Reaction` rows, when the query includes them.
        public var reactionsViaMessage: [Reaction]? = nil
        /// Relation `sender`: the `Profile` row, when the query includes it (and it exists).
        public var sender: Profile? {
            get { _sender?.value }
            set { _sender = newValue.map { Jazz.Included($0) } }
        }
        private var _sender: Jazz.Included<Profile>? = nil

        public init(id: Jazz.ID, chatId: Jazz.ID, text: Swift.String, senderId: Jazz.ID) {
            self.id = id
            self.chatId = chatId
            self.text = text
            self.senderId = senderId
        }

        /// Equal by id, columns and computed fields: included relations don't count.
        public static func == (a: Self, b: Self) -> Swift.Bool {
            a.id == b.id && a.chatId == b.chatId && a.text == b.text && a.senderId == b.senderId
        }

        public func hash(into hasher: inout Swift.Hasher) {
            hasher.combine(id)
            hasher.combine(chatId)
            hasher.combine(text)
            hasher.combine(senderId)
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case id
            case chatId = "chatId"
            case text = "text"
            case senderId = "senderId"
            case _chat = "chat"
            case reactionsViaMessage = "reactionsViaMessage"
            case _sender = "sender"
        }
    }

    /// A new row of `messages`: `nil` leaves a column out, which stores its default (or `null`).
    public struct NewMessage: Jazz.Insert {
        public typealias Row = Message
        public var chatId: Jazz.ID
        public var text: Swift.String
        public var senderId: Jazz.ID

        public init(chatId: Jazz.ID, text: Swift.String, senderId: Jazz.ID) {
            self.chatId = chatId
            self.text = text
            self.senderId = senderId
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case chatId = "chatId"
            case text = "text"
            case senderId = "senderId"
        }
    }

    /// A change to a row of `messages`: only the columns set to `.set(…)` change; `.set(nil)` stores `null`.
    public struct MessagePatch: Jazz.Patch {
        public typealias Row = Message
        public var chatId: Jazz.Change<Jazz.ID>
        public var text: Jazz.Change<Swift.String>
        public var senderId: Jazz.Change<Jazz.ID>

        public init(chatId: Jazz.Change<Jazz.ID> = .unchanged, text: Jazz.Change<Swift.String> = .unchanged, senderId: Jazz.Change<Jazz.ID> = .unchanged) {
            self.chatId = chatId
            self.text = text
            self.senderId = senderId
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case chatId = "chatId"
            case text = "text"
            case senderId = "senderId"
        }

        public func encode(to encoder: any Swift.Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encodeChange(self.chatId, forKey: .chatId)
            try container.encodeChange(self.text, forKey: .text)
            try container.encodeChange(self.senderId, forKey: .senderId)
        }
    }

    /// Queries of `messages` and its columns.
    public enum messages {
        /// Every row of `messages`; narrow it with `where`, `orderBy`, `limit` and `offset`.
        public static func query() -> Jazz.Query<Message> { Jazz.Query() }

        /// The row's id.
        public static let id = Jazz.Column<Message, Jazz.ID>("id")
        /// Column `chatId`.
        public static let chatId = Jazz.Column<Message, Jazz.ID>("chatId")
        /// Column `text`.
        public static let text = Jazz.Column<Message, Swift.String>("text")
        /// Column `senderId`.
        public static let senderId = Jazz.Column<Message, Jazz.ID>("senderId")
        /// Relation `chat`, for `include` (narrow it with `where`, `orderBy`, `limit`, `include`).
        public static let chat = Jazz.Relation<Message, Chat>("chat")
        /// Relation `reactionsViaMessage`, for `include` (narrow it with `where`, `orderBy`, `limit`, `include`).
        public static let reactionsViaMessage = Jazz.Relation<Message, Reaction>("reactionsViaMessage")
        /// Relation `sender`, for `include` (narrow it with `where`, `orderBy`, `limit`, `include`).
        public static let sender = Jazz.Relation<Message, Profile>("sender")
    }

    /// A row of `profiles`.
    public struct Profile: Jazz.Row {
        public static let table = "profiles"
        public let id: Jazz.ID
        public var userId: Jazz.ID
        public var name: Swift.String
        public var avatar: Swift.String?
        /// Relation `messagesViaSender`: the `Message` rows, when the query includes them.
        public var messagesViaSender: [Message]? = nil

        public init(id: Jazz.ID, userId: Jazz.ID, name: Swift.String, avatar: Swift.String?) {
            self.id = id
            self.userId = userId
            self.name = name
            self.avatar = avatar
        }

        /// Equal by id, columns and computed fields: included relations don't count.
        public static func == (a: Self, b: Self) -> Swift.Bool {
            a.id == b.id && a.userId == b.userId && a.name == b.name && a.avatar == b.avatar
        }

        public func hash(into hasher: inout Swift.Hasher) {
            hasher.combine(id)
            hasher.combine(userId)
            hasher.combine(name)
            hasher.combine(avatar)
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case id
            case userId = "userId"
            case name = "name"
            case avatar = "avatar"
            case messagesViaSender = "messagesViaSender"
        }
    }

    /// A new row of `profiles`: `nil` leaves a column out, which stores its default (or `null`).
    public struct NewProfile: Jazz.Insert {
        public typealias Row = Profile
        public var userId: Jazz.ID
        public var name: Swift.String
        public var avatar: Swift.String?

        public init(userId: Jazz.ID, name: Swift.String, avatar: Swift.String? = nil) {
            self.userId = userId
            self.name = name
            self.avatar = avatar
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case userId = "userId"
            case name = "name"
            case avatar = "avatar"
        }
    }

    /// A change to a row of `profiles`: only the columns set to `.set(…)` change; `.set(nil)` stores `null`.
    public struct ProfilePatch: Jazz.Patch {
        public typealias Row = Profile
        public var userId: Jazz.Change<Jazz.ID>
        public var name: Jazz.Change<Swift.String>
        public var avatar: Jazz.Change<Swift.String?>

        public init(userId: Jazz.Change<Jazz.ID> = .unchanged, name: Jazz.Change<Swift.String> = .unchanged, avatar: Jazz.Change<Swift.String?> = .unchanged) {
            self.userId = userId
            self.name = name
            self.avatar = avatar
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case userId = "userId"
            case name = "name"
            case avatar = "avatar"
        }

        public func encode(to encoder: any Swift.Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encodeChange(self.userId, forKey: .userId)
            try container.encodeChange(self.name, forKey: .name)
            try container.encodeChange(self.avatar, forKey: .avatar)
        }
    }

    /// Queries of `profiles` and its columns.
    public enum profiles {
        /// Every row of `profiles`; narrow it with `where`, `orderBy`, `limit` and `offset`.
        public static func query() -> Jazz.Query<Profile> { Jazz.Query() }

        /// The row's id.
        public static let id = Jazz.Column<Profile, Jazz.ID>("id")
        /// Column `userId`.
        public static let userId = Jazz.Column<Profile, Jazz.ID>("userId")
        /// Column `name`.
        public static let name = Jazz.Column<Profile, Swift.String>("name")
        /// Column `avatar`.
        public static let avatar = Jazz.Column<Profile, Swift.String?>("avatar")
        /// Relation `messagesViaSender`, for `include` (narrow it with `where`, `orderBy`, `limit`, `include`).
        public static let messagesViaSender = Jazz.Relation<Profile, Message>("messagesViaSender")
    }

    /// A row of `reactions`.
    public struct Reaction: Jazz.Row {
        public static let table = "reactions"
        public let id: Jazz.ID
        public var messageId: Jazz.ID
        public var userId: Jazz.ID
        public var emoji: Swift.String
        /// Relation `message`: the `Message` row, when the query includes it (and it exists).
        public var message: Message? {
            get { _message?.value }
            set { _message = newValue.map { Jazz.Included($0) } }
        }
        private var _message: Jazz.Included<Message>? = nil

        public init(id: Jazz.ID, messageId: Jazz.ID, userId: Jazz.ID, emoji: Swift.String) {
            self.id = id
            self.messageId = messageId
            self.userId = userId
            self.emoji = emoji
        }

        /// Equal by id, columns and computed fields: included relations don't count.
        public static func == (a: Self, b: Self) -> Swift.Bool {
            a.id == b.id && a.messageId == b.messageId && a.userId == b.userId && a.emoji == b.emoji
        }

        public func hash(into hasher: inout Swift.Hasher) {
            hasher.combine(id)
            hasher.combine(messageId)
            hasher.combine(userId)
            hasher.combine(emoji)
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case id
            case messageId = "messageId"
            case userId = "userId"
            case emoji = "emoji"
            case _message = "message"
        }
    }

    /// A new row of `reactions`: `nil` leaves a column out, which stores its default (or `null`).
    public struct NewReaction: Jazz.Insert {
        public typealias Row = Reaction
        public var messageId: Jazz.ID
        public var userId: Jazz.ID
        public var emoji: Swift.String

        public init(messageId: Jazz.ID, userId: Jazz.ID, emoji: Swift.String) {
            self.messageId = messageId
            self.userId = userId
            self.emoji = emoji
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case messageId = "messageId"
            case userId = "userId"
            case emoji = "emoji"
        }
    }

    /// A change to a row of `reactions`: only the columns set to `.set(…)` change; `.set(nil)` stores `null`.
    public struct ReactionPatch: Jazz.Patch {
        public typealias Row = Reaction
        public var messageId: Jazz.Change<Jazz.ID>
        public var userId: Jazz.Change<Jazz.ID>
        public var emoji: Jazz.Change<Swift.String>

        public init(messageId: Jazz.Change<Jazz.ID> = .unchanged, userId: Jazz.Change<Jazz.ID> = .unchanged, emoji: Jazz.Change<Swift.String> = .unchanged) {
            self.messageId = messageId
            self.userId = userId
            self.emoji = emoji
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case messageId = "messageId"
            case userId = "userId"
            case emoji = "emoji"
        }

        public func encode(to encoder: any Swift.Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encodeChange(self.messageId, forKey: .messageId)
            try container.encodeChange(self.userId, forKey: .userId)
            try container.encodeChange(self.emoji, forKey: .emoji)
        }
    }

    /// Queries of `reactions` and its columns.
    public enum reactions {
        /// Every row of `reactions`; narrow it with `where`, `orderBy`, `limit` and `offset`.
        public static func query() -> Jazz.Query<Reaction> { Jazz.Query() }

        /// The row's id.
        public static let id = Jazz.Column<Reaction, Jazz.ID>("id")
        /// Column `messageId`.
        public static let messageId = Jazz.Column<Reaction, Jazz.ID>("messageId")
        /// Column `userId`.
        public static let userId = Jazz.Column<Reaction, Jazz.ID>("userId")
        /// Column `emoji`.
        public static let emoji = Jazz.Column<Reaction, Swift.String>("emoji")
        /// Relation `message`, for `include` (narrow it with `where`, `orderBy`, `limit`, `include`).
        public static let message = Jazz.Relation<Reaction, Message>("message")
    }

    /// A row of `strokes`.
    public struct Stroke: Jazz.Row {
        public static let table = "strokes"
        public let id: Jazz.ID
        public var canvasId: Jazz.ID
        public var color: Swift.String
        public var width: Swift.Int
        public var pointsJson: Swift.String
        /// Relation `canvas`: the `Canvas` row, when the query includes it (and it exists).
        public var canvas: Canvas? {
            get { _canvas?.value }
            set { _canvas = newValue.map { Jazz.Included($0) } }
        }
        private var _canvas: Jazz.Included<Canvas>? = nil

        public init(id: Jazz.ID, canvasId: Jazz.ID, color: Swift.String, width: Swift.Int, pointsJson: Swift.String) {
            self.id = id
            self.canvasId = canvasId
            self.color = color
            self.width = width
            self.pointsJson = pointsJson
        }

        /// Equal by id, columns and computed fields: included relations don't count.
        public static func == (a: Self, b: Self) -> Swift.Bool {
            a.id == b.id && a.canvasId == b.canvasId && a.color == b.color && a.width == b.width && a.pointsJson == b.pointsJson
        }

        public func hash(into hasher: inout Swift.Hasher) {
            hasher.combine(id)
            hasher.combine(canvasId)
            hasher.combine(color)
            hasher.combine(width)
            hasher.combine(pointsJson)
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case id
            case canvasId = "canvasId"
            case color = "color"
            case width = "width"
            case pointsJson = "pointsJson"
            case _canvas = "canvas"
        }
    }

    /// A new row of `strokes`: `nil` leaves a column out, which stores its default (or `null`).
    public struct NewStroke: Jazz.Insert {
        public typealias Row = Stroke
        public var canvasId: Jazz.ID
        public var color: Swift.String
        public var width: Swift.Int
        public var pointsJson: Swift.String

        public init(canvasId: Jazz.ID, color: Swift.String, width: Swift.Int, pointsJson: Swift.String) {
            self.canvasId = canvasId
            self.color = color
            self.width = width
            self.pointsJson = pointsJson
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case canvasId = "canvasId"
            case color = "color"
            case width = "width"
            case pointsJson = "pointsJson"
        }
    }

    /// A change to a row of `strokes`: only the columns set to `.set(…)` change; `.set(nil)` stores `null`.
    public struct StrokePatch: Jazz.Patch {
        public typealias Row = Stroke
        public var canvasId: Jazz.Change<Jazz.ID>
        public var color: Jazz.Change<Swift.String>
        public var width: Jazz.Change<Swift.Int>
        public var pointsJson: Jazz.Change<Swift.String>

        public init(canvasId: Jazz.Change<Jazz.ID> = .unchanged, color: Jazz.Change<Swift.String> = .unchanged, width: Jazz.Change<Swift.Int> = .unchanged, pointsJson: Jazz.Change<Swift.String> = .unchanged) {
            self.canvasId = canvasId
            self.color = color
            self.width = width
            self.pointsJson = pointsJson
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case canvasId = "canvasId"
            case color = "color"
            case width = "width"
            case pointsJson = "pointsJson"
        }

        public func encode(to encoder: any Swift.Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encodeChange(self.canvasId, forKey: .canvasId)
            try container.encodeChange(self.color, forKey: .color)
            try container.encodeChange(self.width, forKey: .width)
            try container.encodeChange(self.pointsJson, forKey: .pointsJson)
        }
    }

    /// Queries of `strokes` and its columns.
    public enum strokes {
        /// Every row of `strokes`; narrow it with `where`, `orderBy`, `limit` and `offset`.
        public static func query() -> Jazz.Query<Stroke> { Jazz.Query() }

        /// The row's id.
        public static let id = Jazz.Column<Stroke, Jazz.ID>("id")
        /// Column `canvasId`.
        public static let canvasId = Jazz.Column<Stroke, Jazz.ID>("canvasId")
        /// Column `color`.
        public static let color = Jazz.Column<Stroke, Swift.String>("color")
        /// Column `width`.
        public static let width = Jazz.Column<Stroke, Swift.Int>("width")
        /// Column `pointsJson`.
        public static let pointsJson = Jazz.Column<Stroke, Swift.String>("pointsJson")
        /// Relation `canvas`, for `include` (narrow it with `where`, `orderBy`, `limit`, `include`).
        public static let canvas = Jazz.Relation<Stroke, Canvas>("canvas")
    }
}
