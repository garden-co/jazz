// JazzApp.swift: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Regenerate it after changing jazz.app.json.
// swift-format-ignore-file

import Foundation
import Jazz

/// A row of `folders`.
public struct Folder: Jazz.Row {
    public static let table = "folders"
    public let id: Jazz.ID
    public var name: Swift.String

    public init(id: Jazz.ID, name: Swift.String) {
        self.id = id
        self.name = name
    }

    enum CodingKeys: Swift.String, Swift.CodingKey {
        case id
        case name = "name"
    }
}

/// A new row of `folders`: `nil` leaves a column out, which stores its default (or `null`).
public struct NewFolder: Jazz.Insert {
    public typealias Row = Folder
    public var name: Swift.String

    public init(name: Swift.String) {
        self.name = name
    }

    enum CodingKeys: Swift.String, Swift.CodingKey {
        case name = "name"
    }
}

/// A change to a row of `folders`: only the columns set to `.set(…)` change; `.set(nil)` stores `null`.
public struct FolderPatch: Jazz.Patch {
    public typealias Row = Folder
    public var name: Jazz.Change<Swift.String>

    public init(name: Jazz.Change<Swift.String> = .unchanged) {
        self.name = name
    }

    enum CodingKeys: Swift.String, Swift.CodingKey {
        case name = "name"
    }

    public func encode(to encoder: any Swift.Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encodeChange(self.name, forKey: .name)
    }
}

/// Queries of `folders` and its columns.
/// (Its relations are left out: includes are not supported in Swift yet.)
public enum Folders {
    /// Every row of `folders`; narrow it with `where`, `orderBy`, `limit` and `offset`.
    public static func query() -> Jazz.Query<Folder> { Jazz.Query() }

    /// The row's id.
    public static let id = Jazz.Column<Folder, Jazz.ID>("id")
    /// Column `name`.
    public static let name = Jazz.Column<Folder, Swift.String>("name")
}

/// A row of `notes`.
public struct Note: Jazz.Row {
    public static let table = "notes"
    public let id: Jazz.ID
    public var body: Swift.String
    public var pinned: Swift.Bool
    public var folderId: Jazz.ID?

    public init(id: Jazz.ID, body: Swift.String, pinned: Swift.Bool, folderId: Jazz.ID?) {
        self.id = id
        self.body = body
        self.pinned = pinned
        self.folderId = folderId
    }

    enum CodingKeys: Swift.String, Swift.CodingKey {
        case id
        case body = "body"
        case pinned = "pinned"
        case folderId = "folderId"
    }
}

/// A new row of `notes`: `nil` leaves a column out, which stores its default (or `null`).
public struct NewNote: Jazz.Insert {
    public typealias Row = Note
    public var body: Swift.String
    public var pinned: Swift.Bool?
    public var folderId: Jazz.ID?

    public init(body: Swift.String, pinned: Swift.Bool? = nil, folderId: Jazz.ID? = nil) {
        self.body = body
        self.pinned = pinned
        self.folderId = folderId
    }

    enum CodingKeys: Swift.String, Swift.CodingKey {
        case body = "body"
        case pinned = "pinned"
        case folderId = "folderId"
    }
}

/// A change to a row of `notes`: only the columns set to `.set(…)` change; `.set(nil)` stores `null`.
public struct NotePatch: Jazz.Patch {
    public typealias Row = Note
    public var body: Jazz.Change<Swift.String>
    public var pinned: Jazz.Change<Swift.Bool>
    public var folderId: Jazz.Change<Jazz.ID?>

    public init(body: Jazz.Change<Swift.String> = .unchanged, pinned: Jazz.Change<Swift.Bool> = .unchanged, folderId: Jazz.Change<Jazz.ID?> = .unchanged) {
        self.body = body
        self.pinned = pinned
        self.folderId = folderId
    }

    enum CodingKeys: Swift.String, Swift.CodingKey {
        case body = "body"
        case pinned = "pinned"
        case folderId = "folderId"
    }

    public func encode(to encoder: any Swift.Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encodeChange(self.body, forKey: .body)
        try container.encodeChange(self.pinned, forKey: .pinned)
        try container.encodeChange(self.folderId, forKey: .folderId)
    }
}

/// Queries of `notes` and its columns.
/// (Its relations are left out: includes are not supported in Swift yet.)
public enum Notes {
    /// Every row of `notes`; narrow it with `where`, `orderBy`, `limit` and `offset`.
    public static func query() -> Jazz.Query<Note> { Jazz.Query() }

    /// The row's id.
    public static let id = Jazz.Column<Note, Jazz.ID>("id")
    /// Column `body`.
    public static let body = Jazz.Column<Note, Swift.String>("body")
    /// Column `pinned`.
    public static let pinned = Jazz.Column<Note, Swift.Bool>("pinned")
    /// Column `folderId`.
    public static let folderId = Jazz.Column<Note, Jazz.ID?>("folderId")
}

/// The app: its `jazz.app.json`, and opening it.
public enum JazzApp {
    /// `sha256:…` of the `jazz.app.json` these bindings were generated from.
    public static let source = "sha256:320aeff40326a9b6076a5196c5e24d44d5eaec47c105abdecae9c0b5d77a49ae"

    /// The app's `jazz.app.json`: its schema, every earlier version and the
    /// migrations between them.
    public static let app = #"""
{
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
"""#

    /// Opens the app. A backend (`config.permissions` set to its
    /// `jazz.permissions.json`) first publishes the app's permissions bundle
    /// and pins the session it acts in to it.
    public static func open(_ config: Jazz.Config) async throws -> Jazz.Client {
        try await Jazz.Client.open(app: app, config: config)
    }
}
