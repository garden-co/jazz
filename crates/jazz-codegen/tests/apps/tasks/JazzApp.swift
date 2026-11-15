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
    public static let source = "sha256:019eebe8a4aa9f75919d6cfd18bbc36487719aef45b19c6b77175373bd416977"

    /// The app's `jazz.app.json`: its schema, every earlier version and the
    /// migrations between them.
    public static let app = #"""
{
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
"""#

    /// Opens the app. A backend (`config.permissions` set to its
    /// `jazz.permissions.json`) first publishes the app's permissions bundle
    /// and pins the session it acts in to it.
    public static func open(_ config: Jazz.Config, computed: some Computed) async throws -> Jazz.Client {
        try await Jazz.Client.open(app: app, config: config, implementations: computed)
    }

    /// The values of computed field `tasks.priority`.
    public enum TaskPriority: Swift.String, Swift.Codable, Swift.Sendable, Swift.Hashable, Swift.CaseIterable {
        case low = "low"
        case high = "high"
    }

    /// The values of column `tasks.status`.
    public enum TaskStatus: Swift.String, Swift.Codable, Swift.Sendable, Swift.Hashable, Swift.CaseIterable {
        case todo = "todo"
        case doing = "doing"
        case done = "done"
    }

    /// A row of `people`.
    public struct Person: Jazz.Row {
        public static let table = "people"
        public let id: Jazz.ID
        public var name: Swift.String
        public var email: Swift.String
        /// Relation `tasksViaAssignees`: the `Task` rows, when the query includes them.
        public var tasksViaAssignees: [Task]? = nil

        public init(id: Jazz.ID, name: Swift.String, email: Swift.String) {
            self.id = id
            self.name = name
            self.email = email
        }

        /// Equal by id, columns and computed fields: included relations don't count.
        public static func == (a: Self, b: Self) -> Swift.Bool {
            a.id == b.id && a.name == b.name && a.email == b.email
        }

        public func hash(into hasher: inout Swift.Hasher) {
            hasher.combine(id)
            hasher.combine(name)
            hasher.combine(email)
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case id
            case name = "name"
            case email = "email"
            case tasksViaAssignees = "tasksViaAssignees"
        }

        public init(from decoder: any Swift.Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            let computed = try Jazz.implementations(of: (any Computed).self, in: decoder.userInfo)
            self.id = try container.decode(Jazz.ID.self, forKey: .id)
            self.name = try container.decode(Swift.String.self, forKey: .name)
            self.email = computed.lowercaseView(try container.decode(Swift.String.self, forKey: .email))
            self.tasksViaAssignees = try container.decodeIfPresent([Task].self, forKey: .tasksViaAssignees)
        }

        public func encode(to encoder: any Swift.Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            let computed = try Jazz.implementations(of: (any Computed).self, in: encoder.userInfo)
            try container.encode(self.id, forKey: .id)
            try container.encode(self.name, forKey: .name)
            try container.encode(computed.lowercaseStored(self.email), forKey: .email)
            try container.encodeIfPresent(self.tasksViaAssignees, forKey: .tasksViaAssignees)
            _ = computed
        }
    }

    /// A new row of `people`: `nil` leaves a column out, which stores its default (or `null`).
    public struct NewPerson: Jazz.Insert {
        public typealias Row = Person
        public var name: Swift.String
        public var email: Swift.String

        public init(name: Swift.String, email: Swift.String) {
            self.name = name
            self.email = email
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case name = "name"
            case email = "email"
        }

        public func encode(to encoder: any Swift.Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            let computed = try Jazz.implementations(of: (any Computed).self, in: encoder.userInfo)
            try container.encode(self.name, forKey: .name)
            try container.encode(computed.lowercaseStored(self.email), forKey: .email)
            _ = computed
        }
    }

    /// A change to a row of `people`: only the columns set to `.set(…)` change; `.set(nil)` stores `null`.
    public struct PersonPatch: Jazz.Patch {
        public typealias Row = Person
        public var name: Jazz.Change<Swift.String>
        public var email: Jazz.Change<Swift.String>

        public init(name: Jazz.Change<Swift.String> = .unchanged, email: Jazz.Change<Swift.String> = .unchanged) {
            self.name = name
            self.email = email
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case name = "name"
            case email = "email"
        }

        public func encode(to encoder: any Swift.Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            let computed = try Jazz.implementations(of: (any Computed).self, in: encoder.userInfo)
            try container.encodeChange(self.name, forKey: .name)
            try container.encodeChange(self.email.map { computed.lowercaseStored($0) }, forKey: .email)
        }
    }

    /// Queries of `people` and its columns.
    public enum people {
        /// Every row of `people`; narrow it with `where`, `orderBy`, `limit` and `offset`.
        public static func query() -> Jazz.Query<Person> { Jazz.Query() }

        /// The row's id.
        public static let id = Jazz.Column<Person, Jazz.ID>("id")
        /// Column `name`.
        public static let name = Jazz.Column<Person, Swift.String>("name")
        /// Column `email`, as stored: filters compare stored values.
        public static let email = Jazz.Column<Person, Swift.String>("email")
        /// Relation `tasksViaAssignees`, for `include` (narrow it with `where`, `orderBy`, `limit`, `include`).
        public static let tasksViaAssignees = Jazz.Relation<Person, Task>("tasksViaAssignees")
    }

    /// A row of `projects`.
    public struct Project: Jazz.Row {
        public static let table = "projects"
        public let id: Jazz.ID
        public var name: Swift.String
        public var budget: Jazz.BigInt?
        public var logo: Foundation.Data?
        /// Relation `tasksViaProject`: the `Task` rows, when the query includes them.
        public var tasksViaProject: [Task]? = nil

        public init(id: Jazz.ID, name: Swift.String, budget: Jazz.BigInt?, logo: Foundation.Data?) {
            self.id = id
            self.name = name
            self.budget = budget
            self.logo = logo
        }

        /// Equal by id, columns and computed fields: included relations don't count.
        public static func == (a: Self, b: Self) -> Swift.Bool {
            a.id == b.id && a.name == b.name && a.budget == b.budget && a.logo == b.logo
        }

        public func hash(into hasher: inout Swift.Hasher) {
            hasher.combine(id)
            hasher.combine(name)
            hasher.combine(budget)
            hasher.combine(logo)
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case id
            case name = "name"
            case budget = "budget"
            case logo = "logo"
            case tasksViaProject = "tasksViaProject"
        }
    }

    /// A new row of `projects`: `nil` leaves a column out, which stores its default (or `null`).
    public struct NewProject: Jazz.Insert {
        public typealias Row = Project
        public var name: Swift.String
        public var budget: Jazz.BigInt?
        public var logo: Foundation.Data?

        public init(name: Swift.String, budget: Jazz.BigInt? = nil, logo: Foundation.Data? = nil) {
            self.name = name
            self.budget = budget
            self.logo = logo
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case name = "name"
            case budget = "budget"
            case logo = "logo"
        }
    }

    /// A change to a row of `projects`: only the columns set to `.set(…)` change; `.set(nil)` stores `null`.
    public struct ProjectPatch: Jazz.Patch {
        public typealias Row = Project
        public var name: Jazz.Change<Swift.String>
        public var budget: Jazz.Change<Jazz.BigInt?>
        public var logo: Jazz.Change<Foundation.Data?>

        public init(name: Jazz.Change<Swift.String> = .unchanged, budget: Jazz.Change<Jazz.BigInt?> = .unchanged, logo: Jazz.Change<Foundation.Data?> = .unchanged) {
            self.name = name
            self.budget = budget
            self.logo = logo
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case name = "name"
            case budget = "budget"
            case logo = "logo"
        }

        public func encode(to encoder: any Swift.Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encodeChange(self.name, forKey: .name)
            try container.encodeChange(self.budget, forKey: .budget)
            try container.encodeChange(self.logo, forKey: .logo)
        }
    }

    /// Queries of `projects` and its columns.
    public enum projects {
        /// Every row of `projects`; narrow it with `where`, `orderBy`, `limit` and `offset`.
        public static func query() -> Jazz.Query<Project> { Jazz.Query() }

        /// The row's id.
        public static let id = Jazz.Column<Project, Jazz.ID>("id")
        /// Column `name`.
        public static let name = Jazz.Column<Project, Swift.String>("name")
        /// Column `budget`.
        public static let budget = Jazz.Column<Project, Jazz.BigInt?>("budget")
        /// Column `logo`.
        public static let logo = Jazz.Column<Project, Foundation.Data?>("logo")
        /// Relation `tasksViaProject`, for `include` (narrow it with `where`, `orderBy`, `limit`, `include`).
        public static let tasksViaProject = Jazz.Relation<Project, Task>("tasksViaProject")
    }

    /// A row of `tasks`.
    public struct Task: Jazz.Row {
        public static let table = "tasks"
        public let id: Jazz.ID
        public var title: Swift.String
        public var projectId: Jazz.ID
        public var status: TaskStatus
        public var score: Swift.Int
        public var tags: [Swift.String]
        public var dueAt: Swift.String?
        public var meta: Jazz.JSONValue?
        public var extra: Jazz.JSONValue?
        public var assigneeIds: [Jazz.ID]
        /// Computed from `score`, `status` by `JazzApp.Computed.tasksPriority`.
        public internal(set) var priority: TaskPriority
        /// Computed from `title`, `dueAt` by `JazzApp.Computed.tasksLabel`.
        public internal(set) var label: Swift.String?
        /// Relation `assignees`: the `Person` rows, when the query includes them.
        public var assignees: [Person]? = nil
        /// Relation `project`: the `Project` row, when the query includes it (and it exists).
        public var project: Project? {
            get { _project?.value }
            set { _project = newValue.map { Jazz.Included($0) } }
        }
        private var _project: Jazz.Included<Project>? = nil

        public init(id: Jazz.ID, title: Swift.String, projectId: Jazz.ID, status: TaskStatus, score: Swift.Int, tags: [Swift.String], dueAt: Swift.String?, meta: Jazz.JSONValue?, extra: Jazz.JSONValue?, assigneeIds: [Jazz.ID], priority: TaskPriority, label: Swift.String?) {
            self.id = id
            self.title = title
            self.projectId = projectId
            self.status = status
            self.score = score
            self.tags = tags
            self.dueAt = dueAt
            self.meta = meta
            self.extra = extra
            self.assigneeIds = assigneeIds
            self.priority = priority
            self.label = label
        }

        /// Equal by id, columns and computed fields: included relations don't count.
        public static func == (a: Self, b: Self) -> Swift.Bool {
            a.id == b.id && a.title == b.title && a.projectId == b.projectId && a.status == b.status && a.score == b.score && a.tags == b.tags && a.dueAt == b.dueAt && a.meta == b.meta && a.extra == b.extra && a.assigneeIds == b.assigneeIds && a.priority == b.priority && a.label == b.label
        }

        public func hash(into hasher: inout Swift.Hasher) {
            hasher.combine(id)
            hasher.combine(title)
            hasher.combine(projectId)
            hasher.combine(status)
            hasher.combine(score)
            hasher.combine(tags)
            hasher.combine(dueAt)
            hasher.combine(meta)
            hasher.combine(extra)
            hasher.combine(assigneeIds)
            hasher.combine(priority)
            hasher.combine(label)
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case id
            case title = "title"
            case projectId = "projectId"
            case status = "status"
            case score = "score"
            case tags = "tags"
            case dueAt = "dueAt"
            case meta = "meta"
            case extra = "extra"
            case assigneeIds = "assigneeIds"
            case assignees = "assignees"
            case _project = "project"
        }

        public init(from decoder: any Swift.Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            let computed = try Jazz.implementations(of: (any Computed).self, in: decoder.userInfo)
            self.id = try container.decode(Jazz.ID.self, forKey: .id)
            self.title = try container.decode(Swift.String.self, forKey: .title)
            self.projectId = try container.decode(Jazz.ID.self, forKey: .projectId)
            self.status = try container.decode(TaskStatus.self, forKey: .status)
            self.score = try container.decode(Swift.Int.self, forKey: .score)
            self.tags = try container.decode([Swift.String].self, forKey: .tags)
            self.dueAt = (try container.decodeIfPresent(Foundation.Date.self, forKey: .dueAt)).map { computed.isoDateView($0) }
            self.meta = try container.decodeIfPresent(Jazz.JSONValue.self, forKey: .meta)
            self.extra = try container.decodeIfPresent(Jazz.JSONValue.self, forKey: .extra)
            self.assigneeIds = try container.decode([Jazz.ID].self, forKey: .assigneeIds)
            self.assignees = try container.decodeIfPresent([Person].self, forKey: .assignees)
            self._project = try container.decodeIfPresent(Jazz.Included<Project>.self, forKey: ._project)
            self.priority = computed.tasksPriority(score: self.score, status: self.status)
            self.label = computed.tasksLabel(title: self.title, dueAt: self.dueAt)
        }

        public func encode(to encoder: any Swift.Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            let computed = try Jazz.implementations(of: (any Computed).self, in: encoder.userInfo)
            try container.encode(self.id, forKey: .id)
            try container.encode(self.title, forKey: .title)
            try container.encode(self.projectId, forKey: .projectId)
            try container.encode(self.status, forKey: .status)
            try container.encode(self.score, forKey: .score)
            try container.encode(self.tags, forKey: .tags)
            try container.encodeIfPresent(self.dueAt.map { computed.isoDateStored($0) }, forKey: .dueAt)
            try container.encodeIfPresent(self.meta, forKey: .meta)
            try container.encodeIfPresent(self.extra, forKey: .extra)
            try container.encode(self.assigneeIds, forKey: .assigneeIds)
            try container.encodeIfPresent(self.assignees, forKey: .assignees)
            try container.encodeIfPresent(self._project, forKey: ._project)
            _ = computed
        }
    }

    /// A new row of `tasks`: `nil` leaves a column out, which stores its default (or `null`).
    public struct NewTask: Jazz.Insert {
        public typealias Row = Task
        public var title: Swift.String
        public var projectId: Jazz.ID
        public var status: TaskStatus?
        public var score: Swift.Int?
        public var tags: [Swift.String]
        public var dueAt: Swift.String?
        public var meta: Jazz.JSONValue?
        public var extra: Jazz.JSONValue?
        public var assigneeIds: [Jazz.ID]

        public init(title: Swift.String, projectId: Jazz.ID, status: TaskStatus? = nil, score: Swift.Int? = nil, tags: [Swift.String], dueAt: Swift.String? = nil, meta: Jazz.JSONValue? = nil, extra: Jazz.JSONValue? = nil, assigneeIds: [Jazz.ID]) {
            self.title = title
            self.projectId = projectId
            self.status = status
            self.score = score
            self.tags = tags
            self.dueAt = dueAt
            self.meta = meta
            self.extra = extra
            self.assigneeIds = assigneeIds
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case title = "title"
            case projectId = "projectId"
            case status = "status"
            case score = "score"
            case tags = "tags"
            case dueAt = "dueAt"
            case meta = "meta"
            case extra = "extra"
            case assigneeIds = "assigneeIds"
        }

        public func encode(to encoder: any Swift.Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            let computed = try Jazz.implementations(of: (any Computed).self, in: encoder.userInfo)
            try container.encode(self.title, forKey: .title)
            try container.encode(self.projectId, forKey: .projectId)
            try container.encodeIfPresent(self.status, forKey: .status)
            try container.encodeIfPresent(self.score, forKey: .score)
            try container.encode(self.tags, forKey: .tags)
            try container.encodeIfPresent(self.dueAt.map { computed.isoDateStored($0) }, forKey: .dueAt)
            try container.encodeIfPresent(self.meta, forKey: .meta)
            try container.encodeIfPresent(self.extra, forKey: .extra)
            try container.encode(self.assigneeIds, forKey: .assigneeIds)
            _ = computed
        }
    }

    /// A change to a row of `tasks`: only the columns set to `.set(…)` change; `.set(nil)` stores `null`.
    public struct TaskPatch: Jazz.Patch {
        public typealias Row = Task
        public var title: Jazz.Change<Swift.String>
        public var projectId: Jazz.Change<Jazz.ID>
        public var status: Jazz.Change<TaskStatus>
        public var score: Jazz.Change<Swift.Int>
        public var tags: Jazz.Change<[Swift.String]>
        public var dueAt: Jazz.Change<Swift.String?>
        public var meta: Jazz.Change<Jazz.JSONValue?>
        public var extra: Jazz.Change<Jazz.JSONValue?>
        public var assigneeIds: Jazz.Change<[Jazz.ID]>

        public init(title: Jazz.Change<Swift.String> = .unchanged, projectId: Jazz.Change<Jazz.ID> = .unchanged, status: Jazz.Change<TaskStatus> = .unchanged, score: Jazz.Change<Swift.Int> = .unchanged, tags: Jazz.Change<[Swift.String]> = .unchanged, dueAt: Jazz.Change<Swift.String?> = .unchanged, meta: Jazz.Change<Jazz.JSONValue?> = .unchanged, extra: Jazz.Change<Jazz.JSONValue?> = .unchanged, assigneeIds: Jazz.Change<[Jazz.ID]> = .unchanged) {
            self.title = title
            self.projectId = projectId
            self.status = status
            self.score = score
            self.tags = tags
            self.dueAt = dueAt
            self.meta = meta
            self.extra = extra
            self.assigneeIds = assigneeIds
        }

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case title = "title"
            case projectId = "projectId"
            case status = "status"
            case score = "score"
            case tags = "tags"
            case dueAt = "dueAt"
            case meta = "meta"
            case extra = "extra"
            case assigneeIds = "assigneeIds"
        }

        public func encode(to encoder: any Swift.Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            let computed = try Jazz.implementations(of: (any Computed).self, in: encoder.userInfo)
            try container.encodeChange(self.title, forKey: .title)
            try container.encodeChange(self.projectId, forKey: .projectId)
            try container.encodeChange(self.status, forKey: .status)
            try container.encodeChange(self.score, forKey: .score)
            try container.encodeChange(self.tags, forKey: .tags)
            try container.encodeChange(self.dueAt.map { $0.map { computed.isoDateStored($0) } }, forKey: .dueAt)
            try container.encodeChange(self.meta, forKey: .meta)
            try container.encodeChange(self.extra, forKey: .extra)
            try container.encodeChange(self.assigneeIds, forKey: .assigneeIds)
        }
    }

    /// Queries of `tasks` and its columns.
    public enum tasks {
        /// Every row of `tasks`; narrow it with `where`, `orderBy`, `limit` and `offset`.
        public static func query() -> Jazz.Query<Task> { Jazz.Query() }

        /// The row's id.
        public static let id = Jazz.Column<Task, Jazz.ID>("id")
        /// Column `title`.
        public static let title = Jazz.Column<Task, Swift.String>("title")
        /// Column `projectId`.
        public static let projectId = Jazz.Column<Task, Jazz.ID>("projectId")
        /// Column `status`.
        public static let status = Jazz.Column<Task, TaskStatus>("status")
        /// Column `score`.
        public static let score = Jazz.Column<Task, Swift.Int>("score")
        /// Column `tags`.
        public static let tags = Jazz.Column<Task, [Swift.String]>("tags")
        /// Column `dueAt`, as stored: filters compare stored values.
        public static let dueAt = Jazz.Column<Task, Foundation.Date?>("dueAt")
        /// Column `meta`.
        public static let meta = Jazz.Column<Task, Jazz.JSONValue?>("meta")
        /// Column `extra`.
        public static let extra = Jazz.Column<Task, Jazz.JSONValue?>("extra")
        /// Column `assigneeIds`.
        public static let assigneeIds = Jazz.Column<Task, [Jazz.ID]>("assigneeIds")
        /// Relation `assignees`, for `include` (narrow it with `where`, `orderBy`, `limit`, `include`).
        public static let assignees = Jazz.Relation<Task, Person>("assignees")
        /// Relation `project`, for `include` (narrow it with `where`, `orderBy`, `limit`, `include`).
        public static let project = Jazz.Relation<Task, Project>("project")
    }

    /// What the app's code computes: each computed field `jazz.app.json`
    /// declares, and each transform both ways. `JazzApp.open(_:computed:)` takes an
    /// implementation; a missing method is a compile error.
    public protocol Computed: Swift.Sendable {
        /// `tasks.priority`.
        func tasksPriority(score: Swift.Int, status: TaskStatus) -> TaskPriority

        /// `tasks.label`.
        func tasksLabel(title: Swift.String, dueAt: Swift.String?) -> Swift.String?

        /// Transform `isoDate`: a stored value, as the app reads it.
        func isoDateView(_ stored: Foundation.Date) -> Swift.String

        /// Transform `isoDate`: a value the app writes, as stored.
        func isoDateStored(_ view: Swift.String) -> Foundation.Date

        /// Transform `lowercase`: a stored value, as the app reads it.
        func lowercaseView(_ stored: Swift.String) -> Swift.String

        /// Transform `lowercase`: a value the app writes, as stored.
        func lowercaseStored(_ view: Swift.String) -> Swift.String
    }
}
