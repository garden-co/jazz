import Foundation
import Jazz
import StagePlan
import XCTest

final class QueryShapesTests: XCTestCase {
    let user = "00000000-0000-4000-8000-0000000000f1"

    struct NotesHead: Decodable, Sendable {
        var id: String
        var notes: String?
    }

    func testHopsUnionHistoryDeletedPartsAndUploads() async throws {
        let server = try await TestServer()
        let app = try await StagePlanApp.open(user: user, server: server.server)
        let show = try await app.show(named: "Hops")
        let load = try await app.addTask("Load in", to: show)
        let sound = try await app.addTask("Sound check", to: show)
        try await app.move(sound.id, to: .blocked)
        let tasks = JazzApp.tasks.self

        // hopTo: from a task to its show.
        let shows = try await app.jazz.all(tasks.query().where(tasks.id.eq(load.id)).hopTo(tasks.show))
        XCTAssertEqual(shows.map(\.id), [show.id])

        // union: todo tasks and blocked tasks.
        let union = try await app.jazz.all(
            Union(tasks.query().where(tasks.status.eq(.todo)), tasks.query().where(tasks.status.eq(.blocked)))
                .orderBy(tasks.rank.asc))
        XCTAssertEqual(union.map(\.title), ["Load in", "Sound check"])

        // history: every accepted version of a task, from the server.
        try await app.jazz.update(load.id, JazzApp.TaskPatch(title: .set("Load in early")), tier: .global)
        let versions = try await app.jazz.all(tasks.query().where(tasks.id.eq(load.id)).history())
        XCTAssertGreaterThanOrEqual(versions.count, 2)
        XCTAssertEqual(Set(versions.map(\.row.title)), ["Load in", "Load in early"])
        XCTAssertFalse(versions[0].transactionId.isEmpty)

        // includeDeleted.
        try await app.jazz.delete(sound)
        let live = try await app.jazz.all(tasks.query().where(tasks.showId.eq(show.id)))
        let all = try await app.jazz.all(tasks.query().where(tasks.showId.eq(show.id)).includeDeleted())
        XCTAssertEqual(live.count, 1)
        XCTAssertEqual(all.count, 2)

        // A streamed upload into a text column, then a partial read of it.
        let chunks = AsyncStream<Data> { continuation in
            for part in ["Bring ", "gaffer ", "tape."] { continuation.yield(Data(part.utf8)) }
            continuation.finish()
        }
        let uploaded = try await app.jazz.upload(JazzApp.Task.self, id: load.id, column: "notes", from: chunks)
        XCTAssertEqual(uploaded.notes, "Bring gaffer tape.")
        let head = try await app.jazz.all(
            tasks.query().where(tasks.id.eq(load.id)).select(parts: tasks.notes.part(0..<5, in: .utf16)),
            as: NotesHead.self)
        XCTAssertEqual(head.first?.notes, "Bring")
        try await app.jazz.close()
    }
}

private let treeApp = #"""
    {
      "format": 1,
      "tables": {
        "nodes": {
          "columns": { "name": "string", "parentId": "uuid?" },
          "relations": { "parent": { "rel": "nodes", "column": "parentId" } }
        }
      }
    }
    """#

private let treePermissions = #"""
    { "format": 1, "tables": { "nodes": { "read": "true", "insert": "true" } } }
    """#

private struct Node: Jazz.Row {
    static let table = "nodes"
    let id: Jazz.ID
    var name: String
    var parentId: Jazz.ID?
}

private struct NewNode: Jazz.Insert {
    typealias Row = Node
    var name: String
    var parentId: Jazz.ID?
}

private enum Nodes {
    static let name = Jazz.Column<Node, String>("name")
    static let parent = Jazz.Relation<Node, Node>("parent")
}

final class GatherTests: XCTestCase {
    func testGatherFollowsASelfRelation() async throws {
        let server = try await TestServer()
        let jazz = try await Jazz.Client.open(
            app: treeApp,
            config: .backend(
                appId: server.server.appId, permissions: treePermissions, serverUrl: server.server.url,
                backendSecret: server.server.backendSecret, adminSecret: server.server.adminSecret,
                user: "00000000-0000-4000-8000-0000000000f2"))
        let root = try await jazz.insert(NewNode(name: "root", parentId: nil))
        let mid = try await jazz.insert(NewNode(name: "mid", parentId: root.id))
        _ = try await jazz.insert(NewNode(name: "leaf", parentId: mid.id))
        let chain = try await jazz.all(Query<Node>().where(Nodes.name.eq("leaf")).gather(Nodes.parent))
        XCTAssertEqual(Set(chain.map(\.name)), ["leaf", "mid", "root"])
        let short = try await jazz.all(Query<Node>().where(Nodes.name.eq("leaf")).gather(Nodes.parent, maxDepth: 1))
        XCTAssertEqual(Set(short.map(\.name)), ["leaf", "mid"])
        try await jazz.close()
    }
}
