import Foundation
import Jazz
import StagePlan
import XCTest

/// A small app with a `uuid[]` forward relation, typed as the generator
/// types it (`[Tag]?`), to include against a real server.
private let tagsApp = #"""
    {
      "format": 1,
      "tables": {
        "tags": { "columns": { "name": "string" } },
        "items": {
          "columns": { "title": "string", "tagIds": { "array": "uuid" } },
          "relations": { "tags": { "rel": "tags", "column": "tagIds" } }
        }
      }
    }
    """#

private let tagsPermissions = #"""
    {
      "format": 1,
      "tables": {
        "tags": { "read": "true", "insert": "true" },
        "items": { "read": "true", "insert": "true" }
      }
    }
    """#

private struct Tag: Jazz.Row {
    static let table = "tags"
    let id: Jazz.ID
    var name: String
}

private struct NewTag: Jazz.Insert {
    typealias Row = Tag
    var name: String
}

private struct Item: Jazz.Row {
    static let table = "items"
    let id: Jazz.ID
    var title: String
    var tagIds: [Jazz.ID]
    var tags: [Tag]? = nil
}

private struct NewItem: Jazz.Insert {
    typealias Row = Item
    var title: String
    var tagIds: [Jazz.ID]
}

private enum Items {
    static let tags = Jazz.Relation<Item, Tag>("tags")
    static let id = Jazz.Column<Item, Jazz.ID>("id")
}

final class IncludeShapesTests: XCTestCase {
    let user = "00000000-0000-4000-8000-0000000000cc"

    func testNestedIncludesOfTwoForwardRelations() async throws {
        let relay = try await TestRelay()
        let app = try await relay.user()
        let show = try await app.show(named: "Nested")
        let me = try await app.jazz.account()
        let crew = try await app.jazz.insert(JazzApp.NewCrew(account: me, name: "Sam"))
        // Permissions let a task be assigned only to the show's crew.
        try await app.jazz.insert(
            JazzApp.NewShowCrew(showId: show.id, crewId: crew.id, account: me, role: .chief))
        let task = try await app.addTask("Load in", to: show)
        // At the global tier, a refused write fails here instead of rolling back later.
        try await app.jazz.update(task.id, JazzApp.TaskPatch(assigneeId: .set(crew.id)), tier: .global)
        try await app.addTask("Unassigned", to: show)

        let shows = JazzApp.shows.self
        let tasks = JazzApp.tasks.self
        let found = try await app.jazz.one(
            shows.query()
                .where(shows.id.eq(show.id))
                .include(shows.tasks.orderBy(tasks.rank).include(tasks.show, tasks.assignee)))
        let included = try XCTUnwrap(found?.tasks)
        XCTAssertEqual(included.map(\.title), ["Load in", "Unassigned"])
        XCTAssertEqual(included.map { $0.show?.id }, [show.id, show.id])
        XCTAssertEqual(included.map { $0.assignee?.name }, ["Sam", nil])
        try await app.jazz.close()
    }

    func testAnArrayOfIdsIncludesAsAnArray() async throws {
        let relay = try await TestRelay(app: tagsApp, permissions: tagsPermissions)
        let jazz = try await relay.client(auth: .deviceKey(try Jazz.generateDeviceKey()))
        let red = try await jazz.insert(NewTag(name: "red"))
        let blue = try await jazz.insert(NewTag(name: "blue"))
        let item = try await jazz.insert(NewItem(title: "Cable", tagIds: [red.id, blue.id]))
        let found = try await jazz.one(Query<Item>().where(Items.id.eq(item.id)).include(Items.tags))
        XCTAssertEqual(Set(try XCTUnwrap(found?.tags).map(\.name)), ["red", "blue"])
        try await jazz.close()
    }
}
