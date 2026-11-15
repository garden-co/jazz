import Foundation
import Jazz
import StagePlan
import XCTest

final class StagePlanTests: XCTestCase {
    let user = "00000000-0000-4000-8000-0000000000aa"

    func testTheCliSlice() async throws {
        let server = try await TestServer()
        let app = try await StagePlanApp.open(user: user, server: server.server)
        let show = try await app.show(named: "Opening night")
        let me = try await app.jazz.account()
        XCTAssertEqual(show.chiefAccount, me)
        let v1 = try await app.show(named: "Opening night").id
        XCTAssertEqual(v1, show.id)
        let load = try await app.addTask("Load in", to: show)
        let sound = try await app.addTask("Sound check", to: show)
        let v2 = try await app.tasks(of: show).map(\.id)
        XCTAssertEqual(v2, [load.id, sound.id])
        try await app.move(load.id, to: .doing)
        let tasks = try await app.tasks(of: show)
        XCTAssertEqual(tasks.map(\.status), [.doing, .todo])
        XCTAssertLessThan(tasks[0].rank, tasks[1].rank)
        try await app.jazz.close()
    }

    func testTheDocsSnippets() async throws {
        let server = try await TestServer()
        let jazz = try await DocsSnippets.openBackend(server: server.server, userId: user)
        let app = StagePlanApp(jazz: jazz)
        let show = try await app.show(named: "Docs")
        let load = try await app.addTask("Load in", to: show)
        try await app.addTask("Sound check", to: show)
        let v3 = try await DocsSnippets.oneShot(jazz, show: show).count
        XCTAssertEqual(v3, 2)
        let v4 = try await DocsSnippets.filters(jazz, show: show).count
        XCTAssertEqual(v4, 2)
        let v5 = try await DocsSnippets.sorting(jazz, show: show).map(\.title)
        XCTAssertEqual(v5, ["Load in", "Sound check"])
        let v6 = try await DocsSnippets.pagination(jazz, show: show).count
        XCTAssertEqual(v6, 0)
        let updated = try await DocsSnippets.crud(jazz, show: show)
        XCTAssertEqual(updated?.title, "Focus the lights")
        XCTAssertEqual(updated?.status, .doing)
        let v7 = try await DocsSnippets.oneShot(jazz, show: show).count
        XCTAssertEqual(v7, 2)
        try await DocsSnippets.nullableUpdate(jazz, task: load)
        let results = try await DocsSnippets.whereOperators(jazz, show: show)
        XCTAssertEqual(results.map(\.count), [0, 2, 2, 1, 2, 0])
        let seen = try await DocsSnippets.subscriptions(jazz, show: show)
        XCTAssertEqual(seen.first?.map(\.title), ["Load in", "Sound check"])
        XCTAssertEqual(seen.last?.map(\.title), ["Load in", "Sound check", "Strike"])
        try await jazz.close()
    }
}
