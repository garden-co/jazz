import Foundation
import XCTest

@testable import Jazz

/// The app's code for `tests/apps/computed` (ComputedApp.swift).
struct AppComputed: JazzApp.Computed {
    func tasksPriority(score: Int) -> JazzApp.TaskPriority { score > 5 ? .high : .low }
    func tasksLabel(title: String, dueAt: String?) -> String? { dueAt.map { "\(title) (\($0))" } }
    func isoDateView(_ stored: Date) -> String { ISO8601DateFormatter().string(from: stored) }
    func isoDateStored(_ view: String) -> Date { ISO8601DateFormatter().date(from: view)! }
    func lowercaseView(_ stored: String) -> String { stored }
    func lowercaseStored(_ view: String) -> String { view.lowercased() }
}

final class ComputedTests: XCTestCase {
    let userInfo: [CodingUserInfoKey: any Sendable] = [implementationsKey: AppComputed()]

    func testRowsComputeFieldsAndViewTransformedColumns() throws {
        let json = #"{"id":"t1","title":"Load in","score":7,"dueAt":0}"#
        let task = try Coding.decode(JazzApp.Task.self, json, userInfo)
        XCTAssertEqual(task.priority, .high)
        XCTAssertEqual(task.dueAt, "1970-01-01T00:00:00Z")
        XCTAssertEqual(task.label, "Load in (1970-01-01T00:00:00Z)")
        let none = try Coding.decode(
            JazzApp.Task.self, #"{"id":"t2","title":"x","score":1,"dueAt":null}"#, userInfo)
        XCTAssertNil(none.dueAt)
        XCTAssertNil(none.label)
        XCTAssertEqual(none.priority, .low)
        // A row encodes its stored form, without computed fields.
        XCTAssertEqual(
            try Coding.json(task, userInfo), #"{"dueAt":0,"id":"t1","score":7,"title":"Load in"}"#)
    }

    func testInsertsAndPatchesWriteTheStoredForm() throws {
        let insert = JazzApp.NewPerson(name: "Ada", email: "Ada@Example.com")
        XCTAssertEqual(
            try Coding.json(insert, userInfo), #"{"email":"ada@example.com","name":"Ada"}"#)
        let patch = JazzApp.TaskPatch(dueAt: .set("1970-01-01T00:00:01Z"))
        XCTAssertEqual(try Coding.json(patch, userInfo), #"{"dueAt":1000}"#)
        XCTAssertEqual(try Coding.json(JazzApp.TaskPatch(dueAt: .set(nil)), userInfo), #"{"dueAt":null}"#)
    }

    func testWithoutAnImplementationDecodingSaysWhatToDo() {
        XCTAssertThrowsError(
            try Coding.decode(JazzApp.Task.self, #"{"id":"t","title":"x","score":1}"#)
        ) { error in
            XCTAssertTrue("\(error)".contains("JazzApp.open(_:computed:)"), "\(error)")
        }
    }

    func testTransformedColumnsFilterByTheStoredType() throws {
        let query = JazzApp.tasks.query().where(JazzApp.tasks.dueAt.gt(Date(timeIntervalSince1970: 1)))
        XCTAssertTrue(try query.json().contains(#""value":1000"#))
    }
}
