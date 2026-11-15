import XCTest
@testable import Jazz

struct Task: Row {
    static let table = "tasks"
    let id: Jazz.ID
    var title: String
    var rank: Double
    var due: Date?
}

enum Tasks {
    static let title = Column<Task, String>("title")
    static let rank = Column<Task, Double>("rank")
    static let due = Column<Task, Date?>("due")
}

final class QueryTests: XCTestCase {
    func testQueryJSON() throws {
        let query = Query<Task>()
            .where(Tasks.title.eq("Load in"), Tasks.due.isNull(), Tasks.rank.in([1, 2.5]))
            .orderBy(Tasks.rank.desc)
            .limit(10)
            .offset(5)
        XCTAssertEqual(
            try query.json(),
            #"{"limit":10,"offset":5,"orderBy":[{"column":"rank","direction":"desc"}],"table":"tasks","where":[{"column":"title","op":"eq","value":"Load in"},{"column":"due","op":"isNull"},{"column":"rank","op":"in","value":[1,2.5]}]}"#
        )
    }

    func testDatesAreMilliseconds() throws {
        let query = Query<Task>().where(Tasks.due.gt(Date(timeIntervalSince1970: 1.5)))
        XCTAssertTrue(try query.json().contains(#""value":1500"#))
    }

    func testLocalSortFollowsOrderBy() {
        let query = Query<Task>().orderBy(Tasks.rank)
        let rows: [[String: JSONValue]] = [
            ["id": .string("b"), "rank": .number(2)],
            ["id": .string("a"), "rank": .number(1)],
            ["id": .string("c"), "rank": .null],
        ]
        XCTAssertEqual(query.sort(rows).map { $0["id"] }, [.string("c"), .string("a"), .string("b")])
    }

    func testChangeEncodesSetNilAsNull() throws {
        struct P: Encodable {
            var notes: Change<String?>
            var title: Change<String>
            enum CodingKeys: String, CodingKey { case notes, title }
            func encode(to encoder: any Encoder) throws {
                var c = encoder.container(keyedBy: CodingKeys.self)
                try c.encodeChange(notes, forKey: .notes)
                try c.encodeChange(title, forKey: .title)
            }
        }
        XCTAssertEqual(try Coding.json(P(notes: .set(nil), title: .unchanged)), #"{"notes":null}"#)
    }

    func testBigIntIsADecimalString() throws {
        XCTAssertEqual(try Coding.json([BigInt(9_007_199_254_740_993)]), #"["9007199254740993"]"#)
    }
}
