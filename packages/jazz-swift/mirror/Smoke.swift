// The mirror's smoke test: a local app, a write and a read through the
// prebuilt Jazz core. Used by .github/workflows/swift-mirror.yml and by
// the Mac test commands in packages/jazz-swift/README.md.
import Jazz

struct Note: Jazz.Row {
    static let table = "notes"
    let id: Jazz.ID
    var text: String
}

struct NewNote: Jazz.Insert {
    typealias Row = Note
    var text: String
}

let app = #"""
    { "format": 1, "tables": { "notes": { "columns": { "text": "string" } } } }
    """#

// Not top-level code: Swift 6.1 (Xcode 16) crashes compiling top-level
// async code that calls Jazz's typed-throws methods.
@main
struct Smoke {
    static func main() async throws {
        let jazz = try await Jazz.Client.open(app: app, config: .local())
        let note = try await jazz.insert(NewNote(text: "hello from the prebuilt Jazz core"))
        let read = try await jazz.all(Query<Note>())
        precondition(read.map(\.id) == [note.id], "read back \(read)")
        print("ok:", read[0].text)
        try await jazz.close()
    }
}
