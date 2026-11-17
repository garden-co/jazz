// swift-tools-version:6.0
// The docs' Swift snippets: functions over the docs todo app (todos and
// projects), whose jazz.app.json and jazz.permissions.json `jazz-tools
// generate` writes from schema.ts (a re-export of todo-client-localfirst-ts's),
// with the bindings it wrote in JazzApp.swift. The tests run every snippet.
import PackageDescription

let package = Package(
    name: "todo-client-swift",
    platforms: [.macOS(.v13)],
    dependencies: [
        .package(name: "jazz", path: "../../..")
    ],
    targets: [
        .target(
            name: "TodoDocs",
            dependencies: [.product(name: "Jazz", package: "jazz")],
            path: ".",
            exclude: [
                "README.md", "schema.ts", "jazz.app.json", "jazz.permissions.json", "migrations", "Tests",
            ],
            sources: ["JazzApp.swift", "Sources/TodoDocs"]
        ),
        .testTarget(name: "TodoDocsTests", dependencies: ["TodoDocs"]),
    ]
)
