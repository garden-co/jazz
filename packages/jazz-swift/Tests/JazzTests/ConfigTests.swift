import Foundation
import XCTest

@testable import Jazz

final class ConfigTests: XCTestCase {
    func testPrintingAConfigRedactsSecrets() {
        let client = Config.client(appId: "a", serverUrl: "https://app", auth: .deviceKey("jazz-auth-v1:SECRET"))
        for text in ["\(client)", String(reflecting: client)] {
            XCTAssertFalse(text.contains("SECRET"), text)
        }
        XCTAssertTrue("\(client)".contains("<redacted>"))
    }

    func testAPersistentClientWithoutADataDirGetsOne() throws {
        let home = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        setenv("XDG_DATA_HOME", home.path, 1)
        defer { unsetenv("XDG_DATA_HOME") }
        let json = try Config.client(appId: "app-1", serverUrl: "https://app", auth: .deviceKey("k")).json(app: "{}")
        #if !canImport(Darwin)
            XCTAssertTrue(json.contains(home.appendingPathComponent("jazz/app-1").path.replacingOccurrences(of: "/", with: "\\/")), json)
            XCTAssertTrue(FileManager.default.fileExists(atPath: home.appendingPathComponent("jazz/app-1").path))
        #endif
        XCTAssertTrue(json.contains(#""storage":"persistent""#))
    }
}

extension ConfigTests {
    func testAClientSendsNoBackendKeys() throws {
        let json = try Config.client(appId: "a", serverUrl: "https://app", auth: .deviceKey("k"), storage: .memory)
            .json(app: "{}")
        for key in ["permissions", "backendSecret", "adminSecret", "session"] {
            XCTAssertFalse(json.contains("\"\(key)\""), json)
        }
    }
}
