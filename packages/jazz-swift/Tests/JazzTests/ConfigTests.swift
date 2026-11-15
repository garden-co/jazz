import Foundation
import XCTest

@_spi(Backend) @testable import Jazz

final class ConfigTests: XCTestCase {
    func testPrintingAConfigRedactsSecrets() {
        let client = Config.client(appId: "a", serverUrl: "https://app", auth: .deviceKey("jazz-auth-v1:SECRET"))
        let backend = Config.backend(
            appId: "a", permissions: "{}", backendSecret: "BS-SECRET", adminSecret: "AS-SECRET", user: "u")
        for text in ["\(client)", String(reflecting: client), "\(backend)", String(reflecting: backend)] {
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
