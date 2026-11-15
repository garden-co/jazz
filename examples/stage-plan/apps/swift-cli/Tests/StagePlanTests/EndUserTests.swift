import Foundation
import Jazz
import StagePlan
import XCTest

final class EndUserTests: XCTestCase {
    func endUser(_ relay: TestRelay, _ server: TestServer, auth: Jazz.Auth) async throws -> Jazz.Client {
        try await JazzApp.open(.client(appId: server.server.appId, serverUrl: relay.endpoint, auth: auth, storage: .memory))
    }

    func firstSignedIn(_ jazz: Jazz.Client) async throws -> Jazz.User {
        for try await state in jazz.session {
            if case .signedIn(let user) = state { return user }
        }
        throw TestServerError("the session ended before signing in")
    }

    func testADeviceAccountSignsInAndSyncs() async throws {
        let server = try await TestServer()
        let relay = try await TestRelay(server)
        let key = try Jazz.generateDeviceKey()
        let carol = try await endUser(relay, server, auth: .deviceKey(key))
        let user = try await firstSignedIn(carol)
        XCTAssertEqual(user.kind, .device)
        let me = try await carol.account()
        XCTAssertEqual(me, user.account)
        let show = try await carol.insert(
            JazzApp.NewShow(name: "Carol's", venue: "Hall", date: "2026-11-16", doors: "19:00", chiefAccount: me),
            tier: .global)
        try await carol.waitForSync(.global)
        let status = try await carol.syncStatus()
        XCTAssertEqual(status.link, .connected)
        XCTAssertEqual(status.pendingUploads, 0)
        XCTAssertTrue(status.synced)
        let fromServer = try await carol.one(JazzApp.shows.query().where(JazzApp.shows.id.eq(show.id)), tier: .remote)
        XCTAssertEqual(fromServer?.name, "Carol's")
        try await carol.close()

        // The same key signs in as the same account again.
        let again = try await endUser(relay, server, auth: .deviceKey(key))
        let sameUser = try await firstSignedIn(again)
        XCTAssertEqual(sameUser.account, user.account)
        try await again.close()
    }

    final class Reasons: @unchecked Sendable {
        let lock = NSLock()
        var all: [Jazz.TokenReason] = []
        func add(_ r: Jazz.TokenReason) { lock.withLock { all.append(r) } }
        var seen: [Jazz.TokenReason] { lock.withLock { all } }
    }

    func testTheAppsSignInGivesBearerTokens() async throws {
        let server = try await TestServer()
        let relay = try await TestRelay(server)
        let reasons = Reasons()
        let alice = try await endUser(relay, server, auth: .bearer { reason in
            reasons.add(reason)
            return "token-alice"
        })
        let user = try await firstSignedIn(alice)
        XCTAssertEqual(user.kind, .backend)
        XCTAssertEqual(user.subject, "alice")
        XCTAssertTrue(reasons.seen.contains(.connect))
        try await alice.refresh()
        XCTAssertTrue(reasons.seen.contains(.refresh))
        let state = try await alice.sessionState()
        XCTAssertEqual(state, .signedIn(user))
        try await alice.signOut()
        do {
            _ = try await alice.all(JazzApp.shows.query())
            XCTFail("signOut closes the app")
        } catch {
            XCTAssertEqual(error, .closed)
        }
    }

    func testADeviceAccountLinksToTheAppsSignIn() async throws {
        let server = try await TestServer()
        let relay = try await TestRelay(server)
        let device = try await endUser(relay, server, auth: .deviceKey(try Jazz.generateDeviceKey()))
        _ = try await firstSignedIn(device)
        let linked = try await device.link(bearer: "token-bob")
        XCTAssertEqual(linked.subject, "bob")
        try await device.close()
    }
}
