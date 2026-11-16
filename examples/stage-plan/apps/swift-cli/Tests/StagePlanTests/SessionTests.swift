import Foundation
import Jazz
import StagePlan
import XCTest

/// `JazzSession`, as TS's: device accounts, the app's sign-in through
/// `bearer`, a client per user, and guests taken over on sign-in.
final class SessionTests: XCTestCase {
    struct TimedOut: Error {}

    /// The app's sign-in: the token it gives now (`nil`: signed out).
    final class Token: @unchecked Sendable {
        private let lock = NSLock()
        private var value: String?
        init(_ value: String?) { self.value = value }
        var current: String? {
            get { lock.withLock { value } }
            set { lock.withLock { value = newValue } }
        }
    }

    var dirs: [String] = []

    override func tearDown() {
        for dir in dirs { try? FileManager.default.removeItem(atPath: dir) }
    }

    func open(_ relay: TestRelay, token: Token? = nil, start: JazzSession.Start? = nil, linkGuest: Bool = true)
        async throws -> JazzSession
    {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("stage-plan-session-\(UUID().uuidString)").path
        dirs.append(dir)
        return try await JazzSession.open(
            app: JazzApp.app, appId: relay.appId, serverUrl: relay.endpoint,
            bearer: token.map { token in { @Sendable _ in token.current } }, start: start, linkGuest: linkGuest,
            dataDir: dir)
    }

    /// The first snapshot `matches` accepts.
    func snapshot(of session: JazzSession, where matches: @escaping @Sendable (SessionSnapshot) -> Bool)
        async throws -> SessionSnapshot
    {
        let snapshots = session.snapshots
        return try await withThrowingTaskGroup(of: SessionSnapshot.self) { group in
            group.addTask {
                for try await snapshot in snapshots where matches(snapshot) { return snapshot }
                throw TimedOut()
            }
            group.addTask {
                try await Task.sleep(nanoseconds: 10_000_000_000)
                throw TimedOut()
            }
            defer { group.cancelAll() }
            return try await group.next()!
        }
    }

    func ready(_ session: JazzSession) async throws -> SessionSnapshot {
        try await snapshot(of: session) { $0.status == .ready }
    }

    func testADeviceSessionSignsOutAndReopens() async throws {
        let relay = try await TestRelay()
        let session = try await open(relay)
        let first = try await ready(session)
        let user = try XCTUnwrap(first.user)
        XCTAssertEqual(user.kind, .device)
        let jazz = try await session.client()
        // The same client until the user changes.
        let again = try await session.client()
        XCTAssertTrue(jazz === again)
        let me = try await jazz.account()
        XCTAssertEqual(me, user.account)
        _ = try await jazz.insert(
            JazzApp.NewShow(name: "Mine", venue: "TBA", date: "TBA", doors: "TBA", chiefAccount: me), tier: .global)
        let key = try await session.exportDeviceKey()
        XCTAssertTrue(key.hasPrefix("jazz-auth-v1:"), key)

        try await session.signOut()
        let signedOut = try await snapshot(of: session) { $0.status == .signedOut }
        XCTAssertNil(signedOut.user)
        XCTAssertNotEqual(signedOut.clientEpoch, first.clientEpoch)
        do {
            _ = try await jazz.all(JazzApp.shows.query())
            XCTFail("a client from before the sign-out should be replaced")
        } catch let error as JazzError {
            guard case .replaced = error else { return XCTFail("\(error)") }
        }
        do {
            _ = try await session.client()
            XCTFail("no client while signed out")
        } catch let error as JazzError {
            guard case .noUser = error else { return XCTFail("\(error)") }
        }

        try await session.reopenDeviceAccount()
        let reopened = try await ready(session)
        XCTAssertEqual(reopened.user?.account, user.account)
        let shows = try await session.client().all(JazzApp.shows.query(), tier: .remote)
        XCTAssertEqual(shows.map(\.name), ["Mine"])
        try await session.close()
    }

    func testTheAppsSignInSwitchesUsers() async throws {
        let relay = try await TestRelay()
        let token = Token("token-alice")
        let session = try await open(relay, token: token)
        let alice = try await ready(session)
        XCTAssertEqual(alice.user?.subject, "alice")
        XCTAssertEqual(alice.user?.kind, .backend)
        let aliceClient = try await session.client()

        token.current = "token-bob"
        try await session.refresh()
        let bob = try await snapshot(of: session) { $0.status == .ready && $0.user?.subject == "bob" }
        XCTAssertNotEqual(bob.clientEpoch, alice.clientEpoch)
        XCTAssertNotEqual(bob.user?.account, alice.user?.account)
        do {
            _ = try await aliceClient.all(JazzApp.shows.query())
            XCTFail("alice's client should be replaced")
        } catch let error as JazzError {
            guard case .replaced = error else { return XCTFail("\(error)") }
        }
        let bobClient = try await session.client()
        let bobAccount = try await bobClient.account()
        XCTAssertEqual(bobAccount, bob.user?.account)

        token.current = nil
        try await session.refresh()
        _ = try await snapshot(of: session) { $0.status == .signedOut }
        try await session.close()
    }

    func testSigningInTakesOverTheGuest() async throws {
        let relay = try await TestRelay()
        let token = Token(nil)
        let session = try await open(relay, token: token, start: .device)
        let guest = try await ready(session)
        XCTAssertEqual(guest.user?.kind, .device)
        token.current = "token-carol"
        try await session.refresh()
        let carol = try await snapshot(of: session) { $0.status == .ready && $0.user?.subject == "carol" }
        // Carol had no account: she takes over the guest's.
        XCTAssertEqual(carol.user?.account, guest.user?.account)
        try await session.close()
    }

    func testTheDocsSnippetOpensASession() async throws {
        let relay = try await TestRelay()
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("stage-plan-session-\(UUID().uuidString)").path
        dirs.append(dir)
        DocsSnippets.MySignIn.user = "alice"
        let (session, jazz) = try await DocsSnippets.openSession(
            endpoint: relay.endpoint, appId: relay.appId, dataDir: dir)
        // A guest first, as `.device` starts.
        let guest = try await session.snapshot()
        XCTAssertEqual(guest.user?.kind, .device)
        let account = try await jazz.account()
        XCTAssertEqual(account, guest.user?.account)
        // The sign-in names alice: she takes the guest's account over.
        try await session.refresh()
        let alice = try await snapshot(of: session) { $0.status == .ready && $0.user?.subject == "alice" }
        XCTAssertEqual(alice.user?.account, guest.user?.account)
        try await session.close()
    }

    func testSnapshotsFailClosedWhenTheSessionCloses() async throws {
        let relay = try await TestRelay()
        let session = try await open(relay)
        _ = try await ready(session)
        let snapshots = session.snapshots
        try await session.close()
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                do {
                    for try await _ in snapshots {}
                    XCTFail("the stream should fail with .closed")
                } catch let error as JazzError {
                    XCTAssertEqual(error, .closed)
                }
            }
            group.addTask {
                try await Task.sleep(nanoseconds: 10_000_000_000)
                throw TimedOut()
            }
            defer { group.cancelAll() }
            try await group.next()!
        }
    }
}
