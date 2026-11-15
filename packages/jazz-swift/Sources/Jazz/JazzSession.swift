// Apple platforms only: SwiftUI holds it, and on Linux the toolchain's
// libswiftObservation fails to link (swift 6.2.1).
#if canImport(Observation) && !os(Linux)
    import Foundation
    import Observation

    /// An end user's session for SwiftUI: hold it in a view and read
    /// `state` (`.signedIn(user)`, `.expired`, `.signedOut`) and `sync`. It
    /// follows `Client.session` and `Client.syncChanges` until released or
    /// `cancel()`ed.
    @available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
    @MainActor
    @Observable
    public final class JazzSession {
        public private(set) var state: SessionState?
        public private(set) var sync: SyncStatus?
        public private(set) var error: (any Error)?

        @ObservationIgnored private let sessionTask = Following()
        @ObservationIgnored private let syncTask = Following()

        public init(_ jazz: Client) {
            let sessions = jazz.session
            let syncs = jazz.syncChanges
            sessionTask.task = _Concurrency.Task { [weak self] in
                do {
                    for try await state in sessions {
                        guard let self else { return }
                        self.state = state
                    }
                } catch {
                    self?.error = error
                }
            }
            syncTask.task = _Concurrency.Task { [weak self] in
                do {
                    for try await status in syncs {
                        guard let self else { return }
                        self.sync = status
                    }
                } catch {
                    self?.error = error
                }
            }
        }

        public var user: User? {
            if case .signedIn(let user) = state { return user }
            return nil
        }

        public func cancel() {
            sessionTask.cancel()
            syncTask.cancel()
        }

        deinit {
            sessionTask.cancel()
            syncTask.cancel()
        }
    }
#endif
