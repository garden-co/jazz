// Apple platforms only: SwiftUI holds it, and on Linux the toolchain's
// libswiftObservation fails to link (swift 6.2.1).
#if canImport(Observation) && !os(Linux)
    import Foundation
    import Observation

    /// A query's live result for SwiftUI, like TS's `useAll`: hold it in a
    /// view (`@State var tasks = JazzQuery(jazz, JazzApp.tasks.query())`)
    /// and read `rows`. It follows `Client.watch`, so `rows` keeps the query's
    /// `orderBy` order. It stops when released or `cancel()`ed.
    @available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
    @MainActor
    @Observable
    public final class JazzQuery<R: Row> {
        /// The current result: empty until the first one arrives.
        public private(set) var rows: [R] = []
        /// Whether the first result is still on its way.
        public private(set) var isLoading = true
        /// Why the query stopped, if it failed.
        public private(set) var error: (any Error)?

        @ObservationIgnored private let following = Following()

        public init(_ jazz: Client, _ query: Query<R>) {
            let stream = jazz.watch(query)
            following.task = _Concurrency.Task { [weak self] in
                do {
                    for try await rows in stream {
                        guard let self else { return }
                        self.rows = rows
                        self.isLoading = false
                    }
                } catch {
                    guard let self else { return }
                    self.error = error
                    self.isLoading = false
                }
            }
        }

        /// Stops following the query; `rows` keeps its last value.
        public func cancel() {
            following.cancel()
        }

        deinit {
            following.cancel()
        }
    }

    /// The task following the query, cancellable from any thread.
    final class Following: @unchecked Sendable {
        private let lock = NSLock()
        private var _task: _Concurrency.Task<Void, Never>?

        var task: _Concurrency.Task<Void, Never>? {
            get { lock.lock(); defer { lock.unlock() }; return _task }
            set { lock.lock(); defer { lock.unlock() }; _task = newValue }
        }

        func cancel() {
            task?.cancel()
            task = nil
        }
    }
#endif
