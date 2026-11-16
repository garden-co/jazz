// SwiftUI on Apple platforms, as TS's React provider and hooks: `JazzRoot`
// (`<JazzProvider>`), `@JazzAll` (`useAll`), `@Environment(\.jazz)`
// (`useDb`) and `@Environment(\.jazzSession)` (`useJazzSession`).
#if canImport(SwiftUI) && canImport(Observation) && !os(Linux)
    import Foundation
    import Observation
    import SwiftUI

    private struct JazzClientKey: EnvironmentKey {
        static let defaultValue: Client? = nil
    }

    @available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
    private struct JazzSessionKey: EnvironmentKey {
        static var defaultValue: JazzSessionState? { nil }
    }

    extension EnvironmentValues {
        /// The client of the user signed in now, under `JazzRoot` (TS's
        /// `useDb`); `nil` elsewhere. Set it yourself with
        /// `.environment(\.jazz, client)` for a client you opened.
        public var jazz: Client? {
            get { self[JazzClientKey.self] }
            set { self[JazzClientKey.self] = newValue }
        }

        /// The session under `JazzRoot` (TS's `useJazzSession`): who is
        /// signed in, its status, and the session's calls (`signOut()`, …).
        @available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
        public var jazzSession: JazzSessionState? {
            get { self[JazzSessionKey.self] }
            set { self[JazzSessionKey.self] = newValue }
        }
    }

    /// A `JazzSession` for SwiftUI: its latest snapshot and the signed-in
    /// user's client, observable. `JazzRoot` keeps one and puts it in the
    /// environment as `\.jazzSession`.
    @available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
    @MainActor
    @Observable
    public final class JazzSessionState {
        /// The session, for its calls: `signOut()`, `refresh()`, …
        public let session: JazzSession
        /// The session's latest snapshot; `nil` until the first arrives.
        public private(set) var snapshot: SessionSnapshot?
        /// The signed-in user's client while the session is `.ready`.
        public private(set) var client: Client?
        /// Why following the session stopped, if it failed.
        public private(set) var error: (any Error)?

        /// Who is signed in; `nil` when no one is.
        public var user: User? { snapshot?.user }
        public var status: SessionSnapshot.Status { snapshot?.status ?? .starting }

        @ObservationIgnored private let following = Following()

        public init(_ session: JazzSession) {
            self.session = session
            let snapshots = session.snapshots
            following.task = _Concurrency.Task { [weak self] in
                do {
                    for try await snapshot in snapshots {
                        guard let self else { return }
                        if snapshot.status == .ready {
                            if self.client == nil || snapshot.clientEpoch != self.snapshot?.clientEpoch {
                                self.client = try? await session.client()
                            }
                        } else {
                            self.client = nil
                        }
                        self.snapshot = snapshot
                    }
                } catch {
                    guard let self else { return }
                    self.client = nil
                    self.error = error
                }
            }
        }

        deinit {
            following.cancel()
        }
    }

    /// The root of a Jazz app's views, as TS's `<JazzProvider>`: it follows
    /// `session`, shows `content` with the signed-in user's client in the
    /// environment (`\.jazz`, read by `@JazzAll`), `signedOut` while no one
    /// is signed in, and `loading` meanwhile. A change of user rebuilds
    /// `content`, so its queries follow the new user.
    ///
    /// ```swift
    /// JazzRoot(JazzApp.session(appId: appId, serverUrl: endpoint, bearer: …)) {
    ///     ContentView()
    /// } signedOut: {
    ///     SignInView()
    /// }
    /// ```
    @available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
    public struct JazzRoot<Content: View, SignedOut: View, Loading: View>: View {
        @State private var state: JazzSessionState
        private let content: () -> Content
        private let signedOut: () -> SignedOut
        private let loading: () -> Loading

        public init(
            _ session: JazzSession,
            @ViewBuilder content: @escaping () -> Content,
            @ViewBuilder signedOut: @escaping () -> SignedOut,
            @ViewBuilder loading: @escaping () -> Loading
        ) {
            _state = State(initialValue: JazzSessionState(session))
            self.content = content
            self.signedOut = signedOut
            self.loading = loading
        }

        public var body: some View {
            Group {
                if let client = state.client, state.status == .ready {
                    content()
                        .environment(\.jazz, client)
                        .id(state.snapshot?.clientEpoch)
                } else if state.status == .signedOut {
                    signedOut()
                } else {
                    loading()
                }
            }
            .environment(\.jazzSession, state)
        }
    }

    @available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
    extension JazzRoot where Loading == ProgressView<EmptyView, EmptyView> {
        /// `JazzRoot` with a `ProgressView` while the session starts.
        public init(
            _ session: JazzSession,
            @ViewBuilder content: @escaping () -> Content,
            @ViewBuilder signedOut: @escaping () -> SignedOut
        ) {
            self.init(session, content: content, signedOut: signedOut, loading: { ProgressView() })
        }
    }

    /// A query's rows in a view, live, as TS's `useAll`: it reads the client
    /// from the environment (`\.jazz`, set by `JazzRoot`), and follows a new
    /// one when the user changes. `$rows` is its `JazzQuery`, for
    /// `isLoading` and `error`.
    ///
    /// ```swift
    /// @JazzAll(JazzApp.tasks.query().orderBy(JazzApp.tasks.rank)) var tasks
    /// ```
    @available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
    @MainActor
    @propertyWrapper
    public struct JazzAll<R: Row>: DynamicProperty {
        @Environment(\.jazz) private var jazz
        @State private var live = Live<R>()
        private let query: Query<R>

        public init(_ query: Query<R>) {
            self.query = query
        }

        /// The rows: empty without a client, or until the first result.
        public var wrappedValue: [R] {
            live.query?.rows ?? []
        }

        /// The live query, for `isLoading` and `error`; `nil` without a client.
        public var projectedValue: JazzQuery<R>? {
            live.query
        }

        public nonisolated func update() {
            MainActor.assumeIsolated {
                live.follow(jazz, query)
            }
        }
    }

    /// `JazzAll`'s query, kept across view updates and replaced with the client.
    @available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
    @MainActor
    final class Live<R: Row> {
        private(set) var query: JazzQuery<R>?
        private var client: Client?

        func follow(_ client: Client?, _ query: Query<R>) {
            guard client !== self.client else { return }
            self.query?.cancel()
            self.client = client
            self.query = client.map { JazzQuery($0, query) }
        }
    }
#endif
