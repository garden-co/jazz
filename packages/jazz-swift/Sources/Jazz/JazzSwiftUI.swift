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
    /// is signed in, and `loading` otherwise: while the session starts or
    /// switches users, and when it failed or closed (read
    /// `@Environment(\.jazzSession)`'s `status` and `error` there). A change
    /// of user rebuilds `content`, so its queries follow the new user.
    ///
    /// Keep the session for the app's lifetime, in your `App`:
    ///
    /// ```swift
    /// @main struct MyApp: App {
    ///     let session = JazzApp.session(appId: appId, serverUrl: endpoint, bearer: …)
    ///     var body: some Scene {
    ///         WindowGroup {
    ///             JazzRoot(session) { ContentView() } signedOut: { SignInView() }
    ///         }
    ///     }
    /// }
    /// ```
    ///
    /// `JazzRoot` takes `session` once, when it first appears: the same view
    /// given another session later keeps the first.
    @available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
    public struct JazzRoot<Content: View, SignedOut: View, Loading: View>: View {
        @State private var holder = StateHolder()
        private let session: () -> JazzSession
        private let content: () -> Content
        private let signedOut: () -> SignedOut
        private let loading: () -> Loading

        public init(
            _ session: @autoclosure @escaping () -> JazzSession,
            @ViewBuilder content: @escaping () -> Content,
            @ViewBuilder signedOut: @escaping () -> SignedOut,
            @ViewBuilder loading: @escaping () -> Loading
        ) {
            self.session = session
            self.content = content
            self.signedOut = signedOut
            self.loading = loading
        }

        public var body: some View {
            let state = holder.state(session)
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

    /// `JazzRoot`'s session state, made once from the first session given.
    @available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
    @MainActor
    final class StateHolder {
        private var made: JazzSessionState?

        func state(_ session: () -> JazzSession) -> JazzSessionState {
            if let made { return made }
            let state = JazzSessionState(session())
            made = state
            return state
        }
    }

    @available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
    extension JazzRoot where Loading == ProgressView<EmptyView, EmptyView> {
        /// `JazzRoot` with a `ProgressView` while the session starts.
        public init(
            _ session: @autoclosure @escaping () -> JazzSession,
            @ViewBuilder content: @escaping () -> Content,
            @ViewBuilder signedOut: @escaping () -> SignedOut
        ) {
            self.init(session(), content: content, signedOut: signedOut, loading: { ProgressView() })
        }
    }

    /// A query's rows in a view, live, as TS's `useAll`: it reads the client
    /// from the environment (`\.jazz`, set by `JazzRoot`), and follows again
    /// when the client (the user) or the query changes. `$rows` is its `JazzQuery`, for
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

    /// `JazzAll`'s query, kept across view updates; followed again when the
    /// client or the query (by its JSON) changes.
    @available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
    @MainActor
    final class Live<R: Row> {
        private(set) var query: JazzQuery<R>?
        private var client: Client?
        private var key: String?

        func follow(_ client: Client?, _ query: Query<R>) {
            let key = try? query.json()
            guard client !== self.client || key != self.key else { return }
            self.query?.cancel()
            self.client = client
            self.key = key
            self.query = client.map { JazzQuery($0, query) }
        }
    }
#endif
