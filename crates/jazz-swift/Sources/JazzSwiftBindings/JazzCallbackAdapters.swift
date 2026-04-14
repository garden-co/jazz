import Foundation

public final class ClosureBatchedTickCallback: BatchedTickCallback, @unchecked Sendable {
    private let onTickRequested: @Sendable () -> Void

    public init(_ onTickRequested: @escaping @Sendable () -> Void) {
        self.onTickRequested = onTickRequested
    }

    public func requestBatchedTick() {
        onTickRequested()
    }
}

public final class ClosureSubscriptionCallback: SubscriptionCallback, @unchecked Sendable {
    private let onUpdateJSON: @Sendable (String) -> Void

    public init(_ onUpdateJSON: @escaping @Sendable (String) -> Void) {
        self.onUpdateJSON = onUpdateJSON
    }

    public func onUpdate(deltaJson: String) {
        onUpdateJSON(deltaJson)
    }
}

public final class ClosureSyncMessageCallback: SyncMessageCallback, @unchecked Sendable {
    private let onMessage: @Sendable (JazzSyncOutboxMessage) -> Void

    public init(_ onMessage: @escaping @Sendable (JazzSyncOutboxMessage) -> Void) {
        self.onMessage = onMessage
    }

    public func onSyncMessage(
        destinationKind: String,
        destinationId: String,
        payloadJson: String,
        isCatalogue: Bool
    ) {
        onMessage(
            JazzSyncOutboxMessage(
                destinationKind: destinationKind,
                destinationId: destinationId,
                payloadJSON: payloadJson,
                isCatalogue: isCatalogue
            )
        )
    }
}
