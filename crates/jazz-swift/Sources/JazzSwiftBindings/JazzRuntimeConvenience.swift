import Foundation

public struct JazzObservedRows: Sendable {
    public let initialRows: [JazzWireRow]
    public let subscription: JazzSubscription
}

private final class JazzRowsBox: @unchecked Sendable {
    private let lock = NSLock()
    private var rows: [JazzWireRow]

    init(_ rows: [JazzWireRow]) {
        self.rows = rows
    }

    func apply(_ changes: [JazzSubscriptionChange]) -> [JazzWireRow] {
        lock.lock()
        defer { lock.unlock() }

        for change in changes {
            switch change.kind {
            case 0:
                guard let row = change.row else { continue }
                let targetIndex = max(0, min(change.index, rows.count))
                rows.insert(row, at: targetIndex)
            case 1:
                if let existingIndex = rows.firstIndex(where: { $0.id == change.id }) {
                    rows.remove(at: existingIndex)
                } else if rows.indices.contains(change.index) {
                    rows.remove(at: change.index)
                }
            case 2:
                if let existingIndex = rows.firstIndex(where: { $0.id == change.id }) {
                    rows.remove(at: existingIndex)
                }
                if let row = change.row {
                    let targetIndex = max(0, min(change.index, rows.count))
                    rows.insert(row, at: targetIndex)
                }
            default:
                continue
            }
        }

        return rows
    }

    func replace(_ nextRows: [JazzWireRow]) {
        lock.lock()
        defer { lock.unlock() }
        rows = nextRows
    }
}

private final class JazzFirstUpdateGate: @unchecked Sendable {
    private let lock = NSLock()
    private var isFirstUpdate = true

    func take() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        let value = isFirstUpdate
        isFirstUpdate = false
        return value
    }
}

func currentRowsFromSnapshotChanges(_ changes: [JazzSubscriptionChange]) -> [JazzWireRow] {
    JazzRowsBox([]).apply(changes)
}

func diffSubscriptionChanges(
    from previousRows: [JazzWireRow],
    to currentRows: [JazzWireRow]
) -> [JazzSubscriptionChange] {
    let previousById = Dictionary(uniqueKeysWithValues: previousRows.enumerated().map { offset, row in
        (row.id, (offset, row))
    })
    var removed: [JazzSubscriptionChange] = []
    var updated: [JazzSubscriptionChange] = []
    var added: [JazzSubscriptionChange] = []

    for (index, row) in previousRows.enumerated() where !currentRows.contains(where: { $0.id == row.id }) {
        removed.append(JazzSubscriptionChange(kind: 1, id: row.id, index: index, row: nil))
    }

    for (index, row) in currentRows.enumerated() {
        guard let (previousIndex, previousRow) = previousById[row.id] else {
            added.append(JazzSubscriptionChange(kind: 0, id: row.id, index: index, row: row))
            continue
        }
        if previousIndex != index || previousRow != row {
            updated.append(JazzSubscriptionChange(kind: 2, id: row.id, index: index, row: row))
        }
    }

    return removed + updated + added
}

public final class JazzSubscription: @unchecked Sendable {
    private weak var runtime: JazzSwiftRuntime?
    private let callback: SubscriptionCallback
    private var isCancelled = false

    public let handle: UInt64

    init(runtime: JazzSwiftRuntime, handle: UInt64, callback: SubscriptionCallback) {
        self.runtime = runtime
        self.handle = handle
        self.callback = callback
    }

    deinit {
        try? cancel()
    }

    public func cancel() throws {
        guard !isCancelled else { return }
        guard let runtime else {
            isCancelled = true
            return
        }
        try runtime.unsubscribe(handle: handle)
        isCancelled = true
    }
}

public extension JazzSwiftRuntime {
    func insert(table: String, values: [String: JazzValue]) throws -> JazzWireRow {
        let valuesJSON = try JazzJSONCodec.encode(values)
        let rowJSON = try insert(table: table, valuesJson: valuesJSON)
        return try JazzJSONCodec.decode(JazzWireRow.self, from: rowJSON)
    }

    func insertWithSession(
        table: String,
        values: [String: JazzValue],
        writeContextJSON: String?
    ) throws -> JazzWireRow {
        let valuesJSON = try JazzJSONCodec.encode(values)
        let rowJSON = try insertWithSession(
            table: table,
            valuesJson: valuesJSON,
            writeContextJson: writeContextJSON
        )
        return try JazzJSONCodec.decode(JazzWireRow.self, from: rowJSON)
    }

    func update(objectId: String, values: [String: JazzValue]) throws {
        let valuesJSON = try JazzJSONCodec.encode(values)
        try update(objectId: objectId, valuesJson: valuesJSON)
    }

    func updateWithSession(
        objectId: String,
        values: [String: JazzValue],
        writeContextJSON: String?
    ) throws {
        let valuesJSON = try JazzJSONCodec.encode(values)
        try updateWithSession(
            objectId: objectId,
            valuesJson: valuesJSON,
            writeContextJson: writeContextJSON
        )
    }

    func queryRows(
        _ queryJSON: String,
        sessionJSON: String? = nil,
        tier: String? = nil
    ) throws -> [JazzWireRow] {
        let rowsJSON = try query(queryJson: queryJSON, sessionJson: sessionJSON, tier: tier)
        return try JazzJSONCodec.decode([JazzWireRow].self, from: rowsJSON)
    }

    func subscribeRaw(
        queryJSON: String,
        sessionJSON: String? = nil,
        tier: String? = nil,
        onUpdateJSON: @escaping @Sendable (String) -> Void
    ) throws -> JazzSubscription {
        let callback = ClosureSubscriptionCallback(onUpdateJSON)
        let handle = try subscribe(
            queryJson: queryJSON,
            callback: callback,
            sessionJson: sessionJSON,
            tier: tier
        )
        return JazzSubscription(runtime: self, handle: handle, callback: callback)
    }

    func subscribeRows(
        queryJSON: String,
        sessionJSON: String? = nil,
        tier: String? = nil,
        onUpdate: @escaping @Sendable ([JazzSubscriptionChange]) -> Void
    ) throws -> JazzSubscription {
        try subscribeRaw(queryJSON: queryJSON, sessionJSON: sessionJSON, tier: tier) { deltaJSON in
            let changes = (try? JazzJSONCodec.decode([JazzSubscriptionChange].self, from: deltaJSON)) ?? []
            onUpdate(changes)
        }
    }

    func observeRows(
        queryJSON: String,
        sessionJSON: String? = nil,
        tier: String? = nil,
        onUpdate: @escaping @Sendable ([JazzSubscriptionChange]) -> Void
    ) throws -> JazzObservedRows {
        let handle = try createSubscription(
            queryJson: queryJSON,
            sessionJson: sessionJSON,
            tier: tier
        )
        let initialRows = try queryRows(queryJSON, sessionJSON: sessionJSON, tier: tier)
        let firstUpdateGate = JazzFirstUpdateGate()
        let callback = ClosureSubscriptionCallback { deltaJSON in
            guard let changes = try? JazzJSONCodec.decode([JazzSubscriptionChange].self, from: deltaJSON) else {
                return
            }
            guard firstUpdateGate.take() else {
                onUpdate(changes)
                return
            }
            let currentRows = currentRowsFromSnapshotChanges(changes)
            let normalizedChanges = diffSubscriptionChanges(from: initialRows, to: currentRows)
            if !normalizedChanges.isEmpty {
                onUpdate(normalizedChanges)
            }
        }
        try executeSubscription(handle: handle, callback: callback)
        let subscription = JazzSubscription(runtime: self, handle: handle, callback: callback)
        return JazzObservedRows(initialRows: initialRows, subscription: subscription)
    }

    func observeRowCollection(
        queryJSON: String,
        sessionJSON: String? = nil,
        tier: String? = nil,
        onUpdate: @escaping @Sendable ([JazzWireRow]) -> Void
    ) throws -> JazzObservedRows {
        let handle = try createSubscription(
            queryJson: queryJSON,
            sessionJson: sessionJSON,
            tier: tier
        )
        let initialRows = try queryRows(queryJSON, sessionJSON: sessionJSON, tier: tier)
        let rowsBox = JazzRowsBox(initialRows)
        let firstUpdateGate = JazzFirstUpdateGate()
        let callback = ClosureSubscriptionCallback { deltaJSON in
            guard let changes = try? JazzJSONCodec.decode([JazzSubscriptionChange].self, from: deltaJSON) else {
                return
            }
            guard firstUpdateGate.take() else {
                onUpdate(rowsBox.apply(changes))
                return
            }
            let currentRows = currentRowsFromSnapshotChanges(changes)
            rowsBox.replace(currentRows)
            if currentRows != initialRows {
                onUpdate(currentRows)
            }
        }
        try executeSubscription(handle: handle, callback: callback)
        let subscription = JazzSubscription(runtime: self, handle: handle, callback: callback)
        return JazzObservedRows(initialRows: initialRows, subscription: subscription)
    }
}
