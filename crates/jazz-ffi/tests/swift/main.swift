// Compiled with the generated jazz_ffi.swift and run by
// dev/gates/jazz-ffi-compile-bindings.sh: the generated Swift compiles,
// links libjazz_ffi and round-trips a row.
import Foundation

let app = #"{"format": 1, "tables": {"todos": {"columns": {"title": "string", "rank": "float"}}}}"#
let config = #"{"app": ""# + app.replacingOccurrences(of: "\"", with: "\\\"") + #""}"#
let jazz = try JazzHandle.open(configJson: config)
let row = try jazz.insert(table: "todos", rowJson: #"{"title": "milk", "rank": 1.5}"#)
precondition(row.contains(#""title":"milk""#), row)
precondition(try jazz.all(queryJson: #"{"table": "todos"}"#).contains("milk"))
precondition(try jazz.account() == nil)
do {
    _ = try jazz.all(queryJson: #"{"table": "nope"}"#)
    preconditionFailure("expected Refused")
} catch JazzError.Refused(let reason) {
    precondition(reason.contains("no table nope"), reason)
}
try jazz.shutdown()
print("jazz_ffi Swift: ok")
