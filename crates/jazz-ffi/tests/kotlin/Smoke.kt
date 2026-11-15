// Compiled with the generated uniffi.jazz_ffi bindings and run on the JVM by
// dev/gates/jazz-ffi-compile-bindings.sh: the generated Kotlin compiles,
// loads libjazz_ffi and round-trips a row.
import uniffi.jazz_ffi.JazzException
import uniffi.jazz_ffi.JazzHandle

fun main() {
    val app = """{"format": 1, "tables": {"todos": {"columns": {"title": "string", "rank": "float"}}}}"""
    val config = "{\"app\": \"" + app.replace("\"", "\\\"") + "\"}"
    val jazz = JazzHandle.open(config)
    val row = jazz.insert("todos", """{"title": "milk", "rank": 1.5}""")
    check(row.contains("\"title\":\"milk\"")) { row }
    check(jazz.all("""{"table": "todos"}""").contains("milk"))
    check(jazz.account() == null)
    try {
        jazz.all("""{"table": "nope"}""")
        error("expected Refused")
    } catch (refused: JazzException.Refused) {
        check(refused.reason.contains("no table nope")) { refused.reason }
    }
    jazz.shutdown()
    try {
        jazz.all("""{"table": "todos"}""")
        error("expected Closed")
    } catch (_: JazzException.Closed) {}
    println("jazz_ffi Kotlin: ok")
}
