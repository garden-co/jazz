package tools.jazz.gate

import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import tools.jazz.JazzConfig

// The iOS gate's executable for a device (ci/ios-gate.sh): an app's use of
// Jazz, which has to link jazz-ffi.

fun gateMain(): Unit = runBlocking {
    val jazz = jazzapp.open(JazzConfig())
    try {
        val note = jazz.insert(jazzapp.Notes, jazzapp.NewNote(body = "linked"))
        println("${jazz.all(jazzapp.Notes.query())} ${note.id}")
        println(jazz.watch(jazzapp.Notes.query()).first())
    } finally {
        jazz.close()
    }
}
