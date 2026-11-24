package tools.jazz

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class EncryptionTest {
    @Test
    fun account_reset_forwards_the_operation_and_preserves_the_outcome() = runTest {
        var allowed = true
        val native = object : Native by FakeNative() {
            override fun encryptionCall(op: String, argsJson: String): String {
                assertEquals("resetAccount", op)
                assertEquals("{}", argsJson)
                if (!allowed) throw JazzException.Refused("encryption (account-reset-not-allowed): reset is disabled")
                return """"active""""
            }
        }
        val encryption = Encryption(native) { it() }

        assertEquals(JsonPrimitive("active"), encryption.call("resetAccount"))
        allowed = false
        val error = assertFailsWith<JazzException.Refused> { encryption.call("resetAccount") }
        assertEquals("account-reset-not-allowed", Encryption.codeOf(error))
    }

    @Test
    fun the_device_list_keeps_each_removal_request() = runTest {
        val native = object : Native by FakeNative() {
            override fun encryptionCall(op: String, argsJson: String): String {
                assertEquals("devices", op)
                return """[{"id":"a","status":"active","removalRequested":false},{"id":"b","status":"active","removalRequested":true}]"""
            }
        }
        assertEquals(listOf(false, true), Encryption(native) { it() }.devices().map { it.removalRequested })
    }

    @Test
    fun delivery_reports_failed_spaces_even_when_nothing_was_delivered() = runTest {
        var result = """{"delivered":0,"failed":3}"""
        val native = object : Native by FakeNative() {
            override fun encryptionCall(op: String, argsJson: String): String {
                assertEquals("keysDeliver", op)
                assertEquals("{}", argsJson)
                return result
            }
        }
        val encryption = Encryption(native) { it() }

        assertEquals(KeyDeliveryResult(delivered = 0, failed = 3), encryption.keysDeliver())
        result = """{"delivered":5,"failed":1}"""
        assertEquals(KeyDeliveryResult(delivered = 5, failed = 1), encryption.keysDeliver())
    }
}
