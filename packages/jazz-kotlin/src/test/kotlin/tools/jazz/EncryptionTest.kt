package tools.jazz

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

class EncryptionTest {
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
