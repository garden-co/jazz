package tools.jazz

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class EncryptionTest {
    @Test
    fun membership_rows_are_decoded_and_add_forwards_insert_durability() = runTest {
        val row = """{"id":"member","scopeId":"scope","identifier":"project","accountId":"bob"}"""
        val native = object : Native by FakeNative() {
            override fun memberAdd(table: String, id: String, accountId: String, tier: String?): String {
                assertEquals(listOf("projects", "project", "bob", "global"), listOf(table, id, accountId, tier))
                return row
            }

            override fun memberList(table: String, id: String): String {
                assertEquals(listOf("projects", "project"), listOf(table, id))
                return "[$row]"
            }

            override fun memberRemove(table: String, id: String, accountId: String): ULong {
                assertEquals(listOf("projects", "project", "bob"), listOf(table, id, accountId))
                return 1UL
            }
        }
        val encryption = Encryption(native) { it() }
        val member = SpaceMember("member", "scope", "project", "bob")
        assertEquals(member, encryption.addMember("projects", "project", "bob", WriteTier.Global))
        assertEquals(listOf(member), encryption.listMembers("projects", "project"))
        assertEquals(1UL, encryption.removeMember("projects", "project", "bob"))
    }

    @Test
    fun account_reset_forwards_the_operation_and_preserves_the_outcome() = runTest {
        var allowed = true
        val native = object : Native by FakeNative() {
            override fun encryptionCall(op: String, argsJson: String): String {
                assertEquals("resetAccount", op)
                assertEquals("{}", argsJson)
                if (!allowed) throw JazzException.Refused("reset is disabled", "account-reset-not-allowed")
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
