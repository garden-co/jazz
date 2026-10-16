/* jazz_protocol::protocol_limits (crates/jazz/layers/protocol/src/protocol_limits.rs) */
#include "jazz_protocol/protocol.h"

static jz_err jazz_protocol__protocol_limits__validate_len(const char *label, usize len, usize max) {
    if (len > max) return jz_err_fmt(JAZZ_PROTO_ERR_MSG, "%s size %zu exceeds max %zu", label, len, max);
    return JZ_OK;
}

jz_err jazz_protocol__protocol_limits__validate_wire_frame_len(usize len) {
    return jazz_protocol__protocol_limits__validate_len("wire frame", len, JAZZ_MAX_WIRE_FRAME_BYTES);
}

jz_err jazz_protocol__protocol_limits__validate_logical_message_len(usize len) {
    return jazz_protocol__protocol_limits__validate_len("logical message payload", len, JAZZ_MAX_LOGICAL_MESSAGE_BYTES);
}

/* postcard::experimental::serialized_size: serialize into a scratch buffer and measure
 * (one serializer copy instead of a Size flavor instantiation). Serialization of these
 * types cannot fail, so the "failed to measure" branch is unreachable in C. */
jz_err jazz_protocol__protocol_limits__validate_shape_ast_size(const jazz_protocol_ShapeAst *ast) {
    jz_bytes buf = {0};
    jz_pc_w w = { &buf };
    jazz_protocol__protocol__ShapeAst__Serialize__serialize(&w, ast);
    usize size = buf.len;
    jz_bytes_drop(&buf);
    return jazz_protocol__protocol_limits__validate_len("shape AST", size, JAZZ_MAX_SHAPE_AST_BYTES);
}

jz_err jazz_protocol__protocol_limits__validate_shape_registration_size(const jazz_protocol_ShapeAst *ast,
                                                                       const jazz_protocol_RegisterShapeOptions *opts) {
    jz_bytes buf = {0};
    jz_pc_w w = { &buf };
    /* (ast, opts) tuple: fields back to back */
    jazz_protocol__protocol__ShapeAst__Serialize__serialize(&w, ast);
    jazz_protocol__protocol__RegisterShapeOptions__Serialize__serialize(&w, opts);
    usize size = buf.len;
    jz_bytes_drop(&buf);
    return jazz_protocol__protocol_limits__validate_len("shape registration", size, JAZZ_MAX_SHAPE_REGISTRATION_BYTES);
}

bool jazz_protocol__protocol_limits__commit_unit_limit_violation(u32 n_versions, jz_str *out) {
    if (n_versions > JAZZ_MAX_COMMIT_UNIT_VERSIONS) {
        *out = jz_format("commit unit version count %u exceeds max %u", n_versions, JAZZ_MAX_COMMIT_UNIT_VERSIONS);
        return true;
    }
    return false;
}
