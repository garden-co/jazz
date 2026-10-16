/* ingest/catalogue.c: sync-message dispatch and schema-lineage catalogue
 * activation (include!d into jazz_node::node::ingest).
 * Rust: crates/jazz/layers/node/src/node/ingest/catalogue.rs */
#include "../ingest.h"

#define JZN_UNSUPPORTED_SYNC(msg) jz_err_new(JZN_E_UNSUPPORTED_SYNC_MESSAGE, (msg))
#define JZN_INVALID_CATALOGUE(msg) jz_err_new(JZN_E_INVALID_CATALOGUE_UPDATE, (msg))

bool jazz_node__node__ingest__large_value_upload_is_rejected(const jz_err *error) {
    return error->code == GROOVE_DB_ERR_INVALID_LARGE_VALUE_METADATA || error->code == GROOVE_DB_ERR_IVM_LARGE_VALUE ||
           error->code == GROOVE_DB_ERR_IVM_CHUNK_INTEGRITY;
}

/* Apply one sync message and return any outgoing sync messages. */
jz_err jazz_node__node__ingest__NodeState__apply_sync_message(NSA *s, jazz_protocol_SyncMessage message, jazz_node_PublicationOutcome *out) {
    return jazz_node__node__ingest__NodeState__apply_sync_message_with_ingest_context(s, message, NULL, out);
}

/* Apply a catalogue mutation from the trusted local administrative lane. */
jz_err jazz_node__node__ingest__NodeState__apply_trusted_catalogue_message(NSA *s, jazz_protocol_SyncMessage message, jazz_node_PublicationOutcome *out) {
    jazz_node_CommitUnitIngestContext ctx = {
        .identity = jazz_types_AuthorSubject_SYSTEM,
        .trust = JZN_TRUST_BACKEND,
        .admitted_write_authorization = false,
        .version_receipts_validated = false,
    };
    return jazz_node__node__ingest__NodeState__apply_sync_message_with_ingest_context(s, message, &ctx, out);
}

static void jzn_chunk_result(jazz_node_PublicationOutcome *out, groove_LargeValueRef *value_ref, u32 status, jzn_ChunkIdVec need) {
    jazz_protocol_SyncMessage m;
    memset(&m, 0, sizeof m);
    m.tag = JZP_SM_CHUNK_UPLOAD_RESULT;
    m.v.chunk_upload_result.value_ref = value_ref;
    m.v.chunk_upload_result.status = status;
    m.v.chunk_upload_result.need = need;
    jazz_node__node__ingest__settled_one(out, m);
}

static void jzn_chunk_status_from_progress(groove_LargeValueUploadProgress *progress, u32 *status, jzn_ChunkIdVec *need) {
    jzn_ChunkIdVec none = {0};
    if (progress->tag == GROOVE_LV_PROGRESS_MISSING) {
        jzn_ChunkIdVec nodes = progress->missing;
        if (nodes.len > 64) nodes.len = 64; /* nodes.truncate(64) */
        *status = JZP_CHUNK_STATUS_NEED;
        *need = nodes;
    } else {
        *status = JZP_CHUNK_STATUS_STAGED;
        *need = none;
    }
}

static jz_err jzn_apply_publish_schema(NSA *s, jazz_types_AuthorSubject author, const jazz_node_CommitUnitIngestContext *ctx,
                                       jazz_model_SchemaVersion schema, jazz_node_PublicationOutcome *out);
static jz_err jzn_apply_publish_schema_with_lens(NSA *s, jazz_types_AuthorSubject author, const jazz_node_CommitUnitIngestContext *ctx,
                                                 u64 catalogue_seq, jazz_protocol_SchemaLineagePublication publication,
                                                 jazz_node_PublicationOutcome *out);
static jz_err jzn_apply_publish_lens(NSA *s, jazz_types_AuthorSubject author, const jazz_node_CommitUnitIngestContext *ctx,
                                     jazz_protocol_MigrationLens lens, jazz_protocol_SyncMessageVec *out);

/* Apply one sync message from a connection-authenticated upload path. */
jz_err jazz_node__node__ingest__NodeState__apply_sync_message_with_ingest_context(NSA *s, jazz_protocol_SyncMessage message,
                                                                                const jazz_node_CommitUnitIngestContext *ingest_context,
                                                                                jazz_node_PublicationOutcome *out) {
    jz_err err = JZ_OK;
    jzn_ChunkIdVec no_need = {0};
    if (jazz_node__node__is_catalogue_mutation(&message)) {
        if (JZ_IS_ERR(err = groove__db__Database__ensure_usable(jzn_db(s)))) goto drop_message;
        if (groove__db__Database__has_unsettled_publications(jzn_db(s))) {
            err = jz_err_new(GROOVE_DB_ERR_UNSETTLED_PUBLICATIONS, "unsettled publications");
            goto drop_message;
        }
    }
    /* Dispatch the commit before constructing the general message future. */
    if (message.tag == JZP_SM_COMMIT_UNIT) {
        jazz_model_Transaction tx = message.v.commit_unit.tx;
        jazz_protocol_VersionRecordVec versions = message.v.commit_unit.versions;
        if (JZ_IS_ERR(err = jazz_node__node__state__durable__NodeState__require_catalogue_ready(s))) goto drop_commit;
        if (s->catalogue_activation_failed) { err = jz_err_new(JZN_E_CATALOGUE_ACTIVATION_FAILED, "catalogue activation failed"); goto drop_commit; }
        if (ingest_context) {
            jzn_LargeValueRefVec descriptors = {0};
            jazz_node__node__version_indirect_descriptors(&versions, &descriptors);
            err = jazz_node__node__state__lifecycle__NodeState__current_staged_ids_for_descriptors(s, &descriptors, true);
            jz_vec_drop(&descriptors, sizeof(groove_LargeValueRef *), NULL);
            if (JZ_IS_ERR(err)) goto drop_commit;
        }
        u64 now_ms;
        if (ingest_context) {
            if (JZ_IS_ERR(err = jazz_node__node__ingest__authority_wall_clock_ms(&now_ms))) goto drop_commit;
        } else {
            now_ms = jazz_types__time__TxTime__physical_ms(tx.tx_id.time);
        }
        return jazz_node__node__ingest__NodeState__ingest_commit_unit_with_context(s, tx, versions, now_ms, ingest_context, out);
    drop_commit:
        jazz_model_Transaction_drop(&tx);
        jazz_protocol_VersionRecordVec_drop(&versions);
        return err;
    }

    /* Uninitialized clients and local relays must install a complete trusted
     * catalogue snapshot before accepting incremental traffic. */
    if (JZ_IS_ERR(err = jazz_node__node__state__durable__NodeState__require_catalogue_ready(s))) goto drop_message;
    if (s->catalogue_activation_failed) {
        err = jz_err_new(JZN_E_CATALOGUE_ACTIVATION_FAILED, "catalogue activation failed");
        goto drop_message;
    }
    switch (message.tag) {
    case JZP_SM_CHUNK_UPLOAD_START: {
        groove_LargeValueRef *value_ref = message.v.chunk_upload_start.value_ref;
        if (!jazz_node__node__state__lifecycle__NodeState__admit_large_value_ingress(s, JZN_LARGE_VALUE_UPLOAD_START_INGRESS_CHARGE_BYTES)) {
            jzn_chunk_result(out, value_ref, JZP_CHUNK_STATUS_RATE_LIMITED, no_need);
            return JZ_OK;
        }
        groove_LargeValueUploadProgress progress;
        err = groove__db__Database__begin_large_value_upload_with_pending_limit(jzn_db(s), groove_LargeValueRef_clone(value_ref),
                                                                              JZN_MAX_PENDING_LARGE_VALUE_UPLOADS, &progress);
        if (JZ_IS_ERR(err)) {
            if (err.code == GROOVE_DB_ERR_PENDING_LARGE_VALUE_UPLOAD_LIMIT) {
                jz_err_drop(&err);
                jzn_chunk_result(out, value_ref, JZP_CHUNK_STATUS_RATE_LIMITED, no_need);
                return JZ_OK;
            }
            if (jazz_node__node__ingest__large_value_upload_is_rejected(&err)) {
                jz_err_drop(&err);
                jzn_chunk_result(out, value_ref, JZP_CHUNK_STATUS_REJECTED, no_need);
                return JZ_OK;
            }
            return err;
        }
        u32 status;
        jzn_ChunkIdVec need;
        jzn_chunk_status_from_progress(&progress, &status, &need);
        jzn_chunk_result(out, value_ref, status, need);
        return JZ_OK;
    }
    case JZP_SM_CHUNK_UPLOAD_NODES: {
        groove_LargeValueRef *value_ref = message.v.chunk_upload_nodes.value_ref;
        jazz_protocol_ChunkNodeVec chunks = message.v.chunk_upload_nodes.chunks;
        groove_PendingLargeValueUploadVec uploads = {0};
        if (JZ_IS_ERR(err = groove__db__Database__pending_large_value_uploads(jzn_db(s), &uploads))) return err;
        bool upload_exists = false;
        for (u32 i = 0; i < uploads.len; i++) {
            if (uploads.ptr[i].descriptor && groove_LargeValueRef_eq(uploads.ptr[i].descriptor, value_ref)) { upload_exists = true; break; }
        }
        jz_vec_drop(&uploads, sizeof(groove_PendingLargeValueUpload), NULL);
        if (!upload_exists) {
            jzn_chunk_result(out, value_ref, JZP_CHUNK_STATUS_REJECTED, no_need);
            return JZ_OK;
        }
        u64 encoded_bytes = 0, node_count = 0;
        for (u32 i = 0; i < chunks.len; i++) {
            u64 n = chunks.ptr[i].encoded.len;
            if (encoded_bytes + n < encoded_bytes) return JZN_UNSUPPORTED_SYNC("chunk upload accounting overflow");
            encoded_bytes += n;
            if (node_count + 1 == 0) return JZN_UNSUPPORTED_SYNC("chunk upload accounting overflow");
            node_count += 1;
        }
        if (!jazz_node__node__state__lifecycle__NodeState__admit_large_value_ingress(s, encoded_bytes)) {
            jzn_chunk_result(out, value_ref, JZP_CHUNK_STATUS_RATE_LIMITED, no_need);
            return JZ_OK;
        }
        bool some;
        groove_LargeValueUploadProgress progress;
        err = groove__db__Database__continue_large_value_upload_if_current(jzn_db(s), groove_LargeValueRef_clone(value_ref), chunks, &some, &progress);
        if (JZ_IS_ERR(err)) {
            if (jazz_node__node__ingest__large_value_upload_is_rejected(&err)) {
                jz_err_drop(&err);
                jzn_chunk_result(out, value_ref, JZP_CHUNK_STATUS_REJECTED, no_need);
                return JZ_OK;
            }
            return err;
        }
        if (!some) {
            jzn_chunk_result(out, value_ref, JZP_CHUNK_STATUS_REJECTED, no_need);
            return JZ_OK;
        }
        u32 status;
        jzn_ChunkIdVec need;
        jzn_chunk_status_from_progress(&progress, &status, &need);
        jzn_chunk_result(out, value_ref, status, need);
        return JZ_OK;
    }
    case JZP_SM_CHUNK_UPLOAD_RESULT:
        err = JZN_UNSUPPORTED_SYNC("chunk upload result requires peer link context");
        goto drop_message;
    case JZP_SM_SESSION_CLAIMS:
        if (ingest_context && (ingest_context->trust == JZN_TRUST_BACKEND || ingest_context->trust == JZN_TRUST_AUTHORITY)) {
            jazz_node__node__state__lifecycle__NodeState__set_session_claims(s, message.v.session_claims.identity, message.v.session_claims.claims);
        } else {
            jz_btree_drop(&message.v.session_claims.claims);
        }
        jazz_node__node__ingest__settled_empty(out);
        return JZ_OK;
    case JZP_SM_COMMIT_UNIT:
        jz_panic("commit units dispatch before the general message future");
    case JZP_SM_FATE_UPDATE: {
        typeof(message.v.fate_update) *f = &message.v.fate_update;
        err = jazz_node__node__ingest__validate_received_fate_update_global_time_durability(f->has_global_time, f->global_time,
                                                                                          f->has_durability, f->durability);
        if (JZ_IS_ERR(err)) goto drop_message;
        JZ_TRY(jazz_node__node__ingest__NodeState__apply_fate_update(s, f->tx_id, f->fate, f->has_global_time, f->global_time,
                                                                    f->has_durability, f->durability));
        return jazz_node__node__ingest__NodeState__drain_parked_commit_units(s, out);
    }
    case JZP_SM_VIEW_UPDATE: {
        /* ViewUpdateParts { wire_rows: Some(..), defer_settlement: false, reset_input_set: true,
         * result_member_adds/removes: empty, .. } built from the payload. */
        jazz_node_ViewUpdateParts *parts = jazz_node__node__ViewUpdateParts__from_payload(message.v.view_update, false, true);
        JZ_TRY(jazz_node__node__views__NodeState__apply_view_update(s, parts));
        jazz_node__node__ingest__settled_empty(out);
        return JZ_OK;
    }
    case JZP_SM_REGISTER_SHAPE:
        err = jazz_node__protocol_limits__validate_shape_registration_size(message.v.register_shape.ast, message.v.register_shape.opts);
        if (JZ_IS_ERR(err)) {
            jz_err_drop(&err);
            err = JZN_UNSUPPORTED_SYNC("shape registration exceeds byte limit");
            goto drop_message;
        }
        JZ_TRY(jazz_node__node__query_eval__subscriptions__NodeState__register_shape_with_options(
            s, message.v.register_shape.shape_id, message.v.register_shape.ast, message.v.register_shape.opts));
        jazz_node__node__ingest__settled_empty(out);
        return JZ_OK;
    case JZP_SM_RESERVED14:
    case JZP_SM_RESERVED15:
    case JZP_SM_RESERVED29:
        JZ_UNREACHABLE(); /* match retired {} */
    case JZP_SM_CATALOGUE_SNAPSHOT:
        err = JZN_UNSUPPORTED_SYNC("catalogue snapshot requires a trusted upstream link");
        goto drop_message;
    case JZP_SM_READ_BARRIER:
        err = JZN_UNSUPPORTED_SYNC("read barriers are handled by the receiving link");
        goto drop_message;
    case JZP_SM_SUBSCRIBE:
        JZ_TRY(jazz_node__node__query_eval__subscriptions__NodeState__apply_subscribe(s, message.v.subscribe));
        jazz_node__node__ingest__settled_empty(out);
        return JZ_OK;
    case JZP_SM_SUBSCRIBE_REJECTED:
        err = JZN_UNSUPPORTED_SYNC("subscription rejection requires subscription stream context");
        goto drop_message;
    case JZP_SM_UNSUBSCRIBE:
        jazz_node__node__query_eval__subscriptions__NodeState__apply_unsubscribe(s, message.v.unsubscribe.subscription);
        jazz_node__node__ingest__settled_empty(out);
        return JZ_OK;
    case JZP_SM_PUBLISH_SCHEMA: {
        jazz_model_SchemaVersion schema = *message.v.publish_schema.schema;
        jz_free(message.v.publish_schema.schema); /* *schema (Box) */
        return jzn_apply_publish_schema(s, message.v.publish_schema.author, ingest_context, schema, out);
    }
    case JZP_SM_PUBLISH_SCHEMA_WITH_LENS: {
        jazz_protocol_SchemaLineagePublication publication = *message.v.publish_schema_with_lens.publication;
        jz_free(message.v.publish_schema_with_lens.publication);
        return jzn_apply_publish_schema_with_lens(s, message.v.publish_schema_with_lens.author, ingest_context,
                                                  message.v.publish_schema_with_lens.catalogue_seq, publication, out);
    }
    case JZP_SM_PUBLISH_LENS: {
        jazz_protocol_SyncMessageVec acks = {0};
        JZ_TRY(jzn_apply_publish_lens(s, message.v.publish_lens.author, ingest_context, message.v.publish_lens.lens, &acks));
        jazz_node__node__PublicationOutcome__settled(out, acks);
        return JZ_OK;
    }
    case JZP_SM_CATALOGUE_ACK:
        jazz_node__node__ingest__settled_empty(out);
        return JZ_OK;
    case JZP_SM_CHUNK_REQUEST_BATCH:
    case JZP_SM_CHUNK_RESPONSE_BATCH:
        err = JZN_UNSUPPORTED_SYNC("chunk traffic requires peer link context");
        goto drop_message;
    /* The wire transport reassembles parts into one `ViewUpdate`; a bare part never reaches node ingest. */
    case JZP_SM_VIEW_UPDATE_PART:
        err = JZN_UNSUPPORTED_SYNC("view-update parts are reassembled by the wire transport");
        goto drop_message;
    case JZP_SM_CURRENT_ROWS_REQUEST:
    case JZP_SM_CURRENT_ROWS_RECEIPT:
    case JZP_SM_CURRENT_ROWS_CANCEL:
    case JZP_SM_AUTHZ_SCOPE_INTENT:
    case JZP_SM_AUTHZ_SCOPE_VIEW:
    case JZP_SM_AUTHZ_SCOPE_AGGREGATE_RECEIPT:
    case JZP_SM_AUTHZ_SCOPE_UNAVAILABLE:
    case JZP_SM_AUTHZ_SCOPE_DECISION:
        err = JZN_UNSUPPORTED_SYNC("permission advice requires authenticated link context");
        goto drop_message;
    default:
        JZ_UNREACHABLE();
    }
drop_message:
    jazz_protocol_SyncMessage_drop(&message);
    return err;
}

static jz_err jzn_require_catalogue_admin(const NSA *s, jazz_types_AuthorSubject claimed_author, const jazz_node_CommitUnitIngestContext *ctx);
static jz_err jzn_validate_migration_lens(const NSA *s, const jazz_protocol_MigrationLens *lens);

static void jzn_catalogue_ack(jazz_protocol_SyncMessage *m, bool has_revision, u64 revision, const jz_uuid *schema, const jz_uuid *lens) {
    memset(m, 0, sizeof *m);
    m->tag = JZP_SM_CATALOGUE_ACK;
    m->v.catalogue_ack.has_revision = has_revision;
    m->v.catalogue_ack.revision = revision;
    if (schema) { m->v.catalogue_ack.has_schema = true; m->v.catalogue_ack.schema = *schema; }
    if (lens) { m->v.catalogue_ack.has_lens = true; m->v.catalogue_ack.lens = *lens; }
    m->v.catalogue_ack.applied = true;
}

static jz_err jzn_apply_publish_schema(NSA *s, jazz_types_AuthorSubject author, const jazz_node_CommitUnitIngestContext *ctx,
                                       jazz_model_SchemaVersion schema_in, jazz_node_PublicationOutcome *out) {
    jz_err err;
    if (JZ_IS_ERR(err = jzn_require_catalogue_admin(s, author, ctx))) goto drop_in;
    jz_uuid version_id = jazz_model__schema__JazzSchema__version_id(&schema_in.schema);
    if (!jz_uuid_eq(&schema_in.id, &version_id)) { err = JZN_INVALID_CATALOGUE("schema id does not match schema payload"); goto drop_in; }
    if (!jz_btree_contains(&s->catalogue.catalogue_schemas, &schema_in.id)) {
        err = JZN_INVALID_CATALOGUE("non-genesis schema requires lineage publication");
        goto drop_in;
    }
    jazz_model_JazzSchema stripped;
    jazz_model__schema__JazzSchema__without_permissions(&schema_in.schema, &stripped);
    jazz_model_SchemaVersion schema;
    jazz_protocol__protocol__SchemaVersion__new(&stripped, &schema);
    jazz_model_JazzSchema_drop(&stripped);
    jazz_model_SchemaVersion_drop(&schema_in);
    {
        jazz_model_SchemaVersion copy;
        jazz_model_SchemaVersion_clone(&copy, &schema);
        jz_btree_insert(&s->catalogue.catalogue_schemas, &schema.id, &copy, NULL);
    }
    jz_btree_clear(&s->query.version_storage_sources_cache);
    jz_btree_clear(&s->query.read_policy_authorization_request_cache);
    jz_btree_clear(&s->query.policy_authorization_graph_cache);
    jz_btree_clear(&s->query.policy_authorization_graph_replacements);
    if (JZ_IS_ERR(err = jazz_node__node__state__catalogue__NodeState__persist_catalogue_schema(s, &schema))) goto drop_schema;
    if (jz_uuid_eq(&schema.id, &s->catalogue.active_schema.schema)) {
        jazz_node_ActiveSchema active;
        jazz_node_ActiveSchema_clone(&active, &s->catalogue.active_schema);
        jazz_model_JazzSchema compiled;
        jazz_model__schema__JazzSchema__with_permissions_from(&schema.schema, &active.compiled, &compiled);
        jazz_model_JazzSchema_drop(&active.compiled);
        active.compiled = compiled;
        if (JZ_IS_ERR(err = jazz_node__node__state__catalogue__NodeState__persist_active_schema(s, &active))) {
            jazz_node_ActiveSchema_drop(&active);
            goto drop_schema;
        }
        jazz_node__node__state__catalogue__NodeState__install_active_schema(s, active);
    } else if (jz_uuid_eq(&schema.id, &s->catalogue.local_schema_version_id)) {
        jazz_model_JazzSchema merged;
        jazz_model__schema__JazzSchema__with_permissions_from(&schema.schema, &s->catalogue.schema, &merged);
        jazz_model_JazzSchema_drop(&s->catalogue.schema);
        s->catalogue.schema = merged;
    }
    if (JZ_IS_ERR(err = jazz_node__node__state__catalogue__NodeState__ensure_provisional_physical_mapping(s, schema.id))) goto drop_schema;
    jazz_types_SchemaVersionAlias alias;
    if (JZ_IS_ERR(err = jazz_node__node__state__catalogue__NodeState__ensure_schema_version_alias(s, schema.id, &alias))) goto drop_schema;
    if (JZ_IS_ERR(err = jazz_node__node__physical__NodeState__synchronize_physical_version_tables(s))) goto drop_schema;
    jazz_node_PublicationOutcome outcome;
    if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__drain_parked_commit_units(s, &outcome))) goto drop_schema;
    if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__drain_parked_relay_commit_units(s)) ||
        JZ_IS_ERR(err = jazz_node__node__query_eval__subscriptions__NodeState__drain_parked_shape_registrations(s))) {
        jazz_node_PublicationOutcome_drop(&outcome);
        goto drop_schema;
    }
    jazz_protocol_SyncMessage ack;
    jzn_catalogue_ack(&ack, false, 0, &schema.id, NULL);
    jz_vec_insert(&outcome.value, 0, &ack, sizeof ack);
    *out = outcome;
    err = JZ_OK;
drop_schema:
    jazz_model_SchemaVersion_drop(&schema);
    return err;
drop_in:
    jazz_model_SchemaVersion_drop(&schema_in);
    return err;
}

static bool jzn_single_predecessor_lens(const jazz_protocol_SchemaLineagePublication *p, jz_uuid *out) {
    if (p->predecessors.len >= 1 && p->predecessors.len == 1) { *out = p->predecessors.ptr[0].lens.id; return true; }
    return false;
}

static bool jzn_all_predecessor_sources_known(const NSA *s, const jazz_protocol_SchemaLineagePublication *p) {
    for (u32 i = 0; i < p->predecessors.len; i++)
        if (!jz_btree_contains(&s->catalogue.catalogue_schemas, &p->predecessors.ptr[i].lens.source)) return false;
    return true;
}

static jz_err jzn_apply_publish_schema_with_lens(NSA *s, jazz_types_AuthorSubject author, const jazz_node_CommitUnitIngestContext *ctx,
                                                 u64 catalogue_seq, jazz_protocol_SchemaLineagePublication publication,
                                                 jazz_node_PublicationOutcome *out) {
    jz_err err;
    if (JZ_IS_ERR(err = jzn_require_catalogue_admin(s, author, ctx))) goto drop;
    if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__validate_schema_lineage_publication(&publication))) goto drop;
    if (catalogue_seq == 0) { err = JZN_INVALID_CATALOGUE("schema lineage catalogue sequence must be nonzero"); goto drop; }
    const jazz_model_SchemaVersion *schema = &publication.schema;
    jz_uuid acknowledged_lens;
    bool has_ack_lens = jzn_single_predecessor_lens(&publication, &acknowledged_lens);
    const jazz_node_StagedSchemaLineage *existing = jz_btree_get(&s->catalogue.active_lineages_by_target, &schema->id);
    if (existing) {
        if (!jazz_protocol_SchemaLineagePublication_eq(&existing->publication, &publication) || existing->catalogue_seq != catalogue_seq) {
            err = JZN_INVALID_CATALOGUE("schema lineage publication conflicts with catalogue");
            goto drop;
        }
        jazz_protocol_SyncMessage ack;
        jzn_catalogue_ack(&ack, false, 0, &schema->id, has_ack_lens ? &acknowledged_lens : NULL);
        jazz_node__node__ingest__settled_one(out, ack);
        err = JZ_OK;
        goto drop;
    }

    if (catalogue_seq <= s->catalogue.active_catalogue_seq) {
        err = JZN_INVALID_CATALOGUE("schema lineage catalogue sequence conflicts with active catalogue");
        goto drop;
    }
    const jazz_node_PendingSchemaLineage *pending_existing = jz_btree_get(&s->catalogue.pending_lineages, &catalogue_seq);
    if (pending_existing) {
        if (!jazz_protocol_SchemaLineagePublication_eq(&pending_existing->publication, &publication)) {
            err = JZN_INVALID_CATALOGUE("schema lineage catalogue sequence conflict");
            goto drop;
        }
        jazz_protocol_SchemaLineagePublication_drop(&publication);
    } else {
        if (jzn_all_predecessor_sources_known(s, &publication)) {
            err = jazz_node__node__state__catalogue__NodeState__validate_publication_sources(
                &publication, &s->catalogue.catalogue_schemas, &s->catalogue.physical_mappings,
                jazz_node__node__state__catalogue__NodeState__physical_identity_history_for_candidate(s, publication.schema.id, &publication.id));
            if (JZ_IS_ERR(err)) goto drop;
        }
        bool reserved = false;
        u64 *k; jazz_node_PendingSchemaLineage *pv;
        JZ_BTREE_FOR(&s->catalogue.pending_lineages, k, pv) {
            if (jz_uuid_eq(&pv->publication.schema.id, &schema->id)) { reserved = true; break; }
        }
        if (!reserved) {
            jazz_node_StagedSchemaLineage *sv;
            JZ_BTREE_FOR(&s->catalogue.staged_lineages, k, sv) {
                if (jz_uuid_eq(&sv->publication.schema.id, &schema->id)) { reserved = true; break; }
            }
        }
        if (reserved) { err = JZN_INVALID_CATALOGUE("schema lineage target is already reserved"); goto drop; }
        jazz_node_PendingSchemaLineage pending = { .catalogue_seq = catalogue_seq, .publication = publication };
        if (JZ_IS_ERR(err = jazz_node__node__state__catalogue__NodeState__persist_pending_schema_lineage(s, &pending))) {
            jazz_node_PendingSchemaLineage_drop(&pending);
            return err;
        }
        jz_btree_insert(&s->catalogue.pending_lineages, &catalogue_seq, &pending, NULL);
    }
    return jazz_node__node__ingest__NodeState__drain_pending_schema_lineages(s, out);
drop:
    jazz_protocol_SchemaLineagePublication_drop(&publication);
    return err;
}

static jz_err jzn_activate_pending_schema_lineages(NSA *s, u32 mode, jazz_node_PublicationOutcome *out);

jz_err jazz_node__node__ingest__NodeState__recover_pending_schema_lineages(NSA *s) {
    jazz_node_PublicationOutcome outcome;
    JZ_TRY(jzn_activate_pending_schema_lineages(s, JZN_CATALOGUE_ACTIVATION_COLD_OPEN, &outcome));
    jazz_node_PublicationOutcome_drop(&outcome);
    return JZ_OK;
}

jz_err jazz_node__node__ingest__NodeState__drain_pending_schema_lineages(NSA *s, jazz_node_PublicationOutcome *out) {
    return jzn_activate_pending_schema_lineages(s, JZN_CATALOGUE_ACTIVATION_LIVE, out);
}

static void jzn_install_staged_schema_lineage_in_memory(NSA *s, const jazz_node_StagedSchemaLineage *staged);

/* A new logical column widens the shared physical current-row descriptor. */
static bool jzn_widens_shared_current_descriptor(const NSA *s, const jazz_node_StagedSchemaLineage *staged) {
    jz_str *tk; jazz_node_TablePhysicalMapping *target;
    JZ_BTREE_FOR(&staged->mapping.tables, tk, target) {
        jz_btree existing_columns;
        jz_btree_init(&existing_columns, &jzn_desc_u64_set);
        jz_uuid *sk; jazz_node_SchemaPhysicalMapping *mapping;
        JZ_BTREE_FOR(&s->catalogue.physical_mappings, sk, mapping) {
            jz_str *ek; jazz_node_TablePhysicalMapping *existing;
            JZ_BTREE_FOR(&mapping->tables, ek, existing) {
                if (existing->table_id != target->table_id) continue;
                jz_str *ck; u64 *cv;
                JZ_BTREE_FOR(&existing->columns, ck, cv) jz_btree_insert(&existing_columns, cv, NULL, NULL);
            }
        }
        bool widens = false;
        if (existing_columns.len != 0) {
            jz_str *ck; u64 *cv;
            JZ_BTREE_FOR(&target->columns, ck, cv) {
                if (!jz_btree_contains(&existing_columns, cv)) { widens = true; break; }
            }
        }
        jz_btree_drop(&existing_columns);
        if (widens) return true;
    }
    return false;
}

static jz_err jzn_activation_failed(NSA *s, const jazz_node_StagedSchemaLineage *staged) {
    jazz_node__node__ingest__NodeState__remove_staged_schema_lineage_from_memory(s, staged);
    s->catalogue_activation_failed = true;
    return jz_err_new(JZN_E_CATALOGUE_ACTIVATION_FAILED, "catalogue activation failed");
}

static jz_err jzn_stage_next_lineage(NSA *s, u64 next, const jazz_protocol_SchemaLineagePublication *publication,
                                     jazz_node_StagedSchemaLineage *out) {
    /* Provisional identities are only candidates. Reconciliation can replace
     * them with identities inherited from the source schema, so allocate
     * against copies and commit only identities retained by the durable
     * staged mapping. */
    u64 provisional_next_table_id = s->catalogue.next_physical_table_id;
    u64 provisional_next_column_id = s->catalogue.next_physical_column_id;
    jazz_protocol_PhysicalIdentityManifest identities;
    jazz_protocol_PhysicalIdentityManifest_clone(&identities, &publication->physical_identities);
    jazz_node_SchemaPhysicalMapping fresh;
    JZ_TRY(jazz_node__node__physical__allocate_provisional_physical_mapping(&publication->schema.schema, identities,
                                                                           &provisional_next_table_id, &provisional_next_column_id, &fresh));
    jazz_node_SchemaPhysicalMapping mapping;
    jz_err err = jazz_node__node__state__catalogue__NodeState__reconcile_publication_mapping(&s->catalogue, publication, &fresh, &mapping);
    jazz_node_SchemaPhysicalMapping_drop(&fresh);
    if (JZ_IS_ERR(err)) return err;
    u64 next_physical_table_id = s->catalogue.next_physical_table_id;
    u64 next_physical_column_id = s->catalogue.next_physical_column_id;
    jz_str *tk; jazz_node_TablePhysicalMapping *table;
    JZ_BTREE_FOR(&mapping.tables, tk, table) {
        if (table->table_id == UINT64_MAX) { jazz_node_SchemaPhysicalMapping_drop(&mapping); return JZN_ISV("physical table id exhausted"); }
        if (table->table_id + 1 > next_physical_table_id) next_physical_table_id = table->table_id + 1;
        jz_str *ck; u64 *column;
        JZ_BTREE_FOR(&table->columns, ck, column) {
            if (*column == UINT64_MAX) { jazz_node_SchemaPhysicalMapping_drop(&mapping); return JZN_ISV("physical column id exhausted"); }
            if (*column + 1 > next_physical_column_id) next_physical_column_id = *column + 1;
        }
    }
    jazz_node_StagedSchemaLineage staged;
    memset(&staged, 0, sizeof staged);
    staged.catalogue_seq = next;
    jazz_protocol_SchemaLineagePublication_clone(&staged.publication, publication);
    staged.mapping = mapping;
    if (JZ_IS_ERR(err = jazz_node__node__state__catalogue__NodeState__next_schema_version_alias(s, &staged.alias))) {
        jazz_node_StagedSchemaLineage_drop(&staged);
        return err;
    }
    jz_btree candidate_mappings, candidate_aliases;
    jz_btree_clone(&candidate_mappings, &s->catalogue.physical_mappings, NULL, jazz_node_SchemaPhysicalMapping_clone);
    {
        jazz_node_SchemaPhysicalMapping m;
        jazz_node_SchemaPhysicalMapping_clone(&m, &staged.mapping);
        jz_btree_insert(&candidate_mappings, &staged.publication.schema.id, &m, NULL);
    }
    jz_btree_clone(&candidate_aliases, &s->catalogue.schema_version_aliases, NULL, NULL);
    jz_btree_insert(&candidate_aliases, &staged.publication.schema.id, &staged.alias, NULL);
    err = jazz_node__node__physical__validate_scalar_enum_case_provenance(&candidate_mappings, &candidate_aliases);
    if (!JZ_IS_ERR(err)) err = jazz_node__node__physical__validate_payload_enum_case_provenance(&candidate_mappings, &candidate_aliases);
    jz_btree_drop(&candidate_mappings);
    jz_btree_drop(&candidate_aliases);
    if (!JZ_IS_ERR(err)) err = jazz_node__node__state__catalogue__NodeState__persist_catalogue_schema_lineage(s, &staged);
    if (JZ_IS_ERR(err)) { jazz_node_StagedSchemaLineage_drop(&staged); return err; }
    s->catalogue.next_physical_table_id = next_physical_table_id;
    s->catalogue.next_physical_column_id = next_physical_column_id;
    {
        jazz_node_StagedSchemaLineage copy;
        jazz_node_StagedSchemaLineage_clone(&copy, &staged);
        jz_btree_insert(&s->catalogue.staged_lineages, &next, &copy, NULL);
    }
    *out = staged;
    return JZ_OK;
}

static jz_err jzn_activate_pending_schema_lineages(NSA *s, u32 mode, jazz_node_PublicationOutcome *out) {
    jz_err err = JZ_OK;
    jazz_node_PublicationOutcome outcome;
    jazz_node__node__ingest__settled_empty(&outcome);
    for (;;) {
        u64 next = s->catalogue.active_catalogue_seq == UINT64_MAX ? UINT64_MAX : s->catalogue.active_catalogue_seq + 1;
        const jazz_node_PendingSchemaLineage *found = jz_btree_get(&s->catalogue.pending_lineages, &next);
        if (!found) break;
        jazz_node_PendingSchemaLineage pending;
        jazz_node_PendingSchemaLineage_clone(&pending, found);
        const jazz_protocol_SchemaLineagePublication *publication = &pending.publication;
        if (!jzn_all_predecessor_sources_known(s, publication)) { jazz_node_PendingSchemaLineage_drop(&pending); break; }
        jz_err validation = jazz_node__node__state__catalogue__NodeState__validate_publication_sources(
            publication, &s->catalogue.catalogue_schemas, &s->catalogue.physical_mappings,
            jazz_node__node__state__catalogue__NodeState__physical_identity_history_for_candidate(s, publication->schema.id, &publication->id));
        /* A parked sibling was admitted against the catalogue prefix that
         * existed when it arrived. Reconcile it again only when its sequence
         * becomes active. */
        if (!JZ_IS_ERR(validation))
            validation = jazz_node__node__state__catalogue__NodeState__validate_pending_schema_lineage_physical_mapping(s, &pending);
        if (JZ_IS_ERR(validation)) {
            jz_err_drop(&validation);
            err = jazz_node__node__state__catalogue__NodeState__remove_pending_schema_lineage(s, next, publication->id);
            jazz_node_PendingSchemaLineage_drop(&pending);
            if (JZ_IS_ERR(err)) goto fail;
            break;
        }
        if (jz_btree_contains(&s->catalogue.active_lineages_by_target, &publication->schema.id)) {
            jazz_node_PendingSchemaLineage_drop(&pending);
            err = JZN_INVALID_CATALOGUE("schema lineage target already has an active bundle");
            goto fail;
        }
        jazz_node_StagedSchemaLineage staged;
        const jazz_node_StagedSchemaLineage *already = jz_btree_get(&s->catalogue.staged_lineages, &next);
        if (already) {
            if (!jazz_protocol_SchemaLineagePublication_eq(&already->publication, publication)) {
                jazz_node_PendingSchemaLineage_drop(&pending);
                err = JZN_INVALID_CATALOGUE("staged schema lineage conflicts with pending bundle");
                goto fail;
            }
            jazz_node_StagedSchemaLineage_clone(&staged, already);
        } else {
            err = jzn_stage_next_lineage(s, next, publication, &staged);
            if (JZ_IS_ERR(err)) { jazz_node_PendingSchemaLineage_drop(&pending); goto fail; }
        }
        jazz_node_PendingSchemaLineage_drop(&pending);

        /* A new logical column widens the shared physical current-row
         * descriptor; a pure new variant over the same physical columns
         * remains safe to refresh in place. */
        bool widens_shared_current_descriptor = jzn_widens_shared_current_descriptor(s, &staged);

        jzn_install_staged_schema_lineage_in_memory(s, &staged);
        /* Install widened variant cases in the live registry rather than
         * reopening the database. */
        jz_err e = jazz_node__node__physical__NodeState__synchronize_physical_version_tables(s);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = jzn_activation_failed(s, &staged); jazz_node_StagedSchemaLineage_drop(&staged); goto fail; }
        /* Closed raw receivers can otherwise make a cold server look live
         * until a later non-empty notification. Prune them explicitly. */
        if (mode == JZN_CATALOGUE_ACTIVATION_LIVE) {
            e = groove__db__Database__prune_dropped_subscriptions(jzn_db(s));
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = jzn_activation_failed(s, &staged); jazz_node_StagedSchemaLineage_drop(&staged); goto fail; }
        }
        bool rebuild_cold_runtime = mode == JZN_CATALOGUE_ACTIVATION_COLD_OPEN ||
                                    groove__db__Database__runtime_stats_active_subscriptions(jzn_db(s)) == 0;
        if (rebuild_cold_runtime) {
            e = jazz_node__node__state__lifecycle__NodeState__rebuild_database_slot(s);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = jzn_activation_failed(s, &staged); jazz_node_StagedSchemaLineage_drop(&staged); goto fail; }
        }
        if (widens_shared_current_descriptor && !rebuild_cold_runtime) {
            /* Peer-serving and maintained caches are compiled against the
             * shared current-row descriptor too. Retire those handles now. */
            jazz_node__node__state__lifecycle__NodeState__invalidate_runtime_handles_after_database_rebuild(s);
        }
        groove_DatabaseBatch *batch = groove__db__Database__open_batch(jzn_db(s));
        e = jazz_node__node__state__catalogue__NodeState__write_active_schema_lineage_to_batch(batch, &staged);
        if (JZ_IS_ERR(e)) { groove__db__DatabaseBatch__drop(batch); err = e; jazz_node_StagedSchemaLineage_drop(&staged); goto fail; }
        /* A catalogue-only batch writes no history rows, so it applies directly
         * rather than through `apply_node_batch`. */
        groove_AppliedBatch *applied;
        e = groove__db__Database__apply_batch(jzn_db(s), batch, &applied);
        if (!JZ_IS_ERR(e)) {
            groove_PersistProgress *persisted = groove__db__Database__persist_with_progress(jzn_db(s), applied);
            e = groove__db__Database__finish_persistence(jzn_db(s), persisted);
            groove_AppliedBatch_drop(applied);
        }
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); err = jzn_activation_failed(s, &staged); jazz_node_StagedSchemaLineage_drop(&staged); goto fail; }
        jz_btree_remove(&s->catalogue.staged_lineages, &next, NULL, NULL);
        jz_btree_remove(&s->catalogue.pending_lineages, &next, NULL, NULL);
        {
            jazz_node_StagedSchemaLineage copy;
            jazz_node_StagedSchemaLineage_clone(&copy, &staged);
            jz_btree_insert(&s->catalogue.active_lineages_by_target, &staged.publication.schema.id, &copy, NULL);
        }
        s->catalogue.active_catalogue_seq = next;
        if (mode == JZN_CATALOGUE_ACTIVATION_LIVE && widens_shared_current_descriptor)
            s->groove_runtime_token = jazz_node__node__next_groove_runtime_token();
        if (mode == JZN_CATALOGUE_ACTIVATION_LIVE) {
            jz_uuid lens;
            bool has_lens = jzn_single_predecessor_lens(&staged.publication, &lens);
            jazz_protocol_SyncMessage ack;
            jzn_catalogue_ack(&ack, true, next, &staged.publication.schema.id, has_lens ? &lens : NULL);
            JZ_VEC_PUSH(outcome.value, ack);
            jazz_node_PublicationOutcome drained;
            err = jazz_node__node__ingest__NodeState__drain_parked_commit_units(s, &drained);
            if (!JZ_IS_ERR(err)) {
                jazz_node__node__PublicationOutcome__append_outcome(&outcome, &drained);
                err = jazz_node__node__ingest__NodeState__drain_parked_relay_commit_units(s);
            }
            if (!JZ_IS_ERR(err)) err = jazz_node__node__query_eval__subscriptions__NodeState__drain_parked_shape_registrations(s);
            if (JZ_IS_ERR(err)) { jazz_node_StagedSchemaLineage_drop(&staged); goto fail; }
        }
        jazz_node_StagedSchemaLineage_drop(&staged);
    }
    *out = outcome;
    return JZ_OK;
fail:
    jazz_node_PublicationOutcome_drop(&outcome);
    return err;
}

/* lens_path_cache .. policy_authorization_graph_replacements, as cleared by
 * install/remove of a staged lineage. */
static void jzn_clear_lineage_caches(NSA *s) {
    jz_btree_clear(&s->catalogue.lens_path_cache);
    jz_btree_clear(&s->catalogue.compiled_lens_cache);
    jz_btree_clear(&s->catalogue.physical_write_plan_cache);
    jz_btree_clear(&s->catalogue.physical_current_winner_projections);
    jz_btree_clear(&s->query.version_storage_sources_cache);
    jz_btree_clear(&s->query.query_shape_cache);
    jz_btree_clear(&s->query.compiled_query_program_cache);
    jz_btree_clear(&s->query.query_program_templates);
    jz_deque_drop(&s->query.supported_query_program_requests, NULL);
    jz_deque_init(&s->query.supported_query_program_requests, sizeof(void *));
    jz_btree_clear(&s->query.read_policy_authorization_request_cache);
    jz_btree_clear(&s->query.policy_authorization_graph_cache);
    jz_btree_clear(&s->query.policy_authorization_graph_replacements);
}

static void jzn_install_staged_schema_lineage_in_memory(NSA *s, const jazz_node_StagedSchemaLineage *staged) {
    jazz_model_SchemaVersion schema;
    jazz_model_SchemaVersion_clone(&schema, &staged->publication.schema);
    jz_btree_insert(&s->catalogue.catalogue_schemas, &staged->publication.schema.id, &schema, NULL);
    for (u32 i = 0; i < staged->publication.predecessors.len; i++) {
        const jazz_protocol_MigrationLens *lens = &staged->publication.predecessors.ptr[i].lens;
        jazz_protocol_MigrationLens copy;
        jazz_protocol_MigrationLens_clone(&copy, lens);
        jz_btree_insert(&s->catalogue.catalogue_lenses, &lens->id, &copy, NULL);
    }
    jz_btree_insert(&s->catalogue.schema_version_aliases, &staged->publication.schema.id, &staged->alias, NULL);
    jazz_node_SchemaPhysicalMapping mapping;
    jazz_node_SchemaPhysicalMapping_clone(&mapping, &staged->mapping);
    jz_btree_insert(&s->catalogue.physical_mappings, &staged->publication.schema.id, &mapping, NULL);
    jzn_clear_lineage_caches(s);
}

void jazz_node__node__ingest__NodeState__remove_staged_schema_lineage_from_memory(NSA *s, const jazz_node_StagedSchemaLineage *staged) {
    jz_btree_remove(&s->catalogue.catalogue_schemas, &staged->publication.schema.id, NULL, NULL);
    for (u32 i = 0; i < staged->publication.predecessors.len; i++)
        jz_btree_remove(&s->catalogue.catalogue_lenses, &staged->publication.predecessors.ptr[i].lens.id, NULL, NULL);
    jz_btree_remove(&s->catalogue.schema_version_aliases, &staged->publication.schema.id, NULL, NULL);
    jz_btree_remove(&s->catalogue.physical_mappings, &staged->publication.schema.id, NULL, NULL);
    jzn_clear_lineage_caches(s);
}

static jz_err jzn_apply_publish_lens(NSA *s, jazz_types_AuthorSubject author, const jazz_node_CommitUnitIngestContext *ctx,
                                     jazz_protocol_MigrationLens lens, jazz_protocol_SyncMessageVec *out) {
    jz_err err;
    if (JZ_IS_ERR(err = jzn_require_catalogue_admin(s, author, ctx))) goto drop;
    jz_uuid content_id = jazz_protocol__protocol__MigrationLens__content_id(&lens);
    if (!jz_uuid_eq(&lens.id, &content_id)) { err = JZN_INVALID_CATALOGUE("lens id does not match lens payload"); goto drop; }
    if (!jz_btree_contains(&s->catalogue.catalogue_schemas, &lens.source) || !jz_btree_contains(&s->catalogue.catalogue_schemas, &lens.target)) {
        err = JZN_INVALID_CATALOGUE("lens endpoint is unknown");
        goto drop;
    }
    if (JZ_IS_ERR(err = jzn_validate_migration_lens(s, &lens))) goto drop;
    bool installed = !jz_btree_contains(&s->catalogue.catalogue_lenses, &lens.id);
    if (installed) {
        jazz_node_SchemaPhysicalMapping candidate;
        if (JZ_IS_ERR(err = jazz_node__node__state__catalogue__NodeState__reconcile_physical_mapping_for_lens(s, &lens, &candidate))) goto drop;
        const jazz_node_SchemaPhysicalMapping *authoritative = jz_btree_get(&s->catalogue.physical_mappings, &lens.target);
        if (!authoritative) {
            jazz_node_SchemaPhysicalMapping_drop(&candidate);
            err = JZN_ISV("authoritative physical mapping missing");
            goto drop;
        }
        bool same = jazz_node_SchemaPhysicalMapping_eq(&candidate, authoritative);
        jazz_node_SchemaPhysicalMapping_drop(&candidate);
        if (!same) { err = JZN_INVALID_CATALOGUE("cross-lens conflicts with authoritative physical mapping"); goto drop; }
    }
    if (JZ_IS_ERR(err = jazz_node__node__state__catalogue__NodeState__persist_catalogue_lens_with_physical_metadata(s, &lens, NULL))) goto drop;
    jz_uuid lens_id = lens.id;
    if (installed) jz_btree_insert(&s->catalogue.catalogue_lenses, &lens.id, &lens, NULL);
    else jazz_protocol_MigrationLens_drop(&lens);
    jz_btree_clear(&s->catalogue.lens_path_cache);
    jz_btree_clear(&s->catalogue.compiled_lens_cache);
    jz_btree_clear(&s->query.version_storage_sources_cache);
    jz_btree_clear(&s->catalogue.physical_current_winner_projections);
    jz_btree_clear(&s->query.query_shape_cache);
    jz_btree_clear(&s->query.compiled_query_program_cache);
    jz_btree_clear(&s->query.query_program_templates);
    jz_deque_drop(&s->query.supported_query_program_requests, NULL);
    jz_deque_init(&s->query.supported_query_program_requests, sizeof(void *));
    jz_btree_clear(&s->query.read_policy_authorization_request_cache);
    jz_btree_clear(&s->query.policy_authorization_graph_cache);
    jz_btree_clear(&s->query.policy_authorization_graph_replacements);
    /* Both endpoint schemas are already Active and their agreeing physical
     * projection cases were registered during activation; a cross-lens adds a
     * catalogue path only. */
    jazz_protocol_SyncMessage ack;
    jzn_catalogue_ack(&ack, false, 0, NULL, &lens_id);
    jazz_protocol_SyncMessageVec v = {0};
    JZ_VEC_PUSH(v, ack);
    *out = v;
    return JZ_OK;
drop:
    jazz_protocol_MigrationLens_drop(&lens);
    return err;
}

static jz_err jzn_require_catalogue_admin(const NSA *s, jazz_types_AuthorSubject claimed_author, const jazz_node_CommitUnitIngestContext *ctx) {
    (void)s; (void)claimed_author;
    if (ctx && jazz_types__ids__AuthorSubject__eq(&ctx->identity, &jazz_types_AuthorSubject_SYSTEM) &&
        (ctx->trust == JZN_TRUST_BACKEND || ctx->trust == JZN_TRUST_AUTHORITY))
        return JZ_OK;
    return jz_err_new(JZN_E_UNAUTHORIZED_CATALOGUE_UPDATE, "unauthorized catalogue update");
}

static jz_err jzn_validate_migration_lens(const NSA *s, const jazz_protocol_MigrationLens *lens) {
    const jazz_model_SchemaVersion *source = jz_btree_get(&s->catalogue.catalogue_schemas, &lens->source);
    if (!source) return JZN_INVALID_CATALOGUE("lens endpoint is unknown");
    const jazz_model_SchemaVersion *target = jz_btree_get(&s->catalogue.catalogue_schemas, &lens->target);
    if (!target) return JZN_INVALID_CATALOGUE("lens endpoint is unknown");
    return jazz_node__node__ingest__NodeState__validate_migration_lens_between(lens, source, target);
}

jz_err jazz_node__node__ingest__NodeState__validate_migration_lens_between(const jazz_protocol_MigrationLens *lens,
                                                                          const jazz_model_SchemaVersion *source,
                                                                          const jazz_model_SchemaVersion *target) {
    return jazz_node__node__migration_validation__validate_migration_lens_between(lens, source, target);
}

jz_err jazz_node__node__ingest__NodeState__validate_lineage_table_partition(const jazz_model_JazzSchema *source, const jazz_model_JazzSchema *target,
                                                                           const jazz_protocol_MigrationLens *lens, const jzn_StrVec *new_tables,
                                                                           const jzn_StrVec *dropped_tables) {
    return jazz_node__node__migration_validation__validate_lineage_table_partition(source, target, lens, new_tables, dropped_tables);
}

static usize jzn_sat_add(usize a, usize b) { return a + b < a ? (usize)-1 : a + b; }
static bool jzn_lineage_name_in_bounds(const jz_str *name) { return name->len != 0 && name->len <= JZN_MAX_SCHEMA_LINEAGE_NAME_BYTES; }

jz_err jazz_node__node__ingest__NodeState__validate_schema_lineage_publication_bounds(const jazz_protocol_SchemaLineagePublication *p) {
    usize declaration_count = p->predecessors.len;
    usize operation_count = 0;
    bool names_in_bounds = true;
    for (u32 i = 0; i < p->predecessors.len; i++) {
        const jazz_protocol_SchemaPredecessor *pred = &p->predecessors.ptr[i];
        const jazz_protocol_TableLens *tables = (const jazz_protocol_TableLens *)pred->lens.table_lenses.ptr;
        usize n = jzn_sat_add(jzn_sat_add(pred->lens.table_lenses.len, pred->new_tables.len), pred->dropped_tables.len);
        declaration_count = jzn_sat_add(declaration_count, n);
        for (u32 t = 0; t < pred->lens.table_lenses.len; t++) operation_count = jzn_sat_add(operation_count, tables[t].ops.len);
        if (!names_in_bounds) continue;
        for (u32 j = 0; j < pred->new_tables.len && names_in_bounds; j++) names_in_bounds = jzn_lineage_name_in_bounds(&pred->new_tables.ptr[j]);
        for (u32 j = 0; j < pred->dropped_tables.len && names_in_bounds; j++) names_in_bounds = jzn_lineage_name_in_bounds(&pred->dropped_tables.ptr[j]);
        for (u32 t = 0; t < pred->lens.table_lenses.len && names_in_bounds; t++)
            names_in_bounds = jzn_lineage_name_in_bounds(&tables[t].source_table) && jzn_lineage_name_in_bounds(&tables[t].target_table);
    }
    if (declaration_count > JZN_MAX_SCHEMA_LINEAGE_DECLARATIONS || operation_count > JZN_MAX_SCHEMA_LINEAGE_OPS || !names_in_bounds)
        return JZN_INVALID_CATALOGUE("schema lineage publication exceeds structural limits");
    return JZ_OK;
}

jz_err jazz_node__node__ingest__NodeState__validate_schema_lineage_publication(const jazz_protocol_SchemaLineagePublication *p) {
    JZ_TRY(jazz_node__node__ingest__NodeState__validate_schema_lineage_publication_bounds(p));
    jz_uuid content_id = jazz_protocol__protocol__SchemaLineagePublication__content_id(p);
    if (!jz_uuid_eq(&p->id, &content_id)) return JZN_INVALID_CATALOGUE("schema lineage publication id mismatch");
    const char *msg = jazz_protocol__protocol__PhysicalIdentityManifest__validate_for_schema(&p->physical_identities, &p->schema.schema);
    if (msg) return JZN_INVALID_CATALOGUE(msg);
    jz_uuid version_id = jazz_model__schema__JazzSchema__version_id(&p->schema.schema);
    if (!jz_uuid_eq(&p->schema.id, &version_id)) return JZN_INVALID_CATALOGUE("schema id does not match schema payload");
    if (p->predecessors.len == 0) return JZN_INVALID_CATALOGUE("schema requires a predecessor");
    jz_btree sources;
    jz_btree_init(&sources, &jzn_desc_uuid_set_a);
    jz_err err = JZ_OK;
    for (u32 i = 0; i < p->predecessors.len; i++) {
        const jazz_protocol_MigrationLens *lens = &p->predecessors.ptr[i].lens;
        bool inserted = !jz_btree_insert(&sources, &lens->source, NULL, NULL);
        if (!inserted || jz_uuid_eq(&lens->source, &p->schema.id)) { err = JZN_INVALID_CATALOGUE("duplicate or self schema predecessor"); break; }
        jz_uuid lens_content = jazz_protocol__protocol__MigrationLens__content_id(lens);
        if (!jz_uuid_eq(&lens->id, &lens_content)) { err = JZN_INVALID_CATALOGUE("lens id does not match lens payload"); break; }
        if (!jz_uuid_eq(&lens->target, &p->schema.id)) { err = JZN_INVALID_CATALOGUE("lineage lens target does not match schema"); break; }
    }
    jz_btree_drop(&sources);
    return err;
}
