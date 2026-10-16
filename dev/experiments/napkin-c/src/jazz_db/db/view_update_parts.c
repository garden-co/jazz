/* view_update_parts.c: crates/jazz/layers/db/src/db/view_update_parts.rs
 * Bounded parts for a ViewUpdate larger than the routed payload limit. */
#include "view_update_parts.h"

#define PART_LENGTH_PREFIX_SLACK 64u
#define USIZE_MAX ((usize)-1)

typedef JZ_VEC(jazz_protocol_VersionBundle) BundleVec;
typedef JZ_VEC(jazz_protocol_VersionCarrier) CarrierVec;

static usize sat_add(usize a, usize b) { usize r = a + b; return r < a ? USIZE_MAX : r; }

void jazz_db__db__view_update_parts__OversizedViewUpdate__Display__fmt(const jazz_db_OversizedViewUpdate *e, jz_str *f) {
    switch (e->tag) {
    case JAZZ_OVU_HEADER_EXCEEDS_LIMIT:
        jz_str_appendf(f, "unsupported view update: its %zu-byte header exceeds the %zu-byte routed payload limit",
                       (size_t)e->bytes, (size_t)e->bound);
        break;
    case JAZZ_OVU_SUPPORTING_ROW_EXCEEDS_LIMIT:
        jz_str_appendf(f, "unsupported view update: one %zu-byte supporting row exceeds the %zu-byte part capacity",
                       (size_t)e->bytes, (size_t)e->bound);
        break;
    case JAZZ_OVU_VERSION_BUNDLE_EXCEEDS_LIMIT:
        jz_str_appendf(f, "unsupported view update: one %zu-byte transaction bundle exceeds the %zu-byte part capacity",
                       (size_t)e->bytes, (size_t)e->bound);
        break;
    default:
        jz_str_push_cstr(f, "unsupported view update: malformed version-carrier run");
        break;
    }
}

/* The two supporting-row lists of a transition, in declaration order; the
 * template keeps kind and revisions with empty lists. */
static void jazz_db__db__view_update_parts__take_lists(jazz_protocol_SupportingRowsUpdate *rows,
                                                       jazz_protocol_SupportingRowVec *first,
                                                       jazz_protocol_SupportingRowVec *second) {
    *first = rows->first;
    memset(&rows->first, 0, sizeof rows->first);
    if (rows->tag == JAZZ_SRU_SNAPSHOT) {
        memset(second, 0, sizeof *second);
    } else {
        *second = rows->second;
    }
    memset(&rows->second, 0, sizeof rows->second);
}

/* Fill a template's lists. A snapshot has no second list. */
static jazz_protocol_SupportingRowsUpdate jazz_db__db__view_update_parts__with_lists(const jazz_protocol_SupportingRowsUpdate *template_,
                                                                                    jazz_protocol_SupportingRowVec first,
                                                                                    jazz_protocol_SupportingRowVec second) {
    jazz_protocol_SupportingRowsUpdate out;
    memset(&out, 0, sizeof out);
    out.tag = template_->tag;
    memcpy(out.revision, template_->revision, 16);
    out.first = first;
    if (template_->tag != JAZZ_SRU_SNAPSHOT) {
        memcpy(out.predecessor, template_->predecessor, 16);
        out.second = second;
    } else {
        jz_vec_drop(&second, sizeof(jazz_protocol_SupportingRow), NULL);
    }
    return out;
}

typedef struct { u8 kind; u8 predecessor[16]; u8 revision[16]; } TransitionIdentity;

static TransitionIdentity jazz_db__db__view_update_parts__transition_identity(const jazz_protocol_SupportingRowsUpdate *rows) {
    TransitionIdentity id;
    memset(&id, 0, sizeof id);
    id.kind = (u8)rows->tag;
    if (rows->tag != JAZZ_SRU_SNAPSHOT) memcpy(id.predecessor, rows->predecessor, 16);
    memcpy(id.revision, rows->revision, 16);
    return id;
}

static usize jazz_db__db__view_update_parts__encoded_len(const jazz_protocol_SyncMessage *message) {
    u32 n;
    if (!jazz_protocol__wire__encoded_sync_message_len(message, &n)) return USIZE_MAX;
    return n;
}

static usize serialized_size_row(const jazz_protocol_SupportingRow *row) {
    jz_bytes buf = {0};
    jz_pc_w w = { &buf };
    jazz_protocol__protocol__SupportingRow__serialize(row, &w);
    usize n = buf.len;
    jz_bytes_drop(&buf);
    return n;
}

static usize serialized_size_bundle(const jazz_protocol_VersionBundle *b) {
    jz_bytes buf = {0};
    jz_pc_w w = { &buf };
    jazz_protocol__protocol__VersionBundle__serialize(b, &w);
    usize n = buf.len;
    jz_bytes_drop(&buf);
    return n;
}

typedef struct PartContents {
    jazz_protocol_SupportingRowVec first;
    jazz_protocol_SupportingRowVec second;
    BundleVec bundles;
    usize bytes;
} PartContents;

static bool PartContents__is_empty(const PartContents *p) {
    return p->first.len == 0 && p->second.len == 0 && p->bundles.len == 0;
}

static void PartContents__drop(void *p) {
    PartContents *c = p;
    jz_vec_drop(&c->first, sizeof(jazz_protocol_SupportingRow), NULL);
    jz_vec_drop(&c->second, sizeof(jazz_protocol_SupportingRow), NULL);
    jz_vec_drop(&c->bundles, sizeof(jazz_protocol_VersionBundle), jazz_protocol__protocol__VersionBundle__drop);
}

typedef struct Shell {
    jazz_protocol_SubscriptionKey subscription;
    jazz_types_GlobalTime settled_through;
    const jazz_protocol_SupportingRowsUpdate *template_;
} Shell;

static jazz_protocol_ViewUpdatePayload shell(const Shell *s, jazz_protocol_PeerPayloadInventory inventory,
                                             jazz_protocol_SupportingRowVec first, jazz_protocol_SupportingRowVec second,
                                             CarrierVec carriers) {
    jazz_protocol_ViewUpdatePayload p;
    p.subscription = s->subscription;
    p.settled_through = s->settled_through;
    p.version_carriers.ptr = carriers.ptr; p.version_carriers.len = carriers.len; p.version_carriers.cap = carriers.cap;
    p.peer_payload_inventory = inventory;
    p.supporting_rows = jazz_db__db__view_update_parts__with_lists(s->template_, first, second);
    return p;
}

static jazz_protocol_SyncMessage wrap(bool last, jazz_protocol_ViewUpdatePayload payload) {
    jazz_protocol_SyncMessage m;
    memset(&m, 0, sizeof m);
    m.tag = last ? JAZZ_SM_VIEW_UPDATE : JAZZ_SM_VIEW_UPDATE_PART;
    m.v.view_update = payload;
    return m;
}

typedef JZ_VEC(PartContents) PartVec;

static void admit(PartVec *parts, PartContents *current, usize bytes, usize capacity) {
    if (!PartContents__is_empty(current) && sat_add(current->bytes, bytes) > capacity) {
        JZ_VEC_PUSH(*parts, *current);
        memset(current, 0, sizeof *current);
    }
    current->bytes = sat_add(current->bytes, bytes);
}

static jazz_protocol_SupportingRowVec clone_rows(const jazz_protocol_SupportingRowVec *v) {
    jazz_protocol_SupportingRowVec out = {0};
    jz_vec_clone(&out, v, sizeof(jazz_protocol_SupportingRow), jazz_protocol__protocol__SupportingRow__clone);
    return out;
}

/* Split `view` into parts whose encodings each fit `limit` bytes. */
bool jazz_db__db__view_update_parts__split_view_update(jazz_protocol_ViewUpdatePayload view, usize limit,
                                                       jazz_db_SyncMessageVec *out, jazz_db_OversizedViewUpdate *err) {
    memset(out, 0, sizeof *out);
    BundleVec bundles = {0};
    jz_err e = jazz_protocol__protocol__expand_version_carriers(view.version_carriers.ptr, view.version_carriers.len, (void *)&bundles);
    if (JZ_IS_ERR(e)) {
        jz_err_drop(&e);
        jazz_protocol__protocol__ViewUpdatePayload__drop(&view);
        err->tag = JAZZ_OVU_MALFORMED_CARRIER; err->bytes = 0; err->bound = 0;
        return false;
    }
    jz_vec_drop(&view.version_carriers, sizeof(jazz_protocol_VersionCarrier), jazz_protocol__protocol__VersionCarrier__drop);
    jazz_protocol_SupportingRowsUpdate template_ = view.supporting_rows;
    jazz_protocol_SupportingRowVec first, second;
    jazz_db__db__view_update_parts__take_lists(&template_, &first, &second);
    Shell sh = { view.subscription, view.settled_through, &template_ };
    jazz_protocol_PeerPayloadInventory peer_payload_inventory = view.peer_payload_inventory;

    /* The final part's header is the largest (it carries the inventory), so it bounds every part's header. */
    jazz_protocol_PeerPayloadInventory inv_clone;
    jazz_protocol__protocol__PeerPayloadInventory__clone(&inv_clone, &peer_payload_inventory);
    jazz_protocol_SupportingRowVec empty_rows = {0};
    CarrierVec empty_carriers = {0};
    jazz_protocol_SyncMessage header_msg = wrap(true, shell(&sh, inv_clone, empty_rows, empty_rows, empty_carriers));
    usize header_bytes = sat_add(jazz_db__db__view_update_parts__encoded_len(&header_msg), PART_LENGTH_PREFIX_SLACK);
    jazz_protocol__protocol__SyncMessage__drop(&header_msg);

    PartVec parts = {0};
    PartContents current;
    memset(&current, 0, sizeof current);
    usize capacity = 0;
    bool failed = false;

    if (header_bytes >= limit) {
        err->tag = JAZZ_OVU_HEADER_EXCEEDS_LIMIT; err->bytes = header_bytes; err->bound = limit;
        failed = true;
        goto fail;
    }
    capacity = limit - header_bytes;

    for (u32 i = 0; i < first.len; ++i) {
        usize row_bytes = serialized_size_row(&first.ptr[i]);
        if (row_bytes > capacity) {
            err->tag = JAZZ_OVU_SUPPORTING_ROW_EXCEEDS_LIMIT; err->bytes = row_bytes; err->bound = capacity;
            failed = true;
            goto fail;
        }
        admit(&parts, &current, row_bytes, capacity);
        JZ_VEC_PUSH(current.first, first.ptr[i]);
    }
    for (u32 i = 0; i < second.len; ++i) {
        usize row_bytes = serialized_size_row(&second.ptr[i]);
        if (row_bytes > capacity) {
            err->tag = JAZZ_OVU_SUPPORTING_ROW_EXCEEDS_LIMIT; err->bytes = row_bytes; err->bound = capacity;
            failed = true;
            goto fail;
        }
        admit(&parts, &current, row_bytes, capacity);
        JZ_VEC_PUSH(current.second, second.ptr[i]);
    }
    for (u32 i = 0; i < bundles.len; ++i) {
        /* A singleton carrier is the enum tag plus the bundle. */
        usize bundle_bytes = sat_add(serialized_size_bundle(&bundles.ptr[i]), 1);
        if (bundle_bytes > capacity) {
            err->tag = JAZZ_OVU_VERSION_BUNDLE_EXCEEDS_LIMIT; err->bytes = bundle_bytes; err->bound = capacity;
            /* bundles [i..] are still owned by `bundles` */
            for (u32 j = i; j < bundles.len; ++j) jazz_protocol__protocol__VersionBundle__drop(&bundles.ptr[j]);
            bundles.len = 0;
            failed = true;
            goto fail;
        }
        admit(&parts, &current, bundle_bytes, capacity);
        JZ_VEC_PUSH(current.bundles, bundles.ptr[i]);
    }
    bundles.len = 0;
    JZ_VEC_PUSH(parts, current);
    memset(&current, 0, sizeof current);

    {
        u32 count = parts.len;
        jz_vec_reserve(out, count, sizeof(jazz_protocol_SyncMessage));
        for (u32 index = 0; index < count; ++index) {
            PartContents *part = &parts.ptr[index];
            bool last = index + 1 == count;
            jazz_protocol_PeerPayloadInventory inventory;
            if (last) jazz_protocol__protocol__PeerPayloadInventory__clone(&inventory, &peer_payload_inventory);
            else jazz_protocol__protocol__PeerPayloadInventory__default(&inventory);

            /* A packed run is usually smaller than its singletons, but its per-body
             * overrides can make it slightly larger; singletons always fit. */
            bool have_message = false;
            jazz_protocol_SyncMessage message;
            BundleVec bundles_copy = {0};
            jz_vec_clone(&bundles_copy, &part->bundles, sizeof(jazz_protocol_VersionBundle), jazz_protocol__protocol__VersionBundle__clone);
            CarrierVec carriers = {0};
            jz_err pe = jazz_protocol__protocol__build_version_carriers_from_singletons(bundles_copy.ptr, bundles_copy.len, (void *)&carriers);
            jz_free(bundles_copy.ptr);
            if (!JZ_IS_ERR(pe)) {
                jazz_protocol_PeerPayloadInventory inv2;
                jazz_protocol__protocol__PeerPayloadInventory__clone(&inv2, &inventory);
                jazz_protocol_SyncMessage packed = wrap(last, shell(&sh, inv2, clone_rows(&part->first), clone_rows(&part->second), carriers));
                if (jazz_db__db__view_update_parts__encoded_len(&packed) <= limit) {
                    message = packed;
                    have_message = true;
                } else {
                    jazz_protocol__protocol__SyncMessage__drop(&packed);
                }
            } else {
                jz_err_drop(&pe);
            }
            if (have_message) {
                jazz_protocol__protocol__PeerPayloadInventory__drop(&inventory);
                PartContents__drop(part);
            } else {
                CarrierVec singles = {0};
                jz_vec_reserve(&singles, part->bundles.len, sizeof(jazz_protocol_VersionCarrier));
                for (u32 b = 0; b < part->bundles.len; ++b) {
                    jazz_protocol_VersionCarrier c;
                    jazz_protocol__protocol__VersionCarrier__bundle(&c, part->bundles.ptr[b]);
                    JZ_VEC_PUSH(singles, c);
                }
                jz_free(part->bundles.ptr);
                message = wrap(last, shell(&sh, inventory, part->first, part->second, singles));
            }
            JZ_VEC_PUSH(*out, message);
        }
        jz_free(parts.ptr);
    }
    jz_free(first.ptr);
    jz_free(second.ptr);
    jz_free(bundles.ptr);
    jazz_protocol__protocol__PeerPayloadInventory__drop(&peer_payload_inventory);
    return true;

fail:
    (void)failed;
    /* rows already moved into parts/current are dropped there; the rest still in first/second */
    jz_vec_drop(&parts, sizeof(PartContents), PartContents__drop);
    PartContents__drop(&current);
    jz_free(first.ptr);
    jz_free(second.ptr);
    jz_vec_drop(&bundles, sizeof(jazz_protocol_VersionBundle), jazz_protocol__protocol__VersionBundle__drop);
    jazz_protocol__protocol__PeerPayloadInventory__drop(&peer_payload_inventory);
    return false;
}

static bool identity_eq(const TransitionIdentity *a, const TransitionIdentity *b) {
    return a->kind == b->kind && memcmp(a->predecessor, b->predecessor, 16) == 0 && memcmp(a->revision, b->revision, 16) == 0;
}

static const jz_btree_desc row_ref_set_desc = { sizeof(jazz_protocol_SupportingRow), 0, jazz_protocol__protocol__SupportingRow__cmp, NULL, NULL };

/* Reassemble buffered ViewUpdateParts and their final ViewUpdate. */
jz_err jazz_db__db__view_update_parts__merge_view_update_parts(jazz_db_ViewUpdatePayloadVec parts, jazz_protocol_ViewUpdatePayload last,
                                                               jazz_protocol_ViewUpdatePayload *out) {
    TransitionIdentity identity = jazz_db__db__view_update_parts__transition_identity(&last.supporting_rows);
    jazz_protocol_SupportingRowVec first = {0}, second = {0};
    CarrierVec carriers = {0};
    for (u32 i = 0; i < parts.len; ++i) {
        jazz_protocol_ViewUpdatePayload *part = &parts.ptr[i];
        TransitionIdentity pid = jazz_db__db__view_update_parts__transition_identity(&part->supporting_rows);
        if (!jazz_protocol__protocol__SubscriptionKey__eq(&part->subscription, &last.subscription) ||
            part->settled_through != last.settled_through || !identity_eq(&pid, &identity)) {
            jz_vec_drop(&first, sizeof(jazz_protocol_SupportingRow), NULL);
            jz_vec_drop(&second, sizeof(jazz_protocol_SupportingRow), NULL);
            jz_vec_drop(&carriers, sizeof(jazz_protocol_VersionCarrier), jazz_protocol__protocol__VersionCarrier__drop);
            jz_vec_drop(&parts, sizeof(jazz_protocol_ViewUpdatePayload), jazz_protocol__protocol__ViewUpdatePayload__drop);
            jazz_protocol__protocol__ViewUpdatePayload__drop(&last);
            return jz_err_new(JAZZ_DB_STRING_ERROR, "view-update part does not match its final update");
        }
        jazz_protocol_SupportingRowVec pf, ps;
        jazz_db__db__view_update_parts__take_lists(&part->supporting_rows, &pf, &ps);
        jz_vec_extend(&first, pf.ptr, pf.len, sizeof(jazz_protocol_SupportingRow));
        jz_vec_extend(&second, ps.ptr, ps.len, sizeof(jazz_protocol_SupportingRow));
        jz_free(pf.ptr); jz_free(ps.ptr);
        jz_vec_extend(&carriers, part->version_carriers.ptr, part->version_carriers.len, sizeof(jazz_protocol_VersionCarrier));
        jz_free(part->version_carriers.ptr);
        part->version_carriers.ptr = NULL; part->version_carriers.len = part->version_carriers.cap = 0;
        jazz_protocol__protocol__ViewUpdatePayload__drop(part);
    }
    jz_free(parts.ptr);
    jazz_protocol_SupportingRowsUpdate template_ = last.supporting_rows;
    jazz_protocol_SupportingRowVec lf, ls;
    jazz_db__db__view_update_parts__take_lists(&template_, &lf, &ls);
    jz_vec_extend(&first, lf.ptr, lf.len, sizeof(jazz_protocol_SupportingRow));
    jz_vec_extend(&second, ls.ptr, ls.len, sizeof(jazz_protocol_SupportingRow));
    jz_free(lf.ptr); jz_free(ls.ptr);
    jz_vec_extend(&carriers, last.version_carriers.ptr, last.version_carriers.len, sizeof(jazz_protocol_VersionCarrier));
    jz_free(last.version_carriers.ptr);

    jz_btree identities;
    jz_btree_init(&identities, &row_ref_set_desc);
    bool all_unique = true;
    for (u32 i = 0; i < first.len + second.len && all_unique; ++i) {
        const jazz_protocol_SupportingRow *row = i < first.len ? &first.ptr[i] : &second.ptr[i - first.len];
        if (jz_btree_contains(&identities, row)) all_unique = false;
        else jz_btree_insert(&identities, row, NULL, NULL);
    }
    jz_btree_drop(&identities);
    if (!all_unique) {
        jz_vec_drop(&first, sizeof(jazz_protocol_SupportingRow), NULL);
        jz_vec_drop(&second, sizeof(jazz_protocol_SupportingRow), NULL);
        jz_vec_drop(&carriers, sizeof(jazz_protocol_VersionCarrier), jazz_protocol__protocol__VersionCarrier__drop);
        jazz_protocol__protocol__PeerPayloadInventory__drop(&last.peer_payload_inventory);
        return jz_err_new(JAZZ_DB_STRING_ERROR, "view-update parts repeat a supporting row");
    }
    out->subscription = last.subscription;
    out->settled_through = last.settled_through;
    out->version_carriers.ptr = carriers.ptr; out->version_carriers.len = carriers.len; out->version_carriers.cap = carriers.cap;
    out->peer_payload_inventory = last.peer_payload_inventory;
    out->supporting_rows = jazz_db__db__view_update_parts__with_lists(&template_, first, second);
    return JZ_OK;
}
