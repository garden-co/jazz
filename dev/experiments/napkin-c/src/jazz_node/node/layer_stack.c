/* Port of crates/jazz/layers/node/src/node/layer_stack.rs */
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

typedef enum { JZN_REF_LOCAL, JZN_REF_REMOTE_ORIGIN } jazz_node_Ref;
const jazz_node_Ref *jazz_node__node__refs__Ref__stack(jazz_model_DurabilityTier tier, u32 *n);

typedef enum { JZN_MASK_OWN_ROWS, JZN_MASK_PRESENCE } jazz_node_MaskTag;
typedef struct jazz_node_Mask { jazz_node_MaskTag tag; groove_GraphBuilder *presence; } jazz_node_Mask;
typedef struct jazz_node_Layer { groove_GraphBuilder *rows; jazz_node_Mask mask; } jazz_node_Layer;

static const char *const ROW_UUID[1] = { "row_uuid" };

jazz_node_Layer jazz_node__node__layer_stack__Layer__new(groove_GraphBuilder *rows) {
    jazz_node_Layer l = { rows, { JZN_MASK_OWN_ROWS, NULL } };
    return l;
}

jazz_node_Layer jazz_node__node__layer_stack__Layer__masked_by(groove_GraphBuilder *rows, jazz_node_Mask mask) {
    jazz_node_Layer l = { rows, mask };
    return l;
}

void jazz_node__node__layer_stack__Layer__drop(void *p) {
    jazz_node_Layer *l = p;
    groove__ivm__graph__GraphBuilder__drop(l->rows);
    if (l->mask.tag == JZN_MASK_PRESENCE) groove__ivm__graph__GraphBuilder__drop(l->mask.presence);
}

static groove_GraphBuilder *jazz_node__node__layer_stack__Layer__hidden_below(const jazz_node_Layer *l) {
    groove_GraphBuilder *g;
    switch (l->mask.tag) {
    case JZN_MASK_OWN_ROWS: g = groove__ivm__graph__GraphBuilder__clone(l->rows); break;
    case JZN_MASK_PRESENCE:
    default: g = groove__ivm__graph__GraphBuilder__clone(l->mask.presence); break;
    }
    return groove__ivm__graph__GraphBuilder__project(g, ROW_UUID, 1);
}

/* compose(layers): consumes the layers */
groove_GraphBuilder *jazz_node__node__layer_stack__compose(jazz_node_Layer *layers, u32 n) {
    if (n == 0) jz_panic("a layer stack has at least one layer");
    jazz_node_Layer bottom_layer = layers[n - 1];
    groove_GraphBuilder *below = bottom_layer.rows;
    if (bottom_layer.mask.tag == JZN_MASK_PRESENCE) groove__ivm__graph__GraphBuilder__drop(bottom_layer.mask.presence);
    for (u32 i = n - 1; i-- > 0;) {
        jazz_node_Layer *layer = &layers[i];
        groove_GraphBuilder *hidden = jazz_node__node__layer_stack__Layer__hidden_below(layer);
        groove_GraphBuilder *inputs[2] = {
            layer->rows,
            groove__ivm__graph__GraphBuilder__anti_join(below, hidden, ROW_UUID, 1, ROW_UUID, 1),
        };
        below = groove__ivm__graph__GraphBuilder__union(inputs, 2);
        if (layer->mask.tag == JZN_MASK_PRESENCE) groove__ivm__graph__GraphBuilder__drop(layer->mask.presence);
    }
    return below;
}

groove_GraphBuilder *jazz_node__node__layer_stack__exposed(groove_GraphBuilder *rows, const jazz_node_Layer *above, u32 n) {
    for (u32 i = 0; i < n; i++)
        rows = groove__ivm__graph__GraphBuilder__anti_join(rows, jazz_node__node__layer_stack__Layer__hidden_below(&above[i]), ROW_UUID, 1, ROW_UUID, 1);
    return rows;
}

typedef jazz_node_Layer (*jzn_layer_fn)(void *ctx, jazz_node_Ref r);
typedef jz_err (*jzn_try_layer_fn)(void *ctx, jazz_node_Ref r, jazz_node_Layer *out);

void jazz_node__node__layer_stack__ref_layers(jazz_model_DurabilityTier tier, jzn_layer_fn layer, void *ctx, JZ_VEC(jazz_node_Layer) *out) {
    u32 n; const jazz_node_Ref *stack = jazz_node__node__refs__Ref__stack(tier, &n);
    for (u32 i = 0; i < n; i++) { jazz_node_Layer l = layer(ctx, stack[i]); JZ_VEC_PUSH(*out, l); }
}

jz_err jazz_node__node__layer_stack__try_ref_layers(jazz_model_DurabilityTier tier, jzn_try_layer_fn layer, void *ctx, JZ_VEC(jazz_node_Layer) *out) {
    u32 n; const jazz_node_Ref *stack = jazz_node__node__refs__Ref__stack(tier, &n);
    for (u32 i = 0; i < n; i++) {
        jazz_node_Layer l;
        jz_err e = layer(ctx, stack[i], &l);
        if (JZ_IS_ERR(e)) { jz_vec_drop(out, sizeof(jazz_node_Layer), jazz_node__node__layer_stack__Layer__drop); return e; }
        JZ_VEC_PUSH(*out, l);
    }
    return JZ_OK;
}

const void *jazz_node__node__layer_stack__top_wins(const jz_btree *const *layers, u32 n, const void *key) {
    for (u32 i = 0; i < n; i++) { const void *v = jz_btree_get(layers[i], key); if (v) return v; }
    return NULL;
}

jz_err jazz_node__node__layer_stack__branch_view_stack(const jazz_model_JazzSchema *schema, const jazz_model_TableSchema *table,
                                                       const jazz_model_BranchSelector *head_sel, const jazz_protocol_BranchViewBase *base,
                                                       jazz_model_BranchKey **head_out, bool *has_base, jazz_engine_BranchViewSourceBase *base_out,
                                                       jz_str *err_msg) {
    jazz_model_BranchKey *head;
    JZ_TRY(jazz_model__schema__RuntimeSchema__project_branch_view_selector(schema, table, head_sel, &head, err_msg));
    *has_base = false;
    if (base) {
        jazz_model_BranchKey *key;
        jazz_engine_BranchViewSourceBase b = {0};
        switch (base->tag) {
        case 0: { /* Current(selector) */
            jz_err e = jazz_model__schema__RuntimeSchema__project_branch_view_selector(schema, table, base->branch, &key, err_msg);
            if (JZ_IS_ERR(e)) { jazz_model__model__branch__BranchKey__drop(head); return e; }
            b.tag = 0; b.key = key;
            break;
        }
        default: { /* Snapshot { branch, snapshot } */
            jz_err e = jazz_model__schema__RuntimeSchema__project_branch_view_selector(schema, table, base->branch, &key, err_msg);
            if (JZ_IS_ERR(e)) { jazz_model__model__branch__BranchKey__drop(head); return e; }
            b.tag = 1; b.key = key; b.snapshot = jazz_model__tx__SnapshotRef__clone(base->snapshot);
            break;
        }
        }
        if (!jazz_model__model__branch__BranchKey__eq(b.key, head)) { *has_base = true; *base_out = b; }
        else jazz_model__model__branch__BranchKey__drop(b.key);
    }
    *head_out = head;
    return JZ_OK;
}
