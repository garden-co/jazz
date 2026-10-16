/* publication.rs: publication roles of current-row fields. */
#include "mod.h"

u8 jazz_engine__query_engine__publication__CurrentRowResultVisibility__current_row_metadata(jz_slice name) {
    static const char *const provenance[] = {"$createdBy", "$createdAt", "$updatedBy", "$updatedAt"};
    for (u32 i = 0; i < 4; i++) {
        u32 n = (u32)strlen(provenance[i]);
        if (name.len == n && memcmp(name.ptr, provenance[i], n) == 0) return JAZZ_ENGINE_CRRV_PUBLIC_PROVENANCE;
    }
    return JAZZ_ENGINE_CRRV_HIDDEN_METADATA;
}

const jz_str *jazz_engine__query_engine__publication__CurrentRowPublicationField__public_name(const jazz_engine_CurrentRowPublicationField *f) {
    switch (f->tag) {
    case JAZZ_ENGINE_CRPF_STORED_COLUMN:
    case JAZZ_ENGINE_CRPF_UNRESOLVED_SOURCE_CELL:
        return &f->name;
    case JAZZ_ENGINE_CRPF_RESULT_FIELD:
        switch (f->visibility) {
        case JAZZ_ENGINE_CRRV_APPLICATION_CELL:
        case JAZZ_ENGINE_CRRV_PUBLIC_PROVENANCE:
            return &f->name;
        case JAZZ_ENGINE_CRRV_HIDDEN_METADATA:
            return NULL;
        }
    }
    return NULL;
}

const jz_str *jazz_engine__query_engine__publication__CurrentRowPublicationField__application_name(const jazz_engine_CurrentRowPublicationField *f) {
    switch (f->tag) {
    case JAZZ_ENGINE_CRPF_STORED_COLUMN:
    case JAZZ_ENGINE_CRPF_UNRESOLVED_SOURCE_CELL:
        return &f->name;
    case JAZZ_ENGINE_CRPF_RESULT_FIELD:
        return f->visibility == JAZZ_ENGINE_CRRV_APPLICATION_CELL ? &f->name : NULL;
    }
    return NULL;
}

u8 jazz_engine__query_engine__publication__CurrentRowPublicationField__role(const jazz_engine_CurrentRowPublicationField *f) {
    switch (f->tag) {
    case JAZZ_ENGINE_CRPF_STORED_COLUMN:
    case JAZZ_ENGINE_CRPF_UNRESOLVED_SOURCE_CELL:
        return JAZZ_ENGINE_CRBR_PHYSICAL_COLUMN;
    default:
        return JAZZ_ENGINE_CRBR_LOGICAL_FIELD;
    }
}

void jazz_engine__query_engine__publication__CurrentRowPublicationField__Clone__clone(jazz_engine_CurrentRowPublicationField *dst, const jazz_engine_CurrentRowPublicationField *src) {
    *dst = *src;
    dst->name = jz_str_clone(&src->name);
}

void jazz_engine__query_engine__publication__CurrentRowPublicationField__drop(jazz_engine_CurrentRowPublicationField *f) { jz_str_drop(&f->name); }

bool jazz_engine__query_engine__publication__CurrentRowPublicationField__PartialEq__eq(const jazz_engine_CurrentRowPublicationField *a, const jazz_engine_CurrentRowPublicationField *b) {
    if (a->tag != b->tag || !jz_str_eq(&a->name, &b->name)) return false;
    switch (a->tag) {
    case JAZZ_ENGINE_CRPF_STORED_COLUMN: return jz_uuid_eq(&a->id.u, &b->id.u);
    case JAZZ_ENGINE_CRPF_RESULT_FIELD: return a->visibility == b->visibility;
    default: return true;
    }
}

static const char *const VISIBILITY_NAMES[] = {"ApplicationCell", "PublicProvenance", "HiddenMetadata"};

void jazz_engine__query_engine__publication__CurrentRowPublicationField__Debug__fmt(jz_str *out, const jazz_engine_CurrentRowPublicationField *f) {
    switch (f->tag) {
    case JAZZ_ENGINE_CRPF_STORED_COLUMN:
        jazz_engine__fmt__struct_begin(out, "StoredColumn");
        jazz_engine__fmt__field(out, "id", true);
        jazz_types__ids__uuid_newtype__Debug__fmt(out, "PhysicalColumnId", &f->id.u);
        jazz_engine__fmt__field(out, "output_name", false);
        jazz_engine__fmt__str(out, &f->name);
        break;
    case JAZZ_ENGINE_CRPF_RESULT_FIELD:
        jazz_engine__fmt__struct_begin(out, "ResultField");
        jazz_engine__fmt__field(out, "name", true);
        jazz_engine__fmt__str(out, &f->name);
        jazz_engine__fmt__field(out, "visibility", false);
        jz_str_push_cstr(out, VISIBILITY_NAMES[f->visibility]);
        break;
    default:
        jazz_engine__fmt__struct_begin(out, "UnresolvedSourceCell");
        jazz_engine__fmt__field(out, "output_name", true);
        jazz_engine__fmt__str(out, &f->name);
        break;
    }
    jazz_engine__fmt__struct_end(out);
}

void jazz_engine__crpf_drop_v(void *f) { jazz_engine__query_engine__publication__CurrentRowPublicationField__drop((jazz_engine_CurrentRowPublicationField *)f); }
void jazz_engine__crpf_clone_v(void *dst, const void *src) { jazz_engine__query_engine__publication__CurrentRowPublicationField__Clone__clone((jazz_engine_CurrentRowPublicationField *)dst, (const jazz_engine_CurrentRowPublicationField *)src); }
