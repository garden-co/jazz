/* model/metadata.rs: well-known metadata keys and row provenance.
 * Only SYSTEM_PRINCIPAL_ID and RowProvenance (carried by every Row) ship in the
 * browser build; the MetadataKey/ObjectType/DeleteKind helpers and the
 * row_provenance_* functions are absent from the shipped WASM. */
#include "jazz_model/jazz_model.h"

const char jazz_model__model__metadata__SYSTEM_PRINCIPAL_ID[] = "jazz:system";

void jazz_model__model__metadata__RowProvenance__drop(jazz_model_RowProvenance *p) {
    jz_str_drop(&p->created_by);
    jz_str_drop(&p->updated_by);
}

void jazz_model__model__metadata__RowProvenance__Clone__clone(jazz_model_RowProvenance *dst, const jazz_model_RowProvenance *src) {
    dst->created_by = jz_str_clone(&src->created_by);
    dst->created_at = src->created_at;
    dst->updated_by = jz_str_clone(&src->updated_by);
    dst->updated_at = src->updated_at;
}

bool jazz_model__model__metadata__RowProvenance__PartialEq__eq(const jazz_model_RowProvenance *a, const jazz_model_RowProvenance *b) {
    return jz_str_eq(&a->created_by, &b->created_by) && a->created_at == b->created_at &&
           jz_str_eq(&a->updated_by, &b->updated_by) && a->updated_at == b->updated_at;
}
