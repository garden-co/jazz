/* publication_type.h: crates/jazz/layers/db/src/binding_codec/publication_type.rs */
#ifndef JAZZ_DB_BINDING_CODEC_PUBLICATION_TYPE_H
#define JAZZ_DB_BINDING_CODEC_PUBLICATION_TYPE_H
#include "../_ext_db-b.h"
void jazz_db__binding_codec__publication_type__serialize(const groove_ValueType *value, jz_pc_w *w);
void jazz_db__binding_codec__publication_type__NativeValueType__Serialize__serialize(const groove_ValueType *v, jz_pc_w *w);
void jazz_db__binding_codec__publication_type__NativeDescriptor__Serialize__serialize(const groove_RecordDescriptor *d, jz_pc_w *w);
jz_err jazz_db__binding_codec__publication_type__read_descriptor(jz_pc_r *input, const groove_RecordDescriptor **out);
#endif
